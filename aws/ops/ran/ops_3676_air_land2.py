"""ops 3676 — air LANDING v1.6: x-ray-informed parser. Only value-bearing
cells matched (anchor '><v>'), attrs parsed separately (order-agnostic),
header row via 'Unloaded' cell -> Total/YoY cols, Year|Month data rows,
full monthly series. Writes data/air-cargo.json + levels. VALUE-required."""
import io, json, re, sys, urllib.parse, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda  # noqa
from ops_report import report

S3C = boto3.client("s3", "us-east-1", config=Config(retries={"max_attempts": 2}))
B = "justhodl-dashboard-live"
XLSX = "https://www.cad.gov.hk/english/./pdf/Stat Webpage.xlsx"
UA = {"User-Agent": "Mozilla/5.0 Chrome/126.0"}
MONTHS = ("january february march april may june july august september "
          "october november december").split()

def nxt(c2):
    l2 = list(c2); i2 = len(l2) - 1
    while i2 >= 0:
        if l2[i2] != "Z":
            l2[i2] = chr(ord(l2[i2]) + 1); break
        l2[i2] = "A"; i2 -= 1
    else:
        l2 = ["A"] + l2
    return "".join(l2)

with report("3676_air_land2") as rep:
    rep.heading("ops 3676 — air landing v1.6")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3676.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        aj = {"ok": False, "version": "1.6.0-runner",
              "generated_at": datetime.now(timezone.utc).isoformat(),
              "airport": "HKIA (Hong Kong Intl) — world #1 cargo airport",
              "errors": [],
              "attribution": "HK Civil Aviation Department monthly statistics "
                             "(Stat Webpage.xlsx, free public)"}
        rb = urllib.request.urlopen(urllib.request.Request(
            urllib.parse.quote(XLSX, safe=":/"), headers=UA), timeout=40).read()
        zf = zipfile.ZipFile(io.BytesIO(rb))
        sh = zf.read("xl/sharedStrings.xml").decode("utf-8", "replace")
        strings = ["".join(re.findall(r"<t[^>]*>([^<]*)</t>", si))
                   for si in re.split(r"<si>", sh)[1:]]
        xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8", "replace")
        rows = {}
        for rowm in re.finditer(r'<row r="(\d+)"[^>]*>(.*?)</row>', xml, re.S):
            rno = int(rowm.group(1)); cells = {}
            for cm in re.finditer(r"<c ([^>]*)><v>([^<]*)</v>", rowm.group(2)):
                attrs, val = cm.group(1), cm.group(2)
                rm = re.search(r'r="([A-Z]+)\d+"', attrs)
                if not rm:
                    continue
                col = rm.group(1)
                if 't="s"' in attrs:
                    try:
                        cells[col] = ("s", strings[int(val)])
                    except Exception:
                        pass
                else:
                    try:
                        cells[col] = ("n", float(val))
                    except Exception:
                        pass
            if cells:
                rows[rno] = cells
        aj["rows_parsed"] = len(rows)
        unc = hdr = None
        for rno, cs in sorted(rows.items()):
            for col, v in cs.items():
                if v[0] == "s" and v[1].strip().lower() == "unloaded":
                    unc, hdr = col, rno
                    break
            if unc:
                break
        series = []
        if unc:
            totc = nxt(nxt(unc)); yoyc = nxt(totc)
            aj["cols"] = {"unloaded": unc, "total": totc, "yoy": yoyc,
                          "hdr_row": hdr}
            _last_yr=[None]
            for rno, cs in sorted(rows.items()):
                if rno <= hdr:
                    continue
                yr = mo = None
                for col, v in sorted(cs.items()):
                    if v[0] == "n" and 1990 <= v[1] <= 2035 and yr is None:
                        yr = int(v[1])
                    if v[0] == "s" and v[1].strip().lower() in MONTHS \
                            and mo is None:
                        mo = MONTHS.index(v[1].strip().lower()) + 1
                if yr is None and mo is not None:
                    yr = _last_yr[0]
                if yr is not None:
                    _last_yr[0] = yr
                tv = cs.get(totc); yv = cs.get(yoyc)
                if yr and mo and tv and tv[0] == "n" \
                        and 50_000 <= tv[1] <= 900_000:
                    series.append((yr, mo, tv[1],
                                   (round(yv[1], 1) if yv and yv[0] == "n"
                                    and -80 <= yv[1] <= 200 else None)))
            series.sort()
        if series:
            yr, mo, tv, yv = series[-1]
            aj.update({"ok": True, "tonnes": tv,
                       "tonnes_k": round(tv / 1000, 1),
                       "month": f"{yr}-{mo:02d}",
                       "via": "cad_xlsx(runner-v1.6)",
                       "xlsx_n": len(series),
                       "xlsx_tail": [[f"{y2}-{m2:02d}", round(t2 / 1000, 1)]
                                      for y2, m2, t2, _ in series[-13:]]})
            if yv is not None:
                aj["yoy_pct"] = yv
            else:
                pri = [t2 for y2, m2, t2, _ in series
                       if y2 == yr - 1 and m2 == mo]
                if pri:
                    aj["yoy_pct"] = round(100 * (tv / pri[0] - 1), 1)
            y3 = aj.get("yoy_pct") or 0
            aj["read"] = ("HIGH-VALUE FLOW "
                          + ("ACCELERATING" if y3 >= 5 else
                             "CONTRACTING" if y3 <= -5 else "STEADY"))
            lv = {"levels": {f"{y2}-{m2:02d}": round(t2 / 1000, 1)
                             for y2, m2, t2, _ in series[-26:]}}
            S3C.put_object(Bucket=B, Key="air/hkia-cargo-levels.json",
                           Body=json.dumps(lv).encode(),
                           ContentType="application/json")
        else:
            aj["parse_probe"] = {"unc": unc, "hdr": hdr,
                                  "row9": {k: v for k, v in
                                           list(rows.get((hdr or 8) + 1,
                                                          {}).items())[:10]}}
        S3C.put_object(Bucket=B, Key="data/air-cargo.json",
                       Body=json.dumps(aj, default=str).encode(),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        gate("G1_value", aj.get("ok"),
             f"rows={aj.get('rows_parsed')} cols={aj.get('cols')} "
             f"n={aj.get('xlsx_n')} tonnes_k={aj.get('tonnes_k')} "
             f"month={aj.get('month')} yoy={aj.get('yoy_pct')} "
             f"read={aj.get('read')} tail={aj.get('xlsx_tail')} "
             f"probe={str(aj.get('parse_probe'))[:260]}")
        out["air"] = {k: aj.get(k) for k in
                       ("tonnes_k", "month", "yoy_pct", "read", "xlsx_n")}
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3676.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
