"""ops 3673 — DECISIVE: parse CAD 'Stat Webpage.xlsx' IN THE OPS RUNNER
(invoke path to justhodl-air-cargo has failed 3x: ConnectionClosed,
ReadTimeout x2 — even Event 202). Runner fetches+parses the xlsx with the
exact v1.4 logic, writes data/air-cargo.json + levels ledger directly.
Engine+Scheduler remain for daily refresh once invoke path heals. Also
records fn State/LastUpdateStatus + last CW log tail (no invoke)."""
import io, json, re, sys, time, urllib.parse, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda  # noqa: F401
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=30, retries={"max_attempts": 1}))
LOGS = boto3.client("logs", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
XLSX = "https://www.cad.gov.hk/english/./pdf/Stat Webpage.xlsx"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}
MONTHS = ("january february march april may june july august september "
          "october november december").split()

with report("3673_air_cols") as rep:
    rep.heading("ops 3673 — runner-side CAD xlsx landing + fn diag")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3673.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:860]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:820]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # [A] fetch + parse in-runner
        aj = {"ok": False, "version": "1.4.0-runner",
              "generated_at": datetime.now(timezone.utc).isoformat(),
              "airport": "HKIA (Hong Kong Intl) — world #1 cargo airport",
              "errors": [], "via": None,
              "attribution": "HK Civil Aviation Department monthly statistics "
                             "(Stat Webpage.xlsx, free public)"}
        try:
            uq = urllib.parse.quote(XLSX, safe=":/")
            rb = urllib.request.urlopen(
                urllib.request.Request(uq, headers=UA), timeout=40).read()
            aj["xlsx_bytes"] = len(rb)
            zf = zipfile.ZipFile(io.BytesIO(rb))
            sh = zf.read("xl/sharedStrings.xml").decode("utf-8", "replace")
            strings = ["".join(re.findall(r"<t[^>]*>([^<]*)</t>", si))
                       for si in re.split(r"<si>", sh)[1:]]
            aj["strings_n"] = len(strings)
            # v1.5: column-aware — map header cols via r="A5" refs
            def parse_sheet(xml):
                rows = []
                for rowm in re.finditer(r'<row[^>]*r="(\d+)"[^>]*>(.*?)</row>', xml, re.S):
                    rno = int(rowm.group(1)); cells = {}
                    for cm in re.finditer(r'<c[^>]*r="([A-Z]+)\d+"([^>]*)>(?:.*?<v>([^<]*)</v>)?.*?(?:</c>|/>)', rowm.group(2), re.S):
                        col, attrs, val = cm.group(1), cm.group(2), cm.group(3)
                        if val is None: continue
                        if 't="s"' in attrs:
                            try: cells[col] = ("s", strings[int(val)])
                            except Exception: pass
                        else:
                            try: cells[col] = ("n", float(val))
                            except Exception: pass
                    rows.append((rno, cells))
                return rows
            best = None
            for nm2 in sorted(zf.namelist()):
                if not nm2.startswith("xl/worksheets/sheet"): continue
                rows = parse_sheet(zf.read(nm2).decode("utf-8", "replace"))
                allstr = " ".join(v[1].lower() for _, cs in rows for v in cs.values() if v[0] == "s")
                if "unloaded" not in allstr: continue
                # header cols: find 'Unloaded' and 'Loaded' cells (freight block)
                unc = ldc = None; hdr_rno = None
                for rno, cs in rows:
                    for col, v in cs.items():
                        if v[0] == "s":
                            t2 = v[1].strip().lower()
                            if t2 == "unloaded": unc, hdr_rno = col, rno
                            elif t2 == "loaded": ldc = col
                    if unc and ldc: break
                if not (unc and ldc): continue
                def nxt(c2):
                    # next column letter (A..Z, AA..): simple increment
                    l2 = list(c2); i2 = len(l2) - 1
                    while i2 >= 0:
                        if l2[i2] != "Z": l2[i2] = chr(ord(l2[i2]) + 1); break
                        l2[i2] = "A"; i2 -= 1
                    else: l2 = ["A"] + l2
                    return "".join(l2)
                totc = nxt(ldc); yoyc = nxt(totc)
                series = []  # (year, monthIdx, tonnes, yoy)
                for rno, cs in rows:
                    if rno <= (hdr_rno or 0): continue
                    yr = mo = None
                    for col, v in sorted(cs.items()):
                        if v[0] == "n" and 2015 <= v[1] <= 2035 and yr is None: yr = int(v[1])
                        if v[0] == "s" and v[1].strip().lower() in MONTHS and mo is None: mo = MONTHS.index(v[1].strip().lower()) + 1
                        if v[0] == "n" and mo is None and yr is not None and 1 <= v[1] <= 12 and col != sorted(cs)[0]: pass
                    tv = cs.get(totc); yv = cs.get(yoyc)
                    if yr and mo and tv and tv[0] == "n" and 50_000 <= tv[1] <= 900_000:
                        series.append((yr, mo, tv[1], (yv[1] if yv and yv[0] == "n" and -80 <= yv[1] <= 200 else None)))
                if len(series) >= 3:
                    series.sort()
                    best = (f"cols U={unc} L={ldc} T={totc} sheet={nm2}", series)
                    break
            if best:
                series = best[1]
                yr, mo, tv, yv = series[-1]
                aj["tonnes"] = tv
                aj["tonnes_k"] = round(tv / 1000, 1)
                aj["month"] = f"{yr}-{mo:02d}"
                aj["via"] = "cad_xlsx(runner-cols)"
                aj["xlsx_row"] = best[0]
                aj["xlsx_tail"] = [[f"{y2}-{m2:02d}", round(t2 / 1000, 1)] for y2, m2, t2, _ in series[-8:]]
                aj["xlsx_n"] = len(series)
                if yv is not None:
                    aj["yoy_pct"] = round(yv, 1)
                else:
                    pri = [t2 for y2, m2, t2, _ in series if y2 == yr - 1 and m2 == mo]
                    if pri: aj["yoy_pct"] = round(100 * (tv / pri[0] - 1), 1)
                y3 = (aj.get("yoy_pct") or 0)
                aj["read"] = ("HIGH-VALUE FLOW " + ("ACCELERATING" if y3 >= 5 else "CONTRACTING" if y3 <= -5 else "STEADY"))
                aj["ok"] = True
            else:
                aj["xlsx_probe"] = ("no unloaded/loaded sheet; strings: " + " | ".join(strings[:24])[:420])
        except Exception as e:
            aj["errors"].append("runner: " + str(e)[:140])
        S3C.put_object(Bucket=B, Key="data/air-cargo.json",
                       Body=json.dumps(aj, default=str).encode(),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        gate("G1_value", aj.get("ok") and isinstance(aj.get("tonnes_k"), (int, float)),
             f"bytes={aj.get('xlsx_bytes')} strings={aj.get('strings_n')} "
             f"tonnes_k={aj.get('tonnes_k')} yoy={aj.get('yoy_pct')} "
             f"n={aj.get('xlsx_n')} tail={aj.get('xlsx_tail')} "
             f"row={str(aj.get('xlsx_row'))[:90]} "
             f"xp={str(aj.get('xlsx_probe'))[:260]} errs={aj.get('errors')}")
        out["air"] = {k: aj.get(k) for k in ("tonnes_k", "yoy_pct", "read",
                                              "xlsx_n", "xlsx_row", "via")}

        # [B] fn diag (no invoke)
        try:
            c = LAM.get_function_configuration(FunctionName="justhodl-air-cargo")
            diag = {k: c.get(k) for k in ("State", "LastUpdateStatus",
                                           "Timeout", "MemorySize",
                                           "CodeSize", "LastModified")}
            try:
                st = LOGS.describe_log_streams(
                    logGroupName="/aws/lambda/justhodl-air-cargo",
                    orderBy="LastEventTime", descending=True, limit=1)
                s0 = (st.get("logStreams") or [{}])[0]
                diag["last_log_event"] = s0.get("lastEventTimestamp")
                if s0.get("logStreamName"):
                    ev = LOGS.get_log_events(
                        logGroupName="/aws/lambda/justhodl-air-cargo",
                        logStreamName=s0["logStreamName"],
                        limit=6, startFromHead=False)
                    diag["log_tail"] = " | ".join(
                        e.get("message", "")[:80].strip()
                        for e in ev.get("events", []))[:400]
            except Exception as le:
                diag["logs_err"] = str(le)[:100]
            gate("G2_diag", True, str(diag))
            out["fn_diag"] = diag
        except Exception as e:
            gate("G2_diag", True, "cfg err: " + str(e)[:200])
    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3673.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
