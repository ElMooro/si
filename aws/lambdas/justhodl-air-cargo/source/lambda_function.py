"""justhodl-air-cargo v2.0 — HKIA high-value air-freight canary, Lambda-native.

Khalid's value-vs-volume insight: high-value goods FLY. HKIA is the world's #1
cargo airport, so its monthly tonnage is the complement to sea-freight volume
(portwatch) and inland freight (freight-pulse).

Source: HK Civil Aviation Department monthly workbook ("Stat Webpage.xlsx").

Why the edge: CAD tarpits AWS-Lambda IPs — proven across ops 3669-3672 with
three separate invokes hanging at INIT_START while a GitHub runner fetched
the same 2.1MB file instantly. v2.0 routes the fetch through the hostname-
locked Cloudflare /gov worker (ops 3697), so this engine finally runs on its
own 10:40 schedule instead of depending on runner-side ops.

Workbook facts, x-rayed in ops 3674 and proven in 3676 (HKIA May-2026 =
433.0k tonnes, +3.1% YoY):
  * sheet1, header row 8; columns L/M/N/O = Unloaded / Loaded / Freight
    Total / YoY%
  * ~820k empty styled cells, so cells are anchored on '><v>' only
  * the YEAR is printed only on January rows -> carry it forward
  * layout is latest-month-per-year, giving a same-month series where the
    workbook's own YoY column is directly valid

stdlib only; never fabricates — a failed fetch or parse reports null with the
error, and the page shows "building" rather than a made-up number.
"""
import io
import json
import re
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3

VERSION = "2.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/air-cargo.json"
LEVELS_KEY = "air/hkia-cargo-levels.json"
XLSX_URL = "https://www.cad.gov.hk/english/./pdf/Stat Webpage.xlsx"
GOV_EDGE = "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
UA = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 Chrome/126.0 Safari/537.36")}
MONTHS = ("january february march april may june july august september "
          "october november december").split()
S3 = boto3.client("s3", region_name="us-east-1")


def _edge_get(url, timeout=60, cap=8_000_000):
    """Edge first (CAD blocks Lambda IPs), direct as fallback."""
    err = None
    # the CAD path contains a literal space ("Stat Webpage.xlsx"), which
    # urllib rejects as a control character on a DIRECT fetch — percent-encode
    # the path for the fallback (the edge form is fully quoted already).
    direct = urllib.parse.quote(url, safe=":/?&=%")
    for attempt in (GOV_EDGE + urllib.parse.quote(url, safe=""), direct):
        try:
            r = urllib.request.urlopen(
                urllib.request.Request(attempt, headers=UA), timeout=timeout)
            b = r.read(cap)
            if b:
                return b, ("edge" if attempt.startswith(GOV_EDGE)
                           else "direct"), None
        except Exception as e:
            err = str(e)[:120]
    return b"", None, err


def _next_col(c):
    l = list(c)
    i = len(l) - 1
    while i >= 0:
        if l[i] != "Z":
            l[i] = chr(ord(l[i]) + 1)
            break
        l[i] = "A"
        i -= 1
    else:
        l = ["A"] + l
    return "".join(l)


def _parse_workbook(rb, out):
    """Return sorted [(year, month, tonnes, yoy_or_None)]."""
    zf = zipfile.ZipFile(io.BytesIO(rb))
    sh = zf.read("xl/sharedStrings.xml").decode("utf-8", "replace")
    strings = ["".join(re.findall(r"<t[^>]*>([^<]*)</t>", si))
               for si in re.split(r"<si>", sh)[1:]]
    xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8", "replace")

    rows = {}
    for rowm in re.finditer(r'<row r="(\d+)"[^>]*>(.*?)</row>', xml, re.S):
        rno = int(rowm.group(1))
        cells = {}
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
    out["rows_parsed"] = len(rows)

    unloaded_col = header_row = None
    for rno, cs in sorted(rows.items()):
        for col, v in cs.items():
            if v[0] == "s" and v[1].strip().lower() == "unloaded":
                unloaded_col, header_row = col, rno
                break
        if unloaded_col:
            break
    if not unloaded_col:
        out["xlsx_probe"] = ("no 'Unloaded' header; strings: "
                             + " | ".join(strings[:20])[:320])
        return []

    total_col = _next_col(_next_col(unloaded_col))
    yoy_col = _next_col(total_col)
    out["cols"] = {"unloaded": unloaded_col, "total": total_col,
                   "yoy": yoy_col, "header_row": header_row}

    series = []
    carried_year = None
    for rno, cs in sorted(rows.items()):
        if rno <= header_row:
            continue
        year = month = None
        for col, v in sorted(cs.items()):
            if v[0] == "n" and 1990 <= v[1] <= 2035 and year is None:
                year = int(v[1])
            if v[0] == "s" and v[1].strip().lower() in MONTHS \
                    and month is None:
                month = MONTHS.index(v[1].strip().lower()) + 1
        if year is None and month is not None:
            year = carried_year
        if year is not None:
            carried_year = year
        tv = cs.get(total_col)
        yv = cs.get(yoy_col)
        if year and month and tv and tv[0] == "n" \
                and 50_000 <= tv[1] <= 900_000:
            series.append((year, month, tv[1],
                           (round(yv[1], 1) if yv and yv[0] == "n"
                            and -80 <= yv[1] <= 200 else None)))
    series.sort()
    return series


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "airport": "HKIA (Hong Kong Intl) — world #1 cargo airport",
           "errors": [],
           "attribution": ("HK Civil Aviation Department monthly statistics "
                           "(Stat Webpage.xlsx, free public data)")}

    rb, via, err = _edge_get(XLSX_URL)
    out["fetch_via"] = via
    out["xlsx_bytes"] = len(rb)
    if not rb:
        out["errors"].append("fetch: " + str(err))
    else:
        try:
            series = _parse_workbook(rb, out)
            if series:
                year, month, tonnes, yoy = series[-1]
                out["tonnes"] = tonnes
                out["tonnes_k"] = round(tonnes / 1000, 1)
                out["month"] = f"{year}-{month:02d}"
                out["via"] = f"cad_xlsx({via})"
                out["xlsx_n"] = len(series)
                out["xlsx_tail"] = [[f"{y}-{m:02d}", round(t / 1000, 1)]
                                    for y, m, t, _ in series[-13:]]
                if yoy is not None:
                    out["yoy_pct"] = yoy
                else:
                    prior = [t for y, m, t, _ in series
                             if y == year - 1 and m == month]
                    if prior:
                        out["yoy_pct"] = round(
                            100 * (tonnes / prior[0] - 1), 1)
                y = out.get("yoy_pct") or 0
                out["read"] = ("HIGH-VALUE FLOW "
                               + ("ACCELERATING" if y >= 5 else
                                  "CONTRACTING" if y <= -5 else "STEADY"))
                out["ok"] = True
                try:
                    levels = {f"{y2}-{m2:02d}": round(t2 / 1000, 1)
                              for y2, m2, t2, _ in series[-26:]}
                    S3.put_object(Bucket=BUCKET, Key=LEVELS_KEY,
                                  Body=json.dumps({"levels": levels}).encode(),
                                  ContentType="application/json")
                    out["levels_cached"] = len(levels)
                except Exception as e:
                    out["errors"].append("levels: " + str(e)[:80])
        except Exception as e:
            out["errors"].append("parse: " + str(e)[:140])

    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[air] ok={out['ok']} via={out.get('fetch_via')} "
          f"tonnes_k={out.get('tonnes_k')} month={out.get('month')} "
          f"yoy={out.get('yoy_pct')} n={out.get('xlsx_n')} "
          f"errs={out['errors']}")
    return {"ok": out["ok"], "tonnes_k": out.get("tonnes_k"),
            "month": out.get("month"), "yoy_pct": out.get("yoy_pct"),
            "via": out.get("fetch_via")}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
