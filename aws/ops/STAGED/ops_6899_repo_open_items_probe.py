"""ops 6899 - READ-ONLY probe of the four open items on the Repo desk.

Khalid 2026-11-21 22:44 ET: "do all these one by one" - a daily repo rate before August 2014, the tri-party haircuts
that end 2026-07-09, the DTCC daily fails that end 2026-09-08, dealer repo financing dated 2026-10-28.

Nothing is written.  Measured here, per item:
  1. Before August 2014: the NY Fed's primary-dealer survey rate workbook (overnight Treasury GC repo, published
     2018-03-09) - its sheets, columns, first and last rows; the NY Fed operations API for an old month; where the
     board's own rate series begin.
  2. Haircuts: the banked index and series (shape, newest dates) against the NY Fed's tri-party data files.
  3. DTCC daily fails: the old file, and - in a real browser - which requests DTCC's new chart pages make for their
     data (fails and the GCF Repo Index).
  4. Dealer financing: the published document, the engine's log lines, and every acceptance check of the engine run
     here against the sources as they answer today, so the one that refuses is named.
"""
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import gzip
import hashlib
import importlib.util
import io
import json
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) JustHodl research (ops probe)"}
M = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
SURVEY = "https://www.newyorkfed.org/medialibrary/media/markets/HistoricalOvernightTreasGCRepoPriDealerSurvRate.xlsx"
TPR = ("https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/tri-party-repo/tri-party-repo_data",
       "https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/tri-party-repo/tri-party-repo-preNov25_data",
       "https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/tri-party-repo/tri-party-repo_data.xlsx",
       "https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/tri-party-repo/tri-party-repo_data.csv",
       "https://www.newyorkfed.org/medialibrary/media/banking/pdf/tpr_stats_historical.xlsx")
DTCC_DIRECT = ("https://www.dtcc.com/data/failsdata.csv", "https://www.dtcc.com/data/gcfindex.csv", "https://www.dtcc.com/data/gcfdata.csv",
               "https://www.dtcc.com/-/media/Files/Downloads/Charts/failsdata.csv", "https://www.dtcc.com/market-index-data/data/failsdata.csv")
DTCC_PAGES = ("https://www.dtcc.com/market-index-data/charts/daily-total-us-treasury-trade-fails",
              "https://www.dtcc.com/market-index-data/charts/dtcc-gcf-repo-index",
              "https://www.dtcc.com/charts/daily-total-us-treasury-trade-fails")


def column(ref):
    n = 0
    for ch in ref:
        if ch.isalpha():
            n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def xlsx(raw):
    """{sheet name: rows} of a workbook, standard library only; a row is a list of str | float | None."""
    z = zipfile.ZipFile(io.BytesIO(raw))
    names = set(z.namelist())
    shared = []
    if "xl/sharedStrings.xml" in names:
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall(M + "si"):
            shared.append("".join(t.text or "" for t in si.iter(M + "t")))
    target = {rel.get("Id"): rel.get("Target") for rel in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
    out = {}
    for sh in ET.fromstring(z.read("xl/workbook.xml")).find(M + "sheets"):
        path = target[sh.get(R + "id")]
        path = path.lstrip("/") if path.startswith("/") else "xl/" + path
        rows = []
        for row in ET.fromstring(z.read(path)).iter(M + "row"):
            cells = {}
            for c in row.findall(M + "c"):
                v, kind = c.find(M + "v"), c.get("t")
                if kind == "s":
                    val = shared[int(v.text)] if v is not None else None
                elif kind == "inlineStr":
                    val = "".join(t.text or "" for t in c.iter(M + "t"))
                elif kind in ("str", "e", "b"):
                    val = v.text if v is not None else None
                else:
                    try:
                        val = float(v.text) if v is not None and v.text not in (None, "") else None
                    except ValueError:
                        val = v.text
                cells[column(c.get("r") or "A")] = val
            width = max(cells) + 1 if cells else 0
            rows.append([cells.get(i) for i in range(width)])
        out[sh.get("name")] = rows
    return out


def serial_day(n):
    """An Excel day number as an ISO date (1900 date system)."""
    return (date(1899, 12, 30) + timedelta(days=int(n))).isoformat()


def selftest():
    assert column("A1") == 0 and column("AB12") == 27 and serial_day(35828) == "1998-02-02" and serial_day(43185) == "2018-03-26"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("xl/workbook.xml", '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr("xl/_rels/workbook.xml.rels", '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Target="worksheets/sheet1.xml"/></Relationships>')
        z.writestr("xl/sharedStrings.xml", '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>Date</t></si><si><r><t>Ra</t></r><r><t>te</t></r></si></sst>')
        z.writestr("xl/worksheets/sheet1.xml", '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="C1" t="s"><v>1</v></c></row><row r="2"><c r="A2"><v>35828</v></c><c r="C2"><v>5.5</v></c></row></sheetData></worksheet>')
    assert xlsx(buf.getvalue()) == {"Data": [["Date", None, "Rate"], [35828.0, None, 5.5]]}


def http(url, timeout=60, headers=None):
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers or UA), timeout=timeout) as resp:
            raw = resp.read()
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return resp.status, raw, time.time() - t0, {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as e:
        return e.code, b"", time.time() - t0, {}
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, b"", time.time() - t0, {}


def kind_of(raw):
    return "xlsx/zip" if raw[:2] == b"PK" else "html" if b"<html" in raw[:600].lower() or b"<!doctype" in raw[:200].lower() else "json" if raw[:1] in (b"{", b"[") else "text"


def show_book(r, raw, rows_each=7, tail=3):
    try:
        book = xlsx(raw)
    except Exception as e:  # noqa: BLE001
        r.log("    not readable as a workbook: %s %s" % (type(e).__name__, str(e)[:120]))
        return {}
    for name, rows in book.items():
        filled = [x for x in rows if any(c not in (None, "") for c in x)]
        r.log("    sheet %r: %d rows, widest %d columns" % (name, len(filled), max((len(x) for x in filled), default=0)))
        for x in filled[:rows_each]:
            r.log("      %s" % json.dumps(x[:14])[:420])
        if len(filled) > rows_each + tail:
            r.log("      ...")
            for x in filled[-tail:]:
                r.log("      %s" % json.dumps(x[:14])[:420])
    return book


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=32))
    logs = boto3.client("logs", region_name="us-east-1")

    def jget(key):
        try:
            o = s3.get_object(Bucket=LIVE, Key=key)
            raw = o["Body"].read()
            if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return json.loads(raw), o["LastModified"]
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}, None

    with report("ops_6899_repo_open_items_probe") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6899 - the Repo desk's four open items, measured (read-only), %s" % now.isoformat(timespec="minutes"))
        facts = {}

        r.section("1. A daily repo rate before August 2014")
        code, raw, secs, hdr = http(SURVEY)
        r.log("survey-rate workbook: %s, %d bytes, %.1f s, %s, last-modified %s, sha256 %s" % (code, len(raw), secs, kind_of(raw), hdr.get("last-modified"), hashlib.sha256(raw).hexdigest()[:16]))
        book = show_book(r, raw) if raw[:2] == b"PK" else {}
        for name, rows in book.items():
            pairs = [(x[0], x[1]) for x in rows if len(x) >= 2 and isinstance(x[0], float) and 30000 < x[0] < 60000 and isinstance(x[1], float)]
            if pairs:
                gaps = Counter((int(b[0]) - int(a[0])) for a, b in zip(pairs, pairs[1:]))
                r.log("    %r as dates: %d observations, %s to %s | value range %.3f to %.3f | day gaps %s" % (
                    name, len(pairs), serial_day(pairs[0][0]), serial_day(pairs[-1][0]), min(v for _, v in pairs), max(v for _, v in pairs), dict(gaps.most_common(5))))
                by = {serial_day(d): v for d, v in pairs}
                for day in ("1998-10-08", "2001-09-11", "2001-09-17", "2007-08-20", "2008-03-17", "2008-09-15", "2008-09-17", "2008-09-30", "2008-12-31", "2014-08-22", "2016-12-30"):
                    r.log("      %s: %s" % (day, by.get(day)))
                facts["survey_obs"] = len(pairs)
                facts["survey_first"] = serial_day(pairs[0][0])
                facts["survey_last"] = serial_day(pairs[-1][0])
        for url in ("https://markets.newyorkfed.org/api/rp/all/all/results/search.json?startDate=2008-09-12&endDate=2008-09-19",
                    "https://markets.newyorkfed.org/api/rp/repo/all/results/search.json?startDate=2001-09-10&endDate=2001-09-14",
                    "https://markets.newyorkfed.org/api/rp/all/all/results/search.json?startDate=2000-07-01&endDate=2000-07-14"):
            code, raw, secs, _ = http(url)
            try:
                ops = (json.loads(raw).get("repo") or {}).get("operations") or []
            except Exception:  # noqa: BLE001
                ops = []
            r.log("NY Fed operations %s: %s, %d bytes, %d operations%s" % (url.split("results/")[1], code, len(raw), len(ops), "" if not ops else " | first: " + json.dumps(ops[-1])[:520]))
        board, when = jget("data/repo.json")
        rows = board.get("rows") or board.get("series") or []
        r.log("board written %s | %d rows | keys of a row %s" % (when.strftime("%Y-%m-%d %H:%M") if when else "-", len(rows), list(rows[0])[:14] if rows else None))
        early = sorted((x.get("first") or "9", x.get("id"), x.get("group"), (x.get("label") or "")[:70]) for x in rows
                       if re.search(r"rate|SOFR|GCF|repo", (x.get("label") or ""), re.I) and x.get("group") not in ("FRED repo & SOFR complex", "ICE BofA option-adjusted spreads (FRED)"))
        for x in early[:16]:
            r.log("  earliest rate-like series: %s  %-34s %-28s %s" % x)
        fred_early = sorted((x.get("first") or "9", x.get("id"), (x.get("label") or "")[:80]) for x in rows if x.get("group") == "FRED repo & SOFR complex" and re.search(r"repo|repurchase", x.get("label") or "", re.I) and (x.get("first") or "9") < "2014")
        r.log("FRED-group series with 'repo' in the name that begin before 2014: %d" % len(fred_early))
        for x in fred_early[:14]:
            r.log("  %s  %-22s %s" % x)

        r.section("2. Tri-party haircuts")
        hix, when = jget("data/warm/nyfed-research/haircuts-series/_index.json")
        ser = hix.get("series") or []
        r.log("banked index written %s | built_at %s | %d series | entry keys %s" % (when.strftime("%Y-%m-%d") if when else "-", hix.get("built_at"), len(ser), list(ser[0]) if ser else None))
        lasts, shapes = Counter(), Counter()
        sample = None
        for e in ser:
            doc, w = jget("data/warm/nyfed-research/haircuts-series/%s.json" % e["id"])
            obs = doc.get("observations") or []
            lasts[obs[-1].get("date") if obs else None] += 1
            shapes[tuple(sorted(doc))] += 1
            if sample is None and obs:
                sample = (e, {k: (v if not isinstance(v, list) else "[%d] %s ... %s" % (len(v), json.dumps(v[:2]), json.dumps(v[-2:]))) for k, v in doc.items()})
        r.log("banked series by newest date: %s | document shapes: %s" % (dict(lasts), {", ".join(k): n for k, n in shapes.items()}))
        if sample:
            r.log("sample entry %s" % json.dumps(sample[0])[:300])
            r.log("sample document %s" % json.dumps(sample[1])[:700])
        r.log("all ids: %s" % ", ".join(e["id"] for e in ser))
        r.log("all titles (first 30): %s" % " | ".join(e.get("title", "") for e in ser[:30]))
        page = s3.list_objects_v2(Bucket=LIVE, Prefix="data/warm/nyfed-research/", Delimiter="/")
        r.log("data/warm/nyfed-research/: folders %s | files %s" % ([p["Prefix"] for p in page.get("CommonPrefixes", [])], [(o["Key"].split("/")[-1], o["Size"], o["LastModified"].strftime("%Y-%m-%d")) for o in page.get("Contents", [])][:12]))
        for url in TPR:
            code, raw, secs, hdr = http(url)
            r.log("%s -> %s, %d bytes, %.1f s, %s, type %s, disposition %s, last-modified %s" % (
                url.split("newyorkfed.org")[1], code, len(raw), secs, kind_of(raw) if raw else "-", hdr.get("content-type"), hdr.get("content-disposition"), hdr.get("last-modified")))
            if raw[:2] == b"PK":
                show_book(r, raw, rows_each=9, tail=4)
            elif raw and kind_of(raw) in ("text", "json"):
                text = raw.decode("utf-8", "replace")
                lines = text.splitlines()
                r.log("    %d lines | first: %s" % (len(lines), " || ".join(x[:200] for x in lines[:5])))
                r.log("    last: %s" % " || ".join(x[:200] for x in lines[-3:]))
            elif raw:
                title = re.search(rb"<title>(.*?)</title>", raw, re.S)
                r.log("    html title: %s" % (title.group(1).decode("utf-8", "replace").strip()[:120] if title else None))

        r.section("3. DTCC daily fails")
        for key in ("data/warm/dtcc-fails/treasury.json", "data/warm/dtcc-fails/agency.json"):
            doc, w = jget(key)
            obs = doc.get("observations") or []
            r.log("%s: written %s | %d observations %s to %s | source %s" % (key, w.strftime("%Y-%m-%d") if w else "-", len(obs), obs[0]["date"] if obs else None, obs[-1]["date"] if obs else None, doc.get("source")))
        for url in DTCC_DIRECT + DTCC_PAGES:
            code, raw, secs, hdr = http(url, timeout=40)
            r.log("%s -> %s, %d bytes, %s, type %s | %s" % (url.split("dtcc.com")[1], code, len(raw), kind_of(raw) if raw else "-", hdr.get("content-type"),
                                                          raw[:160].decode("utf-8", "replace").replace("\n", " | ") if raw and kind_of(raw) in ("text", "json") else ""))
            if raw and kind_of(raw) == "html":
                hits = sorted(set(m.decode("utf-8", "replace") for m in re.findall(rb"""["'(]([^"'()\s]{0,160}(?:\.csv|\.json|\.xlsx?|/data/|/api/)[^"'()\s]{0,80})["')]""", raw)))
                r.log("    data-like references in the page source: %d %s" % (len(hits), hits[:30]))
                scripts = sorted(set(m.decode("utf-8", "replace") for m in re.findall(rb"""<script[^>]+src=["']([^"']+)["']""", raw)))
                r.log("    scripts: %s" % scripts[:24])
        try:
            spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
            A = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(A)
            sync_playwright = A.ensure_playwright(r.log)
            with sync_playwright() as p:
                browser = A.launch(p, r.log)
                for url in DTCC_PAGES[:2]:
                    ctx = browser.new_context(user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36", viewport={"width": 1366, "height": 900})
                    pg = ctx.new_page()
                    seen = []

                    def on_response(resp, seen=seen):
                        try:
                            ct = (resp.headers.get("content-type") or "").split(";")[0]
                            rt = resp.request.resource_type
                            if rt in ("xhr", "fetch") or any(k in ct for k in ("json", "csv", "excel", "spreadsheet", "octet")) or re.search(r"\.(csv|json|xlsx?)(\?|$)", resp.url):
                                body = b""
                                try:
                                    body = resp.body()
                                except Exception:  # noqa: BLE001
                                    pass
                                seen.append((resp.status, rt, ct, len(body), resp.url[:230], body[:260].decode("utf-8", "replace").replace("\n", " | ")))
                        except Exception:  # noqa: BLE001
                            pass

                    pg.on("response", on_response)
                    try:
                        pg.goto(url, wait_until="domcontentloaded", timeout=60000)
                        pg.wait_for_timeout(15000)
                    except Exception as e:  # noqa: BLE001
                        r.log("  browser: %s %s" % (type(e).__name__, str(e)[:120]))
                    r.log("browser on %s: title %r | %d data responses" % (url.split("dtcc.com")[1], pg.title()[:80], len(seen)))
                    for row in seen[:40]:
                        r.log("    %s %s %s %d bytes %s" % row[:5])
                        if row[3] and any(k in row[2] for k in ("json", "csv", "text/plain")):
                            r.log("        %s" % row[5])
                    links = pg.evaluate("""() => Array.from(document.querySelectorAll('a[href]')).map(a => [a.textContent.trim().slice(0, 60), a.href]).filter(x => /download|click here|csv|data|excel|export/i.test(x[0]) || /\\.(csv|xlsx?|json)(\\?|$)/i.test(x[1])).slice(0, 30)""")
                    r.log("    links that look like a download: %s" % json.dumps(links)[:1500])
                    text = pg.evaluate("() => document.body ? document.body.innerText.replace(/\\s+/g, ' ').slice(0, 900) : ''")
                    r.log("    page text: %s" % text)
                    ctx.close()
                browser.close()
        except Exception as e:  # noqa: BLE001
            r.warn("browser probe did not run: %s %s" % (type(e).__name__, str(e)[:200]))

        r.section("4. Dealer repo financing")
        doc, when = jget("data/nyfed-primary-dealer.json")
        fin = doc.get("financing") or {}
        r.log("published document written %s | version %s | generated_at %s | source_generated_at %s | as_of %s | quality %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", doc.get("version"), doc.get("generated_at"), doc.get("source_generated_at"), doc.get("as_of"), json.dumps(doc.get("quality"))[:200]))
        r.log("financing: as_of %s | repo_out_b %s | reverse_repo_in_b %s | quality %s" % (fin.get("as_of"), fin.get("repo_out_b"), fin.get("reverse_repo_in_b"), json.dumps(fin.get("quality"))[:300]))
        r.log("financing.treasury: %s" % json.dumps({k: v for k, v in (fin.get("treasury") or {}).items() if k != "note"})[:700])
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-nyfed-pd", orderBy="LastEventTime", descending=True, limit=8).get("logStreams", [])
            for st in streams:
                ev = logs.get_log_events(logGroupName="/aws/lambda/justhodl-nyfed-pd", logStreamName=st["logStreamName"], limit=60, startFromHead=True).get("events", [])
                lines = [e["message"].strip()[:260] for e in ev if not e["message"].startswith(("START", "END", "INIT_START"))]
                r.log("log stream %s: %s" % (datetime.fromtimestamp(st.get("lastEventTimestamp", 0) / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M"), " || ".join(lines)[:900]))
        except Exception as e:  # noqa: BLE001
            r.log("logs: %s %s" % (type(e).__name__, str(e)[:160]))
        src = ROOT / "aws" / "lambdas" / "justhodl-nyfed-pd" / "source"
        sys.path.insert(0, str(src))
        sys.path.insert(0, str(ROOT / "aws" / "shared"))
        import dealer_original as native
        raws, failed = {}, []
        for name, url in native.URLS.items():
            code, raw, secs, hdr = http(url, timeout=40, headers={"User-Agent": "JustHodl source research", "Accept": "application/pdf" if name in native.DEFINITIONS else "*/*"})
            raws[name] = raw
            r.log("  source %-18s %s, %d bytes, %.1f s, %s | %s" % (name, code, len(raw), secs, kind_of(raw) if raw else "-", url[:110]))
            if code != 200 or not raw:
                failed.append("%s answers %s" % (name, code))
        for key, d in native.DEFINITIONS.items():
            sha = hashlib.sha256(raws.get(key) or b"").hexdigest()
            if not (raws.get(key) or b"").startswith(b"%PDF-") or sha != d["sha256"]:
                failed.append("reporting instructions %s changed: sha %s, expected %s, pdf %s" % (key, sha[:16], d["sha256"][:16], (raws.get(key) or b"")[:5] == b"%PDF-"))
        try:
            breaks = {row.get("seriesbreak"): row for row in native.strict_json(raws["breaks"]).get("pd", {}).get("seriesbreaks", [])}
            for key, d in native.DEFINITIONS.items():
                row = breaks.get(key, {})
                if (row.get("startdate"), row.get("enddate")) != (d["start"], d["end"]):
                    failed.append("series break %s is %s..%s, expected %s..%s" % (key, row.get("startdate"), row.get("enddate"), d["start"], d["end"]))
            extra = sorted(set(breaks) - set(native.DEFINITIONS))
            r.log("  series breaks at the source: %s | not in the engine's definitions: %s" % (sorted(breaks), extra))
        except Exception as e:  # noqa: BLE001
            failed.append("series-break catalog: %s %s" % (type(e).__name__, str(e)[:100]))
        try:
            catalog = native.strict_json(raws["catalog"]).get("pd", {}).get("timeseries") or []
            seen = {}
            for row in catalog:
                if row.get("keyid") in native.KEYS:
                    seen.setdefault(row["keyid"], []).append(row)
            for key in native.KEYS:
                got = seen.get(key) or []
                if len(got) != 1:
                    failed.append("catalog identity %s appears %d times" % (key, len(got)))
                elif got[0] != native.SERIES[key]["catalog"]:
                    failed.append("catalog row %s changed: now %s | reviewed %s" % (key, json.dumps(got[0])[:260], json.dumps(native.SERIES[key]["catalog"])[:260]))
            r.log("  catalog at the source: %d rows, %d of the engine's %d identities found" % (len(catalog), len(seen), len(native.KEYS)))
        except Exception as e:  # noqa: BLE001
            failed.append("series catalog: %s %s" % (type(e).__name__, str(e)[:100]))
        sha = hashlib.sha256(raws.get("historical_release") or b"").hexdigest()
        if sha != native.REGISTRY["reference_release"]["sha256"]:
            failed.append("historical reference release changed: sha %s, expected %s (%d bytes, %s)" % (sha[:16], native.REGISTRY["reference_release"]["sha256"][:16], len(raws.get("historical_release") or b""), kind_of(raws.get("historical_release") or b"")))
        text = (raws.get("release_schedule") or b"").decode("utf-8", "replace")
        for needle in ("Thursdays", "4:15", "previous week"):
            if needle not in text:
                failed.append("release page no longer says %r" % needle)
        if any(n not in text for n in ("Thursdays", "4:15", "previous week")):
            plain = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)))
            for m in re.finditer(r"(?i)(thursday|4:15|released|publish)", plain):
                r.log("    release page near %r: ...%s..." % (m.group(1), plain[max(0, m.start() - 140):m.end() + 180]))
                if m.start() > 40000:
                    break
        try:
            parsed = native.parse(raws["observations"], now.isoformat())
            newest = Counter(max(v) for v in parsed.values())
            r.log("  observations parse: %d series | newest dates %s" % (len(parsed), dict(newest.most_common(4))))
            for key in ("PDSORA-UTSETTOT", "PDSORA-UTSTTOT", "PDSIRRA-UTSETTOT", "PDSIRRA-UTSTTOT"):
                last = sorted(parsed[key])[-3:]
                r.log("    %-18s %s" % (key, ", ".join("%s %s" % (d, parsed[key][d]["source_value"]) for d in last)))
            facts["pd_source_newest"] = max(newest)
        except Exception as e:  # noqa: BLE001
            failed.append("observations do not parse: %s %s" % (type(e).__name__, str(e)[:160]))
        try:
            import dealer_research_store as store
            prior = store.previous(store.raw_reader(s3, LIVE))
            r.log("  previous published output verifies: %s" % json.dumps(prior)[:200])
        except Exception as e:  # noqa: BLE001
            failed.append("previous output check: %s %s" % (type(e).__name__, str(e)[:160]))
        r.log("acceptance checks that refuse today: %d" % len(failed))
        for f in failed:
            r.log("  REFUSES: %s" % f)
        facts["pd_refusals"] = len(failed)
        facts["pd_published_as_of"] = doc.get("as_of")
        r.kv(**facts)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
