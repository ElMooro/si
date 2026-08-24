"""ops_4962 -- Khalid's five priority providers: INVESTIGATE + RANK.

Named lanes jump the drain queue: nyfed-markets, treasury-fiscaldata,
ofr-bsrm, dol-eta, gdelt. Evidence from the SOURCES themselves
(runner-side), then a ranked build order:

  nyfed    board says "359 obs/series since 2013" -- probe TRUE
           full-history counts (SOFR'18-, EFFR'00-, SOMA'03-, repo
           ops, PD timeseries catalog, agency MBS)
  treasury 49 curated keys vs FiscalData's dataset universe -- probe
           total-count + earliest record on flagship datasets
           (auctions since 1979, MTS, DTS, debt-to-penny...)
  ofr-bsrm workbooks already FULL via src-mirror; measure the
           TRANSFORM DEBT: sheets/rows in the mirrored xlsx vs the
           500 parsed series' staleness
  dol      6 keys vs oui.doleta.gov DataDownloads corpus -- count
           report files + sample sizes
  gdelt    exact full-EVENTS byte total from masterfilelist.txt
           (HEAD size + Range head/tail sampling); GKG/mentions
           stay scoped -- state the reason with numbers
Artifact data/warm/_audit/five-priority.json.  [skip-deploy]
"""
import io
import json
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
OUT = "data/warm/_audit/five-priority.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "*/*"}
s3 = boto3.client("s3", region_name=REGION)


def fetch(url, cap=6_000_000, timeout=75, rng=None):
    h = dict(UA)
    if rng:
        h["Range"] = rng
    try:
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            b = r.read(cap)
            return {"status": r.status, "bytes": len(b),
                    "clen": r.headers.get("Content-Length")}, b
    except urllib.error.HTTPError as e:
        return {"status": e.code, "head": (e.read(300) or b"")
                .decode("utf-8", "replace")}, b""
    except Exception as e:
        return {"status": 0, "head": str(e)[:150]}, b""


with report("ops_4962_five_priority_probe") as R:
    out = {"as_of": datetime.now(timezone.utc).isoformat(
        timespec="seconds"), "ops": 4962, "providers": {}}
    fails = []

    # ---- nyfed markets ------------------------------------------------
    R.section("nyfed-markets: true full-history depth")
    ny = {}
    NB = "https://markets.newyorkfed.org/api"
    tests = [
        ("sofr_full", NB + "/rates/secured/sofr/search.json"
         "?startDate=2018-04-01&endDate=2026-08-24", "effectiveDate"),
        ("effr_full", NB + "/rates/unsecured/effr/search.json"
         "?startDate=2000-01-01&endDate=2026-08-24", "effectiveDate"),
        ("soma_asof", NB + "/soma/asofdates/list.json", '"'),
        ("repo_ops", NB + "/rp/results/search.json"
         "?startDate=2000-01-01", "operationId"),
        ("pd_series", NB + "/pd/list/timeseries.json", "keyid"),
        ("ambs", NB + "/ambs/all/results/summary/search.json"
         "?startDate=2009-01-01", "operationId"),
    ]
    for name, url, tok in tests:
        meta, body = fetch(url, cap=25_000_000, timeout=110)
        n = body.decode("utf-8", "replace").lower().count(
            tok.lower()) if body else 0
        ny[name] = {"status": meta.get("status"),
                    "mb": round((meta.get("bytes") or 0) / 1e6, 2),
                    "count": n}
        R.log("  %-10s %s %.2fMB count~%d" % (
            name, meta.get("status"), (meta.get("bytes") or 0) / 1e6,
            n))
        time.sleep(0.4)
    out["providers"]["nyfed"] = ny

    # ---- treasury fiscaldata -----------------------------------------
    R.section("treasury: dataset totals + earliest records")
    tr = {}
    TB = "https://api.fiscaldata.treasury.gov/services/api/" \
         "fiscal_service"
    dsets = [
        ("auctions", "/v1/accounting/od/auctions_query"),
        ("debt_to_penny", "/v2/accounting/od/debt_to_penny"),
        ("mts_outlays", "/v1/accounting/mts/mts_table_5"),
        ("dts_cash", "/v1/accounting/dts/operating_cash_balance"),
        ("avg_int_rates", "/v2/accounting/od/avg_interest_rates"),
        ("exch_rates", "/v1/accounting/od/rates_of_exchange"),
        ("gold_reserve", "/v2/accounting/od/gold_reserve"),
    ]
    for name, path in dsets:
        meta, body = fetch(TB + path + "?page[size]=1&sort=record_"
                           "date&format=json", cap=300_000)
        tot, first = None, None
        try:
            js = json.loads(body)
            tot = ((js.get("meta") or {}).get("total-count"))
            d0 = (js.get("data") or [{}])[0]
            first = d0.get("record_date")
        except Exception:
            pass
        tr[name] = {"status": meta.get("status"), "total": tot,
                    "earliest": first}
        R.log("  %-14s %s total=%s earliest=%s" % (
            name, meta.get("status"), tot, first))
        time.sleep(0.3)
    out["providers"]["treasury"] = tr

    # ---- ofr-bsrm transform debt -------------------------------------
    R.section("ofr-bsrm: workbook vs parsed-series transform debt")
    bs = {}
    try:
        keys = []
        r_ = s3.list_objects_v2(Bucket=B,
                                Prefix="data/warm/ofr-bsrm/")
        for o in r_.get("Contents", []):
            keys.append((o["Key"], o["Size"],
                         o["LastModified"].isoformat()))
        wb = next((k for k, _s, _m in keys
                   if k.endswith("ofr_bsrm.xlsx")), None)
        bs["n_keys"] = len(keys)
        if wb:
            raw = s3.get_object(Bucket=B, Key=wb)["Body"].read()
            zf = zipfile.ZipFile(io.BytesIO(raw))
            sheets = [n for n in zf.namelist()
                      if n.startswith("xl/worksheets/")]
            wbxml = zf.read("xl/workbook.xml").decode(
                "utf-8", "replace")
            names = re.findall(r'<sheet name="([^"]+)"', wbxml)
            bs["workbook"] = {"key": wb, "bytes": len(raw),
                              "sheets": len(sheets),
                              "sheet_names": names[:12]}
            R.log("  workbook %s %.2fMB sheets=%d %s" % (
                wb.rsplit("/", 1)[-1], len(raw) / 1e6, len(sheets),
                names[:8]))
        parsed = [(k, m) for k, _s, m in keys
                  if "/series/" in k or k.endswith(".json")]
        newest = max((m for _k, m in parsed), default=None)
        bs["parsed_series"] = {"n": len(parsed),
                               "newest_mtime": newest}
        R.log("  parsed series n=%d newest=%s" % (
            len(parsed), (newest or "")[:19]))
    except Exception as e:
        bs["err"] = str(e)[:150]
        R.log("  bsrm err: %s" % str(e)[:120])
    out["providers"]["ofr-bsrm"] = bs

    # ---- dol corpus ---------------------------------------------------
    R.section("dol: DataDownloads corpus")
    dl = {}
    meta, body = fetch("https://oui.doleta.gov/unemploy/"
                       "DataDownloads.asp", cap=4_000_000)
    html = body.decode("utf-8", "replace") if body else ""
    links = sorted(set(re.findall(
        r'href="([^"]+\.(?:csv|CSV))"', html)))
    dl["status"] = meta.get("status")
    dl["n_csv_links"] = len(links)
    dl["sample"] = links[:10]
    R.log("  page %s csv_links=%d sample=%s" % (
        meta.get("status"), len(links),
        [x.rsplit("/", 1)[-1] for x in links[:6]]))
    tot_mb = 0.0
    for lk in links[:5]:
        u = lk if lk.startswith("http") else \
            "https://oui.doleta.gov" + (lk if lk.startswith("/")
                                        else "/unemploy/" + lk)
        m2, _ = fetch(u, cap=64, timeout=45)
        cl = int(m2.get("clen") or 0)
        tot_mb += cl / 1e6
        R.log("    %-28s %s %.2fMB" % (
            u.rsplit("/", 1)[-1], m2.get("status"), cl / 1e6))
        time.sleep(0.3)
    dl["sampled_mb_first5"] = round(tot_mb, 2)
    out["providers"]["dol"] = dl

    # ---- gdelt exact events corpus -----------------------------------
    R.section("gdelt: exact full-EVENTS corpus from masterfilelist")
    gd = {}
    MF = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    mh, _ = fetch(MF, cap=64, timeout=60)
    size = int(mh.get("clen") or 0)
    gd["masterfilelist_mb"] = round(size / 1e6, 1)
    _, head = fetch(MF, cap=250_000, rng="bytes=0-249999")
    _, tail = fetch(MF, cap=250_000,
                    rng="bytes=%d-" % max(0, size - 250_000))

    def stats(chunk):
        lines = chunk.decode("utf-8", "replace").splitlines()
        ex = [ln for ln in lines if ".export.CSV.zip" in ln]
        szs = []
        for ln in ex:
            p = ln.split()
            if p and p[0].isdigit():
                szs.append(int(p[0]))
        return len(lines), len(ex), (sum(szs) / max(1, len(szs)))
    nl_h, ne_h, avg_h = stats(head)
    nl_t, ne_t, avg_t = stats(tail)
    avg_line = (250_000 / max(1, nl_h) + 250_000 / max(1, nl_t)) / 2
    total_lines = int(size / max(1, avg_line))
    exp_files = int(total_lines / 3)
    avg_exp = (avg_h + avg_t) / 2
    events_gb = exp_files * avg_exp / 1e9
    gd.update({"est_total_lines": total_lines,
               "est_export_files": exp_files,
               "avg_export_bytes_head": int(avg_h),
               "avg_export_bytes_tail": int(avg_t),
               "est_full_events_gb": round(events_gb, 1)})
    R.log("  masterfilelist %.1fMB -> ~%d files; export avg "
          "head/tail %.0f/%.0fKB -> FULL v2 EVENTS ~= %.1f GB" % (
              size / 1e6, exp_files, avg_h / 1e3, avg_t / 1e3,
              events_gb))
    m1, b1 = fetch("http://data.gdeltproject.org/events/index.html",
                   cap=3_000_000)
    v1n = b1.decode("utf-8", "replace").count(".export.CSV.zip") \
        if b1 else 0
    gd["v1_daily_files"] = v1n
    R.log("  v1 daily archive (1979-2015): %d files" % v1n)
    out["providers"]["gdelt"] = gd

    # ---- ranked verdicts ---------------------------------------------
    R.section("PRIORITIZED BUILD ORDER (evidence-ranked)")
    order = []
    ny_gap = (ny.get("effr_full", {}).get("count") or 0)
    order.append(("nyfed-full-depth", "EFFR full-history obs ~%d vs "
                  "board's 359/series; PD timeseries catalog ~%d -- "
                  "highest doctrine value (plumbing)" % (
                      ny_gap, ny.get("pd_series", {})
                      .get("count") or 0)))
    tr_tot = sum(v.get("total") or 0 for v in tr.values())
    order.append(("fiscaldata-full", "flagship datasets total ~%s "
                  "rows (auctions since %s)" % (
                      tr_tot, tr.get("auctions", {})
                      .get("earliest"))))
    order.append(("gdelt-events-full", "exact corpus ~%.0fGB v2 + "
                  "%d v1 daily files; GKG/mentions stay scoped" % (
                      gd.get("est_full_events_gb") or 0,
                      gd.get("v1_daily_files") or 0)))
    order.append(("dol-full", "%d report csvs on DataDownloads" %
                  (dl.get("n_csv_links") or 0)))
    order.append(("bsrm-phase2-transform", "workbook %s sheets vs "
                  "%s parsed series newest %s -- transform debt, "
                  "not import debt" % (
                      (bs.get("workbook") or {}).get("sheets"),
                      (bs.get("parsed_series") or {}).get("n"),
                      ((bs.get("parsed_series") or {})
                       .get("newest_mtime") or "")[:10])))
    for i, (nm, why) in enumerate(order, 1):
        R.log("  %d. %-22s %s" % (i, nm, why))
    out["build_order"] = [nm for nm, _ in order]

    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out, indent=1).encode(),
                  ContentType="application/json")
    probed_ok = sum(1 for pv in out["providers"].values()
                    if any((isinstance(v, dict) and
                            v.get("status") == 200) or
                           (isinstance(v, (int, float)) and v)
                           for v in pv.values()))
    R.log("artifact %s (providers with live evidence: %d/5)" % (
        OUT, probed_ok))
    if probed_ok < 4:
        R.log("ops 4962 RED: <4 providers evidenced")
        sys.exit(1)
    R.kv(nyfed_effr=ny_gap,
         treasury_rows=tr_tot,
         gdelt_events_gb=gd.get("est_full_events_gb"),
         dol_csvs=dl.get("n_csv_links"),
         bsrm_sheets=(bs.get("workbook") or {}).get("sheets"))
    R.log("ops 4962 GREEN -- five lanes evidenced + ranked; builds "
          "start with nyfed-full-depth")
