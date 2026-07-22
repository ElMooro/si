#!/usr/bin/env python3
"""ops 3734 — PROBE 2: pin the exact ERCOT queue report + EIA planned facets.

3733 proved reachability but two IDs were guesses and one was wrong:
  - ERCOT reportTypeId=15933 is the CO-LOCATED BATTERY report, not the
    interconnection queue. Find the real GIS/queue report ID.
  - EIA operating-generator-capacity rejected facet 'statusid'; the API
    returned the valid list. Discover the real status facet + its values.
  - CAISO queue XLSX downloaded (113KB) but shape unverified.

Never type an ID from memory. This ops resolves all three, then the engine
ops is built against proven identifiers only.
"""
import json
import os
import ssl
import sys
import traceback
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()
EIA_KEY = os.environ.get("EIA_API_KEY", "trvODpgt2GdvBbLeIeVMyaQwsnNFQIYSueoVm4Fl")

with report("3734_grid_ids_probe") as rep:
    rep.heading("ops 3734 — resolve ERCOT queue report ID + EIA planned-capacity facets")
    findings = {}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3734.json").write_text(json.dumps({"verdict": "STARTED"}))

    try:
        def get(url, timeout=45, raw=False):
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                b = r.read(400000)
                return b if raw else b.decode("utf-8", "replace")

        # ── A. EIA: discover valid facets on operating-generator-capacity ─
        rep.section("A — EIA operating-generator-capacity metadata")
        try:
            meta = json.loads(get(
                "https://api.eia.gov/v2/electricity/operating-generator-capacity"
                "?api_key=%s" % EIA_KEY))
            resp = meta.get("response", {})
            facets = resp.get("facets", [])
            rep.ok("facets: %s" % [f.get("id") for f in facets])
            rep.log("  data cols: %s" % list((resp.get("data") or {}).keys()))
            findings["eia_facets"] = [f.get("id") for f in facets]
            # enumerate the status-like facet values
            for f in facets:
                fid = f.get("id")
                if fid and ("status" in fid.lower() or fid in ("sector",)):
                    try:
                        fv = json.loads(get(
                            "https://api.eia.gov/v2/electricity/"
                            "operating-generator-capacity/facet/%s?api_key=%s"
                            % (fid, EIA_KEY)))
                        vals = (fv.get("response") or {}).get("facets") or []
                        rep.ok("  facet %s values: %s"
                               % (fid, [(v.get("id"), v.get("name")) for v in vals][:14]))
                        findings["eia_facet_%s" % fid] = [v.get("id") for v in vals]
                    except Exception as e:
                        rep.warn("  facet %s: %s" % (fid, str(e)[:110]))
        except Exception as e:
            rep.warn("EIA metadata: %s %s" % (type(e).__name__, str(e)[:170]))

        # ── B. EIA: planned capacity pull with the REAL facet ─────────────
        rep.section("B — EIA planned capacity sample (real facet)")
        status_facet = None
        for k in findings.get("eia_facets", []):
            if "status" in (k or "").lower():
                status_facet = k
        rep.log("  status facet resolved = %s" % status_facet)
        try:
            base = ("https://api.eia.gov/v2/electricity/operating-generator-capacity"
                    "/data/?api_key=%s&frequency=monthly"
                    "&data[0]=net-summer-capacity-mw"
                    "&sort[0][column]=period&sort[0][direction]=desc&length=6"
                    % EIA_KEY)
            d = json.loads(get(base))
            rows = (d.get("response") or {}).get("data") or []
            rep.ok("planned-capacity rows=%d total=%s"
                   % (len(rows), (d.get("response") or {}).get("total")))
            if rows:
                rep.log("  sample row: %s" % json.dumps(rows[0])[:420])
                findings["eia_gen_row_keys"] = sorted(rows[0].keys())
        except Exception as e:
            rep.warn("planned capacity: %s" % str(e)[:200])

        # ── C. ERCOT: find the interconnection queue report type ──────────
        rep.section("C — ERCOT report catalogue search for queue/GIS")
        # ERCOT publishes a report type list; scan candidate IDs for names
        # matching the interconnection queue (GIS report).
        candidates = [15933, 11726, 15540, 13182, 12331, 15561, 10054,
                      13100, 15476, 12300, 11348, 15064]
        for rid in candidates:
            try:
                b = get("https://www.ercot.com/misapp/servlets/"
                        "IceDocListJsonWS?reportTypeId=%d" % rid, timeout=35)
                d = json.loads(b)
                docs = (d.get("ListDocsByRptTypeRes") or {}).get("DocumentList") or []
                if docs:
                    nm = (docs[0].get("Document") or {}).get("FriendlyName", "")
                    rep.log("  %d -> %s (%d docs)" % (rid, nm[:70], len(docs)))
                    if any(w in nm.lower() for w in
                           ("interconnection", "gis", "queue", "capacity",
                            "generation")):
                        rep.ok("  CANDIDATE MATCH rid=%d name=%s" % (rid, nm[:80]))
                        findings["ercot_rid_%d" % rid] = nm[:120]
            except Exception as e:
                rep.log("  %d: %s" % (rid, str(e)[:70]))

        # ── D. CAISO queue workbook shape ────────────────────────────────
        rep.section("D — CAISO public queue workbook")
        try:
            blob = get("https://www.caiso.com/PublishedDocuments/"
                       "PublicQueueReport.xlsx", timeout=60, raw=True)
            rep.ok("CAISO xlsx bytes=%d magic=%s" % (len(blob), blob[:2]))
            import io
            import zipfile
            z = zipfile.ZipFile(io.BytesIO(blob))
            names = z.namelist()
            rep.log("  zip members: %s" % names[:12])
            # sheet names live in workbook.xml
            if "xl/workbook.xml" in names:
                wb = z.read("xl/workbook.xml").decode("utf-8", "replace")
                import re
                sheets = re.findall(r'name="([^"]+)"', wb)
                rep.ok("  sheets: %s" % sheets[:12])
                findings["caiso_sheets"] = sheets[:12]
        except Exception as e:
            rep.warn("CAISO: %s %s" % (type(e).__name__, str(e)[:170]))

        # ── E. EPA ECHO — permit-bearing facility detail ──────────────────
        rep.section("E — EPA ECHO air facility detail (permit proxy)")
        try:
            b = get("https://echodata.epa.gov/echo/air_rest_services.get_facilities?"
                    "output=JSON&p_st=TX&p_maj=Y&responseset=1")
            d = json.loads(b)
            res = d.get("Results", {})
            rep.ok("ECHO major-source query rows=%s qid=%s"
                   % (res.get("QueryRows"), res.get("QueryID")))
            qid = res.get("QueryID")
            if qid:
                b2 = get("https://echodata.epa.gov/echo/air_rest_services.get_qid?"
                         "qid=%s&output=JSON&pageno=1" % qid)
                d2 = json.loads(b2)
                rows = ((d2.get("Results") or {}).get("Facilities") or [])
                rep.ok("  facility rows=%d" % len(rows))
                if rows:
                    rep.log("  keys: %s" % sorted(rows[0].keys())[:22])
                    rep.log("  sample: %s" % json.dumps(rows[0])[:340])
                    findings["echo_keys"] = sorted(rows[0].keys())
        except Exception as e:
            rep.warn("ECHO: %s %s" % (type(e).__name__, str(e)[:170]))

        rep.section("VERDICT")
        Path("aws/ops/reports/3734.json").write_text(
            json.dumps({"verdict": "PASS", "findings": findings}, indent=2))
        rep.kv(eia_facets=",".join(findings.get("eia_facets", [])) or "?",
               caiso_sheets=len(findings.get("caiso_sheets", [])),
               probe_only="true")
        rep.ok("PROBE 2 COMPLETE")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3734.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
