#!/usr/bin/env python3
"""ops 3733 — PROBE: interconnection queues + industrial power load (canary #2).

Canary list item #2: a fab or data center appears in an interconnection queue
and a permit database ~18-24 months before it shows up in revenue. Nobody
retail watches it. Data is free. But "free" hides very different access
models per source, so we probe before building.

Audit (3733, extend-don't-duplicate): capex-pulse measures REPORTED capex
dollars (backward, T-0); structural-pre-signals counts capex MENTIONS in
filings. Neither touches physical grid interconnection or permits. Clean gap.

PROBE TARGETS
  G1  EIA API v2 — electricity retail sales / demand by state+sector
      (EIA_API_KEY already in the fleet). Industrial load = fabs & DCs
      showing up as load before as revenue.
  G2  EIA-860M planned generator additions (capacity coming online).
  G3  LBNL "Queued Up" interconnection dataset (annual, authoritative).
  G4  Per-ISO queue endpoints: PJM Data Miner 2, ERCOT MIS, CAISO, MISO —
      which answer keyless from a Lambda IP?
  G5  gridstatus.io public API shape (if reachable).
  G6  EPA ECHO / Envirofacts air-permit API — industrial construction permits.

NOTHING deployed. Output is a reachability + vocabulary report.
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

with report("3733_grid_permit_probe") as rep:
    rep.heading("ops 3733 — interconnection queue + permit source probe (no deploy)")
    fails = []
    findings = {}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3733.json").write_text(json.dumps({"verdict": "STARTED"}))

    try:
        def hit(url, label, timeout=40, show=420, headers=None):
            h = dict(UA)
            if headers:
                h.update(headers)
            try:
                req = urllib.request.Request(url, headers=h)
                with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                    body = r.read(120000).decode("utf-8", "replace")
                    rep.ok("%s: HTTP %s len=%d" % (label, r.status, len(body)))
                    rep.log("    %s" % body[:show].replace("\n", " "))
                    findings[label] = {"status": r.status, "len": len(body)}
                    return body
            except urllib.error.HTTPError as e:
                det = ""
                try:
                    det = e.read().decode("utf-8", "replace")[:200]
                except Exception:
                    pass
                rep.warn("%s: HTTP %s %s" % (label, e.code, det))
                findings[label] = {"status": e.code, "err": det[:160]}
            except Exception as e:
                rep.warn("%s: %s %s" % (label, type(e).__name__, str(e)[:150]))
                findings[label] = {"err": "%s %s" % (type(e).__name__, str(e)[:120])}
            return None

        # ── G1 EIA electricity demand by sector ──────────────────────────
        rep.section("G1 — EIA v2 electricity retail sales (industrial load)")
        q = ("https://api.eia.gov/v2/electricity/retail-sales/data/"
             "?api_key=%s&frequency=monthly&data[0]=sales&data[1]=customers"
             "&facets[sectorid][]=IND&sort[0][column]=period&sort[0][direction]=desc"
             "&length=5" % EIA_KEY)
        b = hit(q, "eia_retail_sales_IND")
        if b:
            try:
                d = json.loads(b)
                rows = (d.get("response") or {}).get("data") or []
                if rows:
                    rep.ok("  industrial rows: %d, newest period=%s"
                           % (len(rows), rows[0].get("period")))
                    rep.log("    sample: %s" % json.dumps(rows[0])[:300])
            except Exception as e:
                rep.warn("  parse: %s" % str(e)[:120])

        # ── G2 EIA-860M planned additions ────────────────────────────────
        rep.section("G2 — EIA planned capacity additions (860M)")
        for path, lbl in (
            ("electricity/operating-generator-capacity/data/"
             "?api_key=%s&frequency=monthly&data[0]=net-summer-capacity-mw"
             "&facets[statusid][]=P&sort[0][column]=period"
             "&sort[0][direction]=desc&length=5", "eia_planned_gen"),
            ("electricity/facility-fuel/data/"
             "?api_key=%s&frequency=monthly&data[0]=generation"
             "&sort[0][column]=period&sort[0][direction]=desc&length=3",
             "eia_facility_fuel"),
        ):
            hit("https://api.eia.gov/v2/" + (path % EIA_KEY), lbl)

        # route discovery — what child routes exist under electricity
        hit("https://api.eia.gov/v2/electricity?api_key=%s" % EIA_KEY,
            "eia_electricity_routes", show=900)

        # ── G3 LBNL Queued Up ────────────────────────────────────────────
        rep.section("G3 — LBNL interconnection queue dataset")
        hit("https://emp.lbl.gov/queues", "lbnl_queues_page", show=300)

        # ── G4 per-ISO endpoints ─────────────────────────────────────────
        rep.section("G4 — ISO queue endpoints (keyless from Lambda IP?)")
        hit("https://api.pjm.com/api/v1/", "pjm_dataminer_root", show=280)
        hit("https://www.ercot.com/misapp/servlets/IceDocListJsonWS?"
            "reportTypeId=15933", "ercot_mis_json", show=300)
        hit("https://www.caiso.com/PublishedDocuments/"
            "PublicQueueReport.xlsx", "caiso_queue_xlsx", show=120)
        hit("https://api.misoenergy.org/MISORTWD/lmpcontourmap"
            "/getLMPGeneration", "miso_api", show=200)

        # ── G5 gridstatus ────────────────────────────────────────────────
        rep.section("G5 — gridstatus.io")
        hit("https://api.gridstatus.io/v1/datasets", "gridstatus_datasets", show=400)

        # ── G6 EPA permits ───────────────────────────────────────────────
        rep.section("G6 — EPA air construction permits (Envirofacts / ECHO)")
        hit("https://data.epa.gov/efservice/tri_facility/state_abbr/TX/rows/0:2/JSON",
            "epa_envirofacts", show=350)
        hit("https://echodata.epa.gov/echo/air_rest_services.get_facilities?"
            "output=JSON&p_st=TX&p_act=Y&responseset=1", "echo_air", show=350)

        # ── verdict ──────────────────────────────────────────────────────
        rep.section("VERDICT — which sources are buildable")
        good = [k for k, v in findings.items() if v.get("status") == 200]
        bad = [k for k, v in findings.items() if v.get("status") != 200]
        rep.log("  REACHABLE: %s" % good)
        rep.log("  BLOCKED:   %s" % bad)
        Path("aws/ops/reports/3733.json").write_text(
            json.dumps({"verdict": "PASS", "reachable": good,
                        "blocked": bad, "findings": findings}, indent=2))
        rep.kv(reachable=len(good), blocked=len(bad),
               probe_only="true", deployed="nothing")

        if not good:
            rep.fail("no source reachable — canary #2 not buildable as designed")
            sys.exit(1)
        rep.ok("PROBE COMPLETE — build against the reachable set above")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3733.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
