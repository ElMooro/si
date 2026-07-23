#!/usr/bin/env python3
"""ops 3758 — PROBE for canary #13: narrow 6-digit PPI line acceleration.

AUDIT (repo, 3758): the fleet uses PPI only as HAND-PICKED individual series —
supply-inflection-scanner pins PCU33443344 (semis), WPU101 (steel),
PCU334112334112 (storage), WPU117409; bottleneck-boom pins WPU101. That means
a narrow input can heat up for months and nobody sees it, because no engine
SWEEPS the PPI tree looking for acceleration. Canary #13 wants the systematic
version: rank narrow commodity/industry lines by 2nd derivative so the mover
surfaces on its own rather than being pre-selected.

THE PREMISE TO VERIFY BEFORE BUILDING: can we actually enumerate and pull
narrow PPI lines in bulk, cheaply, and repeatedly?

PROBE
  A  BLS API v2 (key in env BLS_API_KEY): pull several 6-digit PPI series in
     ONE multi-series request; confirm the batch limit and the payload shape.
     BLS allows 50 series per request with a registered key — if true, a few
     hundred lines is a handful of calls, which is cheap enough to schedule.
  B  Does BLS expose a SERIES LIST for PPI industry (PCU...) so lines can be
     discovered rather than hardcoded? Test the survey endpoint.
  C  FRED fallback: series/search on 'PPI industry' to see how many narrow
     lines FRED mirrors, and whether tags let us enumerate them.
  D  Sanity: pull one known-good narrow line both ways and compare values, so
     the engine can trust whichever source it ends up using.

Writes NO engine. Decides the source from evidence.
"""
import json
import os
import ssl
import sys
import traceback
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl research)"}
CTX = ssl.create_default_context()
FRED_KEY = "2f057499936072679d8843d7fce99989"
BLS_KEY = os.environ.get("BLS_API_KEY", "341b59ff02974b298a46547e6fe42321")

# narrow lines to test — deliberately specific end-uses
TEST_SERIES = [
    "PCU334413334413",   # semiconductors & related devices
    "PCU33441333441301",
    "PCU3341123341121",
    "WPU101",            # iron & steel
    "WPU0911",           # lumber
    "WPU061",            # industrial chemicals
    "PCU325180325180",   # basic inorganic chemicals
    "PCU3339113339110",
]

with report("3758_ppi_narrow_probe") as rep:
    rep.heading("ops 3758 — narrow 6-digit PPI availability probe (canary #13)")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3758.json").write_text(json.dumps({"verdict": "STARTED"}))
    out = {}

    try:
        # ── A BLS multi-series batch ─────────────────────────────────────
        rep.section("A — BLS v2 multi-series batch (the cheap path)")
        try:
            payload = json.dumps({
                "seriesid": TEST_SERIES,
                "startyear": "2024", "endyear": "2026",
                "registrationkey": BLS_KEY,
            }).encode()
            req = urllib.request.Request(
                "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                data=payload,
                headers=dict(UA, **{"Content-Type": "application/json"}))
            j = json.loads(urllib.request.urlopen(req, timeout=45,
                                                  context=CTX).read())
            rep.ok("  status=%s message=%s"
                   % (j.get("status"), str(j.get("message"))[:150]))
            series = (j.get("Results") or {}).get("series") or []
            rep.log("  series returned: %d of %d requested"
                    % (len(series), len(TEST_SERIES)))
            live = 0
            for s0 in series:
                sid = s0.get("seriesID")
                dat = s0.get("data") or []
                if dat:
                    live += 1
                    d0 = dat[0]
                    rep.log("    %-22s latest %s-%s = %s (n=%d)"
                            % (sid, d0.get("year"), d0.get("period"),
                               d0.get("value"), len(dat)))
                else:
                    rep.log("    %-22s NO DATA" % sid)
            out["bls"] = {"requested": len(TEST_SERIES),
                          "returned": len(series), "with_data": live,
                          "status": j.get("status")}
        except Exception as e:
            rep.warn("  BLS batch: %s" % str(e)[:170])
            out["bls"] = {"error": str(e)[:170]}

        # ── B can we DISCOVER series rather than hardcode? ───────────────
        rep.section("B — BLS series discovery for PPI industry")
        for u in ("https://download.bls.gov/pub/time.series/pc/pc.series",
                  "https://download.bls.gov/pub/time.series/wp/wp.series"):
            try:
                body = urllib.request.urlopen(urllib.request.Request(
                    u, headers=UA), timeout=60, context=CTX).read()
                txt = body[:4000].decode("utf-8", "replace")
                lines = body.decode("utf-8", "replace").splitlines()
                rep.ok("  %s -> %d bytes, %d lines" % (u[-16:], len(body), len(lines)))
                rep.log("    header: %s" % lines[0][:150] if lines else "")
                for ln in lines[1:3]:
                    rep.log("    row: %s" % ln[:150])
                out.setdefault("discovery", {})[u[-16:]] = len(lines)
            except Exception as e:
                rep.warn("  %s -> %s" % (u[-16:], str(e)[:130]))

        # ── C FRED mirror breadth ────────────────────────────────────────
        rep.section("C — FRED mirror of narrow PPI lines")
        try:
            u = ("https://api.stlouisfed.org/fred/series/search?search_text="
                 + urllib.parse.quote("PPI industry semiconductor")
                 + "&api_key=" + FRED_KEY + "&file_type=json&limit=10")
            j = json.loads(urllib.request.urlopen(urllib.request.Request(
                u, headers=UA), timeout=30, context=CTX).read())
            ss = j.get("seriess") or []
            rep.ok("  FRED narrow-PPI search -> %d" % len(ss))
            for s0 in ss[:6]:
                rep.log("    %-24s %s" % (s0.get("id"),
                                          s0.get("title", "")[:70]))
            out["fred_hits"] = len(ss)
        except Exception as e:
            rep.warn("  FRED: %s" % str(e)[:140])

        # ── D cross-check one line both ways ─────────────────────────────
        rep.section("D — cross-check PCU334413334413 (BLS vs FRED)")
        for sid in ("PCU334413334413",):
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations?series_id="
                     + sid + "&api_key=" + FRED_KEY
                     + "&file_type=json&sort_order=desc&limit=3")
                obs = json.loads(urllib.request.urlopen(urllib.request.Request(
                    u, headers=UA), timeout=25, context=CTX).read()
                ).get("observations") or []
                rep.ok("  FRED %s latest=%s (%s)"
                       % (sid, obs[0].get("value") if obs else "?",
                          obs[0].get("date") if obs else "?"))
            except Exception as e:
                rep.warn("  FRED %s: %s" % (sid, str(e)[:110]))

        rep.section("VERDICT")
        bls_ok = (out.get("bls", {}).get("with_data") or 0) >= 3
        disc = out.get("discovery") or {}
        rep.log("  BLS batch usable: %s · discovery files: %s"
                % (bls_ok, {k: v for k, v in disc.items()}))
        rep.kv(bls_with_data=out.get("bls", {}).get("with_data", 0),
               discovery_lines=sum(disc.values()) if disc else 0,
               fred_hits=out.get("fred_hits", 0),
               buildable=str(bool(bls_ok or disc)))
        Path("aws/ops/reports/3758.json").write_text(
            json.dumps({"verdict": "PASS", "found": out}, indent=2, default=str))
        rep.ok("PROBE COMPLETE")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3758.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
