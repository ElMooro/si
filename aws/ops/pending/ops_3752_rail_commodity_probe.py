#!/usr/bin/env python3
"""ops 3752 — PROBE: AAR rail carloads BY COMMODITY (canary #9).

AUDIT (repo, 3752): justhodl-freight-pulse already carries FRED
RAILFRTCARLOADSD11 + RAILFRTINTERMODALD11 — but those are AGGREGATE indices.
Canary #9 is explicitly "carloads by STCC commodity": chemicals, motor
vehicles, lumber, metallic ores, grain, coal. The whole point is knowing
WHICH commodity is moving — an aggregate index cannot answer that, and the
commodity split is what leads an industrial cycle by weeks. Clean gap, but
only buildable if a free commodity-level series exists.

PROBE (no code committed until a source proves out)
  A  FRED — does it carry per-commodity rail series, or only the aggregate?
     Search the FRED series API for rail/carload series and list what exists.
  B  AAR public "Rail Traffic" weekly — railroads publish a weekly PDF/CSV
     with ~20 commodity lines. Test reachability + shape from a Lambda-like
     UA. AAR has historically been scrape-hostile; measure, don't assume.
  C  STB (Surface Transportation Board) — the federal regulator publishes
     the same waybill/carload data as open gov data. Often the DURABLE
     source when the trade association blocks.
  D  EIA coal carloads (rail coal deliveries) as a cross-check leg.

Decision rule (same as #1/#2): prefer the DURABLE government/official
source over a scrape-hostile trade association, and never ship a fabricated
commodity split. If only the aggregate is reachable, say so and DON'T build
a fake breakdown — record the gap and move to the next canary.
"""
import json
import ssl
import sys
import traceback
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl research; admin@justhodl.ai)"}
CTX = ssl.create_default_context()
FRED_KEY = "2f057499936072679d8843d7fce99989"


def get(url, timeout=25, headers=None):
    req = urllib.request.Request(url, headers=headers or UA)
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return r.status, r.read()


with report("3752_rail_commodity_probe") as rep:
    rep.heading("ops 3752 — AAR rail carloads BY COMMODITY (canary #9) probe")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3752.json").write_text(json.dumps({"verdict": "STARTED"}))
    found = {}

    try:
        # ── A FRED: what rail series actually exist? ──────────────────────
        rep.section("A — FRED series search for rail/carload commodity splits")
        for term in ("rail carloads", "rail freight carloads chemicals",
                     "carloads motor vehicles", "rail intermodal"):
            try:
                u = ("https://api.stlouisfed.org/fred/series/search?search_text="
                     + urllib.parse.quote(term)
                     + "&api_key=" + FRED_KEY + "&file_type=json&limit=12")
                st, body = get(u)
                j = json.loads(body)
                seriess = j.get("seriess") or []
                rep.ok("  '%s' -> %d series" % (term, len(seriess)))
                for s in seriess[:8]:
                    rep.log("    %-28s %s" % (s.get("id"), s.get("title", "")[:78]))
                found.setdefault("fred", []).extend(
                    [s.get("id") for s in seriess[:8]])
            except Exception as e:
                rep.warn("  FRED '%s': %s" % (term, str(e)[:110]))

        # ── B AAR weekly rail traffic ────────────────────────────────────
        rep.section("B — AAR public weekly rail traffic")
        aar_urls = [
            "https://www.aar.org/wp-content/uploads/2026/07/railtraffic.csv",
            "https://www.aar.org/data-center/rail-traffic-data/",
            "https://www.aar.org/news/rail-traffic-data/",
        ]
        for u in aar_urls:
            try:
                st, body = get(u, timeout=20)
                txt = body[:400].decode("utf-8", "replace")
                rep.ok("  %s -> HTTP %d len=%d" % (u[:62], st, len(body)))
                rep.log("    head: %s" % txt.replace("\n", " ")[:200])
                found.setdefault("aar", []).append({"url": u, "status": st,
                                                    "len": len(body)})
            except Exception as e:
                rep.warn("  %s -> %s" % (u[:62], str(e)[:110]))

        # ── C STB — the durable federal source ───────────────────────────
        rep.section("C — Surface Transportation Board (federal, durable)")
        stb_urls = [
            "https://www.stb.gov/reports-data/economic-data/",
            "https://www.stb.gov/reports-data/rail-service-data/",
            "https://prod.stb.gov/reports-data/rail-service-data/",
        ]
        for u in stb_urls:
            try:
                st, body = get(u, timeout=20)
                txt = body.decode("utf-8", "replace")
                # look for downloadable data links
                hits = []
                for ext in (".csv", ".xlsx", ".xls"):
                    idx = 0
                    while True:
                        i = txt.find(ext, idx)
                        if i < 0 or len(hits) >= 6:
                            break
                        j0 = max(txt.rfind("href=\"", 0, i), 0)
                        hits.append(txt[j0 + 6:i + len(ext)][:120])
                        idx = i + 1
                rep.ok("  %s -> HTTP %d len=%d" % (u[:60], st, len(body)))
                for h in hits[:6]:
                    rep.log("    data link: %s" % h)
                found.setdefault("stb", []).append({"url": u, "status": st,
                                                    "links": hits[:6]})
            except Exception as e:
                rep.warn("  %s -> %s" % (u[:60], str(e)[:110]))

        # ── D verify the aggregate we ALREADY have, for contrast ─────────
        rep.section("D — the aggregate series we already carry (contrast)")
        for sid in ("RAILFRTCARLOADSD11", "RAILFRTINTERMODALD11"):
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations?series_id="
                     + sid + "&api_key=" + FRED_KEY
                     + "&file_type=json&sort_order=desc&limit=3")
                st, body = get(u)
                obs = json.loads(body).get("observations") or []
                rep.ok("  %s latest=%s (%s)" % (sid,
                                                obs[0].get("value") if obs else "?",
                                                obs[0].get("date") if obs else "?"))
            except Exception as e:
                rep.warn("  %s: %s" % (sid, str(e)[:100]))

        rep.section("VERDICT")
        fred_ids = sorted(set(found.get("fred") or []))
        aar_ok = [a for a in (found.get("aar") or []) if a.get("status") == 200]
        stb_ok = [s for s in (found.get("stb") or []) if s.get("status") == 200]
        rep.log("  distinct FRED series seen: %d" % len(fred_ids))
        rep.log("  AAR reachable: %d · STB reachable: %d" % (len(aar_ok), len(stb_ok)))
        buildable = bool(aar_ok or stb_ok or len(fred_ids) > 4)
        rep.kv(fred_series=len(fred_ids), aar_ok=len(aar_ok),
               stb_ok=len(stb_ok), buildable=str(buildable))
        Path("aws/ops/reports/3752.json").write_text(
            json.dumps({"verdict": "PASS", "found": found,
                        "buildable": buildable}, indent=2, default=str))
        rep.ok("PROBE COMPLETE — decide source from the evidence above")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3752.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
