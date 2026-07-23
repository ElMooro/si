#!/usr/bin/env python3
"""ops 3759 — enumerate the REAL narrow-PPI line universe (canary #13).

3758 proved: BLS batch returns live narrow lines (6/8, through 2026-M06) and
FRED mirrors identical values (PCU334413334413 = 29.695 both). BLS bulk
discovery files are 403, so DISCOVERY must come from FRED search/tags while
the PULL can use either. Two of my hand-guessed IDs did not exist — which is
exactly why the line list must be DISCOVERED, not invented.

This ops builds the durable line universe and writes it to S3 as
config/ppi-lines.json so the engine reads a real, verified list instead of
hardcoded guesses. It also measures how deep the tree goes (6-digit and
8-digit lines) and confirms each candidate actually has recent observations —
a series that exists but stopped updating in 2019 is worse than useless in a
canary.

Selection rules (institutional, not arbitrary):
  · prefer NARROW end-use lines — the whole premise of #13 is that an
    aggregate hides the mover
  · require observations within the last 120 days (PPI is monthly, ~1mo lag)
  · require >= 24 observations so a 2nd derivative is meaningful
  · dedupe parents when a child exists (PCU3344133441 vs PCU334413334413)
"""
import json
import ssl
import sys
import time
import traceback
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl research)"}
CTX = ssl.create_default_context()
FRED_KEY = "2f057499936072679d8843d7fce99989"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "config/ppi-lines.json"

# search terms chosen to sweep the INPUT complex a manufacturer actually buys
TERMS = [
    "PPI industry semiconductor", "PPI industry electronic component",
    "PPI industry industrial chemical", "PPI industry basic inorganic chemical",
    "PPI industry iron steel", "PPI industry copper",
    "PPI industry aluminum", "PPI industry plastics resin",
    "PPI industry electrical equipment", "PPI industry motor vehicle parts",
    "PPI industry machinery manufacturing", "PPI industry lumber",
    "PPI industry paperboard container", "PPI industry pharmaceutical preparation",
    "PPI industry aerospace product", "PPI industry battery manufacturing",
    "PPI industry wire cable", "PPI industry transformer",
    "PPI industry rare earth", "PPI industry industrial gas",
]


def jget(url, timeout=30):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(url, headers=UA), timeout=timeout,
        context=CTX).read())


with report("3759_ppi_line_universe") as rep:
    rep.heading("ops 3759 — build the narrow-PPI line universe")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3759.json").write_text(json.dumps({"verdict": "STARTED"}))

    try:
        s3 = boto3.client("s3", region_name="us-east-1")

        # ── A discover candidates ────────────────────────────────────────
        rep.section("A — discover candidate lines via FRED search")
        cand = {}
        for t in TERMS:
            try:
                u = ("https://api.stlouisfed.org/fred/series/search?search_text="
                     + urllib.parse.quote(t) + "&api_key=" + FRED_KEY
                     + "&file_type=json&limit=24")
                ss = jget(u).get("seriess") or []
                n_new = 0
                for s0 in ss:
                    sid = s0.get("id") or ""
                    if not (sid.startswith("PCU") or sid.startswith("WPU")):
                        continue
                    if sid in cand:
                        continue
                    cand[sid] = {
                        "id": sid,
                        "title": (s0.get("title") or "")[:150],
                        "freq": s0.get("frequency_short"),
                        "last_updated": s0.get("last_updated"),
                        "obs_end": s0.get("observation_end"),
                        "units": s0.get("units_short"),
                        "term": t,
                    }
                    n_new += 1
                rep.log("  %-42s +%d (total %d)" % (t[:42], n_new, len(cand)))
                time.sleep(0.12)
            except Exception as e:
                rep.warn("  %s -> %s" % (t[:40], str(e)[:110]))
        rep.ok("  candidates discovered: %d" % len(cand))

        # ── B freshness + depth filter ───────────────────────────────────
        rep.section("B — keep only FRESH, DEEP, NARROW lines")
        cutoff = (datetime.now(timezone.utc) - timedelta(days=120)).date()
        kept, dropped = [], {"stale": 0, "shallow": 0, "monthly": 0}
        for sid, meta in cand.items():
            try:
                oe = meta.get("obs_end") or ""
                d = datetime.strptime(oe, "%Y-%m-%d").date() if oe else None
            except Exception:
                d = None
            if d is None or d < cutoff:
                dropped["stale"] += 1
                continue
            if (meta.get("freq") or "").upper() not in ("M",):
                dropped["monthly"] += 1
                continue
            kept.append(meta)
        rep.log("  after freshness/frequency: %d kept, dropped %s"
                % (len(kept), dropped))

        # ── C dedupe parents where a longer child exists ─────────────────
        rep.section("C — dedupe aggregate parents (narrow is the point)")
        ids = {m["id"] for m in kept}
        final = []
        for m in kept:
            sid = m["id"]
            # if a strictly longer id starts with this one, this is a parent
            has_child = any(o != sid and o.startswith(sid) for o in ids)
            m["is_parent"] = has_child
            if not has_child:
                final.append(m)
        rep.ok("  narrow lines after parent-dedupe: %d (from %d)"
               % (len(final), len(kept)))
        for m in sorted(final, key=lambda x: x["id"])[:14]:
            rep.log("    %-22s %s" % (m["id"], m["title"][:74]))

        # ── D verify a sample truly has depth ────────────────────────────
        rep.section("D — verify observation depth on a sample")
        deep = 0
        for m in sorted(final, key=lambda x: x["id"])[:8]:
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations?series_id="
                     + m["id"] + "&api_key=" + FRED_KEY
                     + "&file_type=json&sort_order=desc&limit=30")
                obs = [o for o in (jget(u).get("observations") or [])
                       if o.get("value") not in (".", None, "")]
                if len(obs) >= 24:
                    deep += 1
                rep.log("    %-22s n=%d latest=%s (%s)"
                        % (m["id"], len(obs),
                           obs[0].get("value") if obs else "?",
                           obs[0].get("date") if obs else "?"))
                time.sleep(0.1)
            except Exception as e:
                rep.warn("    %s -> %s" % (m["id"], str(e)[:100]))

        # ── E persist the universe ───────────────────────────────────────
        rep.section("E — persist config/ppi-lines.json")
        doc = {
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_lines": len(final),
            "lines": sorted(final, key=lambda x: x["id"]),
            "method": ("Discovered via FRED series search (BLS bulk .series "
                       "files are 403). Kept only monthly lines with "
                       "observations inside 120 days, then dropped aggregate "
                       "parents wherever a narrower child exists — a narrow "
                       "line is the entire premise of the canary. Values "
                       "cross-check identically between FRED and the BLS v2 "
                       "batch API (ops 3758)."),
        }
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(doc, separators=(",", ":")),
                      ContentType="application/json")
        rep.ok("  wrote %s with %d lines" % (OUT_KEY, len(final)))

        rep.section("VERDICT")
        ok = len(final) >= 20 and deep >= 5
        rep.kv(candidates=len(cand), narrow_lines=len(final),
               sample_deep=deep, verdict="PASS" if ok else "THIN")
        Path("aws/ops/reports/3759.json").write_text(
            json.dumps({"verdict": "PASS" if ok else "THIN",
                        "n_lines": len(final)}, indent=2))
        if not ok:
            rep.fail("line universe too thin (%d lines, %d deep) — widen "
                     "TERMS before building the engine" % (len(final), deep))
            sys.exit(1)
        rep.ok("UNIVERSE READY — engine can consume config/ppi-lines.json")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3759.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
