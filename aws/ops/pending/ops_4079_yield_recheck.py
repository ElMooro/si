"""ops_4079 — has the agency yield recovered, or is the payoff tier dry?

ops 4078 measured a ~6% attribution hit rate on the first 117 tier-1
symbols (7 matched) against ~50% when the walk was chewing venues, and
found the only "agency" hits were TVC/CBOE — which the rollup correctly
classifies as MARKET-VENUES.  So the real question is narrow:

    do ECONOMICS: and FRED: symbols come back with a publisher at all?

This op answers it from evidence rather than inference: it counts what
fraction of the walk is now tier-1, samples the ECONOMICS/FRED rows that
DID resolve (if any) to see what shape their attribution takes, and
compares the yield to the venue baseline.  If ECONOMICS is structurally
dry, the extraction route has to change and no amount of walking fixes
it — that is worth knowing an hour before the sweep completes, not after.
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

TIER1 = {"ECONOMICS", "FRED", "TVC", "COT", "COT3", "CBOE",
         "QUANDL", "USCF", "USI", "EIA", "BLS", "BEA"}


def gj(k, d=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return d


def main():
    with report("4079_yield_recheck") as rep:
        rep.heading("ops 4079 — is the ECONOMICS tier actually yielding?")

        sr = gj("data/tv-sources.json", {}) or {}
        diag = sr.get("last_harvest_diag") or {}
        srcs = sr.get("sources") or {}
        gen = sr.get("generated_at")

        rep.section("A. walk progress")
        try:
            age = (datetime.now(timezone.utc)
                   - datetime.fromisoformat(str(gen))).total_seconds() / 60
        except Exception:
            age = None
        done = diag.get("done") or 0
        total = diag.get("total") or 0
        matched = diag.get("matched") or 0
        rep.log(f"  sync age     : {age:.1f} min" if age else "  sync age: ?")
        rep.log(f"  walked       : {done}/{total} "
                f"({done / total * 100:.1f}%)" if total else f"  walked: {done}")
        rep.log(f"  tier1_done   : {diag.get('tier1_done')}")
        rep.log(f"  rate         : {diag.get('rate_per_min')}/min  "
                f"elapsed {diag.get('elapsed_s')}s")
        rep.log(f"  matched      : {matched}")
        rep.log(f"  sc {diag.get('sc_ok')}/{diag.get('sc_err')}  "
                f"sc2 {diag.get('sc2_ok')}/{diag.get('sc2_err')}  "
                f"ss {diag.get('ss_ok')}/{diag.get('ss_err')}")
        yield_pct = (matched / done * 100) if done else 0
        rep.log(f"  → live attribution yield: {yield_pct:.1f}%")
        rep.kv(walked=done, total=total, matched=matched,
               yield_pct=round(yield_pct, 1), sourced=len(srcs),
               rate=diag.get("rate_per_min"))

        rep.section("B. THE question — did ECONOMICS/FRED resolve?")
        econ = {k: v for k, v in srcs.items()
                if k.upper().startswith(("ECONOMICS:", "FRED:"))}
        rep.log(f"  ECONOMICS/FRED rows with a source: {len(econ)}")
        if econ:
            for k, v in list(econ.items())[:15]:
                src = v.get("source") if isinstance(v, dict) else v
                rep.log(f"    {k:28} → {src}")
            rep.log("  ✓ the payoff tier DOES resolve — extraction route is "
                    "fine, it just needs walking time")
        else:
            rep.log("  ✗ ZERO. Every ECONOMICS/FRED symbol walked so far came "
                    "back without a publisher. The scanner/symbol-page routes "
                    "do not carry attribution for TV's macro namespace, so "
                    "more walking will NOT produce agency rows.")

        rep.section("C. yield by prefix — where attribution actually lives")
        pref = Counter(s.split(":")[0].upper() for s in srcs if ":" in s)
        for p, n in pref.most_common(12):
            tag = "  ← tier1" if p in TIER1 else ""
            rep.log(f"  {n:5d}  {p}{tag}")
        t1_sourced = sum(n for p, n in pref.items() if p in TIER1)
        rep.log(f"  tier1 sourced {t1_sourced} of {diag.get('tier1_done')} "
                f"tier1 walked")
        rep.kv(tier1_sourced=t1_sourced, econ_sourced=len(econ))

        rep.section("D. refresh the rollup")
        try:
            r = lam.invoke(FunctionName="justhodl-source-map",
                           InvocationType="RequestResponse",
                           Payload=b'{"source":"ops4079"}')
            rep.log(f"  {r['Payload'].read().decode()[:220]}")
        except Exception as e:
            rep.log(f"  invoke failed: {str(e)[:90]}")
        sm = gj("data/source-map.json", {}) or {}
        rep.log(f"  agency_rows={sm.get('agency_rows')} "
                f"venue_rows={sm.get('venue_rows')} "
                f"economics_symbols={sm.get('economics_symbols')}")

        rep.section("VERDICT")
        if econ:
            rep.log("✅ payoff tier resolving — let the sweep finish.")
        elif (diag.get("tier1_done") or 0) >= 300:
            rep.log("⛔ STRUCTURALLY DRY: 300+ tier-1 symbols walked, zero "
                    "ECONOMICS/FRED attribution. The route is the problem, "
                    "not the ordering. Needs a different extraction path "
                    "before more walking is worth anything.")
        else:
            rep.log("◐ too early to call — fewer than 300 tier-1 walked.")


if __name__ == "__main__":
    main()
