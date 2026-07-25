"""
ops_3867 — is liquidity_regime_mult 0/25 a regression from ops 3866, or a feed
that went stale underneath it? WRITES NO CODE.

ops 3866 wired the sector donor and rotation went 16/25 -> 25/25. In the same
run liquidity went 16 -> 0. Sector resolution IMPROVED, so a sector-driven
overlay cannot have lost coverage for sector reasons — something else changed.

_liq_overlay reads data/liquidity-inflection.json via fetch_json(max_age_h=48)
and returns a flat 1.0 when _liq_score is not numeric. So 0/25 has exactly three
possible causes, and they demand opposite responses:
   (a) the feed aged past 48h  -> the overlay silently switched OFF. REAL BUG,
       and the fix belongs to the producing engine's schedule, not the ranker.
   (b) the feed is fresh but the regime is NEUTRAL with a flat heading -> m
       collapses to 1.0 and 0/25 is CORRECT. Forcing a tilt would manufacture a
       signal, the same error probe 3865 stopped on RORO.
   (c) the feed is fresh and directional but nothing tilted -> genuine defect in
       the overlay.
The ranker publishes stale_feeds_excluded / missing_feeds, so it can be asked
directly rather than inferred.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
FEED = "data/liquidity-inflection.json"
s3 = boto3.client("s3", region_name="us-east-1")


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3868_liq_overlay_truth") as rep:
        rep.heading("ops 3868 — liquidity overlay 0/25: stale feed, neutral regime, or defect")

        rep.section("1. ask the ranker what it excluded")
        try:
            mr, mlm = get("data/master-ranker.json")
        except Exception as e:
            rep.fail(f"  master-ranker.json unreadable: {str(e)[:150]}")
            sys.exit(1)
        stale = mr.get("stale_feeds_excluded") or []
        missing = mr.get("missing_feeds") or []
        hits = [x for x in (list(stale) + list(missing)) if "liquidity" in json.dumps(x).lower()]
        rep.kv(ranker_age_h=round((datetime.now(timezone.utc) - mlm).total_seconds() / 3600, 2),
               n_stale_excluded=len(stale), n_missing=len(missing))
        rep.log(f"  stale_feeds_excluded: {json.dumps(stale)[:400]}")
        rep.log(f"  missing_feeds: {json.dumps(missing)[:300]}")
        if hits:
            rep.fail(f"  liquidity feed IS excluded by the ranker: {json.dumps(hits)[:300]}")
        else:
            rep.ok("  liquidity feed is NOT in the ranker's exclusion lists")

        rep.section("2. the feed itself")
        try:
            liq, llm = get(FEED)
        except Exception as e:
            rep.fail(f"  {FEED} unreadable: {str(e)[:150]} — cause (a), overlay is dark")
            sys.exit(1)
        age_h = (datetime.now(timezone.utc) - llm).total_seconds() / 3600
        # ops 3867 guessed these key names and got None; the ENGINE's read is the
        # contract (master-ranker lines 1001-1003), so mirror it exactly:
        #   _liq_comp   = _liq["composite"]
        #   _liq_regime = _liq_comp["regime"]
        #   _liq_score  = _liq_comp["liquidity_score"]
        comp = liq.get("composite") or {}
        regime = comp.get("regime")
        score = comp.get("liquidity_score")
        rep.log(f"  composite keys: {sorted(comp.keys())[:18]}")
        heading = (liq.get("trajectory") or {}).get("heading")
        ds = (liq.get("dollar_shortage") or {}).get("status")
        rep.kv(feed_age_h=round(age_h, 1), gate_max_age_h=48, regime=str(regime),
               score=str(score), heading=str(heading), dollar_shortage=str(ds))
        rep.log(f"  top-level keys: {sorted(liq.keys())[:16]}")

        rep.section("3. verdict")
        if age_h > 48:
            rep.fail(f"  CAUSE (a): feed is {age_h:.1f}h old vs a 48h gate — the overlay "
                     f"switched off silently. Fix belongs to the producer's schedule "
                     f"(justhodl-liquidity-inflection), NOT to the ranker. ops 3866's "
                     f"donor rewire is exonerated: rotation went 16->25 in the same run.")
            sys.exit(1)
        if not isinstance(score, (int, float)):
            rep.fail(f"  CAUSE (a'): feed is fresh ({age_h:.1f}h) but the score field the "
                     f"overlay reads is {score!r} — a key-contract drift in the producer. "
                     f"Grep the producer before changing the consumer.")
            sys.exit(1)
        flat = (regime not in ("EXPANDING", "CONTRACTING")
                and heading not in ("TIGHTENING AHEAD", "EASING AHEAD")
                and ds != "SCRAMBLE")
        if flat:
            rep.ok(f"  CAUSE (b): feed fresh ({age_h:.1f}h), regime={regime}, heading={heading}, "
                   f"dollar_shortage={ds} — every multiplier collapses to 1.0, so 0/25 is "
                   f"CORRECT. No change warranted; manufacturing a tilt here would be the "
                   f"same false-signal error as forcing RORO at score 2.5.")
            return
        rep.fail(f"  CAUSE (c): feed fresh and directional (regime={regime}, heading={heading}, "
                 f"ds={ds}) but nothing tilted — genuine overlay defect, worth a fix ops.")
        sys.exit(1)


if __name__ == "__main__":
    main()
