"""
ops_3864 — PROBE: close the last 8 unresolved ranker tickers, and establish
whether risk_regime 0/25 is a defect or correct behaviour. WRITES NO CODE.

ops 3863 measured the real state (memory's "1/25" was stale — rotation fires on
16/25 since ops 3832):
  · 17/25 resolvable; unresolved = BE UMC DFTX OVV HCC ALHC IPGP CENX
  · capital-flow-radar and accumulation-radar yield ZERO pairs — dead weight in
    the harvest, walked on every run
  · risk_regime_mult non-neutral on 0/25 while liquidity/nowcast/rotation hit 16

Two questions, both answered with data before any code is written:

(A) WHICH DONOR CLOSES THE GAP — candidate feeds are scored on coverage of the
    8 unresolved names specifically, not on raw size. A 11k-row feed that misses
    all 8 is worth less than a 500-row feed that catches them.

(B) IS RORO ACTUALLY BROKEN — _roro_overlay returns a flat 1.0 whenever
    -12 < risk_regime_score <= 12, and it only tilts sectors inside its
    HIGH_BETA/DEFENSIVE sets. So 0/25 is CORRECT if the regime is neutral today,
    or if the resolved sectors fall outside both sets. Calling that a bug and
    "fixing" it would manufacture a tilt the regime does not justify — the same
    false-corroboration error as the global-recession v1.2 clamp. Measure first.
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
s3 = boto3.client("s3", region_name="us-east-1")

UNRESOLVED = ["BE", "UMC", "DFTX", "OVV", "HCC", "ALHC", "IPGP", "CENX"]
CANDIDATES = ["data/universe.json", "data/finviz-universe.json", "data/finviz-signals.json",
              "data/stock-xray.json", "data/finviz-groups.json", "data/short-book.json",
              "data/best-setups.json", "data/fundamental-census-matrix.json"]
CURRENT = ["screener/data.json", "data/capital-flow-radar.json", "data/deep-value.json",
           "data/accumulation-radar.json", "data/asymmetric-scorer.json"]
HIGH_BETA = {"Technology", "Consumer Cyclical", "Energy", "Financial Services"}
DEFENSIVE = {"Utilities", "Consumer Defensive", "Healthcare", "Real Estate"}


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def harvest(doc):
    m = {}

    def walk(o):
        if isinstance(o, dict):
            t = o.get("ticker") or o.get("symbol") or o.get("t")
            sec = o.get("sector") or o.get("sectorName")
            if isinstance(t, str) and isinstance(sec, str) and t and sec:
                m.setdefault(t.upper(), sec)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc)
    return m


def main():
    with report("3864_sector_donor_probe") as rep:
        rep.heading("ops 3864 — PROBE: donor to close 8 unresolved + is RORO 0/25 correct")

        mr, _ = get("data/master-ranker.json")
        ranked = [str(r["ticker"]).upper() for r in (mr.get("top_tickers") or []) if r.get("ticker")]
        rep.log(f"  {len(ranked)} ranked tickers · {len(UNRESOLVED)} unresolved from ops 3863")

        rep.section("1. current donors — keep or drop")
        cur_map = {}
        for key in CURRENT:
            try:
                d, lm = get(key)
                m = harvest(d)
            except Exception as e:
                rep.fail(f"  {key:<36} UNREADABLE {str(e)[:60]}")
                continue
            hit = sum(1 for t in ranked if t in m)
            verdict = "DROP — contributes nothing" if not m else f"keep ({hit}/{len(ranked)})"
            (rep.fail if not m else rep.ok)(f"  {key:<36} {len(m):>6} pairs · {verdict}")
            cur_map.update({k: v for k, v in m.items() if k not in cur_map})

        rep.section("2. candidate donors — scored on the 8 that actually matter")
        best = []
        for key in CANDIDATES:
            try:
                d, lm = get(key)
            except Exception as e:
                rep.log(f"  {key:<36} unreadable ({str(e)[:50]})")
                continue
            m = harvest(d)
            closes = [t for t in UNRESOLVED if t in m]
            adds = [t for t in ranked if t not in cur_map and t in m]
            age = (datetime.now(timezone.utc) - lm).total_seconds() / 3600
            line = (f"  {key:<36} {len(m):>6} pairs · closes {len(closes)}/8 "
                    f"{closes} · {age:.0f}h old")
            (rep.ok if closes else rep.log)(line)
            if closes:
                best.append((len(closes), len(m), key, closes, adds))

        rep.section("3. recommended wiring")
        if not best:
            rep.fail("  NO candidate resolves any of the 8 — they may be genuinely "
                     "absent from every sector-bearing feed (ADRs/OTC/new listings). "
                     "Honest outcome: leave them unresolved rather than guess a sector.")
        else:
            best.sort(reverse=True)
            for n_close, size, key, closes, adds in best[:3]:
                rep.ok(f"  {key} — closes {n_close}/8 {closes}")
            top = best[0]
            rep.kv(recommended_donor=top[2], closes=top[0], donor_pairs=top[1])

        rep.section("4. is risk_regime 0/25 correct, or broken")
        try:
            rr, rlm = get("data/risk-regime.json")
        except Exception as e:
            rep.fail(f"  risk-regime.json unreadable: {str(e)[:120]}")
            rr = {}
        score = rr.get("risk_regime_score")
        regime = rr.get("risk_regime")
        age = (datetime.now(timezone.utc) - rlm).total_seconds() / 3600 if rr else None
        rep.kv(risk_regime_score=str(score), risk_regime=str(regime),
               feed_age_h=None if age is None else round(age, 1))
        if score is None:
            rep.fail("  score MISSING — overlay silently no-ops on a null. REAL BUG.")
        elif -12 < float(score) <= 12:
            rep.ok(f"  score {score} sits in the engine's neutral band (-12, 12] — "
                   f"0/25 is CORRECT, not a defect. Do not manufacture a tilt.")
        else:
            rep.fail(f"  score {score} is OUTSIDE the neutral band but nothing tilted "
                     f"— that IS a defect; check sector-set membership below.")

        rep.section("5. do the resolved sectors even fall in RORO's tilt sets")
        secs = {}
        for t in ranked:
            s = cur_map.get(t)
            if s:
                secs[s] = secs.get(s, 0) + 1
        for s, n in sorted(secs.items(), key=lambda x: -x[1]):
            tag = "HIGH_BETA" if s in HIGH_BETA else ("DEFENSIVE" if s in DEFENSIVE else "NOT IN EITHER SET")
            (rep.log if tag != "NOT IN EITHER SET" else rep.fail)(f"  {s:<26} {n:>2} tickers · {tag}")
        outside = sum(n for s, n in secs.items() if s not in HIGH_BETA and s not in DEFENSIVE)
        rep.kv(sectors_seen=len(secs), tickers_outside_roro_sets=outside)
        if outside:
            rep.log("  NOTE: RORO uses FinViz vocabulary (Consumer Cyclical / Financial "
                    "Services). Any GICS-named sector lands outside both sets and is "
                    "silently skipped — a vocabulary mismatch, not a data gap.")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
