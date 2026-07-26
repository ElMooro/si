"""
ops_3907 — PROBE: signal-backtest's own deterministic ai_analysis, and the
FULL by_verdict breakdown (not just the 3-entry partial view from ops 3906's
gate log). Partial data already showed HIGH RISK outperforming STRONG
OPPORTUNITY on both win rate (71.3% vs 59.0%) and avg return (4.34% vs
2.23%) - if that holds across all 6 verdicts, the engine's own
_deterministic_analysis() is specifically built to flag this as "conviction
labels are inverted." Reading the complete picture before asserting
anything. Writes no code.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3907_conviction_label_check") as rep:
        rep.heading("ops 3907 — full by_verdict breakdown + the engine's own deterministic analysis")
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/signal-backtest.json")["Body"].read())
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:200]}")
            sys.exit(1)

        rep.section("1. FULL by_verdict — every conviction label, ranked by win rate")
        bv = doc.get("by_verdict") or {}
        ranked = sorted(bv.items(), key=lambda kv: -(kv[1].get("win_rate") or 0))
        for k, v in ranked:
            rep.log(f"  {k:<20} n={v.get('n'):<7} win_rate={v.get('win_rate')}% "
                    f"avg={v.get('avg')}% median={v.get('median')}% "
                    f"hit_5pct={v.get('hit_5pct')}% best={v.get('best')}% worst={v.get('worst')}%")

        rep.section("2. the engine's own deterministic analysis (no LLM needed, always-on)")
        ai = doc.get("ai_analysis") or {}
        rep.log(f"  headline: {ai.get('headline')}")
        rep.log(f"  full ai_analysis: {json.dumps(ai, default=str)[:1500]}")

        rep.section("3. by_compounder_bucket — does the QUALITY axis fare better than the verdict axis")
        bc = doc.get("by_compounder_bucket") or {}
        for k, v in sorted(bc.items(), key=lambda kv: -(kv[1].get("win_rate") or 0)):
            rep.log(f"  {k:<20} n={v.get('n'):<7} win_rate={v.get('win_rate')}% avg={v.get('avg')}%")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
