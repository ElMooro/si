"""
ops_3903 — re-verify ops 3902's fix with a corrected gate. The fix itself
already worked (raw invoke log showed n_updated=1946, total_evaluated_30d
0->1557) — ops 3902's FAILED status was my own bug: the Lambda response is
{"statusCode": 200, "body": "<JSON-encoded string>"}, and I read
payload.get("n_updated") on the OUTER dict instead of parsing the nested
`body` string first. This re-reads the already-updated trade-journal.json
directly from S3 (no re-invoke needed - the fix already ran and wrote real
data) and gates properly this time.
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
    with report("3903_trade_evaluator_verify_clean") as rep:
        rep.heading("ops 3903 — clean re-verification (3902's fix worked, 3902's gate parsing didn't)")
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-journal.json")["Body"].read())
        except Exception as e:
            rep.fail(f"  trade-journal.json unreadable: {str(e)[:200]}")
            sys.exit(1)

        summary = doc.get("summary") or {}
        strategies = doc.get("strategies") or []
        rep.kv(total_evaluated_30d=summary.get("total_evaluated_30d"),
               overall_win_rate_30d_pct=summary.get("overall_win_rate_30d_pct"),
               n_strategies_tracked=summary.get("n_strategies_tracked"))
        for s in strategies:
            rep.log(f"  {s.get('strategy'):<20} evaluated_30d={s.get('evaluated_30d'):<6} "
                    f"win_rate={s.get('win_rate_30d_pct')}% avg_return={s.get('avg_return_30d_pct')}%")

        checks = [
            ("total_evaluated_30d is a real, large positive number",
             (summary.get("total_evaluated_30d") or 0) >= 1000),
            ("at least 3 strategies have real evaluated data",
             sum(1 for s in strategies if (s.get("evaluated_30d") or 0) > 0) >= 3),
            ("overall win rate is a real, plausible number (not null, not 0 or 100)",
             summary.get("overall_win_rate_30d_pct") is not None and
             0 < summary["overall_win_rate_30d_pct"] < 100),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")
        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {summary.get('total_evaluated_30d')} real evaluated calls, "
               f"{summary.get('overall_win_rate_30d_pct')}% overall win rate, first time ever populated")


if __name__ == "__main__":
    main()
