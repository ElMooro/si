"""
Step ___ — Create/update justhodl-backtest-harness Lambda + EB rule + DDB.

Idempotent. The Lambda creates its own DDB table on first run if missing.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-backtest-harness"
SOURCE_DIR = Path("aws/lambdas/justhodl-backtest-harness/source")
EB_RULE_NAME = "justhodl-backtest-harness-daily"
EB_SCHEDULE = "rate(1 day)"

ENV_VARS = {
    "S3_BUCKET": "justhodl-dashboard-live",
    "S3_OUTPUT_KEY": "data/backtest-summary.json",
    "DDB_TABLE": "justhodl-backtest",
    "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
}


def main():
    with report("create_backtest_harness") as r:
        r.heading("Create/update justhodl-backtest-harness Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=600,           # may need time on large scans
            memory=512,
            description="Daily snapshot of all signal types + forward-return tracker. DDB table justhodl-backtest.",
            reserved_concurrency=1,
            create_function_url=False,
            smoke=True,
        )


if __name__ == "__main__":
    main()
