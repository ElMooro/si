"""
# bumped 2026-04-27 22:12 — fix unit normalization (WALCL+TGA both in millions)

Step ___ — Create/update justhodl-liquidity-flow Lambda + EB rule.

TGA + RRP + WALCL daily delta tracker. Idempotent.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-liquidity-flow"
SOURCE_DIR = Path("aws/lambdas/justhodl-liquidity-flow/source")
EB_RULE_NAME = "justhodl-liquidity-flow-daily"
EB_SCHEDULE = "rate(1 day)"

ENV_VARS = {
    "S3_BUCKET": "justhodl-dashboard-live",
    "S3_KEY": "data/liquidity-flow.json",
    "FRED_KEY": "2f057499936072679d8843d7fce99989",
}


def main():
    with report("create_liquidity_flow") as r:
        r.heading("Create/update justhodl-liquidity-flow Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=120,
            memory=256,
            description="TGA + RRP + WALCL daily delta tracker. Net liquidity regime classification.",
            reserved_concurrency=1,
            create_function_url=False,
            smoke=True,
        )


if __name__ == "__main__":
    main()
