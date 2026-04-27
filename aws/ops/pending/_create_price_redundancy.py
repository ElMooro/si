"""
# bumped 2026-04-27 — fix smoke-test failures

Step ___ — Create/update justhodl-price-redundancy Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-price-redundancy"
SOURCE_DIR = Path("aws/lambdas/justhodl-price-redundancy/source")
EB_RULE_NAME = "justhodl-price-redundancy-15min"
EB_SCHEDULE = "rate(15 minutes)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_price_redundancy") as r:
        r.heading("Create/update justhodl-price-redundancy Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=300,
            memory=768,
            description='Stooq + Yahoo Finance free price feed redundancy. Fallback for FMP rate limits. 23 core tickers including BTC/ETH/VIX/DXY/TNX.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
