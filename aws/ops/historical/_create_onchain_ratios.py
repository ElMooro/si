"""
Step ___ — Create/update justhodl-onchain-ratios Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-onchain-ratios"
SOURCE_DIR = Path("aws/lambdas/justhodl-onchain-ratios/source")
EB_RULE_NAME = "justhodl-onchain-ratios-6h"
EB_SCHEDULE = "rate(6 hours)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_onchain_ratios") as r:
        r.heading("Create/update justhodl-onchain-ratios Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=300,
            memory=768,
            description='BTC + ETH on-chain ratios (Glassnode-equivalent): MVRV, NVT, hash rate, ETH gas, ETH supply growth. CoinMetrics + mempool.space + etherscan.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
