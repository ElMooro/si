"""
Step ___ — Create/update justhodl-exchange-flows Lambda + EB rule.

Tracks BTC/ETH on-chain active supply momentum vs price → accumulation
or distribution regime. CoinMetrics community API (free, no key).
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-exchange-flows"
SOURCE_DIR = Path("aws/lambdas/justhodl-exchange-flows/source")
EB_RULE_NAME = "justhodl-exchange-flows-6h"
EB_SCHEDULE = "rate(6 hours)"

ENV_VARS = {
    "S3_BUCKET": "justhodl-dashboard-live",
    "S3_KEY": "data/exchange-flows.json",
}


def main():
    with report("create_exchange_flows") as r:
        r.heading("Create/update justhodl-exchange-flows Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=180,
            memory=512,
            description="BTC/ETH exchange flows — accumulation/distribution regime via CoinMetrics.",
            reserved_concurrency=1,
            create_function_url=False,
            smoke=True,
        )


if __name__ == "__main__":
    main()
