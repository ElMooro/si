"""
Step ___ — Create/update justhodl-sec-10kq Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-sec-10kq"
SOURCE_DIR = Path("aws/lambdas/justhodl-sec-10kq/source")
EB_RULE_NAME = "justhodl-sec-10kq-4h"
EB_SCHEDULE = "rate(4 hours)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_sec_10kq") as r:
        r.heading("Create/update justhodl-sec-10kq Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=300,
            memory=768,
            description='SEC 10-K (annual) + 10-Q (quarterly) + amended (10-K/A, 10-Q/A) filings. Surfaces restatements which often precede stock drawdowns.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
