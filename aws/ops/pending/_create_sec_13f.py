"""
Step ___ — Create/update justhodl-sec-13f Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-sec-13f"
SOURCE_DIR = Path("aws/lambdas/justhodl-sec-13f/source")
EB_RULE_NAME = "justhodl-sec-13f-daily"
EB_SCHEDULE = "rate(1 day)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_sec_13f") as r:
        r.heading("Create/update justhodl-sec-13f Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=300,
            memory=768,
            description='SEC 13F-HR institutional position tracker. Watches 18 major funds for new quarterly filings (Berkshire, Bridgewater, Citadel, Pershing, etc.).',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
