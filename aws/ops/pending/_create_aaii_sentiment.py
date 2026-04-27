"""
# bumped 2026-04-27 — fix smoke-test failures

Step ___ — Create/update justhodl-aaii-sentiment Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-aaii-sentiment"
SOURCE_DIR = Path("aws/lambdas/justhodl-aaii-sentiment/source")
EB_RULE_NAME = "justhodl-aaii-sentiment-daily"
EB_SCHEDULE = "rate(1 day)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_aaii_sentiment") as r:
        r.heading("Create/update justhodl-aaii-sentiment Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=180,
            memory=512,
            description='AAII Investor Sentiment Survey weekly retail bull/bear/neutral. Released Thursdays. Daily check for new release; 26-week z-scores; extreme flags.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
