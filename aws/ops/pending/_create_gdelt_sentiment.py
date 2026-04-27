"""
Step ___ — Create/update justhodl-gdelt-sentiment Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-gdelt-sentiment"
SOURCE_DIR = Path("aws/lambdas/justhodl-gdelt-sentiment/source")
EB_RULE_NAME = "justhodl-gdelt-sentiment-30min"
EB_SCHEDULE = "rate(30 minutes)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_gdelt_sentiment") as r:
        r.heading("Create/update justhodl-gdelt-sentiment Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=300,
            memory=1024,
            description='GDELT 2.0 global news + geopolitical sentiment. Pulls latest 15-min GKG batch, filters financial themes, computes per-asset sentiment.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
