"""
Step ___ — Create/update justhodl-nyfed-dealer-survey Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-nyfed-dealer-survey"
SOURCE_DIR = Path("aws/lambdas/justhodl-nyfed-dealer-survey/source")
EB_RULE_NAME = "justhodl-nyfed-dealer-survey-weekly"
EB_SCHEDULE = "rate(7 days)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_nyfed_dealer_survey") as r:
        r.heading("Create/update justhodl-nyfed-dealer-survey Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=180,
            memory=512,
            description='NY Fed Survey of Primary Dealers — quarterly market expectations from the 24 banks moving the most flow. Weekly check for new release.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
