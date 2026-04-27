"""
Step ___ — Create/update justhodl-labor-leading Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-labor-leading"
SOURCE_DIR = Path("aws/lambdas/justhodl-labor-leading/source")
EB_RULE_NAME = "justhodl-labor-leading-daily"
EB_SCHEDULE = "rate(1 day)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_labor_leading") as r:
        r.heading("Create/update justhodl-labor-leading Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=180,
            memory=512,
            description='JOLTS + Challenger + Initial Claims labor market leading indicators. Computes openings/unemployed ratio, quits rate z-score, claims 4-week MA trend.',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
