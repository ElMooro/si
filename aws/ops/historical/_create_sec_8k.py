"""
Step ___ — Create/update justhodl-sec-8k Lambda + EB rule.

Idempotent: creates Lambda if missing, updates if present, asserts EB rule
+ permissions. Smoke-tests after deploy.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-sec-8k"
SOURCE_DIR = Path("aws/lambdas/justhodl-sec-8k/source")
EB_RULE_NAME = "justhodl-sec-8k-30min"
EB_SCHEDULE = "rate(30 minutes)"

ENV_VARS = {'S3_BUCKET': 'justhodl-dashboard-live'}


def main():
    with report("create_sec_8k") as r:
        r.heading("Create/update justhodl-sec-8k Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=300,
            memory=768,
            description='SEC 8-K material event filings. Atom feed every 30 min, classifies by Item code, flags red flags (4.02 non-reliance, 1.03 bankruptcy, 5.04 trading halt).',
            reserved_concurrency=1,
            create_function_url=True,
            smoke=True,
        )


if __name__ == "__main__":
    main()
