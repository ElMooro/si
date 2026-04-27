"""
Step ___ — Create/update justhodl-vix-curve Lambda + EB rule.

Pulls ^VIX9D / ^VIX / ^VIX3M / ^VIX6M / ^VVIX from Yahoo Finance every
4 hours. Computes term structure slopes and classifies regime.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-vix-curve"
SOURCE_DIR = Path("aws/lambdas/justhodl-vix-curve/source")
EB_RULE_NAME = "justhodl-vix-curve-4h"
EB_SCHEDULE = "rate(4 hours)"

ENV_VARS = {
    "S3_BUCKET": "justhodl-dashboard-live",
    "S3_KEY": "data/vix-curve.json",
}


def main():
    with report("create_vix_curve") as r:
        r.heading("Create/update justhodl-vix-curve Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=120,
            memory=256,
            description="VIX term structure 4h tracker. Yahoo ^VIX family. Regime: contango / flat / backwardation.",
            reserved_concurrency=1,
            create_function_url=False,
            smoke=True,
        )


if __name__ == "__main__":
    main()
