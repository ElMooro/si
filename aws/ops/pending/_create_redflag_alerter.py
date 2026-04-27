"""
Step ___ — Create/update justhodl-redflag-alerter Lambda + EB rule.

Idempotent. Subscribes to data/8k-filings.json on a 30-min cadence and
sends Telegram messages for Item 4.02 / 1.03 / 3.01 / 5.04 / 2.04 / 2.06.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-redflag-alerter"
SOURCE_DIR = Path("aws/lambdas/justhodl-redflag-alerter/source")
EB_RULE_NAME = "justhodl-redflag-alerter-30min"
EB_SCHEDULE = "rate(30 minutes)"

ENV_VARS = {
    "S3_BUCKET": "justhodl-dashboard-live",
    "S3_INPUT_KEY": "data/8k-filings.json",
    "S3_OUTPUT_KEY": "data/redflag-alerts.json",
    "TG_TOKEN_PARAM": "/justhodl/telegram/bot_token",
    "TG_CHAT_ID_PARAM": "/justhodl/telegram/chat_id",
    "LOOKBACK_MINUTES": "60",
    "DEDUPE_WINDOW_HOURS": "24",
}


def main():
    with report("create_redflag_alerter") as r:
        r.heading("Create/update justhodl-redflag-alerter Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=60,
            memory=256,
            description="Telegram alerter for serious 8-K events. Reads data/8k-filings.json, dedupes 24h.",
            reserved_concurrency=1,
            create_function_url=False,    # invoked by EB only
            smoke=True,
        )


if __name__ == "__main__":
    main()
