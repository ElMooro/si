"""
# bumped 2026-05-03 16:55 — fix namespace parsing

Step ___ — Create/update justhodl-13f-positions Lambda + EB rule.

Parses 13F infotable XML for all 18 watchlist funds. Extracts positions,
resolves CUSIPs to tickers, computes BUYS/SELLS/NEW/EXIT vs prior quarter.
Aggregates across funds for "most bought / most sold" rankings.

Memory: 1024MB (large filings + parallel parsing).
Timeout: 600s (parsing 18 filings can take a while on first run).
Cache: 13f-cache/{FUND}/{accession}.json — never re-parses same filing.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "aws/ops")
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

FUNCTION_NAME = "justhodl-13f-positions"
SOURCE_DIR = Path("aws/lambdas/justhodl-13f-positions/source")
EB_RULE_NAME = "justhodl-13f-positions-6h"
EB_SCHEDULE = "rate(6 hours)"

ENV_VARS = {
    "S3_BUCKET": "justhodl-dashboard-live",
    "S3_KEY": "data/13f-positions.json",
    "S3_CACHE_PREFIX": "13f-cache/",
    "S3_FILINGS_KEY": "data/institutional-positions.json",
    "USER_AGENT": "JustHodl Research raafouis@gmail.com",
    "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    "MAX_PARALLEL": "3",
}


def main():
    with report("create_13f_positions") as r:
        r.heading("Create/update justhodl-13f-positions Lambda + EB rule")
        deploy_lambda(
            report=r,
            function_name=FUNCTION_NAME,
            source_dir=SOURCE_DIR,
            env_vars=ENV_VARS,
            eb_rule_name=EB_RULE_NAME,
            eb_schedule=EB_SCHEDULE,
            timeout=600,
            memory=1024,
            description="13F position parser. Extracts holdings + computes buys/sells vs prior quarter.",
            reserved_concurrency=1,
            create_function_url=False,
            smoke=True,
        )


if __name__ == "__main__":
    main()
