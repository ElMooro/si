"""ops 5581 -- refresh the Data page now: invoke justhodl-provider-catalog (after its deploy) and confirm
data/provider-catalog.json lists the 18 providers added in 2a65a00a5 (fmp, openfigi, cryptoquant, ...). Read-only apart
from the engine's own scheduled write."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

PUB = "justhodl-dashboard-live"
NEW = ["fmp", "openfigi", "cryptoquant", "coinbase", "eia", "finra", "benzinga", "quiver", "tradingview", "coingecko", "deribit", "cmc", "alphavantage", "newsapi", "nasdaq-datalink", "perplexity"]


def main() -> int:
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=900, connect_timeout=5, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("ops_5581_provider_catalog_refresh") as R:
        R.heading("ops 5581 -- provider catalog refresh (data.html): invoke after deploy, verify the new providers are listed")
        for _ in range(12):     # wait for the deploy of 2a65a00a5 to land (the function's code must contain the new slugs)
            cfg = lam.get_function_configuration(FunctionName="justhodl-provider-catalog")
            if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("State") == "Active":
                break
            time.sleep(20)
        out = lam.invoke(FunctionName="justhodl-provider-catalog", InvocationType="RequestResponse", Payload=b"{}")
        payload = out["Payload"].read().decode("utf-8", "replace")
        R.ok("invoke status=%s error=%s payload=%s" % (out.get("StatusCode"), out.get("FunctionError"), payload[:300]))
        cat = json.loads(s3.get_object(Bucket=PUB, Key="data/provider-catalog.json")["Body"].read())
        provs = cat.get("providers") or []
        slugs = {p.get("slug") for p in provs}
        missing = [s for s in NEW if s not in slugs]
        R.ok("data/provider-catalog.json: %d providers, generated %s; totals=%s" % (len(provs), cat.get("generated_at"), json.dumps(cat.get("totals"))[:200]))
        for p in provs:
            if p.get("slug") in ("fmp", "openfigi", "cryptoquant"):
                R.log("  %s: keys=%s hot=%s" % (p.get("slug"), p.get("keys") or p.get("key_count"), json.dumps(p.get("hot") or p.get("hot_keys"))[:200]))
        if missing:
            R.fail("catalog still missing: %s (deploy may not have landed; re-run)" % missing); return 1
        R.ok("GREEN -- data.html now lists FMP and every other provider")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
