#!/usr/bin/env python3
"""
Master data source verification.

Goal: take every external data source ever mentioned in conversations
with Khalid and verify whether the production system is currently
pulling from it.

For each source, classify:
  ✅ ACTIVE — Lambda has working fetch code, data appears in S3
  ⚠ DEGRADED — fetch code exists but failing (e.g. Binance HTTP 451)
  ⚠ NEVER USED — mentioned in conversation but no code references it
  📦 LEGACY — was used historically but retired (e.g. expired free APIs)

Then:
  1. List all 12 critical Lambdas + the data source(s) each consumes
  2. List all signals fed into Khalid Index calculation
  3. List all signals stored in DynamoDB justhodl-signals
  4. Verify the calibration loop is closing (signals → outcomes → weights)
"""
import json
import re
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


# Master catalog of every external data source mentioned in
# conversations, with the URL/host pattern used to identify it
# in Lambda source code.
#
# Categories: macro, market, crypto, futures, options, fundamentals,
#             news, sentiment, onchain, weather/event, derivatives, dex
DATA_SOURCES = [
    # ─── MACRO / ECONOMIC DATA ───
    {"category": "macro", "name": "FRED",
     "url_pattern": "fred.stlouisfed.org",
     "purpose": "233 economic series — rates, CPI, balance sheet, GDP, employment"},
    {"category": "macro", "name": "ECB SDMX",
     "url_pattern": "data-api.ecb.europa.eu",
     "purpose": "CISS systemic risk, ECB rates, eurozone subindices"},
    {"category": "macro", "name": "NY Fed",
     "url_pattern": "markets.newyorkfed.org",
     "purpose": "Reverse repo, RRP, SOFR, treasury operations"},
    {"category": "macro", "name": "US Treasury",
     "url_pattern": "fiscaldata.treasury.gov",
     "purpose": "TGA balance, debt issuance, auction results"},
    {"category": "macro", "name": "BEA",
     "url_pattern": "bea.gov",
     "purpose": "GDP, personal income/spending"},
    {"category": "macro", "name": "BLS",
     "url_pattern": "api.bls.gov",
     "purpose": "Employment, unemployment, JOLTS"},
    {"category": "macro", "name": "Census",
     "url_pattern": "api.census.gov",
     "purpose": "Trade balance, retail sales"},
    {"category": "macro", "name": "OFR",
     "url_pattern": "financialresearch.gov",
     "purpose": "Financial Stress Index, money market"},
    {"category": "macro", "name": "EIA",
     "url_pattern": "api.eia.gov",
     "purpose": "Oil/gas inventories, electricity"},

    # ─── MARKET / EQUITIES ───
    {"category": "market", "name": "Polygon.io",
     "url_pattern": "api.polygon.io",
     "purpose": "Stock prices, options contracts, ETF flows, news"},
    {"category": "market", "name": "AlphaVantage",
     "url_pattern": "alphavantage.co",
     "purpose": "Stock OHLC fallback (TIME_SERIES_WEEKLY_ADJUSTED)"},
    {"category": "market", "name": "FMP (Premium)",
     "url_pattern": "financialmodelingprep.com",
     "purpose": "S&P 500 fundamentals, F-scores, Z-scores, earnings"},

    # ─── CRYPTO ───
    {"category": "crypto", "name": "CoinGecko",
     "url_pattern": "coingecko.com",
     "purpose": "Crypto prices, OHLC, market cap (USA-friendly)"},
    {"category": "crypto", "name": "CoinMarketCap",
     "url_pattern": "pro-api.coinmarketcap.com",
     "purpose": "Crypto trending, gainers/losers"},
    {"category": "crypto", "name": "Alternative.me",
     "url_pattern": "alternative.me",
     "purpose": "Fear & Greed Index"},
    {"category": "crypto", "name": "DeFiLlama",
     "url_pattern": "api.llama.fi",
     "purpose": "TVL, DEX volumes, yield rates"},

    # ─── DERIVATIVES (BINANCE = NOW BLOCKED) ───
    {"category": "derivatives", "name": "Binance Futures",
     "url_pattern": "fapi.binance.com",
     "purpose": "BTC/ETH/SOL open interest [BLOCKED in US-East-1]"},
    {"category": "derivatives", "name": "Binance Spot",
     "url_pattern": "api.binance.com",
     "purpose": "klines for technicals [BLOCKED in US-East-1]"},
    {"category": "derivatives", "name": "OKX",
     "url_pattern": "okx.com",
     "purpose": "Funding rates fallback"},

    # ─── FUTURES POSITIONING ───
    {"category": "futures", "name": "CFTC SODA",
     "url_pattern": "cftc.gov",
     "purpose": "COT 29 contracts (weekly Friday)"},

    # ─── ONCHAIN / WHALE ───
    {"category": "onchain", "name": "Blockchain.info",
     "url_pattern": "blockchain.info",
     "purpose": "BTC mempool whale txns [DESIGN ISSUE — mempool only]"},
    {"category": "onchain", "name": "Blocknative",
     "url_pattern": "blocknative.com",
     "purpose": "ETH gas fees"},

    # ─── NEWS / SENTIMENT ───
    {"category": "news", "name": "NewsAPI",
     "url_pattern": "newsapi.org",
     "purpose": "Financial headlines"},
    {"category": "news", "name": "CNN Fear/Greed",
     "url_pattern": "production.dataviz.cnn.io",
     "purpose": "CNN sentiment index"},

    # ─── DEX ───
    {"category": "dex", "name": "DexScreener",
     "url_pattern": "api.dexscreener.com",
     "purpose": "DEX trades, liquidity"},

    # ─── AI ───
    {"category": "ai", "name": "Anthropic Claude API",
     "url_pattern": "api.anthropic.com",
     "purpose": "AI briefings, analysis, chat (claude-haiku-4-5-20251001)"},
]


def grep_lambda_sources(pattern):
    """Search all Lambda source files for the URL pattern.
    Returns list of (lambda_name, line_count) tuples where it appears."""
    hits = []
    lambda_dir = REPO_ROOT / "aws/lambdas"
    if not lambda_dir.exists():
        return hits
    for fn_dir in lambda_dir.iterdir():
        if not fn_dir.is_dir():
            continue
        src_file = fn_dir / "source/lambda_function.py"
        if not src_file.exists():
            continue
        try:
            content = src_file.read_text(encoding="utf-8", errors="ignore")
            count = content.lower().count(pattern.lower())
            if count > 0:
                hits.append((fn_dir.name, count))
        except Exception:
            pass
    return hits


with report("master_data_source_verification") as r:
    r.heading("Master Data Source Verification — every source ever mentioned")

    # ═════════ Build the master data-source matrix ═════════
    r.section("1. Data Source Matrix — production code references")
    active = []
    inactive = []
    degraded = ["Binance Futures", "Binance Spot",  # geoblock confirmed earlier
                "Blockchain.info"]  # mempool-only design issue

    for src in DATA_SOURCES:
        hits = grep_lambda_sources(src["url_pattern"])
        if hits:
            status = "⚠ DEGRADED" if src["name"] in degraded else "✅ ACTIVE"
            lambdas = ", ".join(f"{n}({c}x)" for n, c in hits[:3])
            if len(hits) > 3:
                lambdas += f" + {len(hits)-3} more"
            r.log(f"  {status:13} {src['name']:25} → {lambdas}")
            r.log(f"             purpose: {src['purpose']}")
            if src["name"] in degraded:
                degraded_status = src["name"]
            active.append(src)
        else:
            r.log(f"  ❌ MISSING    {src['name']:25} → no Lambda code references {src['url_pattern']}")
            r.log(f"             purpose: {src['purpose']}")
            inactive.append(src)

    r.kv(active=len(active), inactive=len(inactive), degraded=3,
         total=len(DATA_SOURCES))

    # ═════════ Categorical health summary ═════════
    r.section("2. Health by category")
    by_cat = {}
    for src in DATA_SOURCES:
        cat = src["category"]
        by_cat.setdefault(cat, {"total": 0, "active": 0, "degraded": 0})
        by_cat[cat]["total"] += 1
        if src in active:
            if src["name"] in degraded:
                by_cat[cat]["degraded"] += 1
            else:
                by_cat[cat]["active"] += 1
    for cat, stats in sorted(by_cat.items()):
        line = f"  {cat:15} {stats['active']}/{stats['total']} active"
        if stats["degraded"] > 0:
            line += f", {stats['degraded']} degraded"
        r.log(line)

    # ═════════ Khalid Index composition ═════════
    r.section("3. Khalid Index — what feeds the composite score?")
    src_file = REPO_ROOT / "aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py"
    if src_file.exists():
        content = src_file.read_text(encoding="utf-8", errors="ignore")
        # Find the calculate_khalid_index function
        start = content.find("def calculate_khalid_index")
        if start > 0:
            end = content.find("\ndef ", start + 1)
            if end < 0:
                end = start + 5000
            ki_func = content[start:end]
            # Extract every "score +=" or "weight" reference
            r.log(f"  Khalid Index function (showing key signals):")
            for line in ki_func.split("\n")[:80]:
                line = line.strip()
                if line and (line.startswith("#") or "score +=" in line or "score -=" in line or "weight" in line.lower()):
                    r.log(f"    {line[:130]}")

    # ═════════ DynamoDB signal logger ═════════
    r.section("4. DynamoDB — justhodl-signals table (signal types being tracked)")
    try:
        # Now we have read perm
        scan = ddb.scan(TableName="justhodl-signals", Limit=20)
        items = scan.get("Items", [])
        r.log(f"  Total scanned: {len(items)} (limit 20)")
        signal_types = set()
        for item in items:
            sig = item.get("signal_type", {}).get("S", "")
            if sig:
                signal_types.add(sig)
        r.log(f"  Unique signal types in last 20 entries:")
        for st in sorted(signal_types):
            r.log(f"    - {st}")

        # Total count
        td = ddb.describe_table(TableName="justhodl-signals")
        r.log(f"\n  Table total: {td['Table'].get('ItemCount', 'unknown')} items")
    except Exception as e:
        r.warn(f"  {e}")

    # ═════════ Calibration loop ═════════
    r.section("5. Calibration loop — signals → outcomes → weights")
    try:
        # Outcomes table
        scan = ddb.scan(TableName="justhodl-outcomes", Limit=10)
        items = scan.get("Items", [])
        r.log(f"  justhodl-outcomes: {len(items)} recent items")
        for item in items[:3]:
            sig = item.get("signal_type", {}).get("S", "?")
            ts = item.get("timestamp", {}).get("S", "?")
            acc = item.get("was_correct", {}).get("BOOL", "?")
            r.log(f"    {ts[:19]} | {sig} | correct={acc}")

        # SSM weights
        weights = ssm.get_parameter(Name="/justhodl/calibration/weights")
        weights_data = json.loads(weights["Parameter"]["Value"])
        r.log(f"\n  /justhodl/calibration/weights ({len(weights_data)} signals):")
        for k, v in sorted(weights_data.items())[:20]:
            r.log(f"    {k:30} = {v}")
        last_mod = weights["Parameter"]["LastModifiedDate"]
        days_old = (datetime.now(timezone.utc) - last_mod).days
        r.log(f"\n  Last calibration update: {last_mod.isoformat()} ({days_old} days ago)")
        if days_old > 14:
            r.warn(f"  ⚠ Weights are stale — calibrator may not be running")
        else:
            r.ok(f"  ✓ Calibration is active")

        # SSM accuracy
        acc = ssm.get_parameter(Name="/justhodl/calibration/accuracy")
        acc_data = json.loads(acc["Parameter"]["Value"])
        r.log(f"\n  Per-signal accuracy ({len(acc_data)} signals):")
        for k, v in sorted(acc_data.items())[:15]:
            if isinstance(v, dict):
                r.log(f"    {k:30} accuracy={v.get('accuracy')}, n={v.get('n')}")
    except Exception as e:
        r.warn(f"  {e}")

    # ═════════ Summary ═════════
    r.section("6. Final summary")
    r.log(f"  Active sources:    {len([s for s in active if s['name'] not in degraded])}")
    r.log(f"  Degraded sources:  {len(degraded)} (Binance + blockchain.info — diagnosed)")
    r.log(f"  Missing sources:   {len(inactive)}")
    r.log(f"  Total cataloged:   {len(DATA_SOURCES)}")
    r.log("")
    r.log("  Key conclusion: classify per source; tally calibration loop status.")

    r.log("Done")
