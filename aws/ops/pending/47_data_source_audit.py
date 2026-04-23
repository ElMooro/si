#!/usr/bin/env python3
"""
Data source verification audit.

For each external data source, check:
  - Is the data present?
  - Does it have fresh values (not nulls, not zeros, not stale timestamps)?
  - Is the value count within expected ranges?
  - Is the last-fetch timestamp recent enough for its cadence?

Sources audited:
  1. FRED — 233 series, daily/weekly/monthly cadence
  2. Stocks — 187 tickers via AlphaVantage/Polygon/FMP
  3. Crypto — 25 coins via CoinMarketCap
  4. CFTC — 29 contracts via SODA api
  5. ECB CISS — risk index
  6. Options Flow — flow-data.json
  7. Crypto Intel — crypto-intel.json
  8. Stock Screener — screener/data.json
  9. News — NewsAPI + RSS
 10. AI Briefings — Anthropic-generated content
 11. DynamoDB — signal logger records
 12. DEX Scanner — pushes to dex.html on GitHub
"""
import json
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def safe_get_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        lm = obj["LastModified"]
        age_min = (datetime.now(timezone.utc) - lm).total_seconds() / 60
        size = obj["ContentLength"]
        data = json.loads(obj["Body"].read())
        return {"ok": True, "age_min": age_min, "size": size, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def count_non_null(d, depth=3):
    """Count non-null leaf values in a nested dict/list."""
    if depth <= 0:
        return 0, 0
    if isinstance(d, dict):
        nn, total = 0, 0
        for v in d.values():
            if isinstance(v, (dict, list)):
                sub_nn, sub_total = count_non_null(v, depth - 1)
                nn += sub_nn
                total += sub_total
            else:
                total += 1
                if v is not None and v != 0 and v != "":
                    nn += 1
        return nn, total
    if isinstance(d, list):
        nn, total = 0, 0
        for item in d:
            if isinstance(item, (dict, list)):
                sub_nn, sub_total = count_non_null(item, depth - 1)
                nn += sub_nn
                total += sub_total
            else:
                total += 1
                if item is not None and item != 0 and item != "":
                    nn += 1
        return nn, total
    return 0, 1


with report("data_source_audit") as r:
    r.heading("Data Source Audit — Is everything pulling fresh values?")

    # ═════════ Main report ═════════
    r.section("0. Load data/report.json — anchor for all checks")
    report_obj = safe_get_s3("data/report.json")
    if not report_obj["ok"]:
        r.fail(f"  Cannot load main report: {report_obj['error']}")
        raise SystemExit(1)

    rpt = report_obj["data"]
    r.log(f"  report.json: {report_obj['size']:,} bytes, {report_obj['age_min']:.1f} min old")
    r.log(f"  Top-level keys: {sorted(rpt.keys())}")

    # ═════════ 1. FRED ═════════
    r.section("1. FRED — 233 series")
    fred = rpt.get("fred") or rpt.get("fred_data") or {}
    fred_total = 0
    fred_null = 0
    fred_categories = {}
    for cat_name, cat_data in fred.items():
        if not isinstance(cat_data, dict):
            continue
        cat_count = len(cat_data)
        cat_null_count = sum(
            1 for sid, info in cat_data.items()
            if isinstance(info, dict) and (info.get("value") is None or info.get("value") == 0)
        )
        fred_categories[cat_name] = (cat_count, cat_null_count)
        fred_total += cat_count
        fred_null += cat_null_count

    r.log(f"  Total FRED series in report: {fred_total}")
    r.log(f"  Series with null/zero value: {fred_null}")
    r.log(f"  Category breakdown:")
    for cat, (total, null) in sorted(fred_categories.items()):
        flag = "⚠" if null > total * 0.3 else "✓"
        r.log(f"    {flag} {cat}: {total} series, {null} null/zero")

    # Critical tier-1 indicators — should have fresh values
    critical = {"DGS10": "10-year yield", "WALCL": "Fed balance sheet",
                "UNRATE": "Unemployment", "CPIAUCSL": "CPI", "VIXCLS": "VIX",
                "DTWEXBGS": "Dollar index"}
    r.log("")
    r.log("  Critical indicators:")
    for sid, name in critical.items():
        found = None
        for cat, cat_data in fred.items():
            if isinstance(cat_data, dict) and sid in cat_data:
                found = cat_data[sid]
                break
        if found and isinstance(found, dict):
            val = found.get("value")
            dt = found.get("date", "?")
            chg1d = found.get("chg_1d")
            r.log(f"    {sid} ({name}): value={val}, date={dt}, chg_1d={chg1d}")
        else:
            r.warn(f"    {sid} ({name}): NOT FOUND in report")

    r.kv(fred_total=fred_total, fred_nulls=fred_null,
         fred_coverage_pct=round(100*(fred_total-fred_null)/fred_total, 1) if fred_total else 0)

    # ═════════ 2. Stocks ═════════
    r.section("2. Stocks — expected 187 tickers")
    stocks = rpt.get("stocks") or rpt.get("stock_data") or rpt.get("tickers") or {}
    r.log(f"  Stocks in report: {len(stocks)}")

    null_price = 0
    sample_tickers = ["SPY", "QQQ", "NVDA", "TSLA", "BRK.B", "GLD", "TLT"]
    for t in sample_tickers:
        info = stocks.get(t)
        if info:
            price = info.get("price") if isinstance(info, dict) else info
            chg1d = info.get("change_1d") if isinstance(info, dict) else None
            r.log(f"    ✓ {t}: price={price}, change_1d={chg1d}")
        else:
            r.warn(f"    ✗ {t}: MISSING")

    for t, info in stocks.items():
        if isinstance(info, dict):
            price = info.get("price")
            if price is None or price == 0:
                null_price += 1

    r.log(f"  Stocks with null/zero price: {null_price}")
    r.kv(stocks_count=len(stocks), stocks_null=null_price,
         stocks_coverage_pct=round(100*(len(stocks)-null_price)/len(stocks), 1) if stocks else 0)

    # ═════════ 3. Crypto ═════════
    r.section("3. Crypto — expected 25 coins")
    crypto = rpt.get("crypto") or rpt.get("crypto_data") or {}
    r.log(f"  Coins in report: {len(crypto)}")
    sample_coins = ["BTC", "ETH", "SOL", "XRP"]
    for c in sample_coins:
        info = crypto.get(c)
        if info and isinstance(info, dict):
            price = info.get("price")
            chg7 = info.get("change_7d")
            ath = info.get("ath_pct")
            r.log(f"    ✓ {c}: price={price}, 7d={chg7}%, from_ATH={ath}%")
        else:
            r.warn(f"    ✗ {c}: MISSING or wrong shape")
    r.kv(crypto_count=len(crypto))

    # ═════════ 4. CFTC ═════════
    r.section("4. CFTC — expected 29 contracts, 7 categories")
    cftc = rpt.get("cftc") or rpt.get("cftc_positioning") or {}
    if isinstance(cftc, dict):
        r.log(f"  CFTC keys: {sorted(list(cftc.keys())[:10])}")
        contract_count = 0
        for k, v in cftc.items():
            if isinstance(v, dict) and "net_position" in str(v):
                contract_count += 1
        r.log(f"  Contract entries detected: {contract_count}")
        # Check for known contracts
        for key in ["SP500", "VIX", "GOLD", "crisis_score"]:
            if key in cftc:
                r.log(f"    ✓ {key}: {str(cftc[key])[:100]}")
    else:
        r.warn(f"  CFTC data not a dict: {type(cftc).__name__}")
    r.kv(cftc_present=bool(cftc), cftc_keys=len(cftc) if isinstance(cftc, dict) else 0)

    # ═════════ 5. ECB CISS ═════════
    r.section("5. ECB CISS — systemic risk indicator")
    ciss = rpt.get("ecb_ciss") or rpt.get("ciss") or {}
    if ciss and isinstance(ciss, dict):
        r.log(f"  ECB CISS entries: {len(ciss)}")
        for k, v in list(ciss.items())[:3]:
            r.log(f"    {k}: {str(v)[:120]}")
    else:
        r.warn(f"  ECB CISS missing or empty: {type(ciss).__name__}")

    # ═════════ 6. Options Flow ═════════
    r.section("6. Options Flow — flow-data.json")
    flow_obj = safe_get_s3("flow-data.json")
    if flow_obj["ok"]:
        r.log(f"  flow-data.json: {flow_obj['size']:,} bytes, {flow_obj['age_min']:.1f} min old")
        flow = flow_obj["data"]
        for key in ["put_call_ratio", "pc_signal", "gamma_regime", "net_premium",
                    "spy_price", "sentiment_composite", "trading_signals"]:
            v = flow.get(key) if isinstance(flow, dict) else None
            if isinstance(v, list):
                r.log(f"    {key}: list[{len(v)}]")
            else:
                r.log(f"    {key}: {str(v)[:80]}")
    else:
        r.fail(f"  flow-data.json: {flow_obj['error']}")

    # ═════════ 7. Crypto Intel ═════════
    r.section("7. Crypto Intel — crypto-intel.json")
    ci_obj = safe_get_s3("crypto-intel.json")
    if ci_obj["ok"]:
        r.log(f"  crypto-intel.json: {ci_obj['size']:,} bytes, {ci_obj['age_min']:.1f} min old")
        ci = ci_obj["data"]
        for key in ["btc_dominance", "eth_dominance", "total_mcap_fmt",
                    "fear_greed_value", "risk_score", "funding_summary",
                    "mvrv_approx", "stablecoin_net_signal", "whale_count_24h"]:
            v = ci.get(key) if isinstance(ci, dict) else None
            r.log(f"    {key}: {str(v)[:100]}")
    else:
        r.fail(f"  crypto-intel.json: {ci_obj['error']}")

    # ═════════ 8. Stock Screener ═════════
    r.section("8. Stock Screener — screener/data.json")
    sc_obj = safe_get_s3("screener/data.json")
    if sc_obj["ok"]:
        r.log(f"  screener/data.json: {sc_obj['size']:,} bytes, {sc_obj['age_min']:.1f} min old")
        sc = sc_obj["data"]
        if isinstance(sc, dict):
            for key in list(sc.keys())[:5]:
                v = sc[key]
                if isinstance(v, list):
                    r.log(f"    {key}: list[{len(v)}]")
                else:
                    r.log(f"    {key}: {str(v)[:100]}")
    else:
        r.warn(f"  screener/data.json: {sc_obj['error']}")

    # ═════════ 9. News ═════════
    r.section("9. News — NewsAPI + RSS")
    news = rpt.get("news") or {}
    if isinstance(news, dict):
        total_headlines = 0
        for key, val in news.items():
            if isinstance(val, list):
                r.log(f"    {key}: {len(val)} headlines")
                total_headlines += len(val)
        if total_headlines > 0:
            r.log(f"  Total headlines: {total_headlines}")
    elif isinstance(news, list):
        r.log(f"  News list: {len(news)} headlines")
        if news and isinstance(news[0], dict):
            sample = news[0]
            r.log(f"  Sample: {sample.get('title', '')[:80]}")
    r.kv(news_present=bool(news))

    # ═════════ 10. AI Briefings ═════════
    r.section("10. AI Briefings — Anthropic-generated")
    ai = rpt.get("ai_analysis") or rpt.get("ai_briefing") or {}
    if ai:
        if isinstance(ai, dict):
            for key, val in ai.items():
                preview = str(val)[:120].replace("\n", " ")
                r.log(f"    {key}: {preview}")
        elif isinstance(ai, str):
            r.log(f"    (string length {len(ai)}): {ai[:200].replace(chr(10), ' ')}")
    else:
        r.warn(f"  AI analysis missing")

    # ═════════ 11. DynamoDB — Signal logger ═════════
    r.section("11. DynamoDB — justhodl-signals table")
    try:
        resp = ddb.describe_table(TableName="justhodl-signals")
        td = resp["Table"]
        r.log(f"  Table status: {td['TableStatus']}")
        r.log(f"  Item count (approx): {td.get('ItemCount', 'unknown')}")
        r.log(f"  Size: {td.get('TableSizeBytes', 0):,} bytes")

        # Scan recent items
        scan = ddb.scan(TableName="justhodl-signals", Limit=5)
        items = scan.get("Items", [])
        r.log(f"  Sample recent items: {len(items)}")
        for item in items[:2]:
            ts = item.get("timestamp", {}).get("S", "?")
            sig = item.get("signal_type", {}).get("S", "?")
            r.log(f"    ts={ts}, type={sig}")
    except Exception as e:
        r.warn(f"  {e}")

    # Also outcomes table
    try:
        resp = ddb.describe_table(TableName="justhodl-outcomes")
        td = resp["Table"]
        r.log(f"  justhodl-outcomes: {td.get('ItemCount', 'unknown')} items, {td.get('TableSizeBytes', 0):,} bytes")
    except Exception as e:
        r.warn(f"  justhodl-outcomes: {e}")

    # ═════════ 12. SSM — Calibration weights ═════════
    r.section("12. SSM — Calibration params")
    try:
        for name in ["/justhodl/calibration/weights", "/justhodl/calibration/accuracy"]:
            try:
                p = ssm.get_parameter(Name=name)
                val_preview = p["Parameter"]["Value"][:150]
                last_mod = p["Parameter"].get("LastModifiedDate")
                r.log(f"  ✓ {name}")
                r.log(f"    Last modified: {last_mod}")
                r.log(f"    Value: {val_preview}")
            except Exception as e:
                r.warn(f"  ✗ {name}: {e}")
    except Exception as e:
        r.warn(f"  {e}")

    # ═════════ 13. DEX Scanner ═════════
    r.section("13. DEX Scanner — dex.html pushes to GitHub")
    try:
        # Check its last invocation
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=6)
        import botocore
        cw = boto3.client("cloudwatch", region_name=REGION)
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-dex-scanner"}],
            StartTime=start, EndTime=end, Period=900, Statistics=["Sum"],
        )
        total = sum(p.get("Sum", 0) for p in resp.get("Datapoints", []))
        err_resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": "justhodl-dex-scanner"}],
            StartTime=start, EndTime=end, Period=900, Statistics=["Sum"],
        )
        err_total = sum(p.get("Sum", 0) for p in err_resp.get("Datapoints", []))
        r.log(f"  dex-scanner last 6h: {int(total)} invocations, {int(err_total)} errors")
        if err_total > total * 0.2:
            r.warn(f"  ⚠ Elevated error rate — likely GitHub PAT issue")
    except Exception as e:
        r.warn(f"  {e}")

    # ═════════ 14. Anthropic API key health ═════════
    r.section("14. Anthropic API — are our AI Lambdas actually invoking Claude?")
    ai_lambdas = ["justhodl-ai-chat", "justhodl-investor-agents",
                  "justhodl-morning-intelligence", "justhodl-daily-report-v3"]
    cw = boto3.client("cloudwatch", region_name=REGION)
    for fn in ai_lambdas:
        try:
            streams = logs.describe_log_streams(
                logGroupName=f"/aws/lambda/{fn}",
                orderBy="LastEventTime", descending=True, limit=1,
            ).get("logStreams", [])
            if not streams:
                r.warn(f"  {fn}: no log streams")
                continue
            s = streams[0]
            start = int((datetime.now(timezone.utc) - timedelta(hours=6)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{fn}",
                logStreamName=s["logStreamName"], startTime=start, limit=200, startFromHead=False,
            )
            events = ev.get("events", [])
            anthropic_errors = []
            anthropic_successes = 0
            for e in events:
                m = e.get("message", "")
                if "anthropic" in m.lower() and ("error" in m.lower() or "fail" in m.lower() or " 4" in m or " 5" in m):
                    if "401" in m or "403" in m or "429" in m or "500" in m or "502" in m:
                        anthropic_errors.append(m[:200])
                if "claude-haiku" in m or "claude-opus" in m or "claude-sonnet" in m:
                    anthropic_successes += 1
            r.log(f"  {fn}: {anthropic_successes} Claude model refs, {len(anthropic_errors)} API errors in last 6h")
            for err in anthropic_errors[:2]:
                r.log(f"    {err}")
        except Exception as e:
            r.warn(f"  {fn}: {e}")

    r.log("Done")
