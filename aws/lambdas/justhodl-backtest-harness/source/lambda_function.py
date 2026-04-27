"""
justhodl-backtest-harness — Daily signal snapshot + forward-return tracker

Records the state of every signal source each day, then computes
30/60/90/180-day forward returns once enough time has passed. The
output tells you which signals actually predicted forward returns
and which didn't.

Why this matters
================
Every quant desk has a backtest harness. JustHodl ships dozens of
signals (Khalid Index, regime, asymmetric setups, COT extremes,
divergence scanner, insider clusters, AAII extremes, GEX regime,
…) but until now you had no way to ask "did my BUY signals from
2026-Q1 actually outperform random over 90 days?"

This Lambda answers that question by:
  1. Daily snapshot — record signal state + ticker prices
  2. Forward-return computation — once 30/60/90/180 days have
     passed since a snapshot, compare prices then vs now
  3. Cohort summary — group by signal type and compute hit rate,
     avg return, median return, IR

Storage: DynamoDB table `justhodl-backtest`
  PK = `SIGNAL#TICKER#YYYY-MM-DD`
  SK = `signal_type` (insider_cluster, asymmetric_setup, etc.)
  attrs:
    snapshot_date, ticker, signal_type, signal_strength (0-1),
    price_at_snapshot, fwd_30d_pct, fwd_60d_pct, fwd_90d_pct,
    fwd_180d_pct, computed_at_30, computed_at_60, computed_at_90,
    computed_at_180

Output S3 (data/backtest-summary.json) — daily roll-up:
  {
    "generated_at": ...,
    "by_signal": {
      "insider_cluster": {
        "n_total": 47,                      # historical count
        "n_with_30d": 38,                   # have 30d forward return
        "n_with_90d": 22,
        "hit_rate_30d": 0.74,               # frac with positive return
        "avg_return_30d": 0.038,            # 3.8% avg
        "median_return_30d": 0.022,
        "vs_spy_30d": +0.018,               # excess vs SPY same period
        "info_ratio_30d": 0.42,
        "best_30d": {"ticker": "AAPL", "return": 0.156},
        "worst_30d": {...},
      },
      "asymmetric_setup": {...},
      "8k_red_flag": {...},   (forward return EXPECTED to be negative)
      ...
    }
  }

Schedule: rate(1 day) — runs once per day in the morning.
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import statistics

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_OUTPUT_KEY = os.environ.get("S3_OUTPUT_KEY", "data/backtest-summary.json")
DDB_TABLE = os.environ.get("DDB_TABLE", "justhodl-backtest")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
FMP_KEY = os.environ.get("FMP_KEY", "")

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(DDB_TABLE)


# Signal types we record. Each maps to (input_s3_key, extraction_function).
# extraction_function returns list of {ticker, signal_strength, snapshot_extras}.
# `signal_strength` is 0-1 — used to weight cohort grouping.

def _extract_insider_clusters(data):
    """From data/insider-trades.json — clusters tracked as bullish signals."""
    out = []
    for c in (data or {}).get("clusters", []) or []:
        out.append({
            "ticker": c.get("ticker", "").upper(),
            "signal_strength": min(1.0, c.get("insider_count", 0) / 10),
            "extras": {"insider_count": c.get("insider_count"),
                       "total_value": c.get("total_value")},
        })
    return out


def _extract_big_insider_buys(data):
    """From data/insider-trades.json — single big buys."""
    out = []
    for b in (data or {}).get("big_buys", []) or []:
        out.append({
            "ticker": b.get("ticker", "").upper(),
            "signal_strength": min(1.0, (b.get("value", 0) / 10_000_000)),
            "extras": {"value": b.get("value"),
                       "insider": (b.get("insider") or "")[:60]},
        })
    return out


def _extract_asymmetric_setups(data):
    """From opportunities/asymmetric-equity.json — top setups."""
    out = []
    for s in (data or {}).get("top_setups", []) or []:
        out.append({
            "ticker": s.get("symbol", "").upper(),
            "signal_strength": (s.get("composite_score", 50) / 100),
            "extras": {"dims_passed": s.get("dims_passed"),
                       "stacked_score": s.get("stacked_score")},
        })
    return out


def _extract_8k_red_flags(data):
    """From data/8k-filings.json — red-flag filings (BEARISH expected)."""
    out = []
    red_items = {"4.02", "1.03", "3.01", "5.04", "2.06", "2.04"}
    seen = set()
    for f in (data or {}).get("red_flags", []) or []:
        company = (f.get("company") or "")[:30]
        # Without a company-name → ticker map, we record by company.
        # Hook: when we add the lookup, swap company for ticker here.
        if company in seen:
            continue
        seen.add(company)
        items = [i for i in f.get("items", []) if i in red_items]
        out.append({
            "ticker": company,   # placeholder; refine when ticker mapping ready
            "signal_strength": 1.0 if "4.02" in items else 0.7,
            "extras": {"items": items, "accession": f.get("accession")},
        })
    return out


def _extract_aaii_extreme(data):
    """From data/aaii-sentiment.json — broad-market signal vs SPY."""
    if not data:
        return []
    extremes = data.get("extremes", {})
    if extremes.get("is_bearish_extreme"):
        return [{"ticker": "SPY", "signal_strength": 1.0,
                 "extras": {"type": "extreme_bearish_contrarian"}}]
    if extremes.get("is_bullish_extreme"):
        return [{"ticker": "SPY", "signal_strength": -1.0,
                 "extras": {"type": "extreme_bullish_contrarian"}}]
    return []


SIGNAL_EXTRACTORS = {
    "insider_cluster":   ("data/insider-trades.json",                _extract_insider_clusters),
    "big_insider_buy":   ("data/insider-trades.json",                _extract_big_insider_buys),
    "asymmetric_setup":  ("opportunities/asymmetric-equity.json",    _extract_asymmetric_setups),
    "8k_red_flag":       ("data/8k-filings.json",                    _extract_8k_red_flags),
    "aaii_extreme":      ("data/aaii-sentiment.json",                _extract_aaii_extreme),
}


def _get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def _put_s3_json(key, body):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode(),
        ContentType="application/json",
        CacheControl="no-cache",
    )


def _fetch_price_fmp(ticker: str):
    """FMP quote endpoint. Returns last close or None."""
    if not FMP_KEY or not ticker:
        return None
    try:
        url = f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
        if isinstance(data, list) and data:
            return float(data[0].get("price") or 0) or None
    except Exception:
        pass
    return None


def _fetch_price_redundancy(ticker: str):
    """Fall back to data/price-redundancy.json (free Stooq+Yahoo)."""
    pr = _get_s3_json("data/price-redundancy.json", {})
    if not pr:
        return None
    t = pr.get("tickers", {}).get(ticker)
    if t and isinstance(t.get("price"), (int, float)):
        return float(t["price"])
    return None


def get_price(ticker: str):
    """Best-effort price fetch with FMP primary + redundancy fallback."""
    return _fetch_price_fmp(ticker) or _fetch_price_redundancy(ticker)


def ensure_table_exists():
    try:
        table.load()
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    # Create table on first run
    print(f"creating DDB table {DDB_TABLE}…")
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=DDB_TABLE,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    client.get_waiter("table_exists").wait(TableName=DDB_TABLE)
    return True


def record_snapshot(today_iso, signal_type, items):
    """Write today's signal records to DDB."""
    written = 0
    for item in items:
        if not item.get("ticker"):
            continue
        try:
            price = get_price(item["ticker"])
            row = {
                "pk": f"{signal_type}#{item['ticker']}#{today_iso}",
                "sk": signal_type,
                "snapshot_date": today_iso,
                "ticker": item["ticker"],
                "signal_type": signal_type,
                "signal_strength": str(round(float(item.get("signal_strength", 0.5)), 4)),
                "extras": json.dumps(item.get("extras", {}))[:1000],
            }
            if price is not None:
                row["price_at_snapshot"] = str(round(price, 4))
            table.put_item(Item=row)
            written += 1
            time.sleep(0.05)   # gentle rate limit
        except Exception as e:
            print(f"  write_err {signal_type}/{item.get('ticker')}: {e}")
    return written


def compute_forward_returns_for_horizon(today, horizon_days):
    """For all snapshots exactly horizon_days old, compute forward return now."""
    target_snapshot_date = (today - timedelta(days=horizon_days)).date().isoformat()
    target_attr = f"fwd_{horizon_days}d_pct"
    computed_attr = f"computed_at_{horizon_days}"

    # We don't have a GSI on snapshot_date — use scan with filter
    # (cost is fine; table will be small for a long time)
    updated = 0
    skipped = 0
    try:
        kwargs = {
            "FilterExpression": "snapshot_date = :d AND attribute_not_exists(#fwd)",
            "ExpressionAttributeNames": {"#fwd": target_attr},
            "ExpressionAttributeValues": {":d": target_snapshot_date},
            "ProjectionExpression": "pk, sk, ticker, price_at_snapshot",
        }
        last = None
        items = []
        while True:
            if last:
                kwargs["ExclusiveStartKey"] = last
            resp = table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
    except Exception as e:
        print(f"  scan_err for {horizon_days}d: {e}")
        return 0

    for item in items:
        try:
            ticker = item["ticker"]
            entry_price = float(item.get("price_at_snapshot", 0) or 0)
            if entry_price <= 0:
                skipped += 1
                continue
            now_price = get_price(ticker)
            if not now_price:
                skipped += 1
                continue
            ret = (now_price / entry_price) - 1
            table.update_item(
                Key={"pk": item["pk"], "sk": item["sk"]},
                UpdateExpression=f"SET #fwd = :ret, #cmp = :ts",
                ExpressionAttributeNames={"#fwd": target_attr, "#cmp": computed_attr},
                ExpressionAttributeValues={
                    ":ret": str(round(ret, 5)),
                    ":ts": today.isoformat(),
                },
            )
            updated += 1
            time.sleep(0.05)
        except Exception as e:
            print(f"  update_err {item.get('pk')}: {e}")

    return updated


def build_summary():
    """Roll up DDB into per-signal cohort statistics."""
    summary = {}
    # Scan everything (ok for thousands of records; bigger needs GSI later)
    try:
        items = []
        last = None
        while True:
            kwargs = {}
            if last:
                kwargs["ExclusiveStartKey"] = last
            resp = table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            last = resp.get("LastEvaluatedKey")
            if not last:
                break
    except Exception as e:
        return {"error": f"scan failed: {e}"}

    by_type = defaultdict(list)
    for item in items:
        st = item.get("signal_type")
        if not st:
            continue
        by_type[st].append(item)

    for signal_type, records in by_type.items():
        n_total = len(records)
        cohorts = {}
        for h in (30, 60, 90, 180):
            attr = f"fwd_{h}d_pct"
            with_return = [r for r in records if attr in r and r[attr] not in (None, "")]
            if not with_return:
                continue
            returns = []
            for r in with_return:
                try:
                    returns.append(float(r[attr]))
                except (ValueError, TypeError):
                    continue
            if not returns:
                continue

            best = max(with_return, key=lambda r: float(r.get(attr, 0)))
            worst = min(with_return, key=lambda r: float(r.get(attr, 0)))

            cohorts[f"{h}d"] = {
                "n_with_return": len(returns),
                "hit_rate": round(sum(1 for r in returns if r > 0) / len(returns), 3),
                "avg_return": round(statistics.mean(returns), 4),
                "median_return": round(statistics.median(returns), 4),
                "stdev_return": round(statistics.stdev(returns), 4) if len(returns) > 1 else None,
                "info_ratio": round(statistics.mean(returns) / statistics.stdev(returns), 3)
                              if len(returns) > 1 and statistics.stdev(returns) > 0 else None,
                "best": {"ticker": best.get("ticker"), "return": round(float(best.get(attr, 0)), 4)},
                "worst": {"ticker": worst.get("ticker"), "return": round(float(worst.get(attr, 0)), 4)},
            }

        summary[signal_type] = {
            "n_total_snapshots": n_total,
            "cohorts": cohorts,
        }

    return summary


def lambda_handler(event, context):
    started = time.time()
    today = datetime.now(timezone.utc)
    today_iso = today.date().isoformat()

    ensure_table_exists()

    # 1. Snapshot today's signals
    snapshot_counts = {}
    for signal_type, (s3_key, extractor) in SIGNAL_EXTRACTORS.items():
        try:
            data = _get_s3_json(s3_key, {})
            items = extractor(data)
            written = record_snapshot(today_iso, signal_type, items)
            snapshot_counts[signal_type] = {"extracted": len(items), "written": written}
        except Exception as e:
            snapshot_counts[signal_type] = {"error": str(e)}

    # 2. Compute forward returns for any snapshots reaching maturity today
    fwd_updates = {}
    for h in (30, 60, 90, 180):
        try:
            fwd_updates[f"{h}d"] = compute_forward_returns_for_horizon(today, h)
        except Exception as e:
            fwd_updates[f"{h}d"] = f"error: {e}"

    # 3. Build summary statistics
    summary = build_summary()

    output = {
        "generated_at": today.isoformat(timespec="seconds"),
        "snapshot_today": {
            "date": today_iso,
            "by_signal": snapshot_counts,
        },
        "forward_returns_computed_today": fwd_updates,
        "by_signal": summary,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    _put_s3_json(S3_OUTPUT_KEY, output)

    print(f"backtest harness: snapshots={sum(v.get('written', 0) for v in snapshot_counts.values() if isinstance(v, dict))}, fwd_updates={fwd_updates}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "snapshot_counts": snapshot_counts,
            "fwd_updates": fwd_updates,
            "n_signal_types_in_summary": len(summary),
        }),
    }
