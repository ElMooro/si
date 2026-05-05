"""
justhodl-system-signal-logger — logs hunter signals into DynamoDB justhodl-signals
for downstream calibration by justhodl-outcome-checker + justhodl-calibrator.

Reads all 5 system feeds, picks top picks (score >= MIN_SCORE), logs each
as a directional prediction with horizon, strike price, and origin system.

Schema in justhodl-signals table:
  PK: signal_id (uuid)
  ts_iso: when logged
  source: "insider_cluster" | "smart_money" | "deep_value" | "eps_velocity"
  ticker: SYMBOL
  direction: "UP"
  conviction: score / 100
  baseline_price: price at log-time (from FMP /quote)
  horizon_days_primary: 60-180 depending on system
  metadata: full record from source feed

The existing outcome-checker (Sun 22:30 UTC) will price-check these names
forward and the calibrator (Sun 9 UTC) will produce per-source weights.
"""
import json
import os
import time
import urllib.request
import uuid
import boto3
from decimal import Decimal

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
TABLE = os.environ.get("DDB_TABLE", "justhodl-signals")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
MIN_SCORE = float(os.environ.get("MIN_SCORE", "65"))

S3 = boto3.client("s3", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)


def _quote_price(symbol):
    """Fetch latest price for a symbol from FMP. Returns float or None."""
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            d = json.loads(r.read())
            if isinstance(d, list) and d:
                return float(d[0].get("price", 0)) or None
    except Exception:
        pass
    return None


def _to_decimal(v):
    """Convert float to Decimal for DynamoDB. Recurse through dicts/lists."""
    if isinstance(v, float):
        return Decimal(str(round(v, 6)))
    if isinstance(v, dict):
        return {k: _to_decimal(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_to_decimal(x) for x in v]
    return v


def log_signal(signal_id, ts_iso, source, ticker, direction, conviction,
                baseline_price, horizon_days, metadata):
    item = {
        "signal_id": signal_id,
        "ts_iso": ts_iso,
        "source": source,
        "ticker": ticker,
        "direction": direction,
        "conviction": Decimal(str(round(conviction, 4))),
        "horizon_days_primary": int(horizon_days),
        "check_windows": [int(horizon_days), int(horizon_days * 2)],
        "logged_at": int(time.time()),
        "metadata": _to_decimal(metadata),
    }
    if baseline_price is not None:
        item["baseline_price"] = Decimal(str(round(float(baseline_price), 4)))
    DDB.put_item(Item=item)


def process_insider_clusters(ts_iso):
    """Top insider-cluster signals (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] insider-clusters skipped: {e}")
        return 0
    n = 0
    for c in d.get("clusters", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        ticker = c.get("ticker")
        if not ticker:
            continue
        avg_price = c.get("avg_price") or _quote_price(ticker)
        sid = f"insider_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="insider_cluster", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=avg_price, horizon_days=90,
                metadata={
                    "signal_type": c.get("signal_type"),
                    "n_insiders": c.get("n_insiders"),
                    "total_value": float(c.get("total_value") or 0),
                    "has_ceo": bool(c.get("has_ceo")),
                    "has_cfo": bool(c.get("has_cfo")),
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] insider {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} insider-cluster signals")
    return n


def process_smart_money(ts_iso):
    """Top 13F smart-money clusters (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] smart-money skipped: {e}")
        return 0
    n = 0
    for c in d.get("clusters", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        ticker = c.get("ticker")
        if not ticker:
            continue
        price = _quote_price(ticker)
        sid = f"smartmoney_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="smart_money", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=price, horizon_days=180,
                metadata={
                    "signal_types": c.get("signal_types") or [],
                    "n_buyers": c.get("n_buyers"),
                    "legend_buyers": c.get("legend_buyers") or [],
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] smart-money {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} smart-money signals")
    return n


def process_deep_value(ts_iso):
    """Deep-value tier-A and tier-B (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] deep-value skipped: {e}")
        return 0
    n = 0
    for c in d.get("all_qualifying", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        if c.get("flag") in ("FINANCIAL_BOOK_EXCLUDED", "REIT_EXCLUDED"):
            continue
        ticker = c.get("symbol")
        if not ticker:
            continue
        f = c.get("fundamentals") or {}
        price = f.get("price") or _quote_price(ticker)
        sid = f"deepvalue_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="deep_value", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=price, horizon_days=180,
                metadata={
                    "flag": c.get("flag"),
                    "net_cash_pct": float(f.get("net_cash_pct_of_mcap") or 0),
                    "rev_yield": float(f.get("revenue_yield_of_mcap") or 0),
                    "mcap_to_rev": float(f.get("mcap_to_rev") or 0),
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] deep-value {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} deep-value signals")
    return n


def process_eps_velocity(ts_iso):
    """EPS velocity tier-A and tier-B (score >= 70)."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/eps-revision-velocity.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] eps-velocity skipped: {e}")
        return 0
    n = 0
    for c in d.get("all_qualifying", []):
        score = c.get("score") or 0
        if score < MIN_SCORE:
            continue
        ticker = c.get("symbol")
        if not ticker:
            continue
        f = c.get("fundamentals") or {}
        price = f.get("price") or _quote_price(ticker)
        sid = f"epsvelocity_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="eps_velocity", ticker=ticker,
                direction="UP", conviction=score / 100,
                baseline_price=price, horizon_days=120,
                metadata={
                    "flag": c.get("flag"),
                    "fy2_lift_pct": float((c.get("estimates") or {}).get("fy2_lift_pct") or 0),
                    "fwd_rev_growth_pct": float((c.get("estimates") or {}).get("fwd_rev_growth_pct") or 0),
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] eps-velocity {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} eps-velocity signals")
    return n


def process_compound(ts_iso):
    """Top compound signals (>= 2 systems agreeing). High-conviction."""
    try:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key="data/compound-signals.json")["Body"].read())
    except Exception as e:
        print(f"[signal-logger] compound skipped: {e}")
        return 0
    n = 0
    for c in d.get("compound", []):
        compound_score = c.get("compound_score") or 0
        if compound_score < 150:  # only log high-conviction compounds
            continue
        ticker = c.get("symbol")
        if not ticker:
            continue
        price = _quote_price(ticker)
        sid = f"compound_{ticker}_{int(time.time())}"
        try:
            log_signal(
                signal_id=sid, ts_iso=ts_iso,
                source="compound", ticker=ticker,
                direction="UP",
                conviction=min(compound_score / 300, 1.0),  # 300 is high
                baseline_price=price, horizon_days=120,
                metadata={
                    "n_systems": c.get("n_systems"),
                    "systems": c.get("systems") or [],
                },
            )
            n += 1
        except Exception as e:
            print(f"[signal-logger] compound {ticker} put_item: {e}")
    print(f"[signal-logger] logged {n} compound signals")
    return n


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[signal-logger] starting v1.0, MIN_SCORE={MIN_SCORE}")
    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())

    counts = {
        "insider_cluster": process_insider_clusters(ts_iso),
        "smart_money": process_smart_money(ts_iso),
        "deep_value": process_deep_value(ts_iso),
        "eps_velocity": process_eps_velocity(ts_iso),
        "compound": process_compound(ts_iso),
    }
    total = sum(counts.values())
    print(f"[signal-logger] total logged: {total}, breakdown: {counts}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total": total,
            "by_source": counts,
            "duration_s": round(time.time() - started, 2),
        }),
    }
