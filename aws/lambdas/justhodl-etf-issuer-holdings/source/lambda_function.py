"""Daily issuer-published ETF holdings (Bloomberg parity 10/10).

Ops 1060 restored the EventBridge trigger (justhodl-etf-issuer-holdings-daily,
cron(0 4 * * ? *)); the 8fefa5c deploy had been blocked by the missing rule.

For each Phase 1 ETF, fetches holdings from the issuer's own published file,
retains the raw original under ``audit-private/<date>/etf-issuer-holdings/``,
and publishes ``data/etf-issuer-holdings.json`` plus per-ETF detail files.

``as_of`` is always the date stated inside the issuer file -- never "today".
One ETF failing never fails the run (fail-soft per ETF).
"""

import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

import issuer_holdings as ih

BUCKET = os.environ.get("DATA_BUCKET", "justhodl-dashboard-live")
SCHEMA_VERSION = "1.0"


def _s3():
    """Boto3 S3 client for us-east-1."""
    return boto3.client("s3", region_name="us-east-1")


def _put(s3, key, body, content_type):
    """Put bytes to the data bucket."""
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType=content_type)


def _get(s3, key):
    """Get bytes from the data bucket; None when the key is absent."""
    try:
        return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in (
            "NoSuchKey",
            "404",
            "NoSuchBucket",
        ):
            return None
        raise


def _top_positions(records, n=10):
    """Top holdings by raw weight value, best-effort (source-native units)."""
    def weight_of(record):
        """Numeric weight for ranking; unparseable weights sort last."""
        try:
            return float(record["weight"]) if record["weight"] is not None else float("-inf")
        except (TypeError, ValueError):
            return float("-inf")

    top = sorted(records, key=weight_of, reverse=True)[:n]
    return [
        {"ticker": r["ticker"], "name": r["name"], "weight": r["weight"]}
        for r in top
    ]


def lambda_handler(event, context):
    """Daily run: fetch, retain originals, publish index and detail files."""
    s3 = _s3()
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")

    def read_state(key):
        """Read a JSON state object from the data bucket; None when absent."""
        raw = _get(s3, key)
        return json.loads(raw.decode("utf-8")) if raw else None

    def write_state(key, obj):
        """Write a JSON state object to the data bucket."""
        _put(s3, key, json.dumps(obj).encode("utf-8"), "application/json")

    product_map = ih.load_ishares_product_map(now, read_state, write_state)

    by_etf = {}
    failures = {}
    for ticker, issuer in ih.PHASE1_UNIVERSE:
        try:
            result = ih.fetch_etf_holdings(ticker, issuer, product_map=product_map)
        except Exception as exc:  # fail-soft boundary: one ETF never fails the run
            failures[ticker] = f"{type(exc).__name__}: {exc}"
            continue
        if result is None:
            failures[ticker] = "fetch_or_parse_failed"
            continue
        ext = "csv" if result["raw_kind"] == "csv" else "json"
        raw_key = f"audit-private/{day}/etf-issuer-holdings/{ticker}.{ext}"
        _put(
            s3,
            raw_key,
            result["raw"],
            "text/csv" if ext == "csv" else "application/json",
        )
        records = result["records"]
        detail = {
            "ticker": ticker,
            "issuer": issuer,
            "as_of": result["as_of"],
            "acquired_at": now.isoformat(),
            "source_url": result["source_url"],
            "original_ref": raw_key,
            "n_positions": len(records),
            "positions": records,
            "scope": (
                "Issuer-published holdings. as_of is the date stated in the "
                "issuer file; iShares US equity files can lag ~30 days. "
                "Weights are source-native units, not normalized."
            ),
        }
        _put(
            s3,
            f"data/etf-issuer-holdings/{ticker}.json",
            json.dumps(detail).encode("utf-8"),
            "application/json",
        )
        by_etf[ticker] = {
            "issuer": issuer,
            "as_of": result["as_of"],
            "n_positions": len(records),
            "top_10": _top_positions(records),
            "original_ref": raw_key,
            "status": "ok",
        }

    index = {
        "generated_at": now.isoformat(),
        "schema_version": SCHEMA_VERSION,
        "engine": "justhodl-etf-issuer-holdings",
        "universe_size": len(ih.PHASE1_UNIVERSE),
        "by_etf": by_etf,
        "failures": failures,
        "scope": (
            "Daily issuer-published ETF holdings. as_of comes from each "
            "issuer file, never the fetch date. Fail-soft per ETF; see "
            "failures for skipped funds."
        ),
    }
    _put(
        s3,
        "data/etf-issuer-holdings.json",
        json.dumps(index).encode("utf-8"),
        "application/json",
    )
    return {
        "ok": True,
        "etfs_ok": len(by_etf),
        "etfs_failed": len(failures),
        "failures": sorted(failures),
    }
