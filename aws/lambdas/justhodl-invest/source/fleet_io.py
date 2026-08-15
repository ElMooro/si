"""
aws/lambdas/justhodl-invest/source/fleet_io.py
════════════════════════════════════════════════════════════════════════════
Reads other engines' S3 JSON outputs. Two disciplines borrowed directly
from the existing fleet, on purpose:

1. The "fleet:<s3-key>:<dotted.path>" source string convention already
   used by justhodl-tradingview's vault (aws/lambdas/justhodl-tradingview
   fleet alias syntax, e.g. "fleet:data/asia-leads.json:korea_exports.yoy_pct").
   causal_graph.py's Leg.source strings are written in exactly this form so
   they are copy-paste compatible with that existing adapter if Khalid
   later wants one canonical resolver.

2. impact-graph's append-only factor-history.json pattern (aws/lambdas/
   justhodl-impact-graph writes data/impact/factor-history.json nightly and
   regresses off the accrued series once n_obs>=8). Most leaf engines here
   (canary-grid, asia-leads, portwatch, ...) publish CURRENT state, not a
   time series, in their JSON. INVEST cannot z-score a single point, so it
   keeps its OWN accrual file (data/invest/leg-history.json) exactly the
   same shape/spirit as impact-graph's, appended once per run, trimmed to
   ~400 days. This is new state, not a duplicate of impact-graph's file --
   it accrues the NEW commodity/trade legs impact-graph does not carry.

Every read function returns None (never raises) on: missing S3 key, bad
JSON, missing dotted path, or wrong type. A leg with no data is reported by
scoring.py as unavailable, never coerced into a fake zero.
"""
from __future__ import annotations
import json
import re
from datetime import datetime, timezone
from typing import Optional

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
HISTORY_KEY = "data/invest/leg-history.json"
MAX_HISTORY_DAYS = 400

_s3 = None
_cache: dict = {}


def _client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=REGION)
    return _s3


def _get_json(key: str) -> Optional[dict]:
    if key in _cache:
        return _cache[key]
    try:
        body = _client().get_object(Bucket=BUCKET, Key=key)["Body"].read()
        val = json.loads(body)
    except Exception:
        val = None
    _cache[key] = val
    return val


_SOURCE_RE = re.compile(r"^fleet:(?P<key>[^:]+):(?P<path>.+)$")


def parse_source(source: str):
    m = _SOURCE_RE.match(source)
    if not m:
        return None, None
    return m.group("key"), m.group("path")


def dig(obj, dotted_path: str):
    cur = obj
    for part in dotted_path.split("."):
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(part)
        elif isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return cur


def read_leg_value(source: str) -> Optional[float]:
    """Resolve one Leg.source string to today's numeric value, or None."""
    key, path = parse_source(source)
    if key is None:
        return None
    doc = _get_json(key)
    if doc is None:
        return None
    val = dig(doc, path)
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ── history accrual (mirrors impact-graph's factor-history.json pattern) ──

def load_history() -> dict:
    doc = _get_json(HISTORY_KEY)
    return doc if isinstance(doc, dict) else {"days": []}


def append_today(history: dict, leg_values: dict) -> dict:
    """leg_values: {leg_id: float_or_None}. Appends one day's row, trims to
    MAX_HISTORY_DAYS, drops nothing else -- append-only, same as impact-graph."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    days = list(history.get("days", []))
    days = [d for d in days if d.get("date") != today]  # idempotent re-run same day
    days.append({"date": today, "legs": leg_values})
    days = days[-MAX_HISTORY_DAYS:]
    return {"days": days, "updated_at": datetime.now(timezone.utc).isoformat()}


def leg_history_series(history: dict, leg_id: str) -> list:
    out = []
    for d in history.get("days", []):
        v = (d.get("legs") or {}).get(leg_id)
        if isinstance(v, (int, float)):
            out.append(float(v))
    return out


def save_history(history: dict) -> None:
    _client().put_object(
        Bucket=BUCKET, Key=HISTORY_KEY,
        Body=json.dumps(history).encode(), ContentType="application/json",
    )


def put_json(key: str, payload: dict) -> None:
    _client().put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(payload, default=str).encode(), ContentType="application/json",
    )


def get_json(key: str) -> Optional[dict]:
    """Public passthrough for lambda_function to read arbitrary fleet outputs
    (forward-returns, industry-boom, backlog-miner, catalyst, opportunity-engine,
    impact_mapper graph/betas) that don't fit the single-leg Leg.source shape."""
    return _get_json(key)
