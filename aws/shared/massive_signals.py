"""Shared accessor for the unified Massive-data layer (data/massive-signals.json).

One-line access for any engine:
    from massive_signals import massive_ticker, massive_market, massive_prepump
    tags = massive_ticker("BTBT")     # {gamma_squeeze_score, otm_call_sweep, bullish_flow, prepump_score, ...}
    mkt  = massive_market()           # {gamma_regime, smallcap_bid, sector_flows, fx_signals, futures_signals, ...}
"""
import json
import boto3

_BUCKET = "justhodl-dashboard-live"
_KEY = "data/massive-signals.json"
_CACHE = {}


def _load():
    if "d" in _CACHE:
        return _CACHE["d"]
    try:
        _CACHE["d"] = json.loads(boto3.client("s3", "us-east-1").get_object(
            Bucket=_BUCKET, Key=_KEY)["Body"].read())
    except Exception:
        _CACHE["d"] = {}
    return _CACHE["d"]


def massive_market():
    return _load().get("market", {}) or {}


def massive_ticker(sym):
    return (_load().get("tickers", {}) or {}).get(sym, {}) or {}


def massive_prepump():
    return _load().get("top_prepump", []) or []


def sector_flow_z(sector_etf):
    return (massive_market().get("sector_flows", {}) or {}).get(sector_etf)
