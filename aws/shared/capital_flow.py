"""Shared accessor for the Capital Flow Radar (data/capital-flow-radar.json).

One-line sector-flow context for any engine:
    from capital_flow import capital_flow_context, dollar_tide, leveraged_risk_appetite
    cf = capital_flow_context("NVDA")   # {complex, pump_probability, regime, net_flow_5d_usd, flow_price_divergence}
"""
import json
import boto3

_BUCKET = "justhodl-dashboard-live"
_KEY = "data/capital-flow-radar.json"
_C = {}


def _load():
    if "d" in _C:
        return _C["d"]
    try:
        _C["d"] = json.loads(boto3.client("s3", "us-east-1").get_object(Bucket=_BUCKET, Key=_KEY)["Body"].read())
    except Exception:
        _C["d"] = {}
    return _C["d"]


def _stock_map():
    if "m" in _C:
        return _C["m"]
    m = {}
    for c in (_load().get("complexes") or []):
        ctx = {"complex": c.get("complex"), "pump_probability": c.get("pump_probability"),
               "regime": c.get("regime"), "net_flow_5d_usd": c.get("net_flow_5d_usd"),
               "flow_price_divergence": c.get("flow_price_divergence"), "accelerating": c.get("accelerating")}
        for s in (c.get("ref_stocks") or []):
            s = (s or "").upper()
            # if a stock sits in multiple complexes, keep the highest pump_probability
            if s not in m or (c.get("pump_probability") or 0) > (m[s].get("pump_probability") or 0):
                m[s] = ctx
    _C["m"] = m
    return m


def capital_flow_context(ticker):
    return _stock_map().get((ticker or "").upper())


def dollar_tide():
    return _load().get("dollar_tide", {}) or {}


def leveraged_positioning_board():
    return _load().get("leveraged_positioning", {}) or {}


def leveraged_risk_appetite():
    return leveraged_positioning_board().get("risk_appetite")
