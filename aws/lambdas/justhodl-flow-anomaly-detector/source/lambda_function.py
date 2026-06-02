"""justhodl-flow-anomaly-detector

ETF flow-specific anomaly detection (complement to v2 macro detector).

Where justhodl-anomaly-detector v2 covers macro stress (VIX, credit, rates,
funding, cross-asset), this detector covers the ETF flow + constituent
pressure layer specifically. They run on different schedules and emit
different alerts — together they cover both top-down (macro stress) and
bottom-up (capital flow rotation) early warning.

DETECTORS (6 families):
  1. EXTREME_FLOW: |z|>=2.5σ in any tracked ETF
  2. PERSISTENT_FLOW: persistence_days >= 3 with |z|>=1.5σ
  3. CONSTITUENT_DIVERGENCE: sector ETF flow opposite to its constituents'
     aggregate pressure (institutional rotation WITHIN sector)
  4. FLOW_REGIME_VELOCITY: composite score moved 25+ points day-over-day
  5. CROSS_TIMEFRAME_DIVERGENCE: 5d and 21d flow disagree on direction
  6. SMART_DUMB_REVERSAL: smart-money & dumb-money sectors broke their
     usual relative pattern

OUTPUTS:
  flow-anomalies/daily.json    — all flow anomalies
  flow-anomalies/alerts.json   — high-severity (>=7) subset
  flow-anomalies/history/{date}.json

Schedule: cron(0 23 * * ? *) = 18:00 ET (after constituents 17:45)
"""
import json
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def _list_history_keys(prefix: str, n: int = 30) -> list:
    keys = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in (page.get("Contents") or []):
            k = obj["Key"]
            if k.endswith(".json"):
                keys.append(k)
    return sorted(keys, reverse=True)[:n]


# ═════════════════════════════════════════════════════════════════════
# DETECTORS
# ═════════════════════════════════════════════════════════════════════
def detect_extreme_flow(daily: dict) -> list:
    out = []
    metrics = daily.get("metrics", [])
    for m in metrics:
        z = m.get("flow_zscore_90d")
        if z is None or abs(z) < 2.5:
            continue
        direction = "INFLOW" if z > 0 else "OUTFLOW"
        severity = min(10, int(round(abs(z) * 2)))
        out.append({
            "type": "EXTREME_FLOW",
            "severity": severity,
            "subject": m.get("ticker"),
            "description": (
                f"{m.get('ticker')} at z={z:+.2f}σ ({direction}); "
                f"5d ${(m.get('flow_5d_usd') or 0)/1e6:+,.0f}M, "
                f"%AUM 5d {m.get('pct_aum_5d')}%. "
                f"Top decile move — institutional rarity."
            ),
            "data": {
                "ticker": m.get("ticker"),
                "zscore_90d": z,
                "flow_5d_usd": m.get("flow_5d_usd"),
                "pct_aum_5d": m.get("pct_aum_5d"),
                "signal_label": m.get("signal_label"),
                "subcategory": m.get("subcategory"),
                "persistence_days": m.get("persistence_days"),
            },
            "actionable": True,
        })
    return out


def detect_persistent_flow(daily: dict) -> list:
    out = []
    for m in daily.get("metrics", []):
        z = m.get("flow_zscore_90d")
        days = m.get("persistence_days") or 0
        if z is None or abs(z) < 1.5 or days < 3:
            continue
        severity = min(10, int(round(abs(z) * 1.5 + days * 0.6)))
        direction = "INFLOW" if z > 0 else "OUTFLOW"
        out.append({
            "type": "PERSISTENT_FLOW",
            "severity": severity,
            "subject": m.get("ticker"),
            "description": (
                f"{m.get('ticker')} sustained {direction} for {days} days "
                f"(z={z:+.2f}σ today). Persistent flows historically precede "
                f"5-21 day price continuation."
            ),
            "data": {
                "ticker": m.get("ticker"),
                "zscore_90d": z,
                "persistence_days": days,
                "flow_5d_usd": m.get("flow_5d_usd"),
                "subcategory": m.get("subcategory"),
            },
            "actionable": True,
        })
    return out


def detect_constituent_divergence(daily: dict, constituent_pressure: dict) -> list:
    """Sector ETF flow direction opposite to its constituents' aggregate."""
    out = []
    if not constituent_pressure:
        return out
    metrics = daily.get("metrics", [])
    sector_etfs = {"XLK","XLF","XLE","XLV","XLP","XLY","XLI","XLB","XLU","XLRE","XLC"}
    sector_data = {m["ticker"]: m for m in metrics if m["ticker"] in sector_etfs}
    pressures = constituent_pressure.get("top_constituents_by_pressure", []) or []

    for etf_ticker, etf in sector_data.items():
        z = etf.get("flow_zscore_90d")
        if z is None or abs(z) < 1.0:
            continue
        # Find constituent stocks whose top contributor is THIS sector
        contributing = []
        for p in pressures:
            for c in (p.get("contributing_etfs") or [])[:3]:
                if c.get("etf") == etf_ticker:
                    contributing.append(p)
                    break
        if not contributing:
            continue
        same_dir, opposite_dir = [], []
        for p in contributing:
            s5 = p.get("total_pressure_5d_usd") or 0
            if (z > 0 and s5 > 0) or (z < 0 and s5 < 0):
                same_dir.append(p)
            elif (z > 0 and s5 < 0) or (z < 0 and s5 > 0):
                opposite_dir.append(p)
        if len(opposite_dir) < 2:
            continue
        sector_dir = "INFLOW" if z > 0 else "OUTFLOW"
        top = ", ".join(
            f"{p.get('stock')} ${(p.get('total_pressure_5d_usd') or 0)/1e6:+.0f}M"
            for p in opposite_dir[:5]
        )
        severity = 7 if len(opposite_dir) >= 3 else 6
        out.append({
            "type": "CONSTITUENT_DIVERGENCE",
            "severity": severity,
            "subject": etf_ticker,
            "description": (
                f"{etf_ticker} sector {sector_dir} (z={z:+.2f}σ) but "
                f"{len(opposite_dir)} top constituents show OPPOSITE pressure: "
                f"{top}. Institutional rotation WITHIN sector."
            ),
            "data": {
                "sector_etf": etf_ticker,
                "sector_zscore": z,
                "n_opposite": len(opposite_dir),
                "n_same": len(same_dir),
                "opposite_stocks": [
                    {"stock": p["stock"], "pressure_5d_usd": p["total_pressure_5d_usd"]}
                    for p in opposite_dir[:5]
                ],
            },
            "actionable": True,
        })
    return out


def detect_flow_regime_velocity(today_composite: dict, prior_composites: list) -> list:
    out = []
    if not prior_composites:
        return out
    prior = prior_composites[0]
    prior_scores = prior.get("composite_scores") or {}
    for k in ["defensive_rotation","smart_vs_dumb","risk_on_off",
               "domestic_vs_intl","growth_vs_value","credit_stress"]:
        today_score = (today_composite.get(k) or {}).get("score")
        prior_score = prior_scores.get(k)
        if today_score is None or prior_score is None:
            continue
        delta = today_score - prior_score
        if abs(delta) < 25:
            continue
        severity = min(10, int(round(abs(delta) / 8)))
        out.append({
            "type": "FLOW_REGIME_VELOCITY",
            "severity": severity,
            "subject": k,
            "description": (
                f"Composite {k} moved {delta:+.0f} pts day-over-day "
                f"({prior_score:+.0f} → {today_score:+.0f}). Regime in motion."
            ),
            "data": {"composite": k, "prior": prior_score, "today": today_score, "delta": delta},
            "actionable": True,
        })
    return out


def detect_cross_timeframe_divergence(daily: dict) -> list:
    """5d flow vs 21d flow disagree on direction = trend transition."""
    out = []
    for m in daily.get("metrics", []):
        f5 = m.get("flow_5d_usd") or 0
        f21 = m.get("flow_21d_usd") or 0
        # Significant magnitudes on both
        if abs(f5) < 50e6 or abs(f21) < 200e6:
            continue
        # Opposite signs = direction change in last week
        if (f5 > 0 and f21 < 0) or (f5 < 0 and f21 > 0):
            ratio = abs(f5) / abs(f21) if f21 else 0
            # Higher severity if 5d flow is meaningful fraction of 21d
            severity = min(8, int(round(3 + ratio * 5)))
            direction = "ACCELERATING_INFLOW" if f5 > 0 else "ACCELERATING_OUTFLOW"
            out.append({
                "type": "CROSS_TIMEFRAME_DIVERGENCE",
                "severity": severity,
                "subject": m.get("ticker"),
                "description": (
                    f"{m.get('ticker')} 5d {direction} (${f5/1e6:+.0f}M) reverses "
                    f"21d trend (${f21/1e6:+.0f}M). Inflection point."
                ),
                "data": {
                    "ticker": m.get("ticker"),
                    "flow_5d_usd": f5,
                    "flow_21d_usd": f21,
                    "subcategory": m.get("subcategory"),
                },
                "actionable": True,
            })
    return out


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[flow-anomaly] starting at {datetime.now(timezone.utc).isoformat()}")

    daily = _read_json("etf-flows/daily.json") or {}
    composite = _read_json("etf-flows/composite.json") or {}
    constituent_pressure = _read_json("etf-flows/constituent-pressure.json") or {}

    # Load flow history for trend detectors
    history_keys = _list_history_keys("etf-flows/history/", n=15)
    flow_history = []
    for k in history_keys:
        h = _read_json(k)
        if h:
            comp = h.get("composite") or {}
            flow_history.append({
                "date": k.split("/")[-1].replace(".json", ""),
                "composite_scores": {
                    name: (comp.get(name) or {}).get("score")
                    for name in ["defensive_rotation","smart_vs_dumb","risk_on_off",
                                  "domestic_vs_intl","growth_vs_value","credit_stress"]
                },
            })
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prior_composites = [h for h in flow_history if h["date"] != today_date]
    print(f"[flow-anomaly] {len(prior_composites)} prior days available for trend detection")

    today_composite = composite.get("composite") or {}

    print("[flow-anomaly] running 5 detectors...")
    anomalies = []
    anomalies.extend(detect_extreme_flow(daily))
    anomalies.extend(detect_persistent_flow(daily))
    anomalies.extend(detect_constituent_divergence(daily, constituent_pressure))
    anomalies.extend(detect_flow_regime_velocity(today_composite, prior_composites))
    anomalies.extend(detect_cross_timeframe_divergence(daily))

    anomalies.sort(key=lambda a: (-a["severity"], a["type"]))
    now_iso = datetime.now(timezone.utc).isoformat()
    for a in anomalies:
        a["trigger_at"] = now_iso

    alerts = [a for a in anomalies if a["severity"] >= 7]

    elapsed = round(time.time() - t0, 1)
    print(f"[flow-anomaly] DONE — {len(anomalies)} anomalies, {len(alerts)} high-sev in {elapsed}s")

    by_type = {}
    for a in anomalies:
        by_type[a["type"]] = by_type.get(a["type"], 0) + 1

    output = {
        "generated_at": now_iso,
        "elapsed_s": elapsed,
        "as_of_flow_data": daily.get("generated_at"),
        "n_total": len(anomalies),
        "n_alerts_high_sev": len(alerts),
        "by_type_count": by_type,
        "anomalies": anomalies,
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key="flow-anomalies/daily.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )
    s3.put_object(
        Bucket=S3_BUCKET, Key="flow-anomalies/alerts.json",
        Body=json.dumps({"generated_at": now_iso, "alerts": alerts},
                        default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"flow-anomalies/history/{today_date}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "elapsed_s": elapsed,
            "n_total": len(anomalies),
            "n_alerts": len(alerts),
            "by_type": by_type,
            "top_5_alerts": [
                {"type": a["type"], "severity": a["severity"],
                 "subject": a["subject"], "description": a["description"][:180]}
                for a in alerts[:5]
            ],
        }),
    }
