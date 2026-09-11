"""
justhodl-alpha-confluence — Roadmap #2 + #3

CONFLUENCE DETECTOR (#2)
The Alpha Score Engine gives ONE composite number. But two stocks at α=82
can have very different signal QUALITY.
Stock B is the confluence case. 5+ independent signals firing
simultaneously is rare and high-conviction.

REGIME-DRIVEN WATCHLIST (#3)
Different macro regimes favor different sectors.

OUTPUTS
  S3: signals/confluence.json
  S3: signals/regime-picks.json
"""
import json
import os
import time
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
ALPHA_KEY = "screener/alpha-score.json"
SCREENER_KEY = "screener/data.json"
PREV_STATE_KEY = "signals/confluence-prev.json"
CONFLUENCE_OUT = "signals/confluence.json"
REGIME_OUT = "signals/regime-picks.json"

COMPONENT_FIRING = 70
TIER_S_FIRING_COUNT = 6
TIER_A_FIRING_COUNT = 5
TIER_B_FIRING_COUNT = 4

REGIME_SECTOR_MAP = {
    "EXPANSION": {
        "Technology": +12, "Industrials": +10, "Financial Services": +8,
        "Consumer Cyclical": +8, "Energy": +6, "Communication Services": +4,
        "Basic Materials": +4, "Real Estate": +2,
        "Healthcare": -2, "Consumer Defensive": -4, "Utilities": -6,
    },
    "SLOWING": {
        "Healthcare": +12, "Consumer Defensive": +10, "Utilities": +8,
        "Communication Services": +4, "Real Estate": +2,
        "Technology": -2, "Industrials": -4, "Financial Services": -6,
        "Consumer Cyclical": -8, "Energy": -4, "Basic Materials": -4,
    },
    "STAGFLATION": {
        "Energy": +14, "Basic Materials": +12, "Consumer Defensive": +8,
        "Healthcare": +6, "Utilities": +4, "Real Estate": +2,
        "Technology": -8, "Communication Services": -4,
        "Consumer Cyclical": -10, "Financial Services": -6, "Industrials": -4,
    },
    "RECOVERY": {
        "Financial Services": +14, "Industrials": +12, "Consumer Cyclical": +10,
        "Real Estate": +8, "Basic Materials": +6, "Energy": +4,
        "Technology": +4, "Communication Services": +2,
        "Utilities": -4, "Consumer Defensive": -6, "Healthcare": -2,
    },
    "TIGHTENING": {
        "Energy": +6, "Consumer Defensive": +6, "Healthcare": +4,
        "Utilities": +2, "Financial Services": +2,
        "Technology": -8, "Consumer Cyclical": -6, "Real Estate": -10,
        "Communication Services": -4, "Industrials": -2,
    },
}

DEFAULT_REGIME = "SLOWING"
DEFAULT_REGIME_CONFIDENCE = 0.70

s3 = boto3.client("s3", region_name="us-east-1")
lam_client = boto3.client("lambda", region_name="us-east-1")


def get_current_regime():
    try:
        resp = lam_client.invoke(
            FunctionName="justhodl-macro-nowcast",
            InvocationType="RequestResponse",
            Payload=json.dumps({"action": "current_state"}).encode())
        body = resp["Payload"].read().decode("utf-8")
        d = json.loads(body)
        if isinstance(d, dict) and "body" in d:
            inner = json.loads(d["body"]) if isinstance(d["body"], str) else d["body"]
        else:
            inner = d
        regime = (inner.get("current_regime")
                  or inner.get("regime")
                  or (inner.get("state") or {}).get("regime")
                  or (inner.get("nowcast") or {}).get("regime"))
        conf = (inner.get("confidence")
                or inner.get("regime_confidence")
                or (inner.get("state") or {}).get("confidence")
                or 0.7)
        if regime:
            return str(regime).upper(), float(conf)
    except Exception as e:
        print(f"  macro-nowcast invoke failed: {str(e)[:100]} — using default {DEFAULT_REGIME}")
    return DEFAULT_REGIME, DEFAULT_REGIME_CONFIDENCE


def compute_confluence_count(components):
    if not components: return 0, []
    firing = []
    for k, v in components.items():
        if v is not None and v >= COMPONENT_FIRING:
            firing.append((k, v))
    return len(firing), firing


def classify_confluence_tier(count):
    if count >= TIER_S_FIRING_COUNT: return "S"
    if count >= TIER_A_FIRING_COUNT: return "A"
    if count >= TIER_B_FIRING_COUNT: return "B"
    return "—"


def detect_tier_changes(prev_stocks_by_sym, current_stocks):
    diffs = {"upgrades": [], "downgrades": [], "new_tier_s": [],
             "lost_tier_s": [], "new_tier_a_plus": []}
    for s in current_stocks:
        sym = s["symbol"]
        prev = prev_stocks_by_sym.get(sym)
        if not prev: continue
        cur_tier = s.get("tier") or "—"
        prev_tier = prev.get("tier") or "—"
        if cur_tier == prev_tier: continue
        rec = {"symbol": sym, "from": prev_tier, "to": cur_tier,
               "alpha": s.get("alpha_score"), "prev_alpha": prev.get("alpha_score")}
        tier_order = {"D": 0, "C": 1, "B": 2, "A": 3, "S": 4, "—": -1}
        if tier_order.get(cur_tier, 0) > tier_order.get(prev_tier, 0):
            diffs["upgrades"].append(rec)
        else:
            diffs["downgrades"].append(rec)
        if cur_tier == "S" and prev_tier != "S":
            diffs["new_tier_s"].append(rec)
        if prev_tier == "S" and cur_tier != "S":
            diffs["lost_tier_s"].append(rec)
        if cur_tier in ("S", "A") and prev_tier not in ("S", "A"):
            diffs["new_tier_a_plus"].append(rec)
    return diffs


def apply_regime_adjustment(alpha_score, sector, regime):
    if not sector or alpha_score is None:
        return alpha_score, 0
    sector_map = REGIME_SECTOR_MAP.get(regime, {})
    adj = sector_map.get(sector, 0)
    return max(0, min(100, alpha_score + adj)), adj


def lambda_handler(event, context):
    started = time.time()
    print(f"=== ALPHA CONFLUENCE · {datetime.now(timezone.utc).isoformat()} ===")
    try:
        alpha_data = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=ALPHA_KEY)["Body"].read())
        stocks = alpha_data.get("stocks") or []
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"alpha-score read: {e}"})}
    print(f"  loaded {len(stocks)} stocks from alpha-score")
    regime, regime_confidence = get_current_regime()
    print(f"  regime: {regime} (confidence {regime_confidence:.2f})")
    prev_stocks_by_sym = {}
    try:
        prev = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=PREV_STATE_KEY)["Body"].read())
        for s in (prev.get("stocks_snapshot") or []):
            prev_stocks_by_sym[s["symbol"]] = s
        print(f"  loaded prev state: {len(prev_stocks_by_sym)} stocks")
    except Exception:
        print(f"  no prev state (first run)")
    confluence_records = []
    for s in stocks:
        components = s.get("components") or {}
        confluence_count, firing = compute_confluence_count(components)
        confluence_tier = classify_confluence_tier(confluence_count)
        regime_adj_score, regime_adj = apply_regime_adjustment(
            s.get("alpha_score"), s.get("sector"), regime)
        confluence_records.append({
            "symbol": s["symbol"],
            "name": s.get("name"),
            "sector": s.get("sector"),
            "price": s.get("price"),
            "alpha_score": s.get("alpha_score"),
            "tier": s.get("tier"),
            "rank": s.get("rank"),
            "components": components,
            "confluence_count": confluence_count,
            "confluence_tier": confluence_tier,
            "components_firing": [{"factor": k, "score": v} for k, v in firing],
            "regime_adj": regime_adj,
            "regime_adj_score": regime_adj_score,
            "top_signals": s.get("top_signals") or [],
            "risk_flags": s.get("risk_flags") or [],
        })
    tier_s = [r for r in confluence_records if r["confluence_tier"] == "S"]
    tier_a = [r for r in confluence_records if r["confluence_tier"] == "A"]
    tier_b = [r for r in confluence_records if r["confluence_tier"] == "B"]
    tier_s.sort(key=lambda r: -(r["alpha_score"] or 0))
    tier_a.sort(key=lambda r: -(r["alpha_score"] or 0))
    tier_b.sort(key=lambda r: -(r["alpha_score"] or 0))
    diffs = detect_tier_changes(prev_stocks_by_sym, [
        {"symbol": r["symbol"], "tier": r["tier"], "alpha_score": r["alpha_score"]}
        for r in confluence_records
    ])
    regime_ranked = sorted(
        [r for r in confluence_records if r["alpha_score"] is not None],
        key=lambda r: -r["regime_adj_score"])
    regime_picks = regime_ranked[:40]
    regime_avoids = sorted(
        [r for r in confluence_records if r["regime_adj"] <= -8 and r["alpha_score"] is not None],
        key=lambda r: r["regime_adj_score"])[:20]
    elapsed = time.time() - started
    confluence_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "regime": regime,
        "regime_confidence": regime_confidence,
        "elapsed_seconds": round(elapsed, 2),
        "tier_s_confluence": tier_s,
        "tier_a_confluence": tier_a,
        "tier_b_confluence": tier_b,
        "tier_distribution": {
            "S": len(tier_s), "A": len(tier_a), "B": len(tier_b),
            "below": sum(1 for r in confluence_records if r["confluence_tier"] == "—"),
        },
        "diffs": diffs,
        "thresholds": {
            "component_firing": COMPONENT_FIRING,
            "tier_s_count": TIER_S_FIRING_COUNT,
            "tier_a_count": TIER_A_FIRING_COUNT,
            "tier_b_count": TIER_B_FIRING_COUNT,
        },
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=CONFLUENCE_OUT,
        Body=json.dumps(confluence_payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800")
    regime_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "regime": regime,
        "regime_confidence": regime_confidence,
        "regime_sector_preferences": REGIME_SECTOR_MAP.get(regime, {}),
        "regime_logic": _regime_explanation(regime),
        "regime_picks": regime_picks,
        "regime_avoids": regime_avoids,
        "count_picks": len(regime_picks),
        "count_avoids": len(regime_avoids),
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=REGIME_OUT,
        Body=json.dumps(regime_payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800")
    snapshot = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "stocks_snapshot": [
            {"symbol": r["symbol"], "tier": r["tier"], "alpha_score": r["alpha_score"],
             "confluence_tier": r["confluence_tier"]}
            for r in confluence_records
        ],
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=PREV_STATE_KEY,
        Body=json.dumps(snapshot, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json")
    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "regime": regime,
        "tier_s_count": len(tier_s),
        "tier_a_count": len(tier_a),
        "tier_b_count": len(tier_b),
        "regime_picks": len(regime_picks),
        "upgrades": len(diffs["upgrades"]),
        "new_tier_s": len(diffs["new_tier_s"]),
        "elapsed_seconds": round(elapsed, 2),
    })}


def _regime_explanation(regime):
    return {
        "EXPANSION": "Strong growth + accommodative policy. Cyclicals, tech, financials lead. Defensives lag.",
        "SLOWING":   "Growth decelerating but not yet recessionary. Defensives (Healthcare, Staples, Utilities) outperform. Tech and cyclicals underperform.",
        "STAGFLATION": "Slow growth + high inflation. Real assets (Energy, Materials) win big. Long-duration tech struggles.",
        "RECOVERY":  "Coming out of recession. Cyclicals, financials, industrials lead. Defensives lag.",
        "TIGHTENING": "Fed actively raising rates. Most sectors face headwinds. Quality + cash flow matter most.",
    }.get(regime, "Mixed signals — apply standard alpha rankings.")
