"""justhodl-early-movers

Surfaces tickers whose signals are ACCELERATING — the early-pump signature
that the main convergence radar buries when sorted by raw convergence_score.

The MRVL case study (2026-06-02 +29% pump): MRVL was at rank #13 of 16 ULTRA
tickers by convergence_score, buried under AVGO (100), PLTR (98), AMD (97).
But MRVL had:
  - velocity-acceleration: "ACCELERATION_CONFIRMED (earliness signal)"
  - prior_n_engines 5 → n_engines 6 (engine count growing)
  - momentum-breakout: TIER_B_MOMENTUM at 60D HIGH + RS_20D +28
  - eps-revision-velocity: HIGH_VELOCITY

These are EARLY signals that get drowned out by tickers that have ALREADY
moved (high convergence). This Lambda extracts:
  1. is_ultra_new: just promoted to ULTRA today (new conviction)
  2. is_accelerating: engine count growing
  3. ENGINE-LEVEL earliness signals (any engine with note containing
     'ACCELERATION_CONFIRMED', 'earliness', 'NEW_HIGH', 'BREAKOUT')
  4. Domain-coverage growth (more domains flagging than yesterday)

OUTPUTS:
  data/early-movers.json — sorted list of early-mover candidates
  data/early-movers-history/{date}.json — date-stamped archive (forensic-safe)

Then pre-pump-radar.html can show this as a HERO panel above the main list.
"""
import json
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

EARLINESS_KEYWORDS = [
    "ACCELERATION_CONFIRMED", "earliness", "EARLINESS",
    "NEW_HIGH", "BREAKOUT", "TIER_A", "TIER_B",
    "HIGH_VELOCITY", "RISING_20D",
    "fresh_fire", "FRESH_FIRE", "first_seen",
]


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def extract_engine_signatures(item: dict) -> list:
    """Pull out engine-level earliness signatures. Returns list of strings."""
    sigs = []
    bullish = item.get("bullish_engines") or []
    for e in bullish:
        if not isinstance(e, dict):
            continue
        note = e.get("note") or ""
        for kw in EARLINESS_KEYWORDS:
            if kw.lower() in note.lower():
                sigs.append({
                    "engine": e.get("engine"),
                    "weighted": e.get("weighted"),
                    "note": note[:150],
                    "keyword": kw,
                })
                break
    return sigs


def score_early_mover(item: dict, prior_n_engines: Optional[int]) -> dict:
    """Score how 'early' a ticker's signal is. Higher score = earlier/fresher."""
    score = 0
    factors = []

    # 1. is_ultra_new: +30 (just promoted)
    if item.get("is_ultra_new"):
        score += 30
        factors.append("ultra_new")

    # 2. is_accelerating: +20
    if item.get("is_accelerating"):
        score += 20
        factors.append("accelerating")

    # 3. is_new_high: +20
    if item.get("is_new_high"):
        score += 20
        factors.append("new_high")

    # 4. Engine count growth (rate)
    n_now = item.get("n_engines") or 0
    n_prior = prior_n_engines if prior_n_engines is not None else item.get("prior_n_engines")
    if n_prior is not None and n_now > n_prior:
        delta = n_now - n_prior
        score += min(25, delta * 8)  # +8 per new engine, capped at 25
        factors.append(f"engine_growth_{delta}")

    # 5. Engine-level earliness signatures
    sigs = extract_engine_signatures(item)
    if sigs:
        score += min(20, len(sigs) * 6)
        factors.append(f"earliness_signals_{len(sigs)}")

    # 6. Already in PUMP_PRIMED/PUMP_LIKELY but with prior_n_engines low
    cat = item.get("pump_category")
    if cat in ("PUMP_PRIMED", "PUMP_LIKELY"):
        if n_prior is not None and n_prior <= 3 and n_now >= 4:
            score += 15
            factors.append("category_promotion")

    # 7. ULTRA tier alone gets baseline boost
    if item.get("tier") == "ULTRA":
        score += 5
        factors.append("ultra_tier")

    return {
        "early_score": score,
        "factors": factors,
        "earliness_signatures": sigs,
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[early-movers] starting at {datetime.now(timezone.utc).isoformat()}")

    # Load convergence radar
    rad = _read_json("data/convergence-radar.json") or {}
    items = rad.get("items") or rad.get("tickers") or rad.get("results") or []
    if not items:
        return {"statusCode": 200, "body": json.dumps({"error": "no items in radar"})}
    print(f"[early-movers] processing {len(items)} radar entries")

    # Compute early-mover score for each
    scored = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ticker = item.get("ticker")
        if not ticker:
            continue
        s = score_early_mover(item, None)
        if s["early_score"] >= 10:  # minimum threshold
            scored.append({
                "ticker": ticker,
                "tier": item.get("tier"),
                "pump_category": item.get("pump_category"),
                "convergence_score": item.get("convergence_score"),
                "n_engines": item.get("n_engines"),
                "prior_n_engines": item.get("prior_n_engines"),
                "n_bullish_eng": item.get("n_bullish_eng"),
                "n_bearish_eng": item.get("n_bearish_eng"),
                "pump_likelihood": item.get("pump_likelihood"),
                "directional_score": item.get("directional_score"),
                "is_ultra_new": item.get("is_ultra_new"),
                "is_accelerating": item.get("is_accelerating"),
                "is_new_high": item.get("is_new_high"),
                **s,
            })

    # Sort by early_score desc, then by convergence
    scored.sort(key=lambda x: (-x["early_score"], -(x.get("convergence_score") or 0)))

    # Stratify by tier/category for the UI
    primed_or_likely = [s for s in scored if s.get("pump_category") in ("PUMP_PRIMED", "PUMP_LIKELY")]
    ultra_new = [s for s in scored if s.get("is_ultra_new")]
    accelerating = [s for s in scored if s.get("is_accelerating") or
                    any("engine_growth" in f for f in s.get("factors", []))]
    with_earliness_sigs = [s for s in scored if s.get("earliness_signatures")]

    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    output = {
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "source_generated_at": rad.get("generated_at"),
        "n_radar_items": len(items),
        "n_early_movers": len(scored),
        "top_early_movers": scored[:30],
        "categories": {
            "ultra_new_today":              [s["ticker"] for s in ultra_new],
            "engine_count_accelerating":    [s["ticker"] for s in accelerating],
            "with_earliness_signatures":    [s["ticker"] for s in with_earliness_sigs],
            "in_pump_primed_or_likely":     [s["ticker"] for s in primed_or_likely],
        },
        "alert_tier": [s for s in scored if s.get("early_score") >= 35][:10],
    }

    # Write outputs
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/early-movers.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=600",
    )
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/early-movers-history/{today}.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    # Also write a SNAPSHOT of full radar to history archive (forensic safety)
    s3.put_object(
        Bucket=S3_BUCKET, Key=f"data/convergence-radar-history/{today}.json",
        Body=json.dumps(rad, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=86400",
    )

    print(f"[early-movers] DONE — {len(scored)} early movers, "
          f"{len(output['alert_tier'])} alert-tier (score>=35) in {output['elapsed_s']}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "elapsed_s": output["elapsed_s"],
            "n_early_movers": len(scored),
            "alert_tier": [
                {"ticker": s["ticker"], "score": s["early_score"],
                 "factors": s["factors"], "tier": s["tier"]}
                for s in output["alert_tier"]
            ],
            "top_5": [
                {"ticker": s["ticker"], "score": s["early_score"],
                 "convergence": s["convergence_score"], "n_engines": s["n_engines"],
                 "prior_n_engines": s["prior_n_engines"], "factors": s["factors"]}
                for s in scored[:5]
            ],
        }),
    }
