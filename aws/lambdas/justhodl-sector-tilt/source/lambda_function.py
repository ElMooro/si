"""
justhodl-sector-tilt — Macro-Regime → Sector Tilt Engine.

WHY THIS EXISTS
───────────────
We already have two related systems:
  1. justhodl-sector-rotation — gives MOMENTUM-based sector classifications
     (LEADER/RECOVERING/LAGGING/FALLING based on RS vs SPY)
  2. justhodl-allocator       — gives ASSET-CLASS tilts (SPY/QQQ/TLT/GLD/...)

What's been missing is the bridge: given the current MACRO REGIME
(SLOWING / EXPANSION / CONTRACTION / etc), which of the 11 SPDR
sectors should be overweight/underweight?

The academic playbook is well-established (Stovall, Fidelity sector
strategy, Goldman Marquee):
  EXPANSION       → cyclicals (XLF, XLI, XLB, XLY, XLK) overweight
  SLOWING         → defensives (XLP, XLU, XLV) overweight
  CONTRACTION     → defensives + low-beta overweight, deep cyclicals UW
  STRONG_EXP      → maximal cyclicals, semis-heavy tech
  MUDDLE          → modest defensive bias, neutral on cyclicals

THE INSIGHT (alpha)
───────────────────
Combining (a) regime tilt from this engine with (b) current sector
momentum from sector-rotation surfaces ALIGNMENT vs MISALIGNMENT:

  Regime says XLV should be overweight (defensive in SLOWING)
  +
  sector-rotation says XLV is currently LAGGING (negative 20d RS)
  =
  MISALIGNED → likely mean-reversion BUY opportunity

  Regime says XLF should be underweight (cyclical in SLOWING)
  +
  sector-rotation says XLF is currently LEADING
  =
  MISALIGNED → likely topping pattern, FADE opportunity

INPUTS
──────
  data/macro-nowcast.json      → current regime
  data/sector-rotation.json    → current per-sector momentum/RS

OUTPUT
──────
  data/sector-tilt.json
  {
    regime: "SLOWING",
    tilts: [{ticker, name, regime_tilt, current_state, alignment,
             implication, rationale}],
    summary: {n_overweight, n_underweight, top_buys, top_fades}
  }

SCHEDULE
────────
  cron(45 0/4 * * ? *)  — runs 45 min past every 4h (right after
                          sector-rotation has refreshed at hour:00)

ZERO DETERIORATION
  ✓ Pure consumer of two existing JSON outputs
  ✓ Does NOT touch sector-rotation, allocator, or any upstream Lambda
  ✓ New Lambda + new S3 path + new EB rule
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/sector-tilt.json")
S3_KEY_NOWCAST = os.environ.get("S3_KEY_NOWCAST", "data/macro-nowcast.json")
S3_KEY_ROTATION = os.environ.get("S3_KEY_ROTATION", "data/sector-rotation.json")

S3 = boto3.client("s3", region_name=REGION)


# ─────────────────────────────────────────────────────────────────────────────
# THE PLAYBOOK — academic + practitioner consensus on regime → sector tilts
# Values: -3 (max underweight) … +3 (max overweight)
# Sources: Stovall sector strategy; Fidelity business cycle approach;
#          Goldman cycle playbooks; Pring's six-stage model.
# ─────────────────────────────────────────────────────────────────────────────
SECTOR_TILT_MATRIX = {
    # Sector ETF: { regime: tilt_score, ... }
    "XLF": {  # Financials
        "STRONG EXPANSION": +2, "EXPANSION": +2, "MUDDLE": +1,
        "SLOWING": -1, "CONTRACTION": -2, "RECOVERY": +3,
    },
    "XLE": {  # Energy
        "STRONG EXPANSION": +2, "EXPANSION": +2, "MUDDLE": 0,
        "SLOWING": -1, "CONTRACTION": -2, "RECOVERY": +1,
    },
    "XLB": {  # Materials
        "STRONG EXPANSION": +2, "EXPANSION": +2, "MUDDLE": 0,
        "SLOWING": -2, "CONTRACTION": -2, "RECOVERY": +2,
    },
    "XLI": {  # Industrials
        "STRONG EXPANSION": +2, "EXPANSION": +2, "MUDDLE": +1,
        "SLOWING": -1, "CONTRACTION": -2, "RECOVERY": +2,
    },
    "XLY": {  # Consumer Discretionary
        "STRONG EXPANSION": +2, "EXPANSION": +2, "MUDDLE": +1,
        "SLOWING": -2, "CONTRACTION": -3, "RECOVERY": +2,
    },
    "XLK": {  # Technology
        "STRONG EXPANSION": +2, "EXPANSION": +1, "MUDDLE": 0,
        "SLOWING": 0, "CONTRACTION": -1, "RECOVERY": +1,
    },
    "XLC": {  # Communications
        "STRONG EXPANSION": +1, "EXPANSION": +1, "MUDDLE": 0,
        "SLOWING": 0, "CONTRACTION": -1, "RECOVERY": +1,
    },
    "XLP": {  # Consumer Staples (defensive)
        "STRONG EXPANSION": -2, "EXPANSION": -1, "MUDDLE": +1,
        "SLOWING": +2, "CONTRACTION": +2, "RECOVERY": -1,
    },
    "XLU": {  # Utilities (defensive)
        "STRONG EXPANSION": -2, "EXPANSION": -1, "MUDDLE": +1,
        "SLOWING": +2, "CONTRACTION": +2, "RECOVERY": -1,
    },
    "XLV": {  # Healthcare (defensive but secular growth)
        "STRONG EXPANSION": -1, "EXPANSION": 0, "MUDDLE": +1,
        "SLOWING": +2, "CONTRACTION": +1, "RECOVERY": 0,
    },
    "XLRE": {  # Real Estate (rate-sensitive)
        "STRONG EXPANSION": 0, "EXPANSION": 0, "MUDDLE": +1,
        "SLOWING": -1, "CONTRACTION": -2, "RECOVERY": +2,
    },
}

TILT_LABELS = {
    +3: "MAX OVERWEIGHT", +2: "STRONG OVERWEIGHT", +1: "OVERWEIGHT",
     0: "NEUTRAL",
    -1: "UNDERWEIGHT", -2: "STRONG UNDERWEIGHT", -3: "MAX UNDERWEIGHT",
}

# Brief "why" explanations per regime (the playbook narrative)
REGIME_RATIONALES = {
    "STRONG EXPANSION": "Maximum cyclicality — buy what depends on growth (XLB, XLI, XLY, XLF, XLE). Avoid bond proxies (XLU, XLP) that get reflated away.",
    "EXPANSION": "Cyclicals broadly favored. Tech and Financials lead, defensives lag.",
    "MUDDLE": "Mid-cycle — modest defensive bias as growth slows but no recession yet. Healthcare and Staples become more attractive vs cyclicals.",
    "SLOWING": "Defensives overweight — XLP, XLU, XLV outperform as growth deteriorates. Cyclicals (XLY, XLB, XLI) underperform as earnings get cut.",
    "CONTRACTION": "Maximum defensive posture — XLU and XLP lead, deep cyclicals (XLY, XLB, XLE) get crushed. Bond proxies preferred.",
    "RECOVERY": "Early-cycle reflation — Financials, Industrials, Materials lead the rebound. Defensives lag as flight-to-safety reverses.",
}


# ─────────────────────────────────────────────────────────────────────────────
# CORE LOGIC
# ─────────────────────────────────────────────────────────────────────────────
def load_s3_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[sector-tilt] load fail {key}: {e}")
        return default


def normalize_regime(regime_str):
    """Map various regime label formats to our matrix keys."""
    if not regime_str:
        return "MUDDLE"  # safe default
    r = regime_str.upper().strip()
    # Direct hits
    if r in SECTOR_TILT_MATRIX["XLF"]:
        return r
    # Common variants
    aliases = {
        "STRONG_EXPANSION": "STRONG EXPANSION",
        "STRONGEXPANSION": "STRONG EXPANSION",
        "STRONG-EXPANSION": "STRONG EXPANSION",
        "GROWTH": "EXPANSION",
        "EXPANSIONARY": "EXPANSION",
        "DECELERATION": "SLOWING",
        "DECEL": "SLOWING",
        "RISK_OFF": "CONTRACTION",
        "RISK-OFF": "CONTRACTION",
        "RECESSION": "CONTRACTION",
        "CRISIS": "CONTRACTION",
        "REFLATION": "RECOVERY",
        "EARLY_CYCLE": "RECOVERY",
        "MID_CYCLE": "MUDDLE",
        "NEUTRAL": "MUDDLE",
        "MIXED": "MUDDLE",
    }
    return aliases.get(r, "MUDDLE")


def classify_alignment(tilt_score, momentum_state, rs_20d):
    """Determine if current sector momentum aligns with regime tilt.

    Returns: (alignment, implication, urgency)
      alignment: 'ALIGNED' / 'NEUTRAL' / 'MISALIGNED'
      implication: 'CONFIRMED_BUY' / 'BUY_OPPORTUNITY' / 'FADE_OPPORTUNITY' /
                   'CONFIRMED_AVOID' / 'WATCH' / 'NEUTRAL'
      urgency: 'HIGH' / 'MEDIUM' / 'LOW'
    """
    # Bucketize the regime tilt and current momentum
    regime_dir = "POS" if tilt_score >= 1 else ("NEG" if tilt_score <= -1 else "FLAT")
    rs = float(rs_20d) if rs_20d is not None else 0.0
    mom_dir = "POS" if rs > 1.0 else ("NEG" if rs < -1.0 else "FLAT")

    # Aligned cases
    if regime_dir == "POS" and mom_dir == "POS":
        return ("ALIGNED", "CONFIRMED_BUY", "MEDIUM")
    if regime_dir == "NEG" and mom_dir == "NEG":
        return ("ALIGNED", "CONFIRMED_AVOID", "MEDIUM")

    # Misaligned cases (the alpha)
    if regime_dir == "POS" and mom_dir == "NEG":
        # Regime says OW but currently lagging → mean-reversion buy
        return ("MISALIGNED", "BUY_OPPORTUNITY", "HIGH")
    if regime_dir == "NEG" and mom_dir == "POS":
        # Regime says UW but currently leading → fade/short candidate
        return ("MISALIGNED", "FADE_OPPORTUNITY", "HIGH")

    # Neutral / mixed
    if regime_dir == "FLAT" or mom_dir == "FLAT":
        if regime_dir == "POS":
            return ("NEUTRAL", "WATCH", "LOW")
        if regime_dir == "NEG":
            return ("NEUTRAL", "WATCH", "LOW")
        return ("NEUTRAL", "NEUTRAL", "LOW")

    return ("NEUTRAL", "NEUTRAL", "LOW")


def build_rationale(ticker, name, tilt_score, alignment, implication, regime, momentum_state, rs_20d):
    tilt_label = TILT_LABELS.get(tilt_score, "NEUTRAL")
    rs_str = f"{rs_20d:+.1f}%" if rs_20d is not None else "—"

    if implication == "BUY_OPPORTUNITY":
        return (
            f"Regime ({regime}) calls for {tilt_label} on {name} ({ticker}), "
            f"but current 20d RS is {rs_str} ({momentum_state}). "
            f"Misalignment = mean-reversion entry. The macro tailwind hasn't been "
            f"priced in yet."
        )
    if implication == "FADE_OPPORTUNITY":
        return (
            f"Regime ({regime}) calls for {tilt_label} on {name} ({ticker}), "
            f"but current 20d RS is {rs_str} ({momentum_state}). "
            f"Outperforming despite macro headwind = late-stage rally / topping "
            f"pattern. Fade candidate."
        )
    if implication == "CONFIRMED_BUY":
        return (
            f"Regime ({regime}) favors {name}, and 20d RS is {rs_str} ({momentum_state}) "
            f"— momentum confirms. Continue overweight."
        )
    if implication == "CONFIRMED_AVOID":
        return (
            f"Regime ({regime}) and current momentum both negative on {name} "
            f"(RS {rs_str}). Stay underweight or short."
        )
    return (
        f"{name} is {tilt_label} per regime ({regime}). "
        f"Current 20d RS = {rs_str}. Watch for confirmation."
    )


def build_tilt_card(ticker, regime, sector_data):
    """Build one tilt entry combining regime tilt + current sector data."""
    matrix = SECTOR_TILT_MATRIX.get(ticker, {})
    tilt_score = matrix.get(regime, 0)
    tilt_label = TILT_LABELS.get(tilt_score, "NEUTRAL")

    # Extract current state from sector-rotation entry
    name = sector_data.get("name") or ticker
    emoji = sector_data.get("emoji", "")
    current_state = sector_data.get("regime", "UNKNOWN")  # LEADER/LAGGING/etc
    rs_20d = (sector_data.get("rs_vs_spy") or {}).get("20")
    rs_63d = (sector_data.get("rs_vs_spy") or {}).get("63")
    last_close = sector_data.get("last_close")
    flow_z = sector_data.get("flow_z")
    flow_signal = sector_data.get("flow_signal")
    momentum_quintile = sector_data.get("momentum_quintile")

    alignment, implication, urgency = classify_alignment(
        tilt_score, current_state, rs_20d
    )

    rationale = build_rationale(
        ticker, name, tilt_score, alignment, implication,
        regime, current_state, rs_20d,
    )

    return {
        "ticker": ticker,
        "name": name,
        "emoji": emoji,
        # Regime tilt (the prescription)
        "regime": regime,
        "regime_tilt_score": tilt_score,
        "regime_tilt_label": tilt_label,
        # Current state (the diagnosis)
        "current_state": current_state,
        "rs_20d": rs_20d,
        "rs_63d": rs_63d,
        "last_close": last_close,
        "flow_z": flow_z,
        "flow_signal": flow_signal,
        "momentum_quintile": momentum_quintile,
        # The synthesis (the alpha)
        "alignment": alignment,
        "implication": implication,
        "urgency": urgency,
        "rationale": rationale,
    }


def build_summary(tilts):
    """Aggregate stats across all 11 sector tilts."""
    n_overweight = sum(1 for t in tilts if t["regime_tilt_score"] >= 1)
    n_underweight = sum(1 for t in tilts if t["regime_tilt_score"] <= -1)
    n_neutral = sum(1 for t in tilts if t["regime_tilt_score"] == 0)

    n_aligned = sum(1 for t in tilts if t["alignment"] == "ALIGNED")
    n_misaligned = sum(1 for t in tilts if t["alignment"] == "MISALIGNED")

    # The actionable opportunities (sorted by tilt magnitude × urgency)
    buys = sorted(
        [t for t in tilts if t["implication"] == "BUY_OPPORTUNITY"],
        key=lambda t: (-abs(t["regime_tilt_score"]), t["rs_20d"] or 0),
    )
    fades = sorted(
        [t for t in tilts if t["implication"] == "FADE_OPPORTUNITY"],
        key=lambda t: (-abs(t["regime_tilt_score"]), -(t["rs_20d"] or 0)),
    )
    confirmed_buys = sorted(
        [t for t in tilts if t["implication"] == "CONFIRMED_BUY"],
        key=lambda t: -(t["rs_20d"] or 0),
    )

    return {
        "n_overweight": n_overweight,
        "n_underweight": n_underweight,
        "n_neutral": n_neutral,
        "n_aligned": n_aligned,
        "n_misaligned": n_misaligned,
        "top_buy_opportunities": [t["ticker"] for t in buys[:3]],
        "top_fade_opportunities": [t["ticker"] for t in fades[:3]],
        "top_confirmed_buys": [t["ticker"] for t in confirmed_buys[:3]],
    }


def lambda_handler(event, context):
    started = time.time()

    # 1. Load fresh inputs
    nowcast = load_s3_json(S3_KEY_NOWCAST)
    rotation = load_s3_json(S3_KEY_ROTATION)

    if not nowcast:
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": "Cannot load macro-nowcast"}),
        }
    if not rotation:
        return {
            "statusCode": 500,
            "body": json.dumps({"ok": False, "error": "Cannot load sector-rotation"}),
        }

    # 2. Determine regime
    raw_regime = nowcast.get("regime", "")
    regime = normalize_regime(raw_regime)
    print(f"[sector-tilt] regime: raw={raw_regime!r} normalized={regime!r}")

    # 3. Build per-sector tilt cards
    sectors_list = rotation.get("sectors", [])
    sectors_by_ticker = {s.get("ticker"): s for s in sectors_list}

    tilts = []
    for ticker in SECTOR_TILT_MATRIX.keys():
        sector_data = sectors_by_ticker.get(ticker, {"ticker": ticker, "name": ticker})
        tilts.append(build_tilt_card(ticker, regime, sector_data))

    # Sort: most actionable misalignments first
    tilts.sort(key=lambda t: (
        # MISALIGNED ahead of ALIGNED ahead of NEUTRAL
        {"MISALIGNED": 0, "ALIGNED": 1, "NEUTRAL": 2}.get(t["alignment"], 3),
        # Then by tilt magnitude (strongest tilts first)
        -abs(t["regime_tilt_score"]),
        # Then by urgency
        {"HIGH": 0, "MEDIUM": 1, "LOW": 2}.get(t["urgency"], 3),
    ))

    summary = build_summary(tilts)

    payload = {
        "schema_version": "1.0",
        "method": "macro_regime_to_sector_tilt_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "regime": regime,
        "regime_raw": raw_regime,
        "regime_rationale": REGIME_RATIONALES.get(regime, ""),
        "input_data_age": {
            "macro_nowcast": nowcast.get("generated_at"),
            "sector_rotation": rotation.get("generated_at"),
        },
        "tilts": tilts,
        "summary": summary,
        "matrix_version": "stovall_fidelity_pring_2024",
        "duration_s": round(time.time() - started, 2),
    }

    body = json.dumps(payload, indent=2, default=str).encode()
    S3.put_object(
        Bucket=BUCKET, Key=S3_KEY_OUT, Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "regime": regime,
            "n_overweight": summary["n_overweight"],
            "n_underweight": summary["n_underweight"],
            "n_misaligned": summary["n_misaligned"],
            "top_buys": summary["top_buy_opportunities"],
            "top_fades": summary["top_fade_opportunities"],
            "duration_s": payload["duration_s"],
        }),
    }
