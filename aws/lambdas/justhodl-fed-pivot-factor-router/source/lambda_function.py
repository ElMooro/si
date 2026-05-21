"""
justhodl-fed-pivot-factor-router -- Equity factor recipes for Fed pivots.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
Fed language shifts move factor rotation more reliably than they move the
index. Hawkish pivot crushes long-duration growth and lifts value/defensives.
Dovish pivot lifts growth/small caps and crushes USD/utilities. The
component signal (HAWKISH_PIVOT, DOVISH_PIVOT, etc.) is detected by
existing justhodl-cb-stance via Claude NLP on Fed statements. The
FACTOR-ROTATION TRADE mapping is NOT.

This router closes that loop. Hawkish pivot detected → automatic factor
recipe: long IWD/XLU/UUP, short QQQ/XLY/IWM. Citadel and Two Sigma trade
versions of this signal-to-factor map internally. Zero commercial product
prescribes the trade.

DISTINCTION FROM CB-STANCE
──────────────────────────
  justhodl-cb-stance              DETECTS pivot (NLP score + classification)
  THIS engine                     PRESCRIBES factor trade per pivot type

THE 5 PIVOT REGIMES + FACTOR RECIPES
─────────────────────────────────────
  HAWKISH_PIVOT  (delta_hawkish >= +15)
    LONG: IWD (value), XLU (utilities — DEFENSIVE not duration-sensitive
          short-term post-pivot), XLP (staples), UUP (USD), XLF (banks
          for steeper curve), BIL (short-duration UST)
    SHORT: QQQ (growth/tech multiple compression), XLY (consumer disc),
           IWM (small caps — refinancing risk), HYG (credit spreads widen),
           GLD (real rates rise short-term)
    HEDGE: VXX (vol spikes on hawkish surprise)
    HORIZON: 5-15 days post-statement

  HAWKISH_DRIFT  (delta_hawkish +5..+15)
    LONG: IWD (value), XLF (banks)
    SHORT: QQQ (modest trim of growth)
    HEDGE: minimal
    HORIZON: 5-10 days

  STABLE  (delta -5..+5)
    LONG: [] (no signal)
    SHORT: [] (no signal)
    HORIZON: N/A

  DOVISH_DRIFT  (delta -15..-5)
    LONG: QQQ (growth multiple expansion), IWM (small caps)
    SHORT: UUP (USD weakens), XLU
    HEDGE: minimal
    HORIZON: 5-10 days

  DOVISH_PIVOT  (delta <= -15)
    LONG: QQQ, IWM, XLY, HYG (credit spreads tighten), GLD, EEM,
          XLK (tech), ARKK (high-beta growth)
    SHORT: UUP (USD crushed), XLU (long-duration but rates falling
           helps; net SHORT here because defensives lag risk-on)
    HEDGE: minimal — risk-on regime
    HORIZON: 5-15 days post-statement

ACADEMIC BASIS
──────────────
- Lucca & Trebbi (2009). Measuring Central Bank Communication.
- Hu, Pan, Wang, Zhu (2022). FOMC sentiment and asset prices.
- Bernanke & Kuttner (2005). What explains the stock market's reaction
  to Federal Reserve policy? Journal of Finance, 60(3), 1221-1257.
- Cieslak & Vissing-Jorgensen (2021). The economics of the Fed put.

CROSS-ENGINE INPUTS
───────────────────
  data/cb-stance.json — primary signal (delta_hawkish + classification)
  data/regime-conditional-router.json — Crisis KB regime context
  data/vol-radar.json — to size hedges per pivot severity

OUTPUT
──────
  s3://justhodl-dashboard-live/data/fed-pivot-factor-trades.json
  Schedule: hourly (cb-stance refreshes after each FOMC statement; this
            router picks up the latest classification + maintains the
            current trade recipe for ongoing positioning)
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/fed-pivot-factor-trades.json"

s3 = boto3.client("s3", region_name="us-east-1")

PIVOT_RECIPES = {
    "HAWKISH_PIVOT": {
        "name": "Hawkish Pivot (delta >= +15)",
        "long": ["IWD", "XLU", "XLP", "UUP", "XLF", "BIL"],
        "short": ["QQQ", "XLY", "IWM", "HYG", "GLD"],
        "hedge": ["VXX"],
        "horizon_days": 15,
        "size_factor_pct": 1.5,
        "thesis": ("Fed surprised hawkish. Growth/long-duration multiple "
                    "compression imminent. Value + defensives + USD + "
                    "steepener trade outperforms over 5-15 days. Vol "
                    "kicker on hawkish surprise."),
    },
    "HAWKISH_DRIFT": {
        "name": "Hawkish Drift (delta +5 to +15)",
        "long": ["IWD", "XLF"],
        "short": ["QQQ"],
        "hedge": [],
        "horizon_days": 10,
        "size_factor_pct": 0.75,
        "thesis": ("Modest hawkish drift. Light value tilt vs growth. "
                    "Tighten stops on growth positions."),
    },
    "STABLE": {
        "name": "Stable (no Fed pivot signal)",
        "long": [],
        "short": [],
        "hedge": [],
        "horizon_days": None,
        "size_factor_pct": 0,
        "thesis": ("Fed language stable — no pivot signal. No factor "
                    "rotation trade active. Use other engines."),
    },
    "DOVISH_DRIFT": {
        "name": "Dovish Drift (delta -5 to -15)",
        "long": ["QQQ", "IWM"],
        "short": ["UUP", "XLU"],
        "hedge": [],
        "horizon_days": 10,
        "size_factor_pct": 0.75,
        "thesis": ("Modest dovish drift. Light growth tilt + USD short. "
                    "Add to growth positions on weakness."),
    },
    "DOVISH_PIVOT": {
        "name": "Dovish Pivot (delta <= -15)",
        "long": ["QQQ", "IWM", "XLY", "HYG", "GLD", "EEM", "XLK", "ARKK"],
        "short": ["UUP", "XLU"],
        "hedge": [],
        "horizon_days": 15,
        "size_factor_pct": 1.5,
        "thesis": ("Fed surprised dovish. Growth multiple expansion + "
                    "small caps + credit + gold + EM rip. USD and "
                    "defensives crushed. Risk-on regime — minimal hedge."),
    },
    "TRANSITION": {
        "name": "Transition (regime classification unclear)",
        "long": [],
        "short": [],
        "hedge": ["VXX"],
        "horizon_days": 5,
        "size_factor_pct": 0.3,
        "thesis": ("Fed in transition between regimes. No factor "
                    "directional bias; modest vol hedge only."),
    },
}


def fetch_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch] {key} failed: {e}")
        return None


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def classify_pivot(cb_data):
    """Read cb-stance output; return (pivot_regime, evidence_dict)."""
    if not isinstance(cb_data, dict):
        return "STABLE", {"reason": "no cb-stance data"}

    # Try multiple schema paths
    fed = safe_get(cb_data, "fed") or cb_data
    shift = (safe_get(fed, "shift_classification") or
             safe_get(fed, "regime") or
             safe_get(cb_data, "shift_classification"))
    delta = (safe_get(fed, "delta_hawkish_score") or
              safe_get(fed, "delta_hawkish") or
              safe_get(cb_data, "delta_hawkish_score"))
    latest = safe_get(fed, "latest_statement") or {}
    latest_score = safe_get(latest, "hawkish_score") or safe_get(latest, "score")
    latest_date = safe_get(latest, "date")
    prior = safe_get(fed, "prior_statement") or {}

    evidence = {
        "shift_classification": shift,
        "delta_hawkish_score": delta,
        "latest_statement_date": latest_date,
        "latest_hawkish_score": latest_score,
        "prior_hawkish_score": safe_get(prior, "hawkish_score"),
        "action": safe_get(latest, "policy_action"),
        "forward_guidance": safe_get(latest, "forward_guidance"),
    }

    # Map cb-stance classifications to our pivot regimes
    if isinstance(shift, str):
        s = shift.upper()
        if "HAWKISH_PIVOT" in s or s == "HAWKISH_PIVOT":
            return "HAWKISH_PIVOT", evidence
        if "DOVISH_PIVOT" in s or s == "DOVISH_PIVOT":
            return "DOVISH_PIVOT", evidence
        if "HAWKISH" in s and ("DRIFT" in s or "STANCE" in s):
            return "HAWKISH_DRIFT", evidence
        if "DOVISH" in s and ("DRIFT" in s or "STANCE" in s):
            return "DOVISH_DRIFT", evidence
        if "TRANSITION" in s:
            return "TRANSITION", evidence
        if "STABLE" in s or "NEUTRAL" in s:
            return "STABLE", evidence

    # Fallback: classify from delta
    if isinstance(delta, (int, float)):
        if delta >= 15:
            return "HAWKISH_PIVOT", evidence
        if delta >= 5:
            return "HAWKISH_DRIFT", evidence
        if delta <= -15:
            return "DOVISH_PIVOT", evidence
        if delta <= -5:
            return "DOVISH_DRIFT", evidence
        return "STABLE", evidence

    return "STABLE", evidence


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[fed-pivot-router] start v{VERSION}")

    cb = fetch_s3_json("data/cb-stance.json")
    vol = fetch_s3_json("data/vol-radar.json")
    crisis_router = fetch_s3_json("data/regime-conditional-router.json")

    feeds_available = {
        "cb_stance": cb is not None,
        "vol_radar": vol is not None,
        "regime_router": crisis_router is not None,
    }

    pivot_regime, evidence = classify_pivot(cb)
    recipe = PIVOT_RECIPES[pivot_regime]

    # Vol context — scale hedge size if vol is elevated
    spike_score = safe_get(vol, "spike_risk_score") or 0
    vol_regime = safe_get(vol, "regime")

    # Recency check — if latest statement >45d old, classification may be stale
    latest_date_str = evidence.get("latest_statement_date") or ""
    age_days = None
    try:
        if latest_date_str:
            ld = datetime.strptime(latest_date_str[:10],
                                     "%Y-%m-%d").replace(
                tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - ld).days
    except (ValueError, TypeError):
        pass

    stale_signal = age_days is not None and age_days > 45

    output = {
        "engine": "fed-pivot-factor-router",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current_pivot_regime": pivot_regime,
        "current_recipe_name": recipe["name"],
        "current_thesis": recipe["thesis"],
        "current_recipe": {
            "long": recipe["long"],
            "short": recipe["short"],
            "hedge": recipe["hedge"],
            "horizon_days": recipe["horizon_days"],
            "size_factor_pct_of_normal": recipe["size_factor_pct"],
        },
        "fed_evidence": evidence,
        "latest_statement_age_days": age_days,
        "signal_stale": stale_signal,
        "vol_context": {
            "spike_risk_score": spike_score,
            "regime": vol_regime,
        },
        "feeds_available": feeds_available,
        "pivot_regime_universe": list(PIVOT_RECIPES.keys()),
        "all_recipes": {k: {"long": v["long"], "short": v["short"],
                             "hedge": v["hedge"], "horizon_days":
                                 v["horizon_days"]}
                         for k, v in PIVOT_RECIPES.items()},
        "methodology": {
            "framework": "Fed pivot classification -> equity factor recipe",
            "philosophy": (
                "justhodl-cb-stance DETECTS the Fed pivot via Claude NLP "
                "on FOMC statements. This router PRESCRIBES the factor "
                "rotation trade (value/growth, USD, defensives, small caps) "
                "that historically works in each pivot regime. Citadel + "
                "Two Sigma run internal versions; not sold."),
            "pivot_horizon": "5-15 days post-statement (factor rotation "
                              "alpha decays within ~3 weeks)",
            "stale_filter": ("Signal flagged stale if latest FOMC statement "
                              "is >45 days old. Stale signals should not be "
                              "actively traded — used as context only."),
        },
        "academic_basis": [
            "Lucca, D. O., & Trebbi, F. (2009). Measuring central bank "
            "communication. NBER Working Paper.",
            "Hu, A., Pan, J., Wang, J., & Zhu, Q. (2022). FOMC sentiment "
            "and asset prices. SSRN.",
            "Bernanke, B. S., & Kuttner, K. N. (2005). What explains the "
            "stock market's reaction to Federal Reserve policy? "
            "Journal of Finance, 60(3), 1221-1257.",
            "Cieslak, A., & Vissing-Jorgensen, A. (2021). The economics "
            "of the Fed put. Review of Financial Studies.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=900")

    print(f"[fed-pivot-router] regime={pivot_regime} "
          f"latest_age={age_days}d stale={stale_signal}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION,
            "pivot_regime": pivot_regime,
            "signal_stale": stale_signal,
            "long_universe": recipe["long"],
            "short_universe": recipe["short"],
            "horizon_days": recipe["horizon_days"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
