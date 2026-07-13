"""justhodl-crisis-composite — the Master Crisis Composite (DEFCON 5 -> 1).

The platform runs a dozen excellent but SEPARATE risk engines. In a fast
crisis you need one read, not twelve tabs. This Lambda sits ABOVE every
other risk engine and fuses them into a single crisis-severity score with
a decisive playbook. It does not recompute anything — it consumes the
sidecars the other engines already produce, so it is cheap and always in
sync with the rest of the platform.

INPUTS FUSED (each degrades gracefully if its sidecar is missing/renamed):
  eurodollar-stress    composite_score        USD funding stress 0-100
  crisis-plumbing      composite_stress_score official crisis indices + XCC basis
  credit-stress        composite_regime       IG/HY spread regime
  regime-composite     composite_score        15-module meta-regime (-100..+100)
  vol-surface          composite_stress_score volatility-surface stress
  market-internals     breadth_score          market breadth (inverted = stress)
  global-liquidity     regime                 global central-bank liquidity tide

Each input is normalised to a 0-100 CRISIS CONTRIBUTION (higher = more
crisis). Components are weighted; if some sidecars are missing the weights
are renormalised over what is available, so the score is always defined.

MASTER SCORE -> DEFCON:
  0-20    DEFCON 5  ALL CLEAR        risk-on, press the bet
  20-40   DEFCON 4  NORMAL           standard positioning
  40-60   DEFCON 3  ELEVATED         trim leverage, raise quality
  60-80   DEFCON 2  HIGH STRESS      defensive, hedges on, cash up
  80-100  DEFCON 1  CRISIS           capital preservation -> then hunt

Each level carries a decisive playbook. Telegram fires on a DEFCON change.
Output: data/crisis-composite.json   Schedule: hourly.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/crisis-composite.json"
S3_HISTORY_KEY = "data/crisis-composite-history.json"
HISTORY_MAX = 720  # ~30 days hourly

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[crisis-composite] missing {key}: {e}")
        return None


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def dig(obj, *names):
    """Return the first present value among candidate keys, searching the top
    level and one level of nested dicts. Returns None if nothing matches."""
    if not isinstance(obj, dict):
        return None
    for n in names:
        if n in obj and obj[n] is not None:
            return obj[n]
    for v in obj.values():
        if isinstance(v, dict):
            for n in names:
                if n in v and v[n] is not None:
                    return v[n]
    return None


def clamp(x, lo=0.0, hi=100.0):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return None


def label_to_crisis(label, mapping, default=50.0):
    """Keyword-tolerant label -> crisis score."""
    if not label:
        return None
    s = str(label).upper()
    for kw, val in mapping:
        if kw in s:
            return val
    return default


# keyword maps (checked in order — most severe first)
CREDIT_MAP = [
    ("CRISIS", 92), ("PANIC", 92), ("SEVERE", 85), ("DISTRESS", 82),
    ("WIDE", 72), ("STRESS", 70), ("ELEVATED", 58), ("CAUTION", 56),
    ("NORMAL", 38), ("CALM", 22), ("TIGHT", 20), ("COMPRESS", 18),
]
REGIME_META_MAP = [
    ("CRISIS", 95), ("RISK_OFF", 80), ("RISK-OFF", 80), ("DEFENSIVE", 62),
    ("LATE_CYCLE", 52), ("LATE-CYCLE", 52), ("NORMAL", 40),
    ("GOLDILOCKS", 26), ("MELT_UP", 18), ("MELT-UP", 18),
]
LIQ_MAP = [("CONTRACTING", 70), ("NEUTRAL", 45), ("EXPANDING", 25)]

# CISS (ECB Composite Indicator of Systemic Stress) is a 0..1 index. Regime anchors
# from its historical distribution: calm <0.04, normal ~0.1, elevated ~0.2, stress
# ~0.3, crisis >=0.45 (GFC/COVID/EU-debt peaks ran 0.5-0.8). Mapped 0..1 -> 0..100.
CISS_ANCHORS = [(0.0, 5), (0.04, 20), (0.1, 40), (0.2, 60), (0.3, 75), (0.45, 92), (0.6, 100)]


def comp_eurodollar(d):
    v = dig(d, "composite_score", "score", "stress_score")
    return clamp(v)


def comp_plumbing(d):
    v = dig(d, "composite_stress_score", "composite_score", "score")
    return clamp(v)


def comp_treasury(d):
    # justhodl-treasury-noise: clean funding-stress composite (bill-SOFR / SOFR-EFFR / CP-bill)
    return clamp(dig(d, "treasury_stress", "funding_stress"))


def comp_credit(d):
    # prefer a numeric composite if present, else map the regime label
    v = dig(d, "composite_stress_score", "composite_score")
    if isinstance(v, (int, float)):
        return clamp(v)
    return label_to_crisis(dig(d, "composite_regime", "regime", "composite_signal"),
                            CREDIT_MAP)


def comp_regime(d):
    # composite_score is -100..+100 (positive = risk-on). Invert -> crisis 0-100.
    v = dig(d, "composite_score", "score")
    if isinstance(v, (int, float)):
        return clamp((100.0 - float(v)) / 2.0)
    return label_to_crisis(dig(d, "meta_regime", "regime"), REGIME_META_MAP)


def comp_vol(d):
    v = dig(d, "composite_stress_score", "stress_score", "score")
    return clamp(v)


def comp_internals(d):
    # breadth_score: higher = healthier. Invert -> low breadth = crisis.
    v = dig(d, "breadth_score", "score")
    if isinstance(v, (int, float)):
        return clamp(100.0 - float(v))
    return None


def comp_liquidity(d):
    return label_to_crisis(dig(d, "regime"), LIQ_MAP)


def comp_leading(d):
    # leading-markets emits risk_score 0-100 (high = canary markets contracting)
    return clamp(dig(d, "risk_score"))


def comp_canary(d):
    # canary-grid emits early_warning_level 0-100 (high = leading global stress)
    return clamp(dig(d, "early_warning_level", "level", "score"))


def comp_dollar(d):
    # dollar-radar emits dollar_pressure -100 (DUMP) .. +100 (PUMP). A dollar
    # PUMP is a USD-squeeze / global funding-stress crisis signal; a dollar
    # DUMP (a liquidity flood) carries no crisis weight, so clamp floors it.
    v = dig(d, "dollar_pressure")
    if isinstance(v, (int, float)):
        return clamp(float(v))
    return None


def comp_global_stress(d):
    # global-stress emits global_stress_index 0-100 (high = world equity and
    # bond stress already visible in the tape) -- a direct crisis reading.
    return clamp(dig(d, "global_stress_index"))


def comp_ciss(d):
    # ECB CISS composite (data/ecb-hist/ciss_ea.json: {points:[[date,value],...]}).
    # Latest 0..1 value -> 0..100 crisis score via regime anchors. A premier
    # systemic-stress gauge; euro-area focused but a strong global risk co-indicator.
    pts = d.get("points") or []
    if not pts:
        return None
    try:
        v = float(pts[-1][1])
    except Exception:
        return None
    a = CISS_ANCHORS
    if v <= a[0][0]:
        return float(a[0][1])
    if v >= a[-1][0]:
        return 100.0
    for (x0, y0), (x1, y1) in zip(a, a[1:]):
        if x0 <= v <= x1:
            return clamp(y0 + (y1 - y0) * (v - x0) / (x1 - x0))
    return None


# (sidecar key, weight, extractor, human label)
COMPONENTS = [
    ("data/eurodollar-plumbing.json", 0.20, comp_eurodollar, "USD funding stress (plumbing)"),
    ("data/crisis-plumbing.json",   0.20, comp_plumbing,   "Crisis plumbing / XCC basis"),
    ("data/treasury-noise.json",    0.12, comp_treasury,   "Treasury funding stress (bill-SOFR/repo/CP)"),
    ("data/credit-stress.json",     0.15, comp_credit,     "Credit spreads"),
    ("data/regime-composite.json",  0.15, comp_regime,     "15-module meta-regime"),
    ("data/vol-surface.json",       0.10, comp_vol,        "Volatility surface"),
    ("data/market-internals.json",  0.10, comp_internals,  "Market breadth"),
    ("data/global-liquidity.json",  0.10, comp_liquidity,  "Global liquidity tide"),
    ("data/leading-markets.json",   0.12, comp_leading,    "Global leading markets"),
    ("data/canary-grid.json",       0.15, comp_canary,     "Global early-warning canaries"),
    ("data/global-stress.json",     0.12, comp_global_stress, "Global equity & bond stress"),
    ("data/dollar-radar.json",      0.10, comp_dollar,      "Dollar squeeze pressure"),
    ("data/ecb-hist/ciss_ea.json",  0.12, comp_ciss,        "Euro-area systemic stress (CISS)"),
]

DEFCON = [
    (80, 1, "CRISIS", "var(--red)",
     "Capital preservation FIRST. Cut leverage to zero, hold cash/T-bills/quality. "
     "Do not catch knives early. THEN — once breadth and credit stop deteriorating — "
     "this is where the generational entries are made. Watch the Capitulation engine."),
    (60, 2, "HIGH STRESS", "var(--orange)",
     "Defensive. Trim risk, hedges on, raise cash. Favour quality balance sheets and "
     "cash-flow over story stocks. Stress is active and feeding on itself — do not add "
     "beta. Build a shopping list for when DEFCON 1 capitulation hits."),
    (40, 3, "ELEVATED", "var(--yellow)",
     "Caution. Reduce leverage, tighten stops, lighten the most speculative names. "
     "Not a crisis — but the buffer is thin. Keep dry powder. Quality over junk."),
    (20, 4, "NORMAL", "var(--cyan)",
     "Standard positioning. No systemic stress signal. Run the normal playbook — "
     "stay invested, let the compounders work, keep a modest reserve."),
    (0, 5, "ALL CLEAR", "var(--green)",
     "Risk-on. Liquidity supportive, credit calm, breadth healthy. Press the bet — "
     "this is when leverage and beta are rewarded. Stay alert for late-cycle drift."),
]


def _massive_cross_asset():
    import boto3 as _b
    _c = _b.client("s3", "us-east-1")
    def _r(k):
        try:
            return json.loads(_c.get_object(Bucket=S3_BUCKET, Key=k)["Body"].read())
        except Exception:
            return {}
    fx = _r("data/polygon-fx-regime.json"); fut = _r("data/polygon-futures-curves.json")
    rm = fx.get("regime_metrics") or {}; pd = fx.get("pair_data") or {}
    return {
        "fx_signals": fx.get("regime_signals") or [],
        "usd_synthetic_20d_pct": rm.get("usd_synthetic_20d_pct"),
        "usdjpy": (pd.get("USD_JPY") or {}).get("latest_price"),
        "futures_signals": fut.get("signals") or [],
        "note": "Live Massive FX + futures context (surfaced for the desk; not weighted into the crisis score).",
        "source": "Massive polygon-fx-regime + polygon-futures-curves",
    }


def to_defcon(score):
    for thresh, level, name, color, play in DEFCON:
        if score >= thresh:
            return level, name, color, play
    return 5, "ALL CLEAR", "var(--green)", DEFCON[-1][4]


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[crisis-composite] starting {datetime.now(timezone.utc).isoformat()}")

    components = []
    weight_avail = 0.0
    weighted_sum = 0.0
    for key, weight, extractor, label in COMPONENTS:
        d = get_s3_json(key)
        val = None
        age_h = None
        if d is not None:
            try:
                val = extractor(d)
            except Exception as e:
                print(f"[crisis-composite] extractor {label} err: {e}")
            gen = dig(d, "generated_at", "timestamp", "as_of")
            if gen:
                try:
                    age_h = round((datetime.now(timezone.utc)
                                   - datetime.fromisoformat(str(gen).replace("Z", "+00:00"))
                                   ).total_seconds() / 3600, 1)
                except Exception:
                    pass
        comp = {"source": key.split("/")[-1].replace(".json", ""),
                "label": label, "weight": weight,
                "crisis_contribution": round(val, 1) if val is not None else None,
                "age_hours": age_h, "available": val is not None}
        if val is not None:
            weight_avail += weight
            weighted_sum += weight * val
        components.append(comp)

    if weight_avail == 0:
        return {"statusCode": 500, "body": json.dumps({"error": "no risk sidecars available"})}

    master = weighted_sum / weight_avail  # renormalised over available weight
    level, name, color, playbook = to_defcon(master)

    # which components are screaming
    drivers = sorted([c for c in components if c["available"]],
                     key=lambda c: -(c["crisis_contribution"] or 0))
    top_drivers = [f"{c['label']} ({c['crisis_contribution']:.0f})"
                   for c in drivers[:3] if (c["crisis_contribution"] or 0) >= 55]

    # history + level-change detection
    hist = get_s3_json(S3_HISTORY_KEY) or {"snapshots": []}
    prior_level = hist["snapshots"][-1]["defcon"] if hist.get("snapshots") else None
    prior_score = hist["snapshots"][-1]["score"] if hist.get("snapshots") else None

    trend = None
    if prior_score is not None:
        delta = master - prior_score
        trend = ("deteriorating" if delta > 3 else "improving" if delta < -3
                 else "stable")

    out = {
        "schema_version": "1.0",
        "method": "crisis_composite_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "btp_bund_canary": __import__("wl_series").block({
        "btp_bund": ("TVC:BTPBUND", "BTP–Bund 10y spread"),
        "es_it": ("TVC:ES10Y-TVC:IT10Y", "Bono–BTP 10y spread"),
    }, False),  # ops 3244: series-level fusion
        "wl_research": __import__("wl_fusion").block(('STRESS',)),
        "elapsed_s": round(time.time() - t0, 1),
        "master_crisis_score": round(master, 1),
        "defcon_level": level,
        "defcon_name": name,
        "defcon_color": color,
        "playbook": playbook,
        "trend": trend,
        "components_available": int(round(weight_avail / sum(c[1] for c in COMPONENTS) * len(COMPONENTS))),
        "components": components,
        "primary_drivers": top_drivers or ["no single component in stress territory"],
        "massive_cross_asset": _massive_cross_asset(),
        "scale": {
            "DEFCON 5": "0-20  all clear / risk-on",
            "DEFCON 4": "20-40 normal",
            "DEFCON 3": "40-60 elevated — trim leverage",
            "DEFCON 2": "60-80 high stress — defensive",
            "DEFCON 1": "80-100 crisis — preserve, then hunt",
        },
        "methodology": (
            "Fuses the platform's dedicated risk engines (eurodollar-stress, "
            "crisis-plumbing, credit-stress, regime-composite, vol-surface, "
            "market-internals, global-liquidity) into one crisis-severity score. "
            "Each is normalised to a 0-100 crisis contribution; weights renormalise "
            "over available sidecars so the score is always defined."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=900")

    hist["snapshots"].append({"ts": out["generated_at"], "score": round(master, 1),
                               "defcon": level})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=900")

    if prior_level is not None and prior_level != level:
        arrow = "DETERIORATING ⚠️" if level < prior_level else "improving"
        maybe_telegram(
            f"[crisis] <b>DEFCON CHANGE — {arrow}</b>\n"
            f"<b>DEFCON {prior_level} → DEFCON {level} ({name})</b>\n"
            f"Master crisis score: {master:.0f}/100\n"
            f"Drivers: {', '.join(top_drivers) if top_drivers else 'broad'}\n\n"
            f"{playbook}")

    print(f"[crisis-composite] done {out['elapsed_s']}s score={master:.1f} "
          f"DEFCON {level} ({name}) avail={weight_avail:.2f}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "master_crisis_score": round(master, 1),
        "defcon_level": level, "defcon_name": name, "trend": trend})}
