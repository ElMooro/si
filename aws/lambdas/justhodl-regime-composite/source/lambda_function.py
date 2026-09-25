"""
justhodl-regime-composite — Bloomberg-Gap 15-Module Meta-Regime Engine

WHY THIS EXISTS
═══════════════
The 15 Bloomberg-Gap modules each emit a regime label about their slice of
the market (vol, gamma, credit, funding, retail, news flow, central bank,
global cross-section, commodities, insider behavior, etc.). Reading them
one at a time is useful but loses the forest. This Lambda aggregates them
into a single META_REGIME — like Bloomberg's market-state summary across
the top of every terminal — plus a 7-dimension dashboard.

THE 7 DIMENSIONS
════════════════
  VOL          dealer-gex, vix-curve              negative = stress, positive = calm
  RISK_ON      credit, crypto-funding, retail     positive = risk appetite
  LIQUIDITY    dix, options-flow                  positive = supportive
  POLICY       cb-stance                          negative = hawkish (restrictive)
  REFLATION    commodity, global                  positive = inflation/cyclical bias
  SMART_MONEY  insider, 13f, finra-short          positive = institutional buying
  FUNDAMENTALS earnings-nlp, news-velocity        positive = earnings strength

Each dimension score = mean(constituent polarities), range [-1, +1].
Composite score = weighted mean × 100, range [-100, +100].

THE 7 META-REGIMES (rule-based classifier)
══════════════════════════════════════════
  MELT_UP            risk + liq high, vol low, fundamentals strong
  GOLDILOCKS         risk + reflation moderate, policy dovish, vol low
  LATE_CYCLE         risk-on but policy hawkish + reflation high (← typical pre-correction)
  NORMAL             balanced, no signal extremes
  DEFENSIVE          2-3 dimensions turning negative
  RISK_OFF           vol up, credit widening, retail capitulating
  CRISIS             5+ dimensions negative or vol explosion

OUTPUT
══════
data/regime-composite.json — hourly snapshot
  schema_version, generated_at, duration_s
  meta_regime, meta_narrative, composite_score
  dimensions: { vol, risk_on, liquidity, policy, reflation, smart_money, fundamentals }
  modules: 15 entries each with { label, regime, signal, polarity, dimension, modified, age_minutes }
  changed_from_prior, history (last 24 hourly snapshots)

TELEGRAM ALERT
══════════════
Fires when meta_regime changes from prior run (state in S3 sidecar).

SCHEDULE
════════
cron(15 * ? * * *)  — every hour at :15 (after most modules refresh)
"""
import io
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from ciss_readthrough import context as ciss_context, SERIES as CISS_SERIES
from holdings_authority import context as holdings_context

VERSION = "1.1.0"
REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/regime-composite.json"
HISTORY_KEY = "data/regime-composite-history.json"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

S3 = boto3.client("s3", region_name=REGION)


# ═══════════════════════════════════════════════════════════════════════════
# MODULE CONFIGURATIONS — derived from ops/549 field map
# ═══════════════════════════════════════════════════════════════════════════

def get_path(obj, path):
    """Walk a dotted path through nested dicts. Returns None if any step fails."""
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


# Polarity maps: regime label → score in {-1, 0, +1}
# Positive = market-supportive; Negative = stress; 0 = neutral/unknown
POLARITY_DEALER_GEX = {
    "MOSTLY_POSITIVE_GAMMA": +1, "POSITIVE_GAMMA": +1, "STRONG_POSITIVE_GAMMA": +1,
    "MIXED_GAMMA": 0, "NEAR_FLIP": 0,
    "MOSTLY_NEGATIVE_GAMMA": -1, "NEGATIVE_GAMMA": -1, "STRONG_NEGATIVE_GAMMA": -1,
}
POLARITY_VIX = {
    "STEEP_CONTANGO": +1, "NORMAL_CONTANGO": +1, "FLAT_CONTANGO": 0,
    "FLAT": 0, "MILD_BACKWARDATION": -1, "BACKWARDATION": -1, "DEEP_BACKWARDATION": -1,
    "FRONT_INVERTED": -1, "STRESS": -1,
}
POLARITY_FUNDING = {
    "BALANCED": +1, "MILD_LONG_BIAS": 0, "MILD_SHORT_BIAS": 0,
    "OVERHEATED_LONGS": -1, "EUPHORIA": -1, "LEVERAGE_EXTREME": -1,
    "OVERSHORT": +1,  # shorts crowded = squeeze risk = bullish for spot
    "PANIC_SHORTS": +1,
}
POLARITY_CREDIT = {
    "BENIGN": +1, "MELTUP_PRONE": +1,  # tight = melt-up, complacency, supportive short-term
    "NEUTRAL": 0, "WIDENING": -1, "STRESSED": -1, "DISTRESSED": -1,
    "TIGHT": +1, "BENIGN_RISK_ON": +1,
}
POLARITY_RETAIL = {
    "NORMAL": +1, "ELEVATED_INTEREST": 0, "WSB_FRENZY": -1, "MEME_EUPHORIA": -1,
    "PANIC": -1, "CAPITULATION": +1,  # capitulation = contrarian buy
}
POLARITY_NEWS = {
    "ATTENTION_NORMAL": +1, "ATTENTION_LOW": 0, "ATTENTION_CONCENTRATED": 0,
    "ATTENTION_DISPERSED": +1, "PANIC_HEADLINES": -1, "FRENZY": -1,
    "UNKNOWN": 0,
}
POLARITY_CB = {
    "DOVISH_PIVOT": +1, "DOVISH": +1, "MILD_DOVISH": +1,
    "NEUTRAL": 0,
    "MILD_HAWKISH": -1, "HAWKISH": -1, "HAWKISH_HIKE": -1,
}
POLARITY_GLOBAL = {
    "GLOBAL_BULL": +1, "US_LED_BULL": +1, "INTL_LED_BULL": +1,
    "MIXED": 0, "NARROW_LEADERSHIP": 0,
    "GLOBAL_BEAR": -1, "DECOUPLING_DOWN": -1, "EM_DISTRESS": -1,
}
POLARITY_COMMOD = {
    # Commodity reflation = bullish for cyclicals/EM short-term but bearish for bonds & Fed-sensitive
    # We treat as mildly POSITIVE for equity reflation trade but flag separately
    "INFLATIONARY_PUSH": +1, "REFLATION": +1, "STAGFLATION_RISK": -1,
    "DEFLATION": -1, "DEMAND_DESTRUCTION": -1,
    "BALANCED": 0, "MIXED": 0, "ENERGY_LED": +1,
}
POLARITY_FINRA = {
    "NORMAL": +1, "LOW_SHORT_INTEREST": +1,
    "ELEVATED": 0, "CROWDED_SHORT": +1,  # contrarian-bullish (squeeze risk)
    "PANIC_COVERING": -1,
}
POLARITY_EARNINGS = {
    "EARNINGS_STRENGTH": +1, "MARGIN_EXPANSION": +1, "MIXED_DEMAND": 0,
    "EARNINGS_DETERIORATION": -1, "MARGIN_COMPRESSION": -1, "DEMAND_WEAKNESS": -1,
}


# Modules config: how to read each one
MODULES_CFG = [
    {
        "label": "Dealer Gamma", "emoji": "📐", "key": "data/dealer-gex.json",
        "regime_path": "market_composite.composite_regime",
        "signal_path": "market_composite.composite_signal",
        "polarity_map": POLARITY_DEALER_GEX, "dimension": "vol",
        "page": "/gex/",
    },
    {
        "label": "VIX Term Structure", "emoji": "📈", "key": "data/vix-curve.json",
        "regime_path": "composite_regime",
        "signal_path": "composite_signal",
        "polarity_map": POLARITY_VIX, "dimension": "vol",
        "page": "/vix/",
    },
    {
        "label": "Credit Stress", "emoji": "💳", "key": "data/credit-stress.json",
        "regime_path": "composite_regime",
        "signal_path": "composite_signal",
        "polarity_map": POLARITY_CREDIT, "dimension": "risk_on",
        "page": "/credit/",
    },
    {
        "label": "Crypto Funding", "emoji": "🪙", "key": "data/crypto-funding.json",
        "regime_path": "composite_regime",
        "signal_path": "composite_signal",
        "polarity_map": POLARITY_FUNDING, "dimension": "risk_on",
        "page": "/funding/",
    },
    {
        "label": "Retail Sentiment", "emoji": "🎰", "key": "data/retail-sentiment.json",
        "regime_path": "market_regime",
        "signal_path": "market_regime_signal",
        "polarity_map": POLARITY_RETAIL, "dimension": "risk_on",
        "page": "/retail/",
    },
    {
        "label": "DIX (Dark-Pool Index)", "emoji": "🌑", "key": "data/dix-history.json",
        "regime_path": None,  # derived from latest DIX value
        "signal_path": None,
        "polarity_map": None, "dimension": "liquidity",
        "page": "/dix/",
        "derive": "dix",
    },
    {
        "label": "Options Flow", "emoji": "⚡", "key": "data/options-flow-scanner.json",
        "regime_path": None, "signal_path": None,
        "polarity_map": None, "dimension": "liquidity",
        "page": "/options-flow/",
        "derive": "options_flow",
    },
    {
        "label": "CB Stance (Fed)", "emoji": "🏛️", "key": "data/cb-stance.json",
        "regime_path": "fed.regime",
        "signal_path": "fed.regime_signal",
        "polarity_map": POLARITY_CB, "dimension": "policy",
        "page": "/cb/",
    },
    {
        "label": "Global Markets", "emoji": "🌍", "key": "data/global-markets.json",
        "regime_path": "composite_regime",
        "signal_path": "composite_signal",
        "polarity_map": POLARITY_GLOBAL, "dimension": "reflation",
        "page": "/global/",
    },
    {
        "label": "Commodity Curves", "emoji": "🛢️", "key": "data/commodity-curves.json",
        "regime_path": "composite_regime",
        "signal_path": "composite_signal",
        "polarity_map": POLARITY_COMMOD, "dimension": "reflation",
        "page": "/commodities/",
    },
    {
        "label": "Insider Clusters", "emoji": "💼", "key": "data/insider-clusters.json",
        "regime_path": None, "signal_path": None,
        "polarity_map": None, "dimension": "smart_money",
        "page": "/insider/",
        "derive": "insider",
    },
    {
        "label": "13F Smart Money", "emoji": "🐋", "key": "data/13f-positions.json",
        "regime_path": None, "signal_path": None,
        "polarity_map": None, "dimension": "smart_money",
        "page": "/smart-money/",
        "derive": "thirteenf",
    },
    {
        "label": "FINRA Short-Sale Volume", "emoji": "🩸", "key": "data/finra-short.json",
        "regime_path": "market_composite.regime",
        "signal_path": None,
        "polarity_map": POLARITY_FINRA, "dimension": "smart_money",
        "page": "/short/",
    },
    {
        "label": "Earnings NLP", "emoji": "🎙️", "key": "data/earnings-nlp.json",
        "regime_path": "market_summary.regime",
        "signal_path": "market_summary.signal",
        "polarity_map": POLARITY_EARNINGS, "dimension": "fundamentals",
        "page": "/earnings/",
    },
    {
        "label": "News Velocity", "emoji": "📰", "key": "data/news-velocity.json",
        "regime_path": "composite_regime",
        "signal_path": "composite_signal",
        "polarity_map": POLARITY_NEWS, "dimension": "fundamentals",
        "page": "/news-velocity/",
    },
    {
        "label": "ECB CISS (Systemic Stress)", "emoji": "🌡", "key": "data/ciss-stress.json",
        "regime_path": None, "signal_path": None,
        "polarity_map": None, "dimension": "liquidity",
        "page": "/ciss.html",
        "derive": "ciss",
    },
    {
        "label": "CISS · Money-Market Contribution", "emoji": "💵", "key": "data/ciss-stress.json",
        "regime_path": None, "signal_path": None, "polarity_map": None,
        "dimension": "liquidity", "page": "/ciss.html", "derive": "ciss_mm",
    },
    {
        "label": "CISS · Bond-Market Contribution", "emoji": "📉", "key": "data/ciss-stress.json",
        "regime_path": None, "signal_path": None, "polarity_map": None,
        "dimension": "vol", "page": "/ciss.html", "derive": "ciss_bond",
    },
    {
        "label": "CISS · Equity-Market Contribution", "emoji": "📊", "key": "data/ciss-stress.json",
        "regime_path": None, "signal_path": None, "polarity_map": None,
        "dimension": "vol", "page": "/ciss.html", "derive": "ciss_equity",
    },
]


# ═══════════════════════════════════════════════════════════════════════════
# DERIVATION HELPERS — for modules without an explicit regime field
# ═══════════════════════════════════════════════════════════════════════════

def derive_dix(payload):
    """DIX history → recent percentile.
    Schema: parallel arrays dates[], dix_pct[], price[], gex_billions[].
    Higher DIX = more dark-pool buying = supportive (positive)."""
    try:
        # New schema: parallel arrays
        dix_arr = payload.get("dix_pct")
        if isinstance(dix_arr, list) and len(dix_arr) >= 10:
            vals = [v for v in dix_arr[-60:] if isinstance(v, (int, float))]
        else:
            # Legacy schema: list of dicts
            hist = payload.get("history") or payload.get("data") or []
            if not hist or not isinstance(hist, list): return None, None, 0
            vals = []
            for x in hist[-60:]:
                v = x.get("dix") if isinstance(x, dict) else None
                if isinstance(v, (int, float)): vals.append(v)
        if len(vals) < 10: return None, None, 0
        latest = vals[-1]
        rank = sum(1 for v in vals if v <= latest) / len(vals)
        if rank >= 0.75:
            return "DIX_HIGH", f"DIX {latest:.1f}% — top-quartile dark-pool buying (supportive)", +1
        if rank <= 0.25:
            return "DIX_LOW", f"DIX {latest:.1f}% — bottom-quartile dark-pool buying (cautious)", -1
        return "DIX_NEUTRAL", f"DIX {latest:.1f}% — mid-range dark-pool buying (rank {int(rank*100)}%)", 0
    except Exception:
        return None, None, 0


def derive_options_flow(payload):
    """Options flow → tier_a count = signal density."""
    try:
        summary = payload.get("summary") or {}
        tier_a = summary.get("tier_a") or []
        n_tier_a = len(tier_a) if isinstance(tier_a, list) else 0
        if n_tier_a >= 50:
            return "HIGH_FLOW_DENSITY", f"{n_tier_a} TIER-A flows — active options market, bullish bid", +1
        if n_tier_a >= 20:
            return "NORMAL_FLOW", f"{n_tier_a} TIER-A flows — normal options activity", 0
        return "LOW_FLOW", f"{n_tier_a} TIER-A flows — quiet options market", 0
    except Exception:
        return None, None, 0


def derive_insider(payload):
    """Insider clusters → CEO conviction count → smart money buying.
    Thresholds calibrated to real distribution: typical 30d has 1-2 CEO
    conviction, 5-15 strong signals, 30-50 total clusters."""
    try:
        stats = payload.get("stats") or {}
        n_ceo = stats.get("n_ceo_conviction") or 0
        n_strong = stats.get("n_strong_signals") or 0
        n_clusters = stats.get("n_clusters") or 0
        n_smart_dual = stats.get("n_smart_money_dual") or 0
        if n_ceo >= 3 or n_strong >= 15:
            return "STRONG_INSIDER_BUYING", f"{n_ceo} CEO + {n_strong} strong + {n_clusters} clusters — broad institutional accumulation", +1
        if n_ceo >= 1 or n_strong >= 5 or n_smart_dual >= 2:
            return "INSIDER_BUYING_PRESENT", f"{n_ceo} CEO + {n_strong} strong + {n_smart_dual} dual-C-suite over {n_clusters} clusters", +1
        if n_clusters >= 20:
            return "INSIDER_CLUSTERS_NORMAL", f"{n_clusters} clusters, {n_strong} strong — normal insider activity", 0
        return "INSIDER_QUIET", f"{n_clusters} clusters detected — quiet", 0
    except Exception:
        return None, None, 0


def legacy_derive_thirteenf(payload):
    """13F positions → aggregate net buying vs selling across funds."""
    try:
        funds = payload.get("by_fund") or {}
        if not funds: return None, None, 0
        net_new = 0
        net_exits = 0
        for fund_data in funds.values():
            cs = (fund_data or {}).get("changes_summary") or {}
            net_new += cs.get("n_new") or 0
            net_exits += len(cs.get("exits") or [])
        diff = net_new - net_exits
        n_funds = len(funds)
        if diff > n_funds * 1.5:  # avg >1.5 new positions per fund vs exits
            return "13F_NET_BUYING", f"{net_new} new positions vs {net_exits} exits across {n_funds} funds — institutional accumulation", +1
        if diff < -n_funds * 1.5:
            return "13F_NET_SELLING", f"{net_new} new vs {net_exits} exits — institutional distribution", -1
        return "13F_BALANCED", f"{net_new} new vs {net_exits} exits — balanced positioning", 0
    except Exception:
        return None, None, 0


def derive_thirteenf(payload):
    q = holdings_context(payload)
    return None, q['reason'], None


def _ciss_context_only(payload, key):
    q=ciss_context(payload,series_key=CISS_SERIES[key])
    return None,q['read'],None


def derive_ciss(p):return _ciss_context_only(p,'ciss')
def derive_ciss_mm(p):return _ciss_context_only(p,'ciss_mm')
def derive_ciss_bond(p):return _ciss_context_only(p,'ciss_bond')
def derive_ciss_equity(p):return _ciss_context_only(p,'ciss_equity')


DERIVERS = {
    "dix": derive_dix,
    "options_flow": derive_options_flow,
    "insider": derive_insider,
    "ciss": derive_ciss,
    "ciss_mm": derive_ciss_mm,
    "ciss_bond": derive_ciss_bond,
    "ciss_equity": derive_ciss_equity,
    "thirteenf": derive_thirteenf,
}


# ═══════════════════════════════════════════════════════════════════════════
# FETCH + PROCESS ONE MODULE
# ═══════════════════════════════════════════════════════════════════════════

def fetch_module(cfg, ciss_packet=None, now=None):
    """Returns a dict for the module with regime, signal, polarity, age."""
    out = {
        "label": cfg["label"], "emoji": cfg["emoji"], "key": cfg["key"],
        "dimension": cfg["dimension"], "page": cfg.get("page", ""),
        "regime": None, "signal": None, "polarity": 0, "modified": None, "age_minutes": None,
        "missing": False,
    }
    out['age_basis']='S3 object LastModified; not the underlying observation date'
    if cfg.get('derive') in CISS_SERIES:
        q=ciss_context(ciss_packet,now,series_key=CISS_SERIES[cfg['derive']])
        out.update(regime=None,signal=q['read'],polarity=None,
                   missing=q['status']!='fresh',vote_eligible=False,
                   evidence_family='ecb_ciss',source_context=q,
                   age_basis='See original source observation, acquisition and warehouse clocks')
        return out
    try:
        resp = S3.get_object(Bucket=BUCKET, Key=cfg["key"])
        payload = __import__("option_population_context").guard(cfg["key"],json.loads(resp["Body"].read()))
        mod_at = resp.get("LastModified")
        if mod_at:
            out["modified"] = mod_at.isoformat()[:19]
            age = (datetime.now(timezone.utc) - mod_at).total_seconds() / 60
            out["age_minutes"] = round(age, 1)
    except Exception as e:
        out["missing"] = True
        out["error"] = str(e)[:120]
        return out

    if cfg['key']=='data/finra-short.json':
        q=__import__('short_volume_context').context(payload)
        out.update(regime=None,signal=q['note'],polarity=None,vote_eligible=False,
                   descriptive_eligible=False,source_context=q,evidence_family='finra_reported_equity_activity',
                   missing=not q['native_reference_available'],age_basis='See dated source files; publication age is not observation age')
        return out
    if cfg['key']=='data/dealer-gex.json':
        out.update(regime=None,signal=payload['evidence_note'],polarity=None,vote_eligible=False,
                   descriptive_eligible=False,source_context=payload,evidence_family='captured_option_populations',
                   missing=not payload['native_reference_available'],age_basis='Source acquisition clock; independent OI/Greek observation dates unknown')
        return out
    if cfg['key']=='data/retail-sentiment.json':
        q=__import__('retail_research').context(payload,now)
        out.update(regime=None,signal=q['reason'],polarity=None,vote_eligible=False,
                   descriptive_eligible=False,source_context=q,evidence_family='public_attention_sample')
        return out
    if cfg.get('derive') == 'thirteenf':
        q = holdings_context(payload, cfg['key'])
        out.update(regime=None, signal=q['reason'], polarity=None, vote_eligible=False,
                   descriptive_eligible=False, source_context=q, evidence_family='sec_13f_disclosures')
        return out
    if cfg.get("derive"):
        derived = DERIVERS.get(cfg["derive"], lambda p: (None, None, 0))(payload)
        out["regime"], out["signal"], out["polarity"] = derived
    else:
        regime = get_path(payload, cfg["regime_path"])
        signal = get_path(payload, cfg["signal_path"]) if cfg.get("signal_path") else None
        out["regime"] = regime
        out["signal"] = signal
        out["polarity"] = (cfg["polarity_map"] or {}).get(regime)
    out['descriptive_eligible']=bool(out.get('regime')) and out.get('polarity') is not None
    if not out.get('regime'):out['missing']=True
    return out


# ═══════════════════════════════════════════════════════════════════════════
# DIMENSION + META-REGIME LOGIC
# ═══════════════════════════════════════════════════════════════════════════

DIMENSIONS = ["vol", "risk_on", "liquidity", "policy", "reflation", "smart_money", "fundamentals"]


def compute_dimensions(modules):
    out = {}
    for dim in DIMENSIONS:
        constituents = [m for m in modules if m["dimension"] == dim and not m["missing"] and m["regime"]
                        and m.get("vote_eligible") is not False and m.get('descriptive_eligible') is not False and m.get('polarity') is not None]
        if not constituents:
            out[dim] = {"score": None, "n": 0, "members": []}
            continue
        score = sum(m["polarity"] for m in constituents) / len(constituents)
        out[dim] = {
            "score": round(score, 2),
            "n": len(constituents),
            "members": [{"label": m["label"], "regime": m["regime"], "polarity": m["polarity"]} for m in constituents],
        }
    return out


def classify_meta_regime(dims, modules):
    """Descriptive heuristic; missing dimensions cannot become neutral votes."""
    missing=[d for d in DIMENSIONS if not dims.get(d,{}).get('n') or dims[d].get('score') is None]
    if missing:
        return 'UNAVAILABLE','Missing descriptive dimensions: '+', '.join(missing)+'. No complete meta-regime is available.','unavailable'

    vol = dims.get("vol", {}).get("score", 0)
    risk = dims.get("risk_on", {}).get("score", 0)
    liq = dims.get("liquidity", {}).get("score", 0)
    pol = dims.get("policy", {}).get("score", 0)
    refl = dims.get("reflation", {}).get("score", 0)
    smart = dims.get("smart_money", {}).get("score", 0)
    fund = dims.get("fundamentals", {}).get("score", 0)

    positives = sum(1 for v in [vol, risk, liq, refl, smart, fund] if v >= 0.5)
    negatives = sum(1 for v in [vol, risk, liq, refl, smart, fund] if v <= -0.5)

    # CRISIS — 5+ negative dimensions OR vol fully negative
    if negatives >= 5 or (vol <= -1 and risk <= -0.5):
        return ("CRISIS",
                f"Multiple dimensions in stress (negatives={negatives}). Vol regime fragile. "
                f"This is an unvalidated stress classification; it does not establish a portfolio action.",
                "crisis")

    # RISK_OFF — vol up + risk-on dimension turning
    if vol <= -0.5 and risk <= -0.5:
        return ("RISK_OFF",
                f"Vol regime stressed (vol={vol:+.2f}) + risk appetite weakening (risk_on={risk:+.2f}). "
                f"The classification describes input labels, without a validated return or allocation implication.",
                "risk-off")

    # DEFENSIVE — 2-3 negatives accumulating
    if negatives >= 2:
        return ("DEFENSIVE",
                f"{negatives} dimensions negative. The heuristic indicates disagreement with positive input labels; no validated portfolio action follows.",
                "defensive")

    # MELT_UP — vol low, risk-on high, fundamentals strong
    if vol >= 0.5 and risk >= 0.5 and liq >= 0.3 and fund >= 0.5 and pol >= -0.5:
        return ("MELT_UP",
                f"All risk-supporting dimensions positive (vol={vol:+.1f}, risk={risk:+.1f}, "
                f"fund={fund:+.1f}). The heuristic labels align positively; this does not establish maximum exposure or a return forecast.",
                "melt-up")

    # LATE_CYCLE — risk-on but policy hawkish + reflation elevated (← typical pre-correction)
    if risk >= 0.3 and pol <= -0.5 and refl >= 0.3:
        return ("LATE_CYCLE",
                f"Risk-on continues (risk={risk:+.2f}) BUT policy hawkish (pol={pol:+.2f}) + reflation "
                f"elevated (refl={refl:+.2f}). This descriptive pattern has no validated timing or sizing implication. "
                f"Independent source and model qualification remains required.",
                "late-cycle")

    # GOLDILOCKS — vol low, risk-on moderate, policy not too hawkish, reflation contained
    if vol >= 0.3 and risk >= 0 and pol >= -0.5 and abs(refl) < 0.5 and fund >= 0:
        return ("GOLDILOCKS",
                f"Vol calm, policy supportive, reflation contained. This is a descriptive combination of labels. "
                f"It does not establish an equity or hedging instruction.",
                "goldilocks")

    # NORMAL — balanced
    return ("NORMAL",
            f"Dimensions balanced (positives={positives}, negatives={negatives}). No regime extreme; "
            f"this is a heuristic label, not evidence of portfolio suitability.",
            "normal")


def compute_composite_score(dims):
    """Weighted composite -100..+100. Vol+Risk get higher weight."""
    weights = {"vol": 1.5, "risk_on": 1.5, "liquidity": 1.0, "policy": 1.0,
                "reflation": 0.7, "smart_money": 1.0, "fundamentals": 1.2}
    num = 0.0; den = 0.0
    for dim, w in weights.items():
        s = dims.get(dim, {}).get("score", 0)
        n = dims.get(dim, {}).get("n", 0)
        if n == 0: continue
        num += s * w
        den += w
    if den == 0: return None
    return round((num / den) * 100, 1)


# ═══════════════════════════════════════════════════════════════════════════
# STATE / HISTORY
# ═══════════════════════════════════════════════════════════════════════════

def load_prior():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def load_history():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=HISTORY_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {"snapshots": []}


def save_history(history):
    try:
        body = json.dumps(history, default=str).encode("utf-8")
        S3.put_object(Bucket=BUCKET, Key=HISTORY_KEY, Body=body,
                       ContentType="application/json")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print(f"[regime-composite] history save err: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def telegram_alert(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
                                       headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[regime-composite] telegram err: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    # A shared CISS artifact is read once, so its four views cannot mix runs.
    try:ciss_packet=json.loads(S3.get_object(Bucket=BUCKET,Key="data/ciss-stress.json")["Body"].read())
    except Exception:ciss_packet=None
    ciss_systemic=ciss_context(ciss_packet,now)
    # Fetch all other modules in parallel.
    modules = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_module, cfg, ciss_packet, now): cfg for cfg in MODULES_CFG}
        for f in as_completed(futures):
            try: modules.append(f.result())
            except Exception as e:
                cfg = futures[f]
                modules.append({"label": cfg["label"], "key": cfg["key"],
                                 "dimension": cfg["dimension"], "missing": True, "error": str(e)[:120]})
    # Preserve config order
    label_order = [c["label"] for c in MODULES_CFG]
    modules.sort(key=lambda m: label_order.index(m["label"]) if m["label"] in label_order else 99)

    dims = compute_dimensions(modules)
    composite_score = compute_composite_score(dims)
    meta_regime, meta_narrative, meta_class = classify_meta_regime(dims, modules)

    # Diff vs prior
    prior = load_prior()
    prior_regime = (prior or {}).get("meta_regime")
    regime_changed = prior_regime and prior_regime != meta_regime

    out = {
        "schema_version": "1.0",
        "version": VERSION,
        "generated_at": now.isoformat(),
        "wl_research": __import__("wl_fusion").block(('STRESS', 'DOLLAR')),
        "duration_s": round(time.time() - t0, 2),
        "call": None, "calls_eligible": False, "sizing_eligible": False,
        "validation_status": "UNVALIDATED_DESCRIPTIVE_HEURISTIC",
        "decision": {"verb":"WAIT","meaning":"abstain","reason":"Descriptive input labels do not establish a validated return forecast or portfolio size."},
        "ciss_systemic": ciss_systemic,
        "dependency_groups": [{"id":"ecb_ciss","source":"data/ciss-stress.json",
            "views":list(CISS_SERIES),"configured_source_families":1,
            "usable_source_families":int(ciss_systemic["status"]=="fresh"),"independent_votes":0,
            "source_replay":ciss_systemic.get('source_replay')}],
        "source_qualification_scope": "Original-source CISS context only; remaining modules retain their own unverified source/model limits",
        "meta_regime": meta_regime,
        "meta_narrative": meta_narrative,
        "meta_class": meta_class,
        "composite_score": composite_score,
        "composite_coverage": {"populated_dimensions":[d for d in DIMENSIONS if dims[d]["n"]],
            "missing_dimensions":[d for d in DIMENSIONS if not dims[d]["n"]],
            "basis":"Descriptive mean over populated dimensions; partial coverage changes the denominator"},
        "dimensions": dims,
        "modules": modules,
        "n_modules_total": len(MODULES_CFG),
        "n_modules_with_data": sum(1 for m in modules if not m.get("missing")),
        "n_modules_missing": sum(1 for m in modules if m.get("missing")),
        "prior_regime": prior_regime,
        "regime_changed_from_prior": bool(regime_changed),
    }

    # Save sidecar
    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=OUTPUT_KEY, Body=body,
                   ContentType="application/json",
                   CacheControl="public,max-age=300")

    # Append history (cap at last 168 = 7d of hourly)
    history = load_history()
    snapshots = history.get("snapshots", [])
    snapshots.append({
        "ts": now.isoformat(),
        "model_version": VERSION,
        "ciss_source_replay": ciss_systemic.get('source_replay'),
        "meta_regime": meta_regime,
        "composite_score": composite_score,
        "dim_scores": {d: dims.get(d, {}).get("score", 0) for d in DIMENSIONS},
    })
    snapshots = snapshots[-168:]
    save_history({"snapshots": snapshots})

    # Telegram on regime change
    if regime_changed and composite_score is not None and not (isinstance(event,dict) and event.get("suppress_alerts")):
        msg = (f"⚡ <b>META-REGIME CHANGE</b>\n"
               f"<b>{prior_regime} → {meta_regime}</b>\n\n"
               f"Composite score: {composite_score:+.1f}\n"
               f"{meta_narrative}\n\n"
               f"<a href='https://justhodl.ai/composite/'>justhodl.ai/composite/</a>")
        telegram_alert(msg)

    return {"statusCode": 200,
             "body": json.dumps({"meta_regime": meta_regime,
                                  "composite_score": composite_score,
                                  "regime_changed": regime_changed,
                                  "n_with_data": out["n_modules_with_data"]})}


if __name__ == "__main__":
    print(lambda_handler({}, None))
