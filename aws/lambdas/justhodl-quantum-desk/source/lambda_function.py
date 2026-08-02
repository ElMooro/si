"""
justhodl-quantum-desk -- the Quant PM Desk (cross-engine meta-allocator)
=========================================================================
ops 4257 · v1.0.0

WHY THIS EXISTS
---------------
The fleet has 766 engines answering narrow questions: regime (router /
cycle-clock / nowcast-desk), forward returns (asset-compass 12m,
forward-returns 10y), stock selection (best-setups / master-ranker),
flows (etf-fund-flows quadrants), crypto cycle (crypto-cycle-risk),
plumbing (global-liquidity), and a veto layer (risk-gate). No engine sits
where the PM sits at a hedge fund's Monday meeting: reading every desk's
book at once and writing ONE blotter -- "given everything we know, which
asset class do we hold this quarter, and which names, sized how."

This engine is that meeting. It is DETERMINISTIC (no LLM calls -- the LLM
layer can be down without degrading this desk) and every number it emits
is decomposed into named legs with the source artifact and its age, so
the output is auditable line-by-line, the way GMO / Bridgewater publish
their reasoning, not just their conclusion.

KHALID DOCTRINE (encoded, weight-tunable via env)
-------------------------------------------------
Buy quality below the 200-day, at capitulation/stealth-accumulation
flow quadrants, with huge modeled upside and small modeled downside,
where the macro regime's historical playbook AND today's liquidity
plumbing both favor the class. Never fight the risk-gate. Missing data
is disclosed as ABSTAIN legs -- never invented.

LAYERS
------
  L1 REGIME CONSENSUS  router + cycle-clock + nowcast-desk vote;
                       disagreement is disclosed, not hidden.
  L2 VETO / SIZING     risk-gate posture caps verdicts; its
                       sizing_multiplier scales every position.
  L3 ASSET LADDER      one row per investable class, scored on 6 legs:
                       discount(200dma/drawdown) · asymmetry(up/down) ·
                       strategic(10y ER percentile) · regime_fit
                       (historical playbook matrix) · plumbing(liquidity
                       slope x class beta) · cycle(crypto on-chain floor).
  L4 MONEY MAP         best-setups names gated by their class's ladder
                       verdict, boosted at CAPITULATION /
                       STEALTH_ACCUMULATION, penalized at
                       DISTRIBUTION_RALLY, sized by the risk-gate.

PLAYBOOK BASE RATES (L3 regime_fit leg) -- documented, not hidden
-----------------------------------------------------------------
STAGFLATION      1973-74, 1977-80, 2021-22: commodities/gold/energy led
                 (gold +140% '77-'80), long duration & long-multiple
                 growth worst; T-bills preserved; value > growth.
LATE_CYCLE       1999-2000, 2006-07: quality/energy/defensives lead,
                 small-caps & HY credit deteriorate first.
RECESSION_BUST   2001, 2008-09, 2020Q1: long treasuries + cash lead,
                 equities/commodities/crypto drawdown; the BUY zone for
                 the NEXT cycle forms here (deep-discount leg dominates).
REFLATION        2003, 2009-10, 2020H2-2021: small value, EM, energy,
                 crypto lead violently off the low; bonds lag.
GOLDILOCKS       1995-99, 2013-17: US large growth leads, vol sellers
                 win, gold flat; everything risk-on works, bonds ok.
DISINFLATION     1982-85, 2023-24: duration + large growth lead as real
                 yields fall; commodities lag.
NEUTRAL          uninformative prior 0.5 everywhere.

Sources are sibling artifacts in s3://justhodl-dashboard-live; each is
read defensively (multiple field-name fallbacks -- house wrapper
doctrine: never assume a sibling's schema, read what is there, disclose
what is not).

OUT: data/quantum-desk.json (+ rolling data/quantum-desk-history.json)
"""

import json
import os
import time
from datetime import datetime, timezone

try:
    import boto3
except ImportError:      # fixture-mode unit tests run without the SDK
    boto3 = None

VERSION = "2.0.2"
OPS = 4257
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/quantum-desk.json"
HIST_KEY = "data/quantum-desk-history.json"
LOCAL_DIR = os.environ.get("QD_LOCAL_DIR")  # unit-test fixture mode only

s3 = boto3.client("s3", region_name=REGION) if (boto3 and not LOCAL_DIR) else None  # noqa("s3", region_name=REGION) if not LOCAL_DIR else None
# ── weights (env-tunable; renormalized over legs actually present) ──────
def _w(name, dflt):
    try:
        return float(os.environ.get(name, dflt))
    except Exception:
        return dflt

W = {
    "discount":  _w("W_DISCOUNT", 0.22),   # below 200dma / drawdown depth
    "asymmetry": _w("W_ASYM",     0.22),   # modeled upside vs downside
    "strategic": _w("W_STRAT",    0.14),   # 10y ER vs own history
    "regime":    _w("W_REGIME",   0.16),   # playbook base rate
    "plumbing":  _w("W_PLUMB",    0.13),   # liquidity slope x class beta
    "cycle":     _w("W_CYCLE",    0.13),   # crypto on-chain floor/blowoff
}
MM_W = {  # money-map name-level weights
    "conviction": _w("MM_W_CONV",  0.34),  # best-setups composite (carries
                                           # census moat/quality + factor
                                           # + industry priors already)
    "quadrant":   _w("MM_W_QUAD",  0.22),  # flow-quadrant bottom-buyer bias
    "class_gate": _w("MM_W_CLASS", 0.24),  # its asset class's ladder score
    "flow":       _w("MM_W_FLOW",  0.20),  # squeeze fuel / smart money
}

SOURCES = {
    "sources_census":  {"key": "data/quantum-desk-sources.json",     "max_age_h": 24 * 14},
    "canary_warroom":  {"key": "data/canary-warroom.json",           "max_age_h": 26},
    "ka_metrics":      {"key": "data/ka-metrics.json",               "max_age_h": 30},
    "global_recession": {"key": "data/global-recession.json",        "max_age_h": 30},
    "asset_compass":   {"key": "data/asset-compass.json",            "max_age_h": 30},
    "forward_returns": {"key": "data/forward-returns.json",          "max_age_h": 200},
    "best_setups":     {"key": "data/best-setups.json",              "max_age_h": 8},
    "master_alloc":    {"key": "data/master-allocation.json",        "max_age_h": 30},
    "router":          {"key": "data/regime-conditional-router.json","max_age_h": 30},
    "cycle_clock":     {"key": "data/cycle-clock.json",              "max_age_h": 30},
    "nowcast":         {"key": "data/nowcast-desk.json",             "max_age_h": 30},
    "risk_gate":       {"key": "data/risk-gate.json",                "max_age_h": 30},
    "liquidity":       {"key": "data/global-liquidity.json",         "max_age_h": 80},
    "crypto_cycle":    {"key": "data/crypto-cycle-risk.json",        "max_age_h": 12},
    "etf_flows":       {"key": "etf-flows/daily.json",               "max_age_h": 40},
    "indicator_bus":   {"key": "data/indicator-bus.json",            "max_age_h": 40},
}

# ── canonical classes + liquidity beta (sensitivity of class to rising
#    net liquidity; sign/magnitude from 2009-2025 QE/QT episode betas) ──
CLASSES = {
    "US_LARGE":       0.7, "US_SMALL_VALUE": 0.8, "INTL_DM": 0.6,
    "EM":             0.8, "GOLD":           0.4, "SILVER":  0.6,
    "COMMODITIES":    0.5, "ENERGY":         0.4, "BONDS_LONG": 0.2,
    "TBILLS":        -0.2, "TIPS":           0.1, "CREDIT_HY": 0.6,
    "BTC":            1.0, "ETH":            1.0, "REITS":   0.6,
    "CASH":          -0.3,
}

PLAYBOOK = {  # regime -> class -> historical base-rate fit 0..1
    "STAGFLATION": {
        "GOLD": .95, "SILVER": .85, "COMMODITIES": .9, "ENERGY": .9,
        "TIPS": .75, "TBILLS": .65, "US_SMALL_VALUE": .55, "CASH": .6,
        "EM": .5, "INTL_DM": .45, "US_LARGE": .35, "CREDIT_HY": .3,
        "REITS": .3, "BONDS_LONG": .15, "BTC": .45, "ETH": .4,
    },
    "LATE_CYCLE": {
        "ENERGY": .8, "GOLD": .7, "US_LARGE": .55, "TBILLS": .65,
        "TIPS": .6, "COMMODITIES": .65, "CASH": .6, "INTL_DM": .5,
        "SILVER": .55, "US_SMALL_VALUE": .35, "EM": .4, "CREDIT_HY": .3,
        "REITS": .35, "BONDS_LONG": .45, "BTC": .45, "ETH": .4,
    },
    "RECESSION_BUST": {
        "BONDS_LONG": .9, "TBILLS": .8, "CASH": .75, "GOLD": .65,
        "TIPS": .6, "US_LARGE": .3, "US_SMALL_VALUE": .25, "INTL_DM": .25,
        "EM": .2, "COMMODITIES": .2, "ENERGY": .2, "SILVER": .35,
        "CREDIT_HY": .2, "REITS": .2, "BTC": .25, "ETH": .2,
    },
    "REFLATION": {
        "US_SMALL_VALUE": .9, "EM": .85, "ENERGY": .8, "BTC": .9,
        "ETH": .85, "SILVER": .8, "COMMODITIES": .75, "CREDIT_HY": .75,
        "INTL_DM": .7, "US_LARGE": .65, "REITS": .65, "GOLD": .55,
        "TIPS": .45, "BONDS_LONG": .25, "TBILLS": .2, "CASH": .15,
    },
    "GOLDILOCKS": {
        "US_LARGE": .85, "INTL_DM": .65, "EM": .6, "US_SMALL_VALUE": .6,
        "CREDIT_HY": .65, "REITS": .6, "BTC": .7, "ETH": .65,
        "BONDS_LONG": .55, "TIPS": .5, "GOLD": .35, "SILVER": .35,
        "COMMODITIES": .4, "ENERGY": .4, "TBILLS": .35, "CASH": .3,
    },
    "DISINFLATION": {
        "BONDS_LONG": .85, "US_LARGE": .8, "REITS": .65, "TIPS": .6,
        "CREDIT_HY": .6, "INTL_DM": .55, "EM": .5, "US_SMALL_VALUE": .5,
        "GOLD": .5, "BTC": .6, "ETH": .55, "TBILLS": .45,
        "SILVER": .4, "COMMODITIES": .3, "ENERGY": .3, "CASH": .35,
    },
    "NEUTRAL": {c: .5 for c in CLASSES},
}

REGIME_ALIASES = {
    "STAGFLATION": "STAGFLATION", "STAGFLATIONARY": "STAGFLATION",
    "LATE-CYCLE": "LATE_CYCLE", "LATE_CYCLE": "LATE_CYCLE",
    "LATECYCLE": "LATE_CYCLE", "SLOWDOWN": "LATE_CYCLE",
    "RECESSION": "RECESSION_BUST", "BUST": "RECESSION_BUST",
    "CONTRACTION": "RECESSION_BUST", "DEFLATION": "RECESSION_BUST",
    "RISK_OFF": "RECESSION_BUST",
    "REFLATION": "REFLATION", "RECOVERY": "REFLATION",
    "EARLY-CYCLE": "REFLATION", "EARLY_CYCLE": "REFLATION",
    "EXPANSION": "GOLDILOCKS", "GOLDILOCKS": "GOLDILOCKS",
    "MID-CYCLE": "GOLDILOCKS", "MID_CYCLE": "GOLDILOCKS",
    "DISINFLATION": "DISINFLATION", "DISINFLATIONARY": "DISINFLATION",
    # real sibling vocabularies (nowcast quadrant, cycle-clock
    # investment-clock, router sleeves) -- ops 4258 ground truth
    "SOFT LANDING": "GOLDILOCKS", "SOFT-LANDING": "GOLDILOCKS",
    "HARD LANDING": "RECESSION_BUST", "HARD-LANDING": "RECESSION_BUST",
    "OVERHEAT": "LATE_CYCLE", "DOWNTURN": "RECESSION_BUST",
    "DEFLATION-BUST": "RECESSION_BUST", "DEFLATION_BUST": "RECESSION_BUST",
}
REGIME_KEYS = ("regime", "phase", "cycle_phase", "primary_regime",
               "nowcast_regime", "label", "quadrant_regime", "state")


def deep_find_regime(doc, depth=5):
    """Bounded walk: first alias-mappable regime string under a
    regime-ish key, in stable order. Survives sibling refactors."""
    if depth < 0 or doc is None:
        return None
    if isinstance(doc, dict):
        for k in REGIME_KEYS:                      # direct hits first
            v = doc.get(k)
            if isinstance(v, str):
                r = norm_regime(v)
                if r:
                    return r
        for k in sorted(doc):                      # then descend, stable
            r = deep_find_regime(doc[k], depth - 1)
            if r:
                return r
    elif isinstance(doc, list):
        for item in doc[:20]:
            r = deep_find_regime(item, depth - 1)
            if r:
                return r
    return None


def deep_find_setups(doc, depth=4):
    """First list[dict] that looks like setups rows (ticker/symbol +
    conviction/score on several items)."""
    if depth < 0 or doc is None:
        return None
    if isinstance(doc, list):
        hits = [x for x in doc[:80] if isinstance(x, dict)
                and (x.get("ticker") or x.get("symbol"))
                and (x.get("conviction") is not None
                     or x.get("score") is not None)]
        if len(hits) >= 3:
            return doc
    if isinstance(doc, dict):
        for k in ("setups", "top", "rows", "items", "list"):  # fast paths
            r = deep_find_setups(doc.get(k), depth - 1)
            if r:
                return r
        for k in sorted(doc):
            r = deep_find_setups(doc[k], depth - 1)
            if r:
                return r
    elif isinstance(doc, list):
        for item in doc[:10]:
            r = deep_find_setups(item, depth - 1)
            if r:
                return r
    return None

CLASS_KEYWORDS = [  # asset-name -> canonical class (first match wins)
    (("BITCOIN", "BTC"), "BTC"), (("ETHEREUM", "ETH"), "ETH"),
    (("T-BILL", "TBILL", "BIL", "SHV", "SGOV", "0-3 MONTH"), "TBILLS"),
    (("TIP",), "TIPS"),
    (("LONG TREASUR", "TREASUR", "TLT", "IEF", "20+", "ZROZ", "EDV",
      "LONG BOND"), "BONDS_LONG"),
    (("HIGH YIELD", "HYG", "JNK", "CREDIT"), "CREDIT_HY"),
    (("GOLD", "GLD", "IAU"), "GOLD"), (("SILVER", "SLV"), "SILVER"),
    (("ENERGY", "XLE", "OIL", "USO", "CRUDE"), "ENERGY"),
    (("COMMODIT", "DBC", "GSG", "PDBC"), "COMMODITIES"),
    (("REIT", "VNQ", "REAL ESTATE"), "REITS"),
    (("EMERGING", "EEM", "VWO", " EM "), "EM"),
    (("INTERNATIONAL", "DEVELOPED", "EAFE", "EFA", "VEA", "EX-US"), "INTL_DM"),
    (("SMALL", "VALUE", "IWM", "AVUV", "RUSSELL 2000", "VBR"), "US_SMALL_VALUE"),
    (("CASH", "MONEY MARKET"), "CASH"),
    (("S&P", "SPY", "VOO", "US LARGE", "NASDAQ", "QQQ", "US EQUIT",
      "US STOCK", "LARGE CAP", "TOTAL MARKET", "VTI"), "US_LARGE"),
]


# ── tolerant plumbing ───────────────────────────────────────────────────
def _now():
    return datetime.now(timezone.utc)


def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def num(v):
    try:
        f = float(v)
        return f if f == f else None  # NaN guard
    except (TypeError, ValueError):
        return None


def dig(doc, *paths):
    """First non-None value at any of several dotted paths."""
    for p in paths:
        cur = doc
        ok = True
        for part in p.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok and cur is not None:
            return cur
    return None


def read_source(name, spec):
    """-> (doc|None, meta{status, age_h, key})."""
    key = spec["key"]
    try:
        if LOCAL_DIR:
            path = os.path.join(LOCAL_DIR, os.path.basename(key))
            if not os.path.exists(path):
                return None, {"status": "missing", "age_h": None, "key": key}
            with open(path) as f:
                doc = json.load(f)
            age_h = 0.0
        else:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            doc = json.loads(obj["Body"].read())
            age_h = round(
                (_now() - obj["LastModified"]).total_seconds() / 3600.0, 1)
        stale = age_h is not None and age_h > spec["max_age_h"]
        return doc, {"status": "stale" if stale else "ok",
                     "age_h": age_h, "key": key}
    except Exception as e:
        return None, {"status": "missing", "age_h": None, "key": key,
                      "error": str(e)[:120]}


COMPASS_CLASS = {  # asset-compass row["class"] vocabulary (ops 4259 probe)
    "cash": "CASH", "tbill": "TBILLS", "tbills": "TBILLS",
    "gold": "GOLD", "silver": "SILVER", "energy": "ENERGY",
    "commodities": "COMMODITIES", "commodity": "COMMODITIES",
    "btc": "BTC", "bitcoin": "BTC", "eth": "ETH", "ethereum": "ETH",
    "reit": "REITS", "reits": "REITS", "tips": "TIPS",
    "bonds": "BONDS_LONG", "bond": "BONDS_LONG",
    "duration": "BONDS_LONG", "long_treasury": "BONDS_LONG",
    "bonds_long": "BONDS_LONG", "treasuries": "BONDS_LONG",
    "credit": "CREDIT_HY", "hy": "CREDIT_HY",
    "em": "EM", "em_equity": "EM", "intl": "INTL_DM", "dm": "INTL_DM",
    "intl_dm": "INTL_DM", "ex_us": "INTL_DM",
    "small_value": "US_SMALL_VALUE", "us_small": "US_SMALL_VALUE",
    "small": "US_SMALL_VALUE", "value": "US_SMALL_VALUE",
    "equity": "US_LARGE", "us_equity": "US_LARGE", "us_large": "US_LARGE",
    "spx": "US_LARGE", "growth": "US_LARGE",
}


def classify_asset(name):
    raw = str(name or "").strip().lower()
    if raw in COMPASS_CLASS:
        return COMPASS_CLASS[raw]
    up = " %s " % raw.upper()
    for keys, cls in CLASS_KEYWORDS:
        if any(k in up for k in keys):
            return cls
    return None


def norm_regime(label):
    if not label:
        return None
    up = str(label).upper().replace("·", " ").strip()
    for alias, canon in REGIME_ALIASES.items():
        if alias in up:
            return canon
    return None


# ── L1 regime consensus ─────────────────────────────────────────────────
def regime_consensus(docs):
    votes = []
    for src, paths in (
        ("global_recession", ("regime", "status", "verdict", "label")),
        ("router",      ("regime", "primary_regime", "sleeve.regime")),
        ("cycle_clock", ("regime", "phase", "cycle_phase", "clock.phase",
                         "label")),
        ("nowcast",     ("regime", "nowcast_regime", "label",
                         "desk.regime")),
    ):
        doc = docs.get(src)
        if not doc:
            continue
        r = norm_regime(dig(doc, *paths)) or deep_find_regime(doc)
        if r:
            votes.append({"source": src, "regime": r})
    abst = []
    for src2, doc in (("router", docs.get("router")),):
        if doc:
            lbl = dig(doc, "primary_regime", "regime")
            if lbl and not norm_regime(lbl) and \
                    not any(v["source"] == src2 for v in votes):
                abst.append({"source": src2, "label": str(lbl)[:40],
                             "note": "explicitly uncertain -- abstains"})
    if not votes:
        return {"regime": "NEUTRAL", "votes": [], "abstained": abst,
                "unanimous": False,
                "note": "no regime engine readable -- neutral prior"}
    counts = {}
    for v in votes:
        counts[v["regime"]] = counts.get(v["regime"], 0) + 1
    win = max(counts, key=counts.get)
    return {"regime": win, "votes": votes, "abstained": abst,
            "unanimous": len(counts) == 1 and len(votes) > 1,
            "disagreement": sorted(counts) if len(counts) > 1 else None}


# ── L2 risk-gate ────────────────────────────────────────────────────────
def risk_layer(doc):
    if not doc:
        return {"posture": None, "sizing_multiplier": 1.0,
                "composite": None,
                "note": "risk-gate unreadable -- sizing 1.0x, verdicts uncapped"}
    return {
        "posture": dig(doc, "posture", "live_posture", "gate.posture"),
        "composite": num(dig(doc, "composite", "live_composite",
                             "gate.composite")),
        "sizing_multiplier": num(dig(doc, "sizing_multiplier",
                                     "sizing.multiplier")) or 1.0,
    }


# ── L3 ladder legs ──────────────────────────────────────────────────────
def leg_discount(row):
    """Depth below 200dma (preferred) or drawdown-from-high. 0.5 = at
    trend; 1.0 = deeply below (Khalid's buy zone); 0.0 = far above."""
    tr = row.get("trend")
    if isinstance(tr, dict):
        pv = num(tr.get("px_vs_200dma_pct"))   # ops 4261: the exact
        if pv is not None:                     # field for Khalid's lens
            gap = pv / 100.0
            return clamp(0.5 - gap * 2.5), "px_vs_200dma", round(pv, 1)
        lbl = str(tr.get("label") or "")
        import re as _re
        m = _re.search(r"(-?\d+(?:\.\d+)?)\s*%", lbl)
        gap = None
        if m:
            gap = float(m.group(1)) / 100.0
            if "BELOW" in lbl.upper() and gap > 0:
                gap = -gap
        elif "BELOW" in lbl.upper():
            gap = -0.08          # below trend, magnitude unstated
        elif "ABOVE" in lbl.upper():
            gap = 0.08
        if gap is not None:
            return clamp(0.5 - gap * 2.5), "trend_label", \
                round(gap * 100, 1)
    price = num(dig(row, "price", "last", "close"))
    ma = None
    for f in ("ma200", "sma200", "dma200", "ma_200", "sma_200",
              "ma200_price"):
        ma = num(row.get(f))
        if ma:
            break
    if price and ma and ma > 0:
        gap = (price - ma) / ma           # negative = below trend
        return clamp(0.5 - gap * 2.5), "vs_200dma", round(gap * 100, 1)
    dd = num(dig(row, "drawdown_pct", "drawdown", "dd_pct",
                 "off_high_pct"))
    if dd is not None:
        d = abs(dd) / 100.0 if abs(dd) > 1 else abs(dd)
        return clamp(0.35 + d * 1.3), "drawdown", round(-abs(d) * 100, 1)
    return None, None, None


def leg_asymmetry(row):
    up = num(dig(row, "upside_pct", "upside", "er_upside"))
    dn = num(dig(row, "downside_pct", "downside", "er_downside"))
    if up is not None and dn is not None and abs(dn) > 0.01:
        ratio = up / abs(dn)
        return clamp(ratio / 4.0), round(ratio, 2)   # 4:1 saturates
    asym = row.get("asym")
    if isinstance(asym, dict):
        rt = num(asym.get("ratio"))            # ops 4261: precomputed,
        if rt is not None:                     # capped at 25 upstream
            return clamp(rt / 4.0), round(rt, 2)
        ups = [num(v) for k, v in asym.items()
               if any(t in k.lower() for t in ("up", "bull", "gain"))]
        dns = [num(v) for k, v in asym.items()
               if any(t in k.lower() for t in ("down", "bear", "loss",
                                               "risk", "dd"))]
        u = next((x for x in ups if x is not None), None)
        dn2 = next((x for x in dns if x is not None), None)
        if u is not None and dn2 is not None and abs(dn2) > 1e-9:
            ratio = abs(u) / abs(dn2)
            return clamp(ratio / 4.0), round(ratio, 2)
        rt = num(asym.get("ratio")) or num(asym.get("score"))
        if rt is not None:
            return clamp(rt / 4.0 if rt > 1.2 else rt), rt
    a = num(dig(row, "asymmetry", "asym_score", "asymmetry_score"))
    if a is not None:
        return clamp(a if a <= 1 else a / 100.0), a
    ex = num(row.get("excess_vs_cash_pp"))     # last resort: modeled 1y
    if ex is not None:                         # excess return vs cash
        return clamp(0.5 + ex / 20.0), None
    return None, None


def leg_strategic(fr_doc, cls, name):
    if not fr_doc:
        return None, None
    rows = (dig(fr_doc, "assets", "rows", "classes") or [])
    if isinstance(rows, dict):
        rows = [dict(v, name=k) for k, v in rows.items()
                if isinstance(v, dict)]
    best = None
    for r in rows:
        rn = str(dig(r, "name", "asset", "class", "ticker") or "")
        if classify_asset(rn) == cls or rn.upper() in str(name).upper():
            best = r
            break
    if not best:
        return None, None
    pct = num(dig(best, "current_vs_history_percentile",
                  "er_percentile", "percentile", "er_vs_history_pct"))
    if pct is not None:  # high ER vs own history = historically cheap
        p = pct / 100.0 if pct > 1 else pct
        return clamp(p), round(p * 100)
    er = num(dig(best, "er_10y", "expected_return_10y", "er",
                 "forward_return"))
    if er is not None:
        e = er if abs(er) < 1 else er / 100.0
        return clamp(0.5 + e * 3.0), round(e * 100, 1)
    return None, None


def leg_plumbing(liq_doc, cls):
    if not liq_doc:
        return None, None
    slope = None
    def _scan(d, depth=3):
        if depth < 0:
            return None
        if isinstance(d, dict):
            for k, v in d.items():
                kl = k.lower()
                if any(t in kl for t in ("slope", "delta", "chg",
                                         "change", "momentum", "13w",
                                         "4w", "trend")) \
                        and num(v) is not None:
                    return num(v)
            for v in d.values():
                x = _scan(v, depth - 1)
                if x is not None:
                    return x
        return None
    slope = _scan(liq_doc)
    if slope is None:
        lvl = num(dig(liq_doc, "index", "gli", "global_liquidity_index",
                      "net_liquidity"))
        if lvl is None:
            return None, None
        slope = 0.0  # level known, direction not -> neutral
    direction = clamp(0.5 + (0.5 if slope > 0 else -0.5 if slope < 0
                             else 0.0) * 0.8)
    beta = CLASSES.get(cls, 0.5)
    return clamp(0.5 + (direction - 0.5) * beta * 2.0), \
        ("rising" if slope > 0 else "falling" if slope < 0 else "flat")


def leg_cycle(cc_doc, cls):
    if cls not in ("BTC", "ETH") or not cc_doc:
        return None, None
    mvrv = num(dig(cc_doc, "mvrv", "btc.mvrv", "metrics.mvrv",
                   "cycle.mvrv"))
    nupl = num(dig(cc_doc, "nupl", "btc.nupl", "metrics.nupl"))
    risk = num(dig(cc_doc, "risk_score", "dump_risk", "score"))
    detail = {"mvrv": mvrv, "nupl": nupl, "risk": risk}
    if mvrv is not None:
        if mvrv < 1.0:
            return 0.95, detail       # below realized price: cycle floor
        if mvrv < 1.3:
            return 0.75, detail
        if mvrv > 3.0:
            return 0.10, detail       # blowoff
        return clamp(1.0 - (mvrv - 1.0) / 2.5), detail
    if risk is not None:
        r = risk / 100.0 if risk > 1 else risk
        return clamp(1.0 - r), detail
    return None, None


def score_legs(legs):
    used = {k: v for k, v in legs.items() if v is not None}
    if not used:
        return None, []
    tot = sum(W[k] for k in used)
    return round(sum(W[k] * used[k] for k in used) / tot, 3), sorted(used)


def verdict_for(score, legs, risk, canary_veto=False):
    if canary_veto and score is not None and score >= 0:
        score = min(score, 0.72)  # RED barometer: never BUY_ZONE
    if score is None:
        return "ABSTAIN"
    v = ("BUY_ZONE" if score >= 0.66 and (legs.get("discount") or 0) >= 0.5
         else "ACCUMULATE" if score >= 0.55
         else "NEUTRAL" if score >= 0.42 else "AVOID")
    if (risk.get("posture") or "").upper().startswith("RISK_OFF") \
            and v == "BUY_ZONE":
        v = "ACCUMULATE"  # gate caps aggression, never inverted
    return v


def _khalid_index_block(doc):
    """Khalid's original composite, honored beside the risk-gate."""
    if not doc:
        return None
    ri = num(dig(doc, "risk_index", "khalid_index", "score"))
    return {"risk_index": ri, "grade": dig(doc, "grade", "rating"),
            "phase": dig(doc, "phase", "market_phase"),
            "llm_status": doc.get("llm_status"),
            "note": "original Khalid Index composite (ka-metrics)"}


def _canary_block(doc):
    """Canary war-room master barometer -- equal weight per canary
    (Khalid spec 2026-07-09). Second veto layer at RED."""
    if not doc:
        return None
    # real doc shape (warroom source, ops 4278): master.band,
    # master.early_warning_0_100, master.n_firing, firing[] names
    level = dig(doc, "master.band", "master_barometer.level", "level")
    score = num(dig(doc, "master.early_warning_0_100",
                    "master_barometer.score", "score"))
    firing = doc.get("firing") or dig(doc, "master_barometer.triggered")
    if isinstance(firing, list):
        def _fname(f):
            if isinstance(f, dict):
                return (f.get("label") or f.get("canary")
                        or f.get("name") or f.get("key")
                        or "%s:%s" % (f.get("mechanism", "?"),
                                      f.get("mech_key")
                                      or f.get("metric") or "?"))
            return str(f)
        firing = [str(_fname(f))[:30] for f in firing[:8]]
    n_f = num(dig(doc, "master.n_firing"))
    n_c = num(dig(doc, "master.n_canaries"))
    return {"level": str(level)[:20] if level else None,
            "score": score,
            "n_firing": n_f, "n_canaries": n_c,
            "triggered": firing if isinstance(firing, list) else None,
            "headline": (dig(doc, "master.headline") or "")[:160] or None,
            "veto_active": str(level or "").upper() in
            ("RED", "CRITICAL", "ALERT", "SEVERE")}


def _coverage_block(census):
    c = (census or {}).get("census") or {}
    return {"engines": c.get("engines"),
            "live_artifacts": c.get("live_artifacts"),
            "fresh_26h": c.get("fresh_26h"),
            "ticker_sources_consulted":
            len((census or {}).get("per_ticker_sources") or []),
            "census_at": (census or {}).get("generated_at")}


def build_ladder(docs, regime, risk):
    CANARY_VETO = bool((_canary_block(docs.get("canary_warroom"))
                        or {}).get("veto_active"))
    compass = docs.get("asset_compass") or {}
    rows = dig(compass, "assets", "rows") or []
    if isinstance(rows, dict):
        rows = [dict(v, name=k) for k, v in rows.items()
                if isinstance(v, dict)]
    ladder, seen = [], set()
    for row in rows:
        name = dig(row, "name", "asset", "ticker") or "?"
        cls = row.get("class") if row.get("class") in CLASSES else \
            classify_asset(row.get("class")) or classify_asset(name)
        if not cls or cls in seen:
            continue
        seen.add(cls)
        d, d_basis, d_val = leg_discount(row)
        a, a_ratio = leg_asymmetry(row)
        st, st_val = leg_strategic(docs.get("forward_returns"), cls, name)
        pl, pl_dir = leg_plumbing(docs.get("liquidity"), cls)
        cy, cy_det = leg_cycle(docs.get("crypto_cycle"), cls)
        legs = {"discount": d, "asymmetry": a, "strategic": st,
                "regime": PLAYBOOK.get(regime, PLAYBOOK["NEUTRAL"])
                .get(cls, 0.5),
                "plumbing": pl, "cycle": cy}
        score, used = score_legs(legs)
        ladder.append({
            "class": cls, "asset": name, "score": score,
            "verdict": verdict_for(score, legs, risk, canary_veto=CANARY_VETO),
            "legs": {k: (round(v, 3) if isinstance(v, float) else v)
                     for k, v in legs.items()},
            "legs_used": used,
            "audit": {
                "discount": {"basis": d_basis, "pct_vs_trend": d_val},
                "asymmetry_ratio": a_ratio,
                "strategic": st_val, "liquidity": pl_dir,
                "cycle": cy_det,
                "dd_now_pct": num((row.get("asym") or {}).get("dd_now_pct"))
                if isinstance(row.get("asym"), dict) else None,
                "asym_status": (row.get("asym") or {}).get("status")
                if isinstance(row.get("asym"), dict) else None,
                "compass": {k: row.get(k) for k in
                            ("price", "er_1y_pct", "excess_vs_cash_pp")
                            if k in row},
            },
        })
    cc = docs.get("crypto_cycle")
    if cc:  # compass carries no crypto -- build rows from the real
        # on-chain artifact. MVRV = price/realized-price, a legitimate
        # discount-vs-fair analog (below 1.0 = below aggregate cost basis).
        for cls in ("BTC", "ETH"):
            if cls in seen:
                continue
            cy, cy_det = leg_cycle(cc, cls)
            if cy is None:
                continue
            seen.add(cls)
            mvrv = (cy_det or {}).get("mvrv")
            disc = clamp(0.5 + (1.0 - mvrv) * 0.8) if mvrv else None
            pl, pl_dir = leg_plumbing(docs.get("liquidity"), cls)
            st, st_val = leg_strategic(docs.get("forward_returns"),
                                       cls, cls)
            legs = {"discount": disc, "asymmetry": None,
                    "strategic": st,
                    "regime": PLAYBOOK.get(regime,
                                           PLAYBOOK["NEUTRAL"]).get(cls, .5),
                    "plumbing": pl, "cycle": cy}
            score, used = score_legs(legs)
            ladder.append({
                "class": cls, "asset": "%s (on-chain)" % cls,
                "score": score,
                "verdict": verdict_for(score, legs, risk, canary_veto=CANARY_VETO),
                "legs": {k: (round(v, 3) if isinstance(v, float) else v)
                         for k, v in legs.items()},
                "legs_used": used,
                "audit": {"strategic": st_val,
                          "discount": {"basis": "mvrv_vs_realized",
                                       "pct_vs_trend":
                                       round((mvrv - 1) * 100, 1)
                                       if mvrv else None},
                          "cycle": cy_det, "liquidity": pl_dir,
                          "source": "crypto-cycle-risk (compass has no "
                                    "crypto rows)"},
            })
    ladder.sort(key=lambda r: (r["score"] is not None, r["score"] or 0),
                reverse=True)
    return ladder


# ── L4 money map ────────────────────────────────────────────────────────
QUAD_BONUS = {"STEALTH_ACCUMULATION": 1.0, "CAPITULATION": 0.85,
              "TREND_CONFIRMED": 0.55, "DISTRIBUTION_RALLY": 0.10}


def _nav(doc, path):
    """Navigate a census path like '$.congress.top_buys' or
    '$.entries[].flagged_scores'; [] flattens lists."""
    cur = [doc]
    for seg in path.split(".")[1:]:
        nxt = []
        flat = seg.endswith("[]")
        seg = seg[:-2] if flat else seg
        for c in cur:
            v = c.get(seg) if isinstance(c, dict) else None
            if v is None:
                continue
            if flat and isinstance(v, list):
                nxt.extend(v)
            else:
                nxt.append(v)
        cur = nxt
    return cur


def _chip_fields(entry, fields):
    keep = {}
    for f in fields:
        if f in ("ticker", "symbol", "Ticker", "name") or f not in entry:
            continue
        v = entry[f]
        if isinstance(v, (dict, list)):
            continue
        keep[f] = round(v, 3) if isinstance(v, float) else str(v)[:26]
        if len(keep) >= 3:
            break
    return keep


def build_evidence(census, tickers):
    """ops 4278: consult EVERY ticker-keyed artifact the fleet produces
    (the 4277 census) for each candidate name. Data-driven -- new
    engines join automatically when the census re-runs. Fresh only."""
    srcs = (census or {}).get("per_ticker_sources") or []
    ev = {t: [] for t in tickers}
    used, skipped = set(), 0
    for meta in srcs:
        key = meta.get("key")
        if not key or key == "data/best-setups.json":
            continue
        try:
            if LOCAL_DIR:
                import os as _o
                fp = _o.path.join(LOCAL_DIR, key.split("/")[-1])
                if not _o.path.exists(fp):
                    skipped += 1
                    continue
                doc = json.load(open(fp))
            else:
                o = s3.get_object(Bucket=BUCKET, Key=key)
                age_h = (datetime.now(timezone.utc)
                         - o["LastModified"]).total_seconds() / 3600
                if age_h > 72:
                    skipped += 1
                    continue
                doc = json.loads(o["Body"].read())
        except Exception:
            skipped += 1
            continue
        short = key.replace("data/", "").replace(".json", "")
        hit = False
        for node in _nav(doc, meta.get("path") or "$"):
            if meta.get("mode") == "dict" and isinstance(node, dict):
                for t in tickers:
                    e = node.get(t)
                    if isinstance(e, dict):
                        ev[t].append({"src": short, **_chip_fields(
                            e, meta.get("fields") or [])})
                        hit = True
            else:
                rows = node if isinstance(node, list) else [node]
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    rt = str(row.get("ticker") or row.get("symbol")
                             or row.get("Ticker") or "").upper()
                    if rt in ev:
                        ev[rt].append({"src": short, **_chip_fields(
                            row, meta.get("fields") or [])})
                        hit = True
        if hit:
            used.add(short)
    for t in ev:
        seen, uniq = set(), []
        for c in ev[t]:
            if c["src"] in seen:
                continue
            seen.add(c["src"])
            uniq.append(c)
        ev[t] = uniq[:8]
    return ev, {"consulted": len(srcs), "hit_sources": len(used),
                "skipped_stale_or_err": skipped}


def build_money_map(docs, ladder, risk, top_n=12):
    bs = docs.get("best_setups") or {}
    setups = deep_find_setups(bs) or []
    if not setups:
        return [], "best-setups unreadable -- money map ABSTAINS " \
                   "(names never invented)"
    class_score = {r["class"]: (r["score"] or 0.5) for r in ladder}
    size_mult = risk.get("sizing_multiplier") or 1.0
    out = []
    for srow in setups[:60]:
        tk = dig(srow, "ticker", "symbol")
        if not tk:
            continue
        conv = num(srow.get("conviction"))
        conv_n = clamp((conv or 50) / 100.0)
        quad = srow.get("industry_flow_quadrant")
        quad_n = QUAD_BONUS.get(quad, 0.5)
        etf = str(dig(srow, "industry_etf", "rotation_etf") or "").upper()
        # sector/ETF-aware class gate: miners gate on GOLD, energy on
        # ENERGY, etc.; generic equities gate on US_LARGE
        ETF_CLASS = {"GDX": "GOLD", "GDXJ": "GOLD", "SIL": "SILVER",
                     "XLE": "ENERGY", "XOP": "ENERGY", "OIH": "ENERGY",
                     "XME": "COMMODITIES", "URA": "COMMODITIES",
                     "IYR": "REITS", "VNQ": "REITS", "HYG": "CREDIT_HY"}
        cls = ETF_CLASS.get(etf) \
            or classify_asset(dig(srow, "sector", "industry_tag",
                                  "class") or "") \
            or "US_LARGE"
        gate = clamp(class_score.get(cls, 0.5))
        sq = srow.get("squeeze_fuel")
        sm = dig(srow, "khalid_note.stance", "smart_money")
        flow_n = clamp(0.5 + (0.25 if sq else 0)
                       + (0.25 if str(sm).upper() in
                          ("BULL", "BULLISH", "LONG", "ACCUMULATE")
                          else 0))
        fit = round(MM_W["conviction"] * conv_n + MM_W["quadrant"] * quad_n
                    + MM_W["class_gate"] * gate + MM_W["flow"] * flow_n, 3)
        out.append({
            "ticker": tk, "name": srow.get("name"),
            "class": cls, "khalid_fit": fit,
            "size_hint_x": round(size_mult, 2),
            "conviction": conv, "flow_quadrant": quad,
            "squeeze_fuel": bool(sq),
            "earnings_in_days": srow.get("earnings_in_days"),
            "setup_verdict": srow.get("verdict"),
            "red_flags": len(srow.get("red_flags") or []) or None,
            "panel_mult": srow.get("khalid_panel_multiplier"),
            "why": (srow.get("why") or "")[:300] or None,
            "legs": {"conviction": round(conv_n, 3),
                     "quadrant": quad_n, "class_gate": round(gate, 3),
                     "flow": round(flow_n, 3)},
        })
    ev_map, ev_stats = build_evidence(docs.get("sources_census"),
                                      [r["ticker"] for r in out[:40]])
    W_EV = float(os.environ.get("MM_W_EVIDENCE", "0.15"))
    for r in out:
        chips = ev_map.get(r["ticker"]) or []
        r["evidence"] = chips
        r["n_corroborating"] = len(chips)
        r["khalid_fit"] = round(clamp(
            r["khalid_fit"] * (1 - W_EV)
            + W_EV * clamp(len(chips) / 6.0)), 3)
    globals()["_EV_STATS"] = ev_stats
    out.sort(key=lambda r: r["khalid_fit"], reverse=True)
    return out[:top_n], None


# ── handler ─────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    t0 = time.time()
    docs, health = {}, {}
    for name, spec in SOURCES.items():
        doc, meta = read_source(name, spec)
        docs[name] = doc
        health[name] = meta

    reg = regime_consensus(docs)
    regime = reg["regime"]
    risk = risk_layer(docs.get("risk_gate"))
    ladder = build_ladder(docs, regime, risk)
    money_map, mm_note = build_money_map(docs, ladder, risk)

    ok_n = sum(1 for m in health.values() if m["status"] == "ok")
    top = ladder[0] if ladder else None
    out = {
        "version": VERSION, "ops": OPS,
        "generated_at": _now().isoformat(),
        "doctrine": ("Buy quality below the 200-day at capitulation with "
                     "modeled asymmetry, where the regime playbook and "
                     "liquidity plumbing agree. Missing data ABSTAINS; "
                     "the risk-gate is never overridden."),
        "regime": reg,
        "risk_gate": risk,
        "khalid_index": _khalid_index_block(docs.get("ka_metrics")),
        "canary_barometer": _canary_block(docs.get("canary_warroom")),
        "fleet_coverage": _coverage_block(docs.get("sources_census")),
        "evidence_stats": globals().get("_EV_STATS"),
        "best_asset_class": ({"class": top["class"],
                              "verdict": top["verdict"],
                              "score": top["score"]} if top else None),
        "asset_ladder": ladder,
        "money_map": money_map,
        "money_map_note": mm_note,
        "weights": {"ladder": W, "money_map": MM_W},
        "data_health": {"sources_ok": ok_n,
                        "sources_total": len(SOURCES),
                        "detail": health},
        "elapsed_s": round(time.time() - t0, 2),
    }

    if not LOCAL_DIR:
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=300")
        try:  # rolling history (regime + winners), capped
            try:
                hist = json.loads(s3.get_object(
                    Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
            except Exception:
                hist = {"rows": []}
            hist["rows"] = (hist.get("rows") or [])[-179:] + [{
                "t": out["generated_at"], "regime": regime,
                "top_class": top["class"] if top else None,
                "top_names": [m["ticker"] for m in money_map[:3]],
                "sizing_x": risk.get("sizing_multiplier"),
            }]
            s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                          Body=json.dumps(hist).encode(),
                          ContentType="application/json")
        except Exception as e:
            print("[quantum-desk] history append failed: %s" % e)

    print("[quantum-desk] regime=%s sources=%d/%d ladder=%d map=%d "
          "top=%s %.1fs" % (regime, ok_n, len(SOURCES), len(ladder),
                            len(money_map),
                            top["class"] if top else "-",
                            out["elapsed_s"]))
    return {"ok": True, "regime": regime, "sources_ok": ok_n,
            "ladder": len(ladder), "money_map": len(money_map),
            "best_class": top["class"] if top else None}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
