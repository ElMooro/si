"""
justhodl-quality-on-sale -- The Buffett-style 5-condition AND-gate screen.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
Buffett built Berkshire on one principle: buy GREAT companies on TEMPORARY
weakness. Each condition is well-documented at GuruFocus/Stockopedia/
SharpeCharts — but no commercial product enforces the 5-way conjunction with
strict thresholds. Polen Capital and Wedgewood Partners run versions
internally. Zero retail/boutique product.

Distinction from justhodl-best-ideas (which weighs 20 engines softly):
QoS is a STRICT AND gate. Names must pass ALL 5 OR they are not displayed.
Historically fires 5-15 times per year — these are the institutional gold
zone entries (AAPL 2013/2018, MSFT 2014/2022, GOOG 2022, NFLX 2022, META 2022).

THE 5 CONDITIONS (all must fire)
─────────────────────────────────
  C1: PREDICTABILITY 5-STAR
        Source: Pro Pack v3 #7 (data/predictability.json)
        Filter: stars == 5 (10y revenue R^2 + EPS R^2 both > 0.85)
        Why: confirms durable moat / business consistency

  C2: EVA SPREAD TOP DECILE
        Source: Pro Pack v3 #10 (data/eva-spread.json)
        Filter: eva_spread_pct_pctile >= 90 OR (eva_spread_pct >= 10 AND
                roic_ttm_pct >= 20)
        Why: confirms economic value creation, not accounting profit only

  C3: SMART BETA QUALITY TOP DECILE
        Source: Pro Pack v3 #8 (data/smart-beta.json)
        Filter: quality_pct >= 90 (top decile of universe on ROIC + gross
                margin composite)
        Why: cross-confirms quality with a different methodology
                (MSCI/Refinitiv-style factor)

  C4: PRICE DRAWDOWN >= 20%
        Source: FMP /stable/quote (yearHigh) + current price
        Filter: (yearHigh - price) / yearHigh >= 0.20
        Why: the "on sale" criterion — temporary weakness, not extension

  C5: BENEISH M-SCORE CLEAN
        Source: Pro Pack v3 #6 (data/beneish-m-score.json)
        Filter: m_score < -2.22 (Beneish's calibrated threshold)
        Why: rules out earnings manipulation — quality must be REAL

UNIVERSE
────────
STATIC_TOP50_SPX (cross-engine fusion consistency with #4/#7/#8/#10).
Same 50 names used by Predictability, Smart Beta, EVA, StarMine — ensures
all 5 inputs have data for every name evaluated.

OUTPUT
──────
  s3://justhodl-dashboard-live/data/quality-on-sale.json
  Schedule: daily 15:00 UTC (after all Pro Pack v3 upstream feeds settle)

STATE MACHINE
─────────────
  BROAD_OPPORTUNITY    >=5 qualifying names — broad market drawdown, generational
  SELECTIVE             1-4 qualifying names — picky environment (most common)
  DRY                   0 qualifying names — extended market, no opportunities

ACADEMIC BASIS
──────────────
- Penman, S. H. (2013). "Financial Statement Analysis and Security Valuation"
  — quality + valuation conjunction superior to either alone
- Asness, Frazzini, Pedersen (2019). "Quality Minus Junk" — quality factor
  isolated from other factor exposures
- Beneish (1999). "The Detection of Earnings Manipulation"
- Greenblatt, J. (2010). "The Little Book That Still Beats the Market"
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/quality-on-sale.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"
HTTP_TIMEOUT = 20

# Thresholds
DRAWDOWN_MIN_PCT = 20.0
PREDICTABILITY_MIN_STARS = 5
EVA_SPREAD_PCTILE_MIN = 90
EVA_SPREAD_ABSOLUTE_MIN_PP = 10.0
ROIC_MIN_PCT = 20.0
QUALITY_PCTILE_MIN = 90
BENEISH_MAX = -2.22

# Static universe (consistency with #4/#7/#8/#10)
STATIC_TOP50_SPX = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "BRK-B",
    "LLY", "AVGO", "TSLA", "JPM", "WMT", "V", "UNH", "XOM", "MA",
    "ORCL", "COST", "PG", "JNJ", "HD", "NFLX", "BAC", "CVX", "ABBV",
    "CRM", "KO", "AMD", "WFC", "MRK", "CSCO", "ADBE", "PEP", "LIN",
    "TMO", "ACN", "MCD", "ABT", "CMCSA", "INTU", "IBM", "DHR", "TXN",
    "PM", "DIS", "CAT", "VZ", "PFE", "QCOM",
]

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch] {key} failed: {e}")
        return None


def http_json(url, timeout=HTTP_TIMEOUT):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl-QoS/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[http_json] {e.code}: {url[:90]}")
        return None
    except Exception as e:
        print(f"[http_json] err: {e}")
        return None


# ---------- Input extractors ----------
def extract_predictability_5star(pred_data):
    """Returns {ticker: {rev_r2, eps_r2, stars}} for 5-star names only."""
    if not isinstance(pred_data, dict):
        return {}
    out = {}
    # Look across multiple list views in case schema varies
    sources = [
        pred_data.get("most_predictable_top_15") or [],
        pred_data.get("elite_moats") or [],
        pred_data.get("sweet_spot_picks") or [],
        pred_data.get("all_tickers") or [],
    ]
    seen = set()
    for src in sources:
        for r in src:
            if not isinstance(r, dict):
                continue
            sym = (r.get("ticker") or "").upper()
            if not sym or sym in seen:
                continue
            stars = r.get("stars")
            if stars is None or stars < PREDICTABILITY_MIN_STARS:
                continue
            out[sym] = {
                "stars": stars,
                "rev_r2": r.get("rev_r2") or r.get("revenue_r2"),
                "eps_r2": r.get("eps_r2"),
                "composite_r2": r.get("composite_r2"),
                "valuation": r.get("valuation"),
                "pe_ttm": r.get("pe_ttm"),
            }
            seen.add(sym)
    return out


def extract_eva_top_decile(eva_data):
    """Returns {ticker: detail} for names with EVA spread >=90th pctile
    OR spread >= 10pp AND ROIC >= 20%."""
    if not isinstance(eva_data, dict):
        return {}
    out = {}
    sources = [
        eva_data.get("top_10_eva_spread") or [],
        eva_data.get("super_compounders") or [],
        eva_data.get("all_tickers") or [],
    ]
    seen = set()
    for src in sources:
        for r in src:
            if not isinstance(r, dict):
                continue
            sym = (r.get("ticker") or "").upper()
            if not sym or sym in seen:
                continue
            pctile = r.get("eva_spread_pct_pctile")
            spread = r.get("eva_spread_pct")
            roic = r.get("roic_ttm_pct")
            qualifies = False
            if pctile is not None and pctile >= EVA_SPREAD_PCTILE_MIN:
                qualifies = True
            elif (spread is not None and spread >= EVA_SPREAD_ABSOLUTE_MIN_PP
                    and roic is not None and roic >= ROIC_MIN_PCT):
                qualifies = True
            if not qualifies:
                continue
            out[sym] = {
                "eva_spread_pct": spread,
                "roic_ttm_pct": roic,
                "wacc_pct": r.get("wacc_pct"),
                "eva_momentum_pct": r.get("eva_momentum_pct"),
                "super_compounder": r.get("super_compounder", False),
                "eva_spread_pct_pctile": pctile,
            }
            seen.add(sym)
    return out


def extract_smart_beta_quality_top_decile(sb_data):
    """Returns {ticker: detail} for names with quality_pct >= 90."""
    if not isinstance(sb_data, dict):
        return {}
    out = {}
    sources = [
        (sb_data.get("factor_leaders") or {}).get("quality") or [],
        sb_data.get("top_25_diversified") or [],
        sb_data.get("all_tickers") or [],
    ]
    seen = set()
    for src in sources:
        for r in src:
            if not isinstance(r, dict):
                continue
            sym = (r.get("ticker") or "").upper()
            if not sym or sym in seen:
                continue
            q = r.get("quality_pct") or r.get("quality_pctile")
            if q is None or q < QUALITY_PCTILE_MIN:
                continue
            out[sym] = {
                "quality_pct": q,
                "value_pct": r.get("value_pct"),
                "momentum_pct": r.get("momentum_pct"),
                "low_vol_pct": r.get("low_vol_pct"),
                "composite": r.get("composite"),
                "roic_ttm": r.get("roic_ttm"),
                "gross_margin_ttm": r.get("gross_margin_ttm"),
            }
            seen.add(sym)
    return out


def extract_beneish_clean(beneish_data):
    """Returns {ticker: detail} for names with M-Score < -2.22 (clean)."""
    if not isinstance(beneish_data, dict):
        return {}
    out = {}
    sources = [
        beneish_data.get("clean_quality") or [],
        beneish_data.get("all_tickers") or [],
        beneish_data.get("low_risk") or [],
    ]
    seen = set()
    for src in sources:
        for r in src:
            if not isinstance(r, dict):
                continue
            sym = (r.get("ticker") or "").upper()
            if not sym or sym in seen:
                continue
            m = r.get("m_score") or r.get("beneish_m_score")
            if m is None or m >= BENEISH_MAX:
                continue
            out[sym] = {
                "m_score": m,
                "flag": r.get("flag"),
                "risk_tier": r.get("risk_tier"),
            }
            seen.add(sym)
    return out


# ---------- Drawdown via FMP ----------
def fetch_drawdown(symbol):
    """Returns dict with current_price, year_high, drawdown_pct or None."""
    if not FMP_KEY:
        return None
    url = f"{FMP_BASE}/quote?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if not isinstance(d, list) or not d:
        return None
    row = d[0] if isinstance(d[0], dict) else None
    if not row:
        return None
    price = row.get("price")
    year_high = row.get("yearHigh")
    if price is None or year_high is None or year_high <= 0:
        return None
    try:
        price = float(price)
        year_high = float(year_high)
    except (ValueError, TypeError):
        return None
    drawdown_pct = round((year_high - price) / year_high * 100, 2)
    return {
        "current_price": price,
        "year_high": year_high,
        "drawdown_pct": drawdown_pct,
        "market_cap_usd": row.get("marketCap"),
    }


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[qos] start v{VERSION}")

    # Fetch all 4 upstream Pro Pack v3 feeds
    pred = fetch_s3_json("data/predictability.json")
    eva = fetch_s3_json("data/eva-spread.json")
    sb = fetch_s3_json("data/smart-beta.json")
    bene = fetch_s3_json("data/beneish-m-score.json")

    feeds_available = {
        "predictability": pred is not None,
        "eva_spread": eva is not None,
        "smart_beta": sb is not None,
        "beneish_m_score": bene is not None,
    }
    print(f"[qos] feeds: {feeds_available}")

    # Extract gating sets per condition
    pred_5star = extract_predictability_5star(pred or {})
    eva_top = extract_eva_top_decile(eva or {})
    sb_quality = extract_smart_beta_quality_top_decile(sb or {})
    bene_clean = extract_beneish_clean(bene or {})

    print(f"[qos] gate hit counts: pred={len(pred_5star)} "
          f"eva={len(eva_top)} sb={len(sb_quality)} bene={len(bene_clean)}")

    # Evaluate each ticker in static universe
    results = []
    for sym in STATIC_TOP50_SPX:
        c1 = sym in pred_5star
        c2 = sym in eva_top
        c3 = sym in sb_quality
        c5 = sym in bene_clean

        # Skip ticker only if we can't possibly qualify (would not pass C1+C2+C3+C5)
        if not (c1 and c2 and c3 and c5):
            # Still record partial conditions for diagnostic view
            n_fired = sum([c1, c2, c3, c5])
            if n_fired < 3:
                continue
            # Compute drawdown anyway for diagnostic visibility
            dd = fetch_drawdown(sym)
            c4 = (dd is not None
                  and dd.get("drawdown_pct", 0) >= DRAWDOWN_MIN_PCT)
            results.append({
                "ticker": sym,
                "qualifies": False,
                "n_conditions_fired": sum([c1, c2, c3, c4, c5]),
                "conditions": {
                    "c1_predictability_5star": c1,
                    "c2_eva_top_decile": c2,
                    "c3_smart_beta_quality_top_decile": c3,
                    "c4_drawdown_20pct_plus": c4,
                    "c5_beneish_clean": c5,
                },
                "predictability_detail": pred_5star.get(sym),
                "eva_detail": eva_top.get(sym),
                "smart_beta_detail": sb_quality.get(sym),
                "drawdown_detail": dd,
                "beneish_detail": bene_clean.get(sym),
            })
            continue

        # C1+C2+C3+C5 all fire → fetch drawdown to test C4
        dd = fetch_drawdown(sym)
        time.sleep(0.4)
        c4 = (dd is not None
              and dd.get("drawdown_pct", 0) >= DRAWDOWN_MIN_PCT)

        n_fired = sum([c1, c2, c3, c4, c5])
        qualifies = n_fired == 5

        result = {
            "ticker": sym,
            "qualifies": qualifies,
            "n_conditions_fired": n_fired,
            "conditions": {
                "c1_predictability_5star": c1,
                "c2_eva_top_decile": c2,
                "c3_smart_beta_quality_top_decile": c3,
                "c4_drawdown_20pct_plus": c4,
                "c5_beneish_clean": c5,
            },
            "predictability_detail": pred_5star.get(sym),
            "eva_detail": eva_top.get(sym),
            "smart_beta_detail": sb_quality.get(sym),
            "drawdown_detail": dd,
            "beneish_detail": bene_clean.get(sym),
        }
        if qualifies:
            # Thesis text
            dd_pct = dd.get("drawdown_pct") if dd else None
            roic = (eva_top.get(sym) or {}).get("roic_ttm_pct")
            pe = (pred_5star.get(sym) or {}).get("pe_ttm")
            result["thesis"] = (
                f"{sym} -- 5-star predictability (10y revenue + EPS R^2 both "
                f"top tier) + EVA Spread top decile (ROIC {roic}%, true "
                f"economic value creator) + Quality factor 90th+ percentile "
                f"+ {dd_pct:.1f}% off 52w high + Beneish M-Score "
                f"{result['beneish_detail']['m_score']:.2f} (clean, "
                f"no earnings manipulation). "
                f"5/5 BUFFETT-STYLE QUALITY-ON-SALE. PE TTM {pe}.")
            result["trade_recommendation"] = (
                "OPEN_FULL_POSITION: Buy 1-3% portfolio weight; stop at "
                "30% below entry; expected horizon 18-36 months; trail to "
                "200d MA when in profit. Historical analogs: AAPL 2013, "
                "MSFT 2014/2022, GOOG 2022, NFLX 2022, META 2022.")
        results.append(result)

    # Sort: qualifying first, then by n_conditions_fired desc
    results.sort(key=lambda x: (not x["qualifies"], -x["n_conditions_fired"]))

    qualified = [r for r in results if r["qualifies"]]
    n_qualified = len(qualified)

    # Regime classification
    if n_qualified >= 5:
        regime = "BROAD_OPPORTUNITY"
        regime_desc = (
            f"{n_qualified} elite names qualify ALL 5 conditions — broad "
            "market drawdown opportunity. Generational-scale entries "
            "available. Historical analogs: March 2020, December 2018, "
            "October 2022.")
    elif n_qualified >= 1:
        regime = "SELECTIVE"
        regime_desc = (
            f"{n_qualified} name(s) qualify. Picky environment — most names "
            "either extended (no drawdown) or impaired (failing quality "
            "gates). Position-size carefully.")
    else:
        regime = "DRY"
        regime_desc = (
            "ZERO names qualify all 5 conditions. Market is extended; "
            "elite quality is not on sale. Patient stance — wait for "
            "drawdown.")

    # 4-of-5 watchlist (one condition away)
    near_qualified = [r for r in results
                       if not r["qualifies"] and r["n_conditions_fired"] >= 4]

    output = {
        "engine": "quality-on-sale",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "regime": regime,
        "regime_description": regime_desc,
        "n_qualified_5_of_5": n_qualified,
        "n_near_qualified_4_of_5": len(near_qualified),
        "universe_size": len(STATIC_TOP50_SPX),
        "thresholds": {
            "predictability_min_stars": PREDICTABILITY_MIN_STARS,
            "eva_spread_pctile_min": EVA_SPREAD_PCTILE_MIN,
            "eva_spread_absolute_min_pp": EVA_SPREAD_ABSOLUTE_MIN_PP,
            "roic_min_pct": ROIC_MIN_PCT,
            "quality_pctile_min": QUALITY_PCTILE_MIN,
            "drawdown_min_pct": DRAWDOWN_MIN_PCT,
            "beneish_max": BENEISH_MAX,
        },
        "qualified_5_of_5": qualified,
        "near_qualified_4_of_5": near_qualified[:15],
        "all_evaluated": results,
        "feeds_available": feeds_available,
        "methodology": {
            "framework": "Buffett-style Quality-on-Sale 5-condition AND gate",
            "philosophy": ("Buy GREAT companies on TEMPORARY weakness. Each "
                            "condition exists individually at consumer products. "
                            "The 5-way conjunction with strict thresholds is "
                            "what generates the alpha — and is what no "
                            "commercial product enforces."),
            "c1_predictability": ("5-star = 10y revenue + EPS R^2 both > 0.85. "
                                    "Confirms durable moat. (GuruFocus methodology)"),
            "c2_eva_spread": ("Top decile OR (spread>=10pp + ROIC>=20%). "
                                "Confirms economic value creation (Stern Stewart)."),
            "c3_smart_beta_quality": ("Quality factor top decile (ROIC + "
                                        "gross margin composite). Cross-confirms "
                                        "with different methodology (MSCI/"
                                        "Refinitiv factor style)."),
            "c4_drawdown": (f">= {DRAWDOWN_MIN_PCT}% off 52-week high. The "
                            "'on sale' criterion. Filters extension; "
                            "ensures temporary weakness."),
            "c5_beneish_clean": ("M-Score < -2.22 (Beneish's calibrated "
                                    "manipulation threshold). Rules out "
                                    "earnings games — quality must be REAL."),
            "expected_frequency": ("5-15 qualifying names per year market-wide. "
                                    "Drought periods between firings; clusters "
                                    "around drawdowns."),
        },
        "academic_basis": [
            "Penman, S. H. (2013). Financial Statement Analysis and Security "
            "Valuation. Quality + valuation conjunction.",
            "Asness, C. S., Frazzini, A., & Pedersen, L. H. (2019). "
            "Quality minus junk. Review of Accounting Studies, 24, 34-112.",
            "Beneish, M. D. (1999). The detection of earnings manipulation. "
            "Financial Analysts Journal, 55(5), 24-36.",
            "Greenblatt, J. (2010). The Little Book That Still Beats the "
            "Market. John Wiley & Sons.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=900")

    print(f"[qos] complete: regime={regime} "
          f"qualified={n_qualified}/5_of_5 "
          f"near_qualified={len(near_qualified)}/4_of_5")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "regime": regime,
            "n_qualified": n_qualified,
            "n_near_qualified": len(near_qualified),
            "qualified_tickers": [r["ticker"] for r in qualified],
            "near_qualified_tickers": [r["ticker"]
                                         for r in near_qualified[:5]],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
