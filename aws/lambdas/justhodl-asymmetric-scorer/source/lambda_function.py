"""
justhodl-asymmetric-scorer — Phase 2B.

Reads screener/data.json, scores stocks on 4 dimensions:
  quality, safety, value, momentum.
Filters for stocks ranking well on ≥3 of 4 with quality gate passed.
Outputs top 30 setups + value_traps list to S3.
"""
import json
import os
import statistics
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


# Quality gate — minimum to be considered a setup
MIN_PIOTROSKI = 6      # 6 of 9 — improving company
MAX_DEBT_EQUITY = 1.5  # not over-leveraged
MIN_CURRENT_RATIO = 1.0  # can pay short-term bills
MIN_PRICE = 5.0        # avoid penny stocks
MIN_MARKET_CAP = 1_000_000_000  # $1B minimum (liquidity)

# Setup filter — must rank well on ≥3 of 5 dimensions
# (Phase 11A: added stacked_conviction as 5th dimension. Was 3 of 4.)
DIMS_REQUIRED = 3
TOP_PCT_PER_DIM = 0.40  # rank within top 40% on the dimension


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def safe_float(v, default=None):
    try:
        if v is None: return default
        f = float(v)
        return f if f == f else default  # NaN check
    except Exception:
        return default


def passes_quality_gate(s):
    """Return (passes, fail_reason). Hard filters before any scoring."""
    pe = safe_float(s.get("peRatio"))
    pio = safe_float(s.get("piotroski"), 0)
    de = safe_float(s.get("debtToEquity"))
    cr = safe_float(s.get("currentRatio"))
    pr = safe_float(s.get("price"), 0)
    mc = safe_float(s.get("marketCap"), 0)
    fcfg = safe_float(s.get("fcfGrowth"))

    if pr is None or pr < MIN_PRICE:
        return False, "price_too_low"
    if mc < MIN_MARKET_CAP:
        return False, "market_cap_small"
    if pio is None or pio < MIN_PIOTROSKI:
        return False, "piotroski_low"
    if de is not None and de > MAX_DEBT_EQUITY:
        return False, "debt_high"
    if cr is not None and cr < MIN_CURRENT_RATIO:
        return False, "liquidity_weak"
    if pe is None or pe <= 0:
        return False, "no_earnings"
    if fcfg is None or fcfg < 0:
        return False, "fcf_negative"
    return True, None


def quality_score(s):
    """Quality dimension: Piotroski + margins + ROE."""
    pio = safe_float(s.get("piotroski"), 0)
    roe = safe_float(s.get("roe"), 0)
    om = safe_float(s.get("operatingMargin"), 0)
    nm = safe_float(s.get("netMargin"), 0)
    # Normalize Piotroski 0-9 → 0-100
    pio_norm = (pio / 9) * 100
    # Margins: cap at sensible upper bounds
    om_norm = max(0, min(100, om * 250)) if om else 0  # 40% margin → 100
    nm_norm = max(0, min(100, nm * 333)) if nm else 0  # 30% margin → 100
    roe_norm = max(0, min(100, roe * 333)) if roe else 0  # 30% ROE → 100
    # Average
    return round((pio_norm + om_norm + nm_norm + roe_norm) / 4, 1)


def safety_score(s):
    """Safety dimension: balance sheet."""
    de = safe_float(s.get("debtToEquity"))
    cr = safe_float(s.get("currentRatio"))
    ic = safe_float(s.get("interestCoverage"))

    # D/E: 0 = best, 1.5 = limit
    if de is None: de_norm = 50
    else: de_norm = max(0, min(100, (1.5 - de) / 1.5 * 100))

    # Current ratio: 1.0 = ok, 2.0+ = great
    if cr is None: cr_norm = 50
    else: cr_norm = max(0, min(100, (cr - 1.0) * 100))

    # Interest coverage: ≥3x = healthy, ≥10x = great
    if ic is None: ic_norm = 50
    else: ic_norm = max(0, min(100, ic * 10))

    return round((de_norm + cr_norm + ic_norm) / 3, 1)


def value_score(s, sector_medians):
    """Value dimension: rank within sector by valuation multiples.
    Lower multiples = higher value score.
    """
    sector = s.get("sector", "")
    medians = sector_medians.get(sector, {})

    pe = safe_float(s.get("peRatio"))
    ps = safe_float(s.get("psRatio"))
    ev = safe_float(s.get("evEbitda"))

    scores = []

    if pe and medians.get("pe"):
        # 50% of median = score 100; 200% of median = score 0
        ratio = pe / medians["pe"]
        sc = max(0, min(100, (2.0 - ratio) * 100))
        scores.append(sc)
    if ps and medians.get("ps"):
        ratio = ps / medians["ps"]
        sc = max(0, min(100, (2.0 - ratio) * 100))
        scores.append(sc)
    if ev and medians.get("ev") and medians["ev"] > 0:
        ratio = ev / medians["ev"]
        sc = max(0, min(100, (2.0 - ratio) * 100))
        scores.append(sc)

    if not scores:
        return None
    return round(sum(scores) / len(scores), 1)


def momentum_score(s):
    """Momentum dimension: revenue + EPS + FCF growth."""
    rg = safe_float(s.get("revenueGrowth"), 0)
    eg = safe_float(s.get("epsGrowth"), 0)
    fg = safe_float(s.get("fcfGrowth"), 0)
    # Each growth metric: 0% = 30, 20% = 70, 40%+ = 100
    def norm(g):
        if g is None or g != g: return 30
        return max(0, min(100, 30 + g * 175))
    return round((norm(rg) + norm(eg) + norm(fg)) / 3, 1)


# ─────────────────────────────────────────────────────────────────────
#  CROSS-POLLINATION (Phase 11A) — stacked conviction from new feeds
# ─────────────────────────────────────────────────────────────────────
#  Reads the Tier S+A data files and builds a per-ticker dict that maps
#  ticker → {stacked_score, signals: [...]}. Folded into the per-stock
#  output as a 5th dimension. Setups now require ≥3 of 5 (DIMS_REQUIRED).
#
#  Each contributing signal type:
#    insider_cluster_buy     +25 pts  (3+ insiders / 14d, ≥$25k each)
#    big_insider_buy         +15 pts  ($1M+ single Form 4 buy)
#    institutional_new_add   +15 pts  (13F new filing including ticker)
#    bullish_8k_event        +10 pts  (Item 1.01 / 2.01 / 5.02 / 7.01)
#    bearish_8k_event        -25 pts  (Item 4.02 / 1.03 / 3.01 / 5.04 — RED)
#    extreme_aaii_bear        +5 pts  (broad market tailwind)
#    extreme_aaii_bull        -5 pts  (broad market headwind)
#    onchain_btc_oversold     +0 pts (informational only — affects portfolio
#                                     not individual ticker conviction)
#
#  Cap: total stacked_score is clipped to [-50, +50] then normalized to
#  0-100 via (score + 50). So 0 = max bearish stack, 100 = max bullish.
#  Ties default to 50 (neutral).

# 8-K item taxonomy (from justhodl-sec-8k Lambda)
BULLISH_8K_ITEMS = {"1.01", "2.01", "2.02", "5.02", "7.01", "8.01"}  # contracts, M&A, earnings, leadership, FD, other
BEARISH_8K_ITEMS = {"4.02", "1.03", "3.01", "5.04", "2.06", "2.04"}  # restatement, bankruptcy, delisting, halt, impairment, accelerated debt

STACKED_CAP_POS = 50
STACKED_CAP_NEG = -50


def _index_by_ticker(items, ticker_field="ticker"):
    """Group a list of records by ticker symbol. Multiple per ticker → list."""
    out = {}
    for item in items or []:
        t = (item.get(ticker_field) or "").upper().strip()
        if not t:
            continue
        out.setdefault(t, []).append(item)
    return out


def _company_name_to_ticker_match(company, ticker_set):
    """Best-effort: 8-K filings have company names not tickers.
    Match by checking if any ticker's company name appears in the 8-K title.
    This is fuzzy; we keep it conservative (exact-substring only)."""
    if not company:
        return None
    company_upper = company.upper()
    # If "APPLE INC" in the 8-K and "AAPL" in our set with name matching, link them.
    # We don't have name→ticker in the 8-K data, so for now we skip and just
    # return None. A future iteration can use a CIK→ticker lookup.
    return None


def load_cross_pollination():
    """Load all 5 ticker-level cross-pollination files and build a per-ticker
    score dict. Missing files are tolerated — the corresponding signal just
    contributes 0.

    Returns: {ticker_upper: {"stacked_score": float 0-100,
                              "signals": [str, ...],
                              "raw_pts": int}}
    """
    insider_data = get_s3_json("data/insider-trades.json", {}) or {}
    inst_data    = get_s3_json("data/institutional-positions.json", {}) or {}
    sec8k_data   = get_s3_json("data/8k-filings.json", {}) or {}
    aaii_data    = get_s3_json("data/aaii-sentiment.json", {}) or {}
    onchain_data = get_s3_json("data/onchain-ratios.json", {}) or {}

    # 1. Index insider clusters + big buys by ticker
    clusters = insider_data.get("clusters", []) or []
    big_buys = insider_data.get("big_buys", []) or []
    insider_clusters_by_tkr = _index_by_ticker(clusters)
    insider_bigs_by_tkr     = _index_by_ticker(big_buys)

    # 2. Index 13F new filings (filings since prior daily run)
    inst_new = inst_data.get("new_filings", []) or []
    # 13F new filings don't directly include tickers — they include the
    # filer + accession. The position-level data would require parsing the
    # 13F-HR XML. For now we surface a "watch_list" tag per fund. Until
    # we add 13F-XML parsing, this signal contributes nothing per-ticker.
    # (Hook is here so we can wire it later without re-architecting.)
    inst_filings_by_tkr = {}   # ticker → list of {fund_name, accession}

    # 3. Index 8-K filings by ticker.
    # The 8-K data has 'company' (not ticker). To link, we'd need a
    # company-name → ticker lookup. Conservative: skip until name-mapping
    # is built. Hook stays so later we just populate this dict.
    sec8k_filings = sec8k_data.get("filings", []) or []
    sec8k_by_tkr = {}   # ticker → list of {items, accession, filed_at}
    # Future: build company-name → ticker map from screener data and
    # populate sec8k_by_tkr here.

    # 4. AAII broad-market signal (applies to ALL tickers equally)
    aaii_latest = aaii_data.get("latest", {}) or {}
    aaii_extremes = aaii_data.get("extremes", {}) or {}
    aaii_market_pts = 0
    aaii_market_signal = None
    if aaii_extremes.get("is_bearish_extreme"):
        aaii_market_pts = +5
        aaii_market_signal = f"aaii_extreme_bearish (spread {aaii_latest.get('bull_bear_spread', 0)*100:+.0f}% — contrarian tailwind)"
    elif aaii_extremes.get("is_bullish_extreme"):
        aaii_market_pts = -5
        aaii_market_signal = f"aaii_extreme_bullish (spread {aaii_latest.get('bull_bear_spread', 0)*100:+.0f}% — contrarian headwind)"

    # Now build per-ticker scores. Iterate the union of all tickers we know.
    all_tickers = set(insider_clusters_by_tkr.keys()) | set(insider_bigs_by_tkr.keys()) \
                | set(inst_filings_by_tkr.keys()) | set(sec8k_by_tkr.keys())

    out = {}
    for t in all_tickers:
        signals = []
        pts = 0

        if t in insider_clusters_by_tkr:
            clusters_for = insider_clusters_by_tkr[t]
            cluster = clusters_for[0]   # take the strongest (already sorted by total_value)
            signals.append(f"insider_cluster_buy ({cluster.get('insider_count', '?')} insiders, ${cluster.get('total_value', 0):,.0f})")
            pts += 25

        if t in insider_bigs_by_tkr:
            bigs = insider_bigs_by_tkr[t]
            top = max(bigs, key=lambda b: b.get("value", 0))
            signals.append(f"big_insider_buy (${top.get('value', 0):,.0f} by {top.get('insider', '?')[:30]})")
            pts += 15

        if t in inst_filings_by_tkr:
            f0 = inst_filings_by_tkr[t][0]
            signals.append(f"institutional_new_filing ({f0.get('fund_name', '?')})")
            pts += 15

        if t in sec8k_by_tkr:
            for filing in sec8k_by_tkr[t]:
                items = filing.get("items", [])
                bullish_items = [i for i in items if i in BULLISH_8K_ITEMS]
                bearish_items = [i for i in items if i in BEARISH_8K_ITEMS]
                if bearish_items:
                    signals.append(f"bearish_8k_event (Item {' / '.join(bearish_items)} — RED FLAG)")
                    pts -= 25
                elif bullish_items:
                    signals.append(f"bullish_8k_event (Item {' / '.join(bullish_items)})")
                    pts += 10

        # Apply broad-market AAII signal to every ticker
        if aaii_market_signal:
            signals.append(aaii_market_signal)
            pts += aaii_market_pts

        # Cap and normalize to 0-100 (50 = neutral)
        capped = max(STACKED_CAP_NEG, min(STACKED_CAP_POS, pts))
        score_0_100 = round(((capped - STACKED_CAP_NEG) / (STACKED_CAP_POS - STACKED_CAP_NEG)) * 100, 1)

        out[t] = {
            "stacked_score": score_0_100,
            "raw_pts": pts,
            "signals": signals,
        }

    # Also produce a "no-stack" baseline for tickers that have NO ticker-level
    # signals but still get the broad-market AAII contribution. We don't
    # populate one entry per universe ticker (would be 500+); instead we
    # surface aaii_market_pts so the per-stock loop can apply it as a default.
    return {
        "by_ticker": out,
        "broad_market": {
            "aaii_pts": aaii_market_pts,
            "aaii_signal": aaii_market_signal,
            "btc_mvrv": (onchain_data.get("btc") or {}).get("mvrv"),
            "onchain_extreme_signals": (onchain_data.get("btc") or {}).get("extreme_signals", []),
        },
        "summary": {
            "tickers_with_stacking": len(out),
            "n_clusters": len(clusters),
            "n_big_buys": len(big_buys),
            "aaii_extreme": bool(aaii_market_signal),
        },
    }


def stacked_score_for_ticker(ticker, cross_data):
    """Look up a ticker's stacked-conviction score, falling back to the
    broad-market default (which captures only AAII for now)."""
    t = (ticker or "").upper()
    if not t:
        return None, [], 0
    by_tkr = cross_data["by_ticker"]
    if t in by_tkr:
        x = by_tkr[t]
        return x["stacked_score"], x["signals"], x["raw_pts"]

    # No ticker-level signals → only broad-market contributes
    pts = cross_data["broad_market"]["aaii_pts"]
    capped = max(STACKED_CAP_NEG, min(STACKED_CAP_POS, pts))
    score_0_100 = round(((capped - STACKED_CAP_NEG) / (STACKED_CAP_POS - STACKED_CAP_NEG)) * 100, 1)
    sigs = []
    aaii_sig = cross_data["broad_market"].get("aaii_signal")
    if aaii_sig:
        sigs.append(aaii_sig)
    return score_0_100, sigs, pts


def compute_sector_medians(stocks):
    """For each sector, compute median P/E, P/S, EV/EBITDA."""
    by_sector = {}
    for s in stocks:
        sector = s.get("sector", "")
        if not sector: continue
        by_sector.setdefault(sector, []).append(s)

    medians = {}
    for sector, group in by_sector.items():
        pes = [safe_float(s.get("peRatio")) for s in group]
        pes = [p for p in pes if p and p > 0]
        pss = [safe_float(s.get("psRatio")) for s in group]
        pss = [p for p in pss if p and p > 0]
        evs = [safe_float(s.get("evEbitda")) for s in group]
        evs = [p for p in evs if p and p > 0]
        medians[sector] = {
            "pe": statistics.median(pes) if pes else None,
            "ps": statistics.median(pss) if pss else None,
            "ev": statistics.median(evs) if evs else None,
            "n": len(group),
        }
    return medians


def get_telegram_creds():
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        return token, chat_id
    except Exception:
        return None, None


def send_telegram(message):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id: return False
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": message, "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            return r.status == 200
    except Exception as e:
        print(f"[TG] {e}")
        return False


def lambda_handler(event, context):
    print("=== ASYMMETRIC EQUITY SCORER v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Load screener output
    screener_data = get_s3_json("screener/data.json", {})
    stocks = screener_data.get("results", []) or screener_data.get("stocks", []) or []
    if not stocks:
        # Try alternate shape
        for k in ("data", "items", "rows"):
            if isinstance(screener_data.get(k), list):
                stocks = screener_data[k]
                break

    if not stocks:
        return {"statusCode": 500,
                "body": json.dumps({"error": "no_screener_data",
                                    "screener_keys": list(screener_data.keys())[:5]})}
    print(f"  Loaded {len(stocks)} stocks from screener")

    # 2. Compute sector medians
    sector_medians = compute_sector_medians(stocks)
    print(f"  Computed medians for {len(sector_medians)} sectors")

    # 2b. Load Tier S+A cross-pollination data (Phase 11A)
    cross_data = load_cross_pollination()
    print(f"  Cross-pollination: {cross_data['summary']['tickers_with_stacking']} tickers with stacking signals "
          f"| {cross_data['summary']['n_clusters']} clusters | {cross_data['summary']['n_big_buys']} big buys "
          f"| AAII extreme: {cross_data['summary']['aaii_extreme']}")

    # 3. Score each stock
    scored = []
    quality_failures = {}
    for s in stocks:
        passes, reason = passes_quality_gate(s)
        if not passes:
            quality_failures[reason] = quality_failures.get(reason, 0) + 1
            # Special case: looks cheap but fails quality? Track as value trap
            ps = safe_float(s.get("psRatio"))
            pe = safe_float(s.get("peRatio"))
            if reason in ("piotroski_low", "fcf_negative", "debt_high"):
                if pe and pe > 0 and pe < 12:  # looks cheap on P/E
                    scored.append({
                        **{k: s.get(k) for k in ("symbol", "name", "sector", "price", "marketCap", "peRatio", "psRatio", "piotroski", "debtToEquity")},
                        "category": "value_trap",
                        "trap_reason": reason,
                    })
            continue

        q = quality_score(s)
        sf_score = safety_score(s)
        v = value_score(s, sector_medians)
        m = momentum_score(s)
        # 5th dimension: stacked conviction from Tier S+A feeds
        stacked, stacked_signals, stacked_raw = stacked_score_for_ticker(s.get("symbol"), cross_data)

        # Count dimensions where this stock is top 40% in sample
        # We'll determine percentile cutoffs after scoring all
        scored.append({
            **{k: s.get(k) for k in ("symbol", "name", "sector", "price", "marketCap",
                                      "peRatio", "psRatio", "evEbitda",
                                      "roe", "operatingMargin", "netMargin",
                                      "revenueGrowth", "epsGrowth", "fcfGrowth",
                                      "debtToEquity", "currentRatio", "interestCoverage",
                                      "piotroski", "beta")},
            "quality_score": q,
            "safety_score": sf_score,
            "value_score": v,
            "momentum_score": m,
            "stacked_score": stacked,
            "stacked_signals": stacked_signals,
            "stacked_raw_pts": stacked_raw,
            "category": "candidate",
        })

    candidates = [s for s in scored if s.get("category") == "candidate"]
    value_traps = [s for s in scored if s.get("category") == "value_trap"]

    print(f"  Quality gate failures: {quality_failures}")
    print(f"  Candidates: {len(candidates)}, Value traps tracked: {len(value_traps)}")

    if not candidates:
        return {"statusCode": 200,
                "body": json.dumps({"warning": "no_candidates_after_quality_gate",
                                    "failures": quality_failures})}

    # 4. Compute percentile cutoffs across candidates
    def cutoff(field, pct=1 - TOP_PCT_PER_DIM):
        vals = [s.get(field) for s in candidates if s.get(field) is not None]
        if not vals: return None
        sv = sorted(vals)
        idx = int(len(sv) * pct)
        return sv[min(idx, len(sv) - 1)]

    cutoffs = {
        "quality": cutoff("quality_score"),
        "safety": cutoff("safety_score"),
        "value": cutoff("value_score"),
        "momentum": cutoff("momentum_score"),
        "stacked": cutoff("stacked_score"),
    }
    print(f"  Cutoffs (60th pct): {cutoffs}")

    # 5. Mark how many dimensions each candidate passes
    for s in candidates:
        n_pass = 0
        passes = []
        for dim, key in [("quality", "quality_score"), ("safety", "safety_score"),
                         ("value", "value_score"), ("momentum", "momentum_score"),
                         ("stacked", "stacked_score")]:
            v = s.get(key)
            if v is not None and cutoffs[dim] is not None and v >= cutoffs[dim]:
                n_pass += 1
                passes.append(dim)
        s["dims_passed"] = n_pass
        s["dims_passed_list"] = passes
        # Composite score for ranking — average of all 5 dims (None → skipped)
        valid_scores = [s.get(k) for k in ("quality_score", "safety_score", "value_score", "momentum_score", "stacked_score")
                        if s.get(k) is not None]
        s["composite_score"] = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else 0

    # 6. Filter to setups
    setups = [s for s in candidates if s.get("dims_passed", 0) >= DIMS_REQUIRED]
    setups.sort(key=lambda x: (x["dims_passed"], x["composite_score"]), reverse=True)

    print(f"  Setups passing ≥{DIMS_REQUIRED} dims: {len(setups)}")
    print(f"  Top 5: {[s['symbol'] for s in setups[:5]]}")

    # 7. Sector breakdown of setups
    sector_counts = {}
    for s in setups:
        sector_counts[s.get("sector", "Unknown")] = sector_counts.get(s.get("sector", "Unknown"), 0) + 1

    # 8. Detect new setups vs last run
    prior = get_s3_json("opportunities/asymmetric-equity.json", {})
    prior_setups_set = {s["symbol"] for s in prior.get("top_setups", [])[:30]}
    cur_setups_set = {s["symbol"] for s in setups[:30]}
    new_this_week = sorted(cur_setups_set - prior_setups_set)
    dropped_this_week = sorted(prior_setups_set - cur_setups_set)

    # 9. Build snapshot
    snapshot = {
        "as_of": now.isoformat(),
        "v": "1.1",   # 1.1 — added stacked_conviction (Phase 11A)
        "summary": {
            "n_screener_total": len(stocks),
            "n_quality_passed": len(candidates),
            "n_setups": len(setups),
            "n_value_traps": len(value_traps),
            "quality_gate_failures": quality_failures,
            "new_this_week": new_this_week,
            "dropped_this_week": dropped_this_week,
            "n_with_stacking_signals": cross_data["summary"]["tickers_with_stacking"],
            "n_insider_clusters_market_wide": cross_data["summary"]["n_clusters"],
            "n_big_insider_buys_market_wide": cross_data["summary"]["n_big_buys"],
            "broad_market_aaii_extreme": cross_data["summary"]["aaii_extreme"],
        },
        "cross_pollination": cross_data["broad_market"],
        "cutoffs": cutoffs,
        "sector_breakdown": sector_counts,
        "top_setups": setups[:30],
        "value_traps": sorted(value_traps,
                              key=lambda x: safe_float(x.get("peRatio"), 999))[:15],
        "filter_logic": {
            "quality_gate": {
                "min_piotroski": MIN_PIOTROSKI,
                "max_debt_equity": MAX_DEBT_EQUITY,
                "min_current_ratio": MIN_CURRENT_RATIO,
                "min_price": MIN_PRICE,
                "min_market_cap": MIN_MARKET_CAP,
            },
            "setup_filter": {
                "dims_required": DIMS_REQUIRED,
                "dims_total": 5,
                "top_pct_per_dim": TOP_PCT_PER_DIM,
            },
            "stacked_conviction": {
                "version": "1.0",
                "sources": ["data/insider-trades.json", "data/8k-filings.json",
                           "data/institutional-positions.json", "data/aaii-sentiment.json",
                           "data/onchain-ratios.json"],
                "point_weights": {
                    "insider_cluster_buy": 25,
                    "big_insider_buy": 15,
                    "institutional_new_filing": 15,
                    "bullish_8k_event": 10,
                    "bearish_8k_event": -25,
                    "aaii_extreme_bear (broad market)": 5,
                    "aaii_extreme_bull (broad market)": -5,
                },
                "cap_pos": STACKED_CAP_POS,
                "cap_neg": STACKED_CAP_NEG,
                "score_normalization": "(raw_pts - cap_neg) / (cap_pos - cap_neg) * 100 → 0-100",
            },
        },
    }

    put_s3_json("opportunities/asymmetric-equity.json", snapshot)

    # 10. Telegram alert: 5+ new high-conviction setups appear (rare event)
    if len(new_this_week) >= 5 and len(prior.get("top_setups", [])) > 0:
        new_top = [s for s in setups[:30] if s["symbol"] in new_this_week]
        lines = [f"🎯 *{len(new_this_week)} NEW Asymmetric Equity Setups*\n"]
        for s in new_top[:8]:
            sectors = s.get("sector", "?")
            lines.append(
                f"• *{s['symbol']}* ({sectors}) {s.get('price', '?'):.2f}\n"
                f"  composite: {s.get('composite_score', '?')} | "
                f"dims: {' '.join(s.get('dims_passed_list', []))}\n"
            )
        lines.append("\n_4-dimension filter: quality + safety + value + momentum_")
        message = "\n".join(lines)
        sent = send_telegram(message)
        snapshot["alert_sent"] = sent
        print(f"  New setups alert sent: {sent}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "n_setups": len(setups),
            "n_value_traps": len(value_traps),
            "n_new_this_week": len(new_this_week),
            "n_dropped_this_week": len(dropped_this_week),
            "top_5_symbols": [s["symbol"] for s in setups[:5]],
            "sector_breakdown": sector_counts,
        }),
    }
