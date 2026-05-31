"""justhodl-bagger-engine — the 100x Bagger Engine.

Finds multibagger candidates by scoring every nano/micro/small/mid-cap stock
on the DNA that actual 100-baggers shared (Chris Mayer "100 Baggers",
Phelps "100 to 1", Thorndike "The Outsiders").

A 100-bagger is NOT what momentum screens find. It is quiet, small, and
boring at the entry point. The return is a TWIN ENGINE:
    100x  =  (earnings growth)  ×  (multiple change)
Mayer's study of real 100-baggers: small at start, ~20-25%/yr sales growth
sustained 15-25 years, high & rising ROIC, owner-operators, bought at a
sane multiple. The hard part is not finding them — it is HOLDING them.

HARD GATE: only cap_bucket in {nano, micro, small, mid}. A $50B company
cannot 100x (that would be $5T).

SEVEN PILLARS (weights sum to 100):
  1. Small Base / Runway        18  — peak score $100M-$1B, taper outside
  2. Revenue Durability         20  — 5yr CAGR 15-40% + low variance
  3. ROIC Quality & Trend       18  — high & rising ROIC = compounding engine
  4. Reinvestment Runway        14  — ROIC x reinvestment = intrinsic compounding
  5. Margin Trend / Moat        12  — expanding gross+operating margin = moat
  6. Owner Alignment             8  — insiders net buyers + low dilution
  7. Entry Valuation            10  — sane multiple vs growth (no 40x sales)

SURVIVAL GATE (not scored — a filter): net cash OR D/E<1 AND current>1.2.
Fragile balance sheets cannot be held through the drawdowns a 100x requires.
Failing it caps the score at 55 and flags FRAGILE_BALANCE_SHEET.

TWIN-ENGINE PROJECTION: for each name, projects earnings 10yr & 15yr out at
capped growth, then bagger multiple with flat vs re-rated multiple.

OUTPUT: data/bagger-engine.json
  top_100 ranked, plus tiers potential_100x / potential_25x / potential_10x
  / emerging, each with pillar breakdown + twin-engine math + one-line thesis.

Schedule: weekly cron(0 12 ? * SUN *) — fundamentals move quarterly.
"""
import json, os, time, math
from datetime import datetime, timezone
from urllib import request, error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/bagger-engine.json"
UNIVERSE_KEY = "data/universe.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
CAP_BUCKETS = {"nano", "micro", "small", "mid"}  # hard gate

s3 = boto3.client("s3", region_name="us-east-1")


# ───────────────────────── http ─────────────────────────
def _get_json(url, timeout=12, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-Bagger/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.4 * (i + 1))
    return None


def fmp(path, symbol, limit=None):
    url = f"https://financialmodelingprep.com/stable/{path}?symbol={symbol}&apikey={FMP_KEY}"
    if limit:
        url += f"&limit={limit}"
    return _get_json(url)


# ───────────────────────── helpers ─────────────────────────
def safe_div(a, b):
    try:
        if b in (0, None) or a is None:
            return None
        return a / b
    except Exception:
        return None


def cagr(first, last, years):
    """CAGR from first->last over `years`. Handles sign issues."""
    try:
        if first is None or last is None or years <= 0:
            return None
        if first <= 0 or last <= 0:
            return None
        return (last / first) ** (1.0 / years) - 1.0
    except Exception:
        return None


def stdev(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    return (sum((v - m) ** 2 for v in vals) / len(vals)) ** 0.5


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


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


# ───────────────────────── pillar scoring ─────────────────────────
def score_small_base(market_cap):
    """Pillar 1 (18). Peak $100M-$1B; taper below $50M (junk risk) and above."""
    if not market_cap or market_cap <= 0:
        return 50.0, "unknown cap"
    mc_m = market_cap / 1e6
    if mc_m < 25:
        return 55.0, f"${mc_m:.0f}M nano — max runway but liquidity/quality risk"
    if mc_m < 50:
        return 80.0, f"${mc_m:.0f}M nano — large runway"
    if mc_m <= 1000:
        return 100.0, f"${mc_m:.0f}M — sweet spot for multibagger runway"
    if mc_m <= 2000:
        return 82.0, f"${mc_m:.0f}M small — solid runway"
    if mc_m <= 5000:
        return 60.0, f"${mc_m:.0f}M mid — runway narrowing"
    return 38.0, f"${mc_m:.0f}M mid — limited 100x runway"


def score_revenue_durability(rev_series):
    """Pillar 2 (20). rev_series newest-first. 5yr CAGR + consistency."""
    revs = [r for r in rev_series if r is not None and r > 0]
    if len(revs) < 3:
        return 50.0, None, "insufficient revenue history"
    # newest-first -> oldest is last
    newest, oldest = revs[0], revs[-1]
    yrs = len(revs) - 1
    c = cagr(oldest, newest, yrs)
    if c is None:
        return 50.0, None, "could not compute CAGR"
    # YoY growth rates for variance
    yoy = []
    for i in range(len(revs) - 1):
        g = safe_div(revs[i] - revs[i + 1], revs[i + 1])
        if g is not None:
            yoy.append(g)
    cv = None
    if len(yoy) >= 2:
        m = sum(yoy) / len(yoy)
        sd = stdev(yoy)
        cv = safe_div(sd, abs(m)) if m else None
    # CAGR score: ideal 15-40%
    cp = c * 100
    if cp < 0:
        base = 20.0
    elif cp < 8:
        base = 35.0 + cp * 2.5
    elif cp <= 40:
        base = 75.0 + (cp - 8) * 0.78  # 8%->75, 40%->100
    elif cp <= 80:
        base = 100.0 - (cp - 40) * 0.9  # taper — too hot is unsustainable
    else:
        base = 60.0
    # consistency bonus/penalty
    if cv is not None:
        if cv < 0.4:
            base += 8
        elif cv > 1.5:
            base -= 12
    note = f"5yr revenue CAGR {cp:.0f}%" + (f", consistency cv={cv:.2f}" if cv is not None else "")
    return clamp(base), c, note


def score_roic(km_series):
    """Pillar 3 (18). km_series newest-first list of key-metrics dicts."""
    roics = []
    for k in km_series:
        v = k.get("returnOnInvestedCapital")
        if v is not None:
            roics.append(v)
    if not roics:
        return 50.0, None, "no ROIC data"
    latest = roics[0]
    lp = latest * 100
    # trend: latest vs avg of prior 3
    prior = roics[1:4]
    trend = None
    if prior:
        trend = latest - (sum(prior) / len(prior))
    # base on level
    if lp < -10:
        base = 22.0
    elif lp < 0:
        base = 40.0
    elif lp < 8:
        base = 50.0 + lp * 2.5
    elif lp <= 25:
        base = 70.0 + (lp - 8) * 1.4
    elif lp <= 60:
        base = 94.0
    else:
        base = 80.0  # suspiciously high — possibly tiny capital base
    inflection = False
    if trend is not None:
        if trend > 0.03:
            base += 10
            if latest > 0 and (roics[-1] if len(roics) > 2 else 0) <= 0:
                inflection = True
                base += 6
        elif trend < -0.04:
            base -= 12
    note = f"ROIC {lp:.0f}%" + (f", trend {'+' if (trend or 0)>=0 else ''}{trend*100:.0f}pp" if trend is not None else "")
    if inflection:
        note += " — ROIC INFLECTION (turned positive & rising)"
    return clamp(base), latest, note


def score_reinvestment(km_series, cf_series, is_series):
    """Pillar 4 (14). Intrinsic compounding ≈ ROIC × reinvestment rate.
    Reinvestment rate proxied by (capex + acquisitions) / operating cash flow."""
    if not cf_series:
        return 50.0, None, "no cash-flow data"
    cf = cf_series[0]
    capex = abs(cf.get("capitalExpenditure") or 0)
    acq = abs(cf.get("acquisitionsNet") or 0)
    ocf = cf.get("operatingCashFlow") or cf.get("netCashProvidedByOperatingActivities")
    reinvest_rate = safe_div(capex + acq, ocf) if ocf and ocf > 0 else None
    roic = (km_series[0].get("returnOnInvestedCapital") if km_series else None)
    if reinvest_rate is None or roic is None:
        # fall back: a growing company with negative FCF is still reinvesting
        fcf = cf.get("freeCashFlow")
        if fcf is not None and fcf < 0 and ocf and ocf != 0:
            return 62.0, None, "reinvesting ahead of FCF (growth phase)"
        return 50.0, None, "reinvestment rate unavailable"
    intrinsic = roic * min(reinvest_rate, 1.2)  # cap reinvest rate sanity
    ip = intrinsic * 100
    # ip is approx the intrinsic compounding rate
    if ip <= 0:
        base = 35.0
    elif ip < 5:
        base = 45.0 + ip * 3
    elif ip <= 20:
        base = 60.0 + (ip - 5) * 2.4
    else:
        base = 96.0
    note = (f"reinvest rate {reinvest_rate*100:.0f}% × ROIC "
            f"→ intrinsic compounding ~{ip:.0f}%/yr")
    return clamp(base), intrinsic, note


def score_margins(ratio_series):
    """Pillar 5 (12). Gross + operating margin trend over available years."""
    gm = [r.get("grossProfitMargin") for r in ratio_series if r.get("grossProfitMargin") is not None]
    om = [r.get("operatingProfitMargin") for r in ratio_series if r.get("operatingProfitMargin") is not None]
    if not gm:
        return 50.0, "no margin data"
    gm_latest = gm[0]
    # trend: latest vs oldest available
    gm_trend = gm[0] - gm[-1] if len(gm) > 1 else 0
    om_trend = (om[0] - om[-1]) if len(om) > 1 else 0
    # base on level + trend of gross margin (moat proxy)
    base = 45.0 + min(gm_latest, 0.85) * 45  # high gross margin = pricing power
    if gm_trend > 0.02:
        base += 14   # expanding moat
    elif gm_trend < -0.04:
        base -= 14   # eroding moat
    if om_trend > 0.03:
        base += 8    # operating leverage kicking in — bagger DNA
    elif om_trend < -0.04:
        base -= 6
    note = (f"gross margin {gm_latest*100:.0f}% "
            f"({'expanding' if gm_trend>0.02 else 'eroding' if gm_trend<-0.04 else 'stable'})")
    if om_trend > 0.03:
        note += ", operating leverage rising"
    return clamp(base), note


def score_owner_alignment(insider_stats, is_series):
    """Pillar 6 (8). Insiders net buyers + low share dilution."""
    base = 50.0
    notes = []
    # insider behaviour: average acquiredDisposedRatio over recent ~8 quarters
    if insider_stats:
        recent = insider_stats[:8]
        ratios = [q.get("acquiredDisposedRatio") for q in recent
                  if q.get("acquiredDisposedRatio") is not None]
        if ratios:
            avg_r = sum(ratios) / len(ratios)
            if avg_r >= 1.0:
                base += 28
                notes.append(f"insiders NET BUYERS (ratio {avg_r:.2f})")
            elif avg_r >= 0.5:
                base += 8
                notes.append(f"insiders balanced (ratio {avg_r:.2f})")
            else:
                base -= 6
                notes.append(f"insiders net sellers (ratio {avg_r:.2f})")
    # dilution: share count growth (income-statement weightedAverageShsOut)
    shs = [s.get("weightedAverageShsOut") for s in is_series
           if s.get("weightedAverageShsOut")]
    if len(shs) >= 3:
        dilution = safe_div(shs[0] - shs[-1], shs[-1])  # newest vs oldest
        if dilution is not None:
            ann = dilution / max(len(shs) - 1, 1)
            if ann < 0.005:
                base += 14
                notes.append("minimal dilution (disciplined share count)")
            elif ann > 0.06:
                base -= 16
                notes.append(f"heavy dilution (~{ann*100:.0f}%/yr — bagger killer)")
    return clamp(base), "; ".join(notes) if notes else "alignment data limited"


def score_valuation(km_series, ratio_series, rev_cagr):
    """Pillar 7 (10). Sane multiple vs growth — you can't 100x from 40x sales."""
    if not km_series:
        return 50.0, "no valuation data"
    km = km_series[0]
    ev_sales = km.get("evToSales")
    fcf_yield = km.get("freeCashFlowYield")
    pe = ratio_series[0].get("priceToEarningsRatio") if ratio_series else None
    g = (rev_cagr or 0) * 100
    base = 55.0
    notes = []
    # EV/Sales relative to growth — a growth-adjusted sales multiple
    if ev_sales is not None and ev_sales > 0:
        # "sales PEG": ev/sales divided by growth rate
        if g > 0:
            sales_peg = ev_sales / max(g, 1)
            if sales_peg < 0.15:
                base += 24; notes.append(f"EV/Sales {ev_sales:.1f}x cheap vs {g:.0f}% growth")
            elif sales_peg < 0.4:
                base += 8; notes.append(f"EV/Sales {ev_sales:.1f}x fair vs growth")
            elif sales_peg > 1.0:
                base -= 22; notes.append(f"EV/Sales {ev_sales:.1f}x rich — priced for perfection")
        if ev_sales > 25:
            base -= 18; notes.append("EV/Sales >25x — re-rating headroom limited")
    # FCF yield bonus
    if fcf_yield is not None and fcf_yield > 0.04:
        base += 10; notes.append(f"FCF yield {fcf_yield*100:.0f}%")
    if pe is not None and 0 < pe < 20:
        base += 8; notes.append(f"P/E {pe:.0f}")
    return clamp(base), "; ".join(notes) if notes else "valuation neutral"


def survival_check(km_series, ratio_series):
    """Hard filter. Net cash OR (D/E<1 AND current ratio>1.2)."""
    if not ratio_series:
        return True, "balance-sheet data unavailable (assumed ok)"
    r = ratio_series[0]
    de = r.get("debtToEquityRatio")
    cr = r.get("currentRatio")
    km = km_series[0] if km_series else {}
    net_debt_ebitda = km.get("netDebtToEBITDA")
    # net cash if netDebt/EBITDA negative
    net_cash = net_debt_ebitda is not None and net_debt_ebitda < 0
    if net_cash:
        return True, "net cash position — fortress balance sheet"
    de_ok = de is not None and de < 1.0
    cr_ok = cr is not None and cr > 1.2
    if de_ok and cr_ok:
        return True, f"healthy balance sheet (D/E {de:.1f}, current {cr:.1f})"
    if de is not None and de > 2.5:
        return False, f"FRAGILE — high leverage (D/E {de:.1f})"
    if cr is not None and cr < 1.0:
        return False, f"FRAGILE — current ratio {cr:.1f} below 1.0"
    return True, "balance sheet acceptable"


def twin_engine(rev_cagr, roic, market_cap, net_margin_latest):
    """Project the bagger math. Real 100-baggers take 20-25 years (Chris Mayer's
    study found a median holding of ~26 years). We project 10/15/20/25-yr
    horizons so the POTENTIAL_100X tier reflects an honest long-hold thesis,
    not a 15-year fantasy."""
    if rev_cagr is None:
        return {"available": False}
    # cap growth assumption — even great companies decelerate
    g = max(min(rev_cagr, 0.25), 0.0)
    out = {"available": True, "assumed_growth_capped": round(g * 100, 1)}
    for yrs in (10, 15, 20, 25):
        earnings_mult = (1 + g) ** yrs  # earnings engine if margin holds
        flat = earnings_mult
        rerated = earnings_mult * 1.8   # modest 1.8x multiple expansion
        out[f"yr{yrs}"] = {
            "earnings_engine_x": round(earnings_mult, 1),
            "flat_multiple_x": round(flat, 1),
            "with_rerating_x": round(rerated, 1),
        }
    # classification keyed off the long-hold horizon — that is how 100x happens
    peak_long = out["yr20"]["with_rerating_x"]
    peak_mid = out["yr15"]["with_rerating_x"]
    if peak_long >= 100:
        out["classification"] = "POTENTIAL_100X"
    elif peak_mid >= 40 or peak_long >= 40:
        out["classification"] = "POTENTIAL_25X_PLUS"
    elif peak_mid >= 12:
        out["classification"] = "POTENTIAL_10X"
    elif peak_mid >= 4:
        out["classification"] = "STEADY_COMPOUNDER"
    else:
        out["classification"] = "MODEST"
    out["years_to_100x"] = None
    if g > 0:
        # solve (1+g)^n * 1.8 >= 100  ->  n = (ln(100/1.8)) / ln(1+g)
        try:
            n = math.log(100 / 1.8) / math.log(1 + g)
            out["years_to_100x"] = round(n, 1)
        except Exception:
            pass
    return out


# ───────────────────────── per-stock ─────────────────────────
WEIGHTS = {
    "small_base": 0.15, "revenue": 0.17, "roic": 0.15,
    "reinvestment": 0.12, "margins": 0.10, "alignment": 0.07, "valuation": 0.09,
    "future_intel": 0.15,   # NEW — forward-orders + rotation + buzz composite
}


# ─── Future Intelligence cache ─────────────────────────────────
# Loaded once at handler init, then queried per-stock. Reads from
# data/future-intelligence.json (the composite of forward-orders,
# rotation-chain, and buzz-velocity engines).
_future_intel_cache = None

def _load_future_intel():
    global _future_intel_cache
    if _future_intel_cache is not None:
        return _future_intel_cache
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/future-intelligence.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        # Build per-ticker lookup
        _future_intel_cache = {}
        for r in (data.get("all_results") or []):
            sym = r.get("ticker")
            if sym:
                _future_intel_cache[sym] = r
        print(f"[bagger] loaded future-intel for {len(_future_intel_cache)} tickers")
    except Exception as e:
        print(f"[bagger] future-intel load failed: {e}")
        _future_intel_cache = {}
    return _future_intel_cache


def score_future_intelligence(sym):
    """Forward-looking pillar: RPO + rotation + buzz composite.
    Returns (score 0-100, note). When the ticker has no future-intel
    signal, returns 50 (neutral) so it doesn't dominate backward pillars."""
    fi = _load_future_intel()
    rec = fi.get(sym)
    if not rec:
        return 50, "no future signal"
    score = rec.get("future_intel_score", 50)
    n_sigs = rec.get("n_independent_signals", 0)
    
    # Build a tight note
    notes = []
    fo = rec.get("forward_orders") or {}
    if (fo.get("rpo_yield_pct") or 0) >= 30:
        notes.append(f"RPO {fo['rpo_yield_pct']:.0f}% of mcap")
    elif (fo.get("rpo_growth_pct") or 0) >= 20:
        notes.append(f"RPO growing {fo['rpo_growth_pct']:.0f}% YoY")
    
    rc = rec.get("rotation_chain") or {}
    if rc.get("role", "").startswith("next_up"):
        notes.append(f"next-up in {rc.get('chain','?')} chain")
    
    bv = rec.get("buzz_velocity") or {}
    if bv.get("stealth"):
        notes.append("stealth buzz")
    elif (bv.get("composite_velocity") or 0) >= 2.5:
        notes.append(f"buzz {bv['composite_velocity']:.1f}x")
    
    if not notes and score < 50:
        notes.append("muted forward signal")
    elif not notes:
        notes.append(f"composite {score:.0f}")
    
    note = " · ".join(notes[:3])
    return score, note



def analyze_stock(stock):
    sym = stock.get("symbol")
    if not sym:
        return None
    market_cap = stock.get("market_cap")
    km = fmp("key-metrics", sym, limit=6) or []
    ratios = fmp("ratios", sym, limit=6) or []
    income = fmp("income-statement", sym, limit=6) or []
    cashflow = fmp("cash-flow-statement", sym, limit=6) or []
    insider = fmp("insider-trading/statistics", sym) or []

    if not km and not income:
        return None  # no fundamentals at all — skip

    rev_series = [s.get("revenue") for s in income]

    p1, n1 = score_small_base(market_cap)
    p2, rev_cagr, n2 = score_revenue_durability(rev_series)
    p3, roic_latest, n3 = score_roic(km)
    p4, intrinsic, n4 = score_reinvestment(km, cashflow, income)
    p5, n5 = score_margins(ratios)
    p6, n6 = score_owner_alignment(insider, income)
    p7, n7 = score_valuation(km, ratios, rev_cagr)
    p8, n8 = score_future_intelligence(sym)   # NEW — forward-looking pillar

    raw_score = (p1 * WEIGHTS["small_base"] + p2 * WEIGHTS["revenue"]
                 + p3 * WEIGHTS["roic"] + p4 * WEIGHTS["reinvestment"]
                 + p5 * WEIGHTS["margins"] + p6 * WEIGHTS["alignment"]
                 + p7 * WEIGHTS["valuation"] + p8 * WEIGHTS["future_intel"])

    survives, surv_note = survival_check(km, ratios)
    score = raw_score
    if not survives:
        score = min(score, 55.0)

    net_margin = None
    if ratios:
        net_margin = ratios[0].get("netProfitMargin")
    twin = twin_engine(rev_cagr, roic_latest, market_cap, net_margin)

    # one-line thesis
    cls = twin.get("classification", "—")
    thesis = (f"{stock.get('sector','?')} {stock.get('cap_bucket','?')}-cap: "
              f"{n2}; {n3}; {n5}. {cls.replace('_',' ').title()}.")

    return {
        "symbol": sym,
        "name": stock.get("name"),
        "sector": stock.get("sector"),
        "industry": stock.get("industry"),
        "cap_bucket": stock.get("cap_bucket"),
        "market_cap": market_cap,
        "price": stock.get("price"),
        "bagger_score": round(score, 1),
        "raw_score": round(raw_score, 1),
        "survives": survives,
        "survival_note": surv_note,
        "pillars": {
            "small_base": {"score": round(p1, 1), "note": n1},
            "revenue_durability": {"score": round(p2, 1), "note": n2},
            "roic_quality": {"score": round(p3, 1), "note": n3},
            "reinvestment_runway": {"score": round(p4, 1), "note": n4},
            "margin_moat": {"score": round(p5, 1), "note": n5},
            "owner_alignment": {"score": round(p6, 1), "note": n6},
            "entry_valuation": {"score": round(p7, 1), "note": n7},
            "future_intelligence": {"score": round(p8, 1), "note": n8},
        },
        "twin_engine": twin,
        "key_stats": {
            "revenue_cagr_pct": round(rev_cagr * 100, 1) if rev_cagr is not None else None,
            "roic_pct": round(roic_latest * 100, 1) if roic_latest is not None else None,
            "intrinsic_compounding_pct": round(intrinsic * 100, 1) if intrinsic is not None else None,
            "net_margin_pct": round(net_margin * 100, 1) if net_margin is not None else None,
        },
        "thesis": thesis,
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[bagger-engine] starting {datetime.now(timezone.utc).isoformat()}")
    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FMP_KEY not set"})}

    # load universe
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=UNIVERSE_KEY)
        universe = json.loads(obj["Body"].read()).get("stocks", [])
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"universe load: {e}"})}

    candidates = [s for s in universe if s.get("cap_bucket") in CAP_BUCKETS]
    print(f"[bagger-engine] universe {len(universe)} -> {len(candidates)} in cap range")

    # optional cap for time-boxed runs
    limit = int(event.get("limit", 0)) if isinstance(event, dict) else 0
    if limit:
        candidates = candidates[:limit]
        print(f"[bagger-engine] limited to {len(candidates)}")

    results = []
    errors = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(analyze_stock, s): s for s in candidates}
        done = 0
        for f in as_completed(futs):
            done += 1
            try:
                r = f.result()
                if r:
                    results.append(r)
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"[bagger-engine] err: {e}")
            if done % 200 == 0:
                print(f"[bagger-engine] progress {done}/{len(candidates)} "
                      f"scored={len(results)} errs={errors} t={time.time()-t0:.0f}s")

    results.sort(key=lambda x: -x["bagger_score"])
    for i, r in enumerate(results):
        r["rank"] = i + 1

    # tiers by twin-engine classification (among score>=60 survivors)
    strong = [r for r in results if r["bagger_score"] >= 60 and r["survives"]]
    tiers = {
        "potential_100x": [r for r in strong
                            if r["twin_engine"].get("classification") == "POTENTIAL_100X"][:30],
        "potential_25x":  [r for r in strong
                            if r["twin_engine"].get("classification") == "POTENTIAL_25X_PLUS"][:30],
        "potential_10x":  [r for r in strong
                            if r["twin_engine"].get("classification") == "POTENTIAL_10X"][:30],
        "steady_compounders": [r for r in strong
                                if r["twin_engine"].get("classification") == "STEADY_COMPOUNDER"][:30],
    }

    out = {
        "schema_version": "1.0",
        "method": "bagger_engine_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "universe_size": len(universe),
        "candidates_in_range": len(candidates),
        "n_scored": len(results),
        "n_errors": errors,
        "pillar_weights": WEIGHTS,
        "top_100": results[:100],
        "tiers": tiers,
        "tier_counts": {k: len(v) for k, v in tiers.items()},
        "methodology": (
            "100-bagger DNA: small base + durable 15-40% revenue growth + "
            "high/rising ROIC + reinvestment runway + expanding margins + "
            "owner alignment + sane entry multiple. Survival gate filters "
            "fragile balance sheets. Twin-engine projects earnings x multiple."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=3600")

    # Telegram: top potential-100x names
    p100 = tiers["potential_100x"][:5]
    if p100:
        lines = [f"- <b>{r['symbol']}</b> {r['name'][:24]} "
                 f"score {r['bagger_score']} | {r['key_stats'].get('revenue_cagr_pct')}% rev CAGR"
                 for r in p100]
        maybe_telegram("[100x] <b>BAGGER ENGINE</b> — top POTENTIAL_100X:\n" + "\n".join(lines))

    print(f"[bagger-engine] done {out['elapsed_s']}s scored={len(results)} "
          f"100x={len(tiers['potential_100x'])} 25x={len(tiers['potential_25x'])} "
          f"10x={len(tiers['potential_10x'])}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_scored": len(results),
        "tier_counts": out["tier_counts"],
        "top_5": [r["symbol"] for r in results[:5]],
    })}
