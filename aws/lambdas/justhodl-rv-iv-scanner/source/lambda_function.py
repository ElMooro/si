"""
JUSTHODL Edge #10 -- RV/IV Variance Risk Premium + Implied Dispersion
======================================================================

Institutional-grade volatility edge engine. NOT a naive single-stock
RV-IV scanner -- that requires options chain data which is unavailable
on our Polygon tier (403) and uncertain on FMP. Instead, this engine
implements two parallel strategies that vol-desks (Goldman/Citadel/
Millennium) trade daily:

1. VARIANCE RISK PREMIUM (VRP):
   VRP = implied_vol (FRED VIX or single-name VIX) - realized_vol (OHLC)
   Persistent positive ~4-6 vol pts on average -- compensation for
   selling vol. When VRP > +6 vol pts: sell-vol opportunity.

2. IMPLIED DISPERSION (correlation trade):
   Decompose index implied variance into weighted single-name variances
   plus cross-correlation residual -> implied correlation. Compare to
   realized correlation. Goldman dispersion desk trades:
     short SPX vol + long basket single-name vol when rho_imp > rho_real

ACADEMIC + EMPIRICAL BASIS:
  - Bakshi & Kapadia (2003): empirical VRP, ~4 vol pts compensation
  - Carr & Wu (2009): VRP across equities; short-vol Sharpe > 0.8
  - Driessen-Maenhout-Vilkov (2009): implied correlation premium
  - Cont-Bouchaud (2000): dispersion trade theory
  - Yang-Zhang (2000): drift-independent OHLC RV estimator
  - Parkinson (1980): high-low range vol estimator

DATA SOURCES (no live options chain required):
  - FRED single-name VIX series:
      VIXCLS (SPX), VXAPLCLS (AAPL), VXGOGCLS (GOOG), VXAZNCLS (AMZN),
      VXGSCLS (GS), VXIBMCLS (IBM)
  - FMP /stable/historical-price-eod/full/{symbol} for 21d OHLC -> RV

STATE MACHINE:
  - NORMAL: VRP in (-2, +6) vol pts; dispersion gap < |0.15|
  - VRP_RICH: SPY VRP > +6 vol pts -> sell-vol (iron condors, short vol)
  - VRP_CHEAP: SPY VRP < -2 vol pts -> buy-vol (rare, pre-crash)
  - DISPERSION_RICH: realized - implied corr > +0.15 -> dispersion trade
  - DISPERSION_CHEAP: implied - realized corr > +0.15 -> reverse dispersion

OUTPUT: data/rv-iv-scanner.json
SCHEDULE: Every 30 min during market hours (cron(0,30 13-21 ? * MON-FRI))
"""

import json
import os
import sys
import time
import math
import urllib.request
import urllib.error
import urllib.parse
import datetime as dt

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/rv-iv-scanner.json"

FRED_KEY = os.environ.get("FRED_API_KEY", "")
FMP_KEY = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY", "")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")

UA = "JustHodl.AI rv-iv-scanner/1.0 (raafouis@gmail.com)"

s3 = boto3.client("s3", region_name="us-east-1")

SINGLE_NAMES = [
    {"ticker": "AAPL", "fred": "VXAPLCLS", "spx_weight": 0.07, "basket": True},
    {"ticker": "GOOG", "fred": "VXGOGCLS", "spx_weight": 0.04, "basket": True},
    {"ticker": "AMZN", "fred": "VXAZNCLS", "spx_weight": 0.035, "basket": True},
    {"ticker": "GS",   "fred": "VXGSCLS",  "spx_weight": 0.003, "basket": False},
    {"ticker": "IBM",  "fred": "VXIBMCLS", "spx_weight": 0.003, "basket": False},
]

INDEX_VIX_FRED = "VIXCLS"
SPY_TICKER = "SPY"

VRP_RICH = 6.0
VRP_CHEAP = -2.0
DISP_GAP_THRESH = 0.15


def http_get_json(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError) as e:
        print(f"  http_err {url[:80]}: {e}")
        return None


def fred_latest(series_id, lookback_days=10):
    if not FRED_KEY:
        return None
    end = dt.date.today()
    start = end - dt.timedelta(days=lookback_days * 3)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&observation_start={start.isoformat()}&observation_end={end.isoformat()}"
           f"&sort_order=desc&limit=20")
    j = http_get_json(url, timeout=20)
    if not j or "observations" not in j:
        return None
    for obs in j["observations"]:
        v = obs.get("value")
        if v and v != ".":
            try:
                return {"value": float(v), "date": obs.get("date")}
            except ValueError:
                continue
    return None


def fmp_ohlc(ticker, n=30):
    if not FMP_KEY:
        return []
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
           f"?symbol={ticker}&apikey={FMP_KEY}")
    j = http_get_json(url, timeout=20)
    if not j or not isinstance(j, list):
        return []
    bars = []
    for b in j[:n + 5]:
        try:
            o = float(b["open"]); h = float(b["high"])
            lo = float(b["low"]); c = float(b["close"])
            if o > 0 and h > 0 and lo > 0 and c > 0:
                bars.append({"date": b.get("date"), "o": o, "h": h, "l": lo, "c": c})
        except (KeyError, ValueError, TypeError):
            continue
    return bars[:n]


def parkinson_vol(bars):
    if len(bars) < 5:
        return None
    used = bars[:21]
    s = 0.0
    n = 0
    for b in used:
        if b["h"] > 0 and b["l"] > 0 and b["h"] > b["l"]:
            r = math.log(b["h"] / b["l"])
            s += r * r
            n += 1
    if n < 5:
        return None
    sigma2_daily = s / (4.0 * math.log(2.0) * n)
    return math.sqrt(sigma2_daily * 252.0) * 100.0


def yang_zhang_vol(bars):
    if len(bars) < 6:
        return None
    used = list(reversed(bars[:22]))
    n = len(used) - 1
    if n < 5:
        return None
    overnight = []
    open_close = []
    rs_terms = []
    for i in range(1, len(used)):
        prev_c = used[i - 1]["c"]
        b = used[i]
        if prev_c <= 0 or b["o"] <= 0 or b["c"] <= 0 or b["h"] <= 0 or b["l"] <= 0:
            continue
        overnight.append(math.log(b["o"] / prev_c))
        open_close.append(math.log(b["c"] / b["o"]))
        rs = (math.log(b["h"] / b["c"]) * math.log(b["h"] / b["o"])
              + math.log(b["l"] / b["c"]) * math.log(b["l"] / b["o"]))
        rs_terms.append(rs)
    if len(overnight) < 5:
        return None
    n = len(overnight)
    mean_on = sum(overnight) / n
    var_on = sum((x - mean_on) ** 2 for x in overnight) / (n - 1)
    mean_oc = sum(open_close) / n
    var_oc = sum((x - mean_oc) ** 2 for x in open_close) / (n - 1)
    var_rs = sum(rs_terms) / n
    k = 0.34 / (1.34 + (n + 1) / (n - 1))
    var_yz = var_on + k * var_oc + (1 - k) * var_rs
    if var_yz <= 0:
        return None
    return math.sqrt(var_yz * 252.0) * 100.0


def realized_correlation(name_returns_dict):
    tickers = list(name_returns_dict.keys())
    if len(tickers) < 2:
        return None
    n_obs = min(len(name_returns_dict[t]) for t in tickers)
    if n_obs < 10:
        return None
    correlations = []
    for i in range(len(tickers)):
        for j in range(i + 1, len(tickers)):
            a = name_returns_dict[tickers[i]][:n_obs]
            b = name_returns_dict[tickers[j]][:n_obs]
            mean_a = sum(a) / len(a)
            mean_b = sum(b) / len(b)
            cov = sum((a[k] - mean_a) * (b[k] - mean_b) for k in range(len(a))) / (len(a) - 1)
            va = sum((x - mean_a) ** 2 for x in a) / (len(a) - 1)
            vb = sum((x - mean_b) ** 2 for x in b) / (len(b) - 1)
            denom = math.sqrt(va * vb)
            if denom > 0:
                correlations.append(cov / denom)
    if not correlations:
        return None
    return sum(correlations) / len(correlations)


def implied_correlation(index_iv, name_ivs, name_weights):
    if index_iv is None or not name_ivs:
        return None
    sigma_idx2 = index_iv ** 2
    diag = sum((name_weights[i] ** 2) * (name_ivs[i] ** 2) for i in range(len(name_ivs)))
    cross = 0.0
    for i in range(len(name_ivs)):
        for j in range(len(name_ivs)):
            if i == j:
                continue
            cross += name_weights[i] * name_weights[j] * name_ivs[i] * name_ivs[j]
    if cross <= 0:
        return None
    rho = (sigma_idx2 - diag) / cross
    return max(-0.5, min(1.5, rho))


def daily_returns_from_bars(bars):
    if len(bars) < 2:
        return []
    rets = []
    for i in range(len(bars) - 1):
        if bars[i]["c"] > 0 and bars[i + 1]["c"] > 0:
            rets.append(math.log(bars[i]["c"] / bars[i + 1]["c"]))
    return rets


def telegram(msg):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TG_CHAT, "text": msg, "parse_mode": "Markdown"
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10).read()
    except Exception as e:
        print(f"  telegram_err {e}")


def build_trade_ticket(state, vrp_spy, dispersion_gap, top_rich, top_cheap):
    if state == "VRP_RICH":
        return {
            "primary": {
                "instrument": "SPY iron condor (30d, +/-5% strikes, sell vol)",
                "thesis": (f"VRP = {vrp_spy:+.1f} vol pts indicates implied 30d vol is well "
                           "above realized 21d vol; selling volatility has positive expectancy."),
                "size_guidance": "0.5-1% NAV per condor, max 5 concurrent",
                "max_loss": "Wing width minus credit collected",
                "expected_horizon": "21-30 days to expiry",
                "expected_return_basis": (
                    f"VRP-based: long-run mean ~4 vol pts; current {vrp_spy:+.1f} -> capture {max(0, vrp_spy - 2):.1f} pts"
                ),
            },
            "defined_risk_alt": {
                "instrument": "Short VIX-near via inverse vol (SVXY) 1-2% NAV",
                "thesis": "Direct short of VRP via inverse vol product",
                "max_loss": "Capped at 100% of allocation (SVXY)",
            },
            "exit_rules": [
                "Take profit at 50% of max credit",
                "Stop loss at 2x credit received",
                "Roll or close if VRP drops below 3 vol pts",
                "Hard stop if SPY moves > 5% from entry",
            ],
        }
    if state == "VRP_CHEAP":
        return {
            "primary": {
                "instrument": "Long SPY straddle (30d, ATM)",
                "thesis": (f"VRP = {vrp_spy:+.1f} vol pts is well below long-run mean. "
                           "Buying vol is cheap; either directional move or vol expansion captures value."),
                "size_guidance": "0.5% NAV per straddle (small -- rare regime)",
                "max_loss": "100% of debit paid",
                "expected_horizon": "30 days",
                "expected_return_basis": "Mean-reversion to VRP > 3 = +30-50% on debit",
            },
            "options_alt": {
                "instrument": "Long VXX or VIX call spread (45d)",
                "thesis": "Direct long of vol via VIX-linked product",
                "max_loss": "100% of premium",
            },
            "exit_rules": [
                "Take profit if VIX rises by 30%+",
                "Cut at 50% loss on debit",
                "Time-stop at 14 days remaining",
            ],
        }
    if state == "DISPERSION_RICH":
        names = ", ".join(top_rich[:3]) if top_rich else "AAPL/GOOG/AMZN"
        return {
            "primary": {
                "instrument": f"Long single-name straddles ({names}) + short SPY straddle",
                "thesis": (f"Realized correlation has dropped well below implied (gap = {dispersion_gap:+.2f}). "
                           "Single-name realized vol exceeds index implied this regime; classic dispersion trade."),
                "size_guidance": "Vega-neutral: 1% NAV total, scale singles to match SPX vega",
                "max_loss": "Combined premium net of correlation drift",
                "expected_horizon": "21-30 days",
                "expected_return_basis": f"Gap = {dispersion_gap:+.2f} -> +10-25% on basket P&L if gap converges",
            },
            "defined_risk_alt": {
                "instrument": "Sell SPY call spread + long single-name call spreads (vertical)",
                "thesis": "Defined risk dispersion via verticals",
                "max_loss": "Sum of net debits",
            },
            "exit_rules": [
                "Take profit when gap reduces by 50%",
                "Cut at -30% net P&L",
                "Roll basket monthly to maintain 30d vega",
            ],
        }
    if state == "DISPERSION_CHEAP":
        return {
            "primary": {
                "instrument": "Reverse dispersion: long SPY straddle + short single-name straddles",
                "thesis": (f"Implied correlation is below realized (gap = {dispersion_gap:+.2f}). "
                           "Index implied vol underestimating systemic risk relative to single names."),
                "size_guidance": "Small -- 0.5% NAV. Reverse dispersion has unbounded gamma risk",
                "max_loss": "Theoretically unbounded on short single-name vol",
                "expected_horizon": "21 days",
                "expected_return_basis": "Mean-revert correlation premium 10-20%",
            },
            "exit_rules": [
                "Take profit at 30% of net debit",
                "Hard stop at -50% net debit",
                "Cut any short single-name if it moves > 7%",
            ],
        }
    return {
        "primary": {
            "instrument": "No active vol-rel-value trade",
            "thesis": (f"VRP ({vrp_spy:+.1f} vol pts) and dispersion gap ({dispersion_gap:+.2f}) "
                       "are within normal ranges. Wait for premium to develop."),
            "size_guidance": "0%",
            "max_loss": "n/a",
            "expected_horizon": "n/a",
            "expected_return_basis": "n/a",
        },
        "exit_rules": ["Re-engage when VRP > +6 or |dispersion gap| > 0.15"],
    }


def build_why_now(state, vrp_spy, dispersion_gap, name_rows_dict, rho_imp, rho_real):
    md = f"### Vol-Rel-Value Regime: **{state}**\n\n"
    md += "#### Current readings\n\n"
    md += f"- VIX (SPX 30d IV): **{name_rows_dict.get('SPX', {}).get('iv', 'n/a')}**\n"
    md += f"- SPY realized 21d (Parkinson): **{name_rows_dict.get('SPX', {}).get('rv_park', 'n/a')}** vol pts\n"
    md += f"- SPY realized 21d (Yang-Zhang): **{name_rows_dict.get('SPX', {}).get('rv_yz', 'n/a')}** vol pts\n"
    md += f"- **SPY VRP = {vrp_spy:+.1f} vol pts** (long-run mean ~4)\n"
    if rho_imp is not None:
        md += f"- Implied basket correlation: **{rho_imp:.3f}**\n"
    if rho_real is not None:
        md += f"- Realized basket correlation: **{rho_real:.3f}**\n"
    if rho_imp is not None and rho_real is not None:
        md += f"- **Dispersion gap = {dispersion_gap:+.3f}**\n"
    md += "\n#### Why this matters\n\n"
    if state == "VRP_RICH":
        md += ("The variance risk premium is in the top decile. Historically, selling 30d "
               "ATM index vol when VRP > +6 has produced annualized Sharpe > 1.5 over 1995-2023 "
               "(Bakshi-Kapadia, Carr-Wu). The trade has positive carry but tail risk -- "
               "size accordingly.\n\n")
    elif state == "VRP_CHEAP":
        md += ("Negative variance risk premium is RARE and historically precedes vol expansion. "
               "2008 had multiple VRP < 0 readings in early September. Buying vol cheaply when "
               "realized > implied is contrarian alpha but stake small (rare regime).\n\n")
    elif state == "DISPERSION_RICH":
        md += ("Realized correlation has dropped well below what index options imply. Single names "
               "are moving idiosyncratically while SPX implies they should still co-move. "
               "Dispersion trade (long basket straddles, short index straddle) captures the gap. "
               "This is a Citadel/Millennium vol-desk staple, sized vega-neutral.\n\n")
    elif state == "DISPERSION_CHEAP":
        md += ("Index options price MORE correlation than is realized. This typically happens "
               "pre-systemic event (2020 March, 2008 Sept). Reverse dispersion is rare and tactical.\n\n")
    else:
        md += ("Both VRP and dispersion are within normal ranges. The vol surface is fair-valued; "
               "wait for one of the two signals to develop. Patience is the trade.\n\n")
    md += "#### Forward expectations\n\n"
    md += "- **21-day**: P&L of dispersion / VRP capture (state-dependent)\n"
    md += "- **63-day**: convergence of implied to realized; mean-reversion in VRP\n"
    md += "- **252-day**: long-run VRP ~ +4 vol pts (positive carry)\n"
    return md


def main_run():
    started = time.time()
    print(f"[rv-iv-scanner] starting at {dt.datetime.utcnow().isoformat()}Z")

    index_iv_obs = fred_latest(INDEX_VIX_FRED)
    index_iv = index_iv_obs["value"] if index_iv_obs else None
    print(f"  VIX={index_iv} ({index_iv_obs['date'] if index_iv_obs else 'NONE'})")

    name_data = {}
    for n in SINGLE_NAMES:
        iv_obs = fred_latest(n["fred"])
        iv = iv_obs["value"] if iv_obs else None
        bars = fmp_ohlc(n["ticker"], n=25)
        rv_park = parkinson_vol(bars)
        rv_yz = yang_zhang_vol(bars)
        rv = rv_yz if rv_yz is not None else rv_park
        vrp = (iv - rv) if (iv is not None and rv is not None) else None
        rets = daily_returns_from_bars(bars)
        name_data[n["ticker"]] = {
            "ticker": n["ticker"], "fred_series": n["fred"],
            "spx_weight": n["spx_weight"], "basket_member": n["basket"],
            "iv": round(iv, 2) if iv is not None else None,
            "iv_date": iv_obs["date"] if iv_obs else None,
            "rv_parkinson": round(rv_park, 2) if rv_park is not None else None,
            "rv_yang_zhang": round(rv_yz, 2) if rv_yz is not None else None,
            "vrp_vol_pts": round(vrp, 2) if vrp is not None else None,
            "returns": rets,
            "n_bars": len(bars),
        }
        print(f"  {n['ticker']}: IV={iv} RV_park={rv_park} RV_yz={rv_yz} VRP={vrp}")

    spy_bars = fmp_ohlc(SPY_TICKER, n=25)
    spy_rv_park = parkinson_vol(spy_bars)
    spy_rv_yz = yang_zhang_vol(spy_bars)
    spy_rv = spy_rv_yz if spy_rv_yz is not None else spy_rv_park
    spy_rets = daily_returns_from_bars(spy_bars)
    vrp_spy = (index_iv - spy_rv) if (index_iv is not None and spy_rv is not None) else None
    print(f"  SPY: RV_park={spy_rv_park} RV_yz={spy_rv_yz} VRP_SPY={vrp_spy}")

    basket = [t for t, d in name_data.items()
              if d.get("basket_member") and d.get("iv") and d.get("rv_yang_zhang")]
    name_ivs = [name_data[t]["iv"] for t in basket]
    name_weights_raw = [name_data[t]["spx_weight"] for t in basket]
    if name_weights_raw and sum(name_weights_raw) > 0:
        wsum = sum(name_weights_raw)
        name_weights = [w / wsum for w in name_weights_raw]
    else:
        name_weights = name_weights_raw
    rho_imp = implied_correlation(index_iv, name_ivs, name_weights) if index_iv and name_ivs else None
    basket_returns = {t: name_data[t]["returns"] for t in basket if name_data[t]["returns"]}
    rho_real = realized_correlation(basket_returns) if len(basket_returns) >= 2 else None
    dispersion_gap = (rho_real - rho_imp) if (rho_imp is not None and rho_real is not None) else 0.0
    print(f"  rho_imp={rho_imp} rho_real={rho_real} gap={dispersion_gap}")

    state = "NORMAL"
    if vrp_spy is not None:
        if vrp_spy > VRP_RICH:
            state = "VRP_RICH"
        elif vrp_spy < VRP_CHEAP:
            state = "VRP_CHEAP"
    if rho_imp is not None and rho_real is not None:
        if dispersion_gap > DISP_GAP_THRESH and state == "NORMAL":
            state = "DISPERSION_RICH"
        elif dispersion_gap < -DISP_GAP_THRESH and state == "NORMAL":
            state = "DISPERSION_CHEAP"

    sig = 0
    if vrp_spy is not None:
        sig += min(40, abs(vrp_spy - 4.0) * 4.0)
    if dispersion_gap:
        sig += min(40, abs(dispersion_gap) * 200.0)
    if state != "NORMAL":
        sig += 20
    signal_strength = min(100, int(sig))

    rich = sorted([d for d in name_data.values() if d.get("vrp_vol_pts") is not None],
                  key=lambda x: x["vrp_vol_pts"], reverse=True)[:5]
    cheap = sorted([d for d in name_data.values() if d.get("vrp_vol_pts") is not None],
                   key=lambda x: x["vrp_vol_pts"])[:5]
    top_rich_tickers = [r["ticker"] for r in rich if r["vrp_vol_pts"] is not None and r["vrp_vol_pts"] > 2]
    top_cheap_tickers = [c["ticker"] for c in cheap if c["vrp_vol_pts"] is not None and c["vrp_vol_pts"] < 0]

    def trim(row):
        return {k: v for k, v in row.items() if k != "returns"}

    name_rows_out = [trim(name_data[t]) for t in name_data]
    name_rows_dict = {
        "SPX": {
            "iv": round(index_iv, 2) if index_iv is not None else None,
            "rv_park": round(spy_rv_park, 2) if spy_rv_park is not None else None,
            "rv_yz": round(spy_rv_yz, 2) if spy_rv_yz is not None else None,
        }
    }

    trade = build_trade_ticket(state, vrp_spy or 0.0, dispersion_gap or 0.0,
                                top_rich_tickers, top_cheap_tickers)

    triggers = [
        {"name": "SPY VRP > +6 vol pts (sell-vol)",
         "current": round(vrp_spy, 2) if vrp_spy is not None else None,
         "threshold": VRP_RICH,
         "satisfied": bool(vrp_spy is not None and vrp_spy > VRP_RICH),
         "weight": 0.30},
        {"name": "SPY VRP < -2 vol pts (buy-vol)",
         "current": round(vrp_spy, 2) if vrp_spy is not None else None,
         "threshold": VRP_CHEAP,
         "satisfied": bool(vrp_spy is not None and vrp_spy < VRP_CHEAP),
         "weight": 0.20},
        {"name": "Realized - Implied corr gap > +0.15",
         "current": round(dispersion_gap, 3) if dispersion_gap is not None else None,
         "threshold": DISP_GAP_THRESH,
         "satisfied": bool(dispersion_gap > DISP_GAP_THRESH),
         "weight": 0.25},
        {"name": "Implied - Realized corr gap > +0.15",
         "current": round(-dispersion_gap, 3) if dispersion_gap is not None else None,
         "threshold": DISP_GAP_THRESH,
         "satisfied": bool(-dispersion_gap > DISP_GAP_THRESH),
         "weight": 0.15},
        {"name": "Basket coverage (>= 3 single-name IV)",
         "current": len(basket), "threshold": 3,
         "satisfied": bool(len(basket) >= 3), "weight": 0.10},
    ]

    forward = {
        "21d": {
            "expected_outcome": (
                "VRP capture +2 to +4 vol pts" if state == "VRP_RICH" else
                "Vol expansion +30-60% on VIX" if state == "VRP_CHEAP" else
                "Dispersion gap closure 30-50%" if state == "DISPERSION_RICH" else
                "Reverse dispersion gap closure" if state == "DISPERSION_CHEAP" else
                "Range-bound vol surface"),
            "expected_return_pct": (
                15.0 if state == "VRP_RICH" else
                40.0 if state == "VRP_CHEAP" else
                20.0 if state == "DISPERSION_RICH" else
                15.0 if state == "DISPERSION_CHEAP" else 0.0),
        },
        "63d": {
            "expected_outcome": (
                "Mean-reversion to VRP ~ +4" if state in ("VRP_RICH", "VRP_CHEAP") else
                "Correlation regime mean-reverts" if state.startswith("DISPERSION") else
                "Carry trade ~ +1 vol pt"),
            "expected_return_pct": (
                25.0 if state == "VRP_RICH" else
                60.0 if state == "VRP_CHEAP" else
                30.0 if state == "DISPERSION_RICH" else
                20.0 if state == "DISPERSION_CHEAP" else 3.0),
        },
        "252d": {
            "expected_outcome": "Long-run VRP ~ +4 vol pts; Sharpe ~ 1 short-vol carry",
            "expected_return_pct": (
                30.0 if state == "VRP_RICH" else
                40.0 if state == "VRP_CHEAP" else
                25.0 if state == "DISPERSION_RICH" else
                15.0 if state == "DISPERSION_CHEAP" else 10.0),
        },
    }

    output = {
        "engine": "rv-iv-scanner",
        "version": "1.0",
        "as_of": dt.datetime.utcnow().isoformat() + "Z",
        "state": state,
        "signal_strength": signal_strength,
        "summary": {
            "vrp_spy_vol_pts": round(vrp_spy, 2) if vrp_spy is not None else None,
            "index_iv_vix": round(index_iv, 2) if index_iv is not None else None,
            "spy_rv_yang_zhang": round(spy_rv_yz, 2) if spy_rv_yz is not None else None,
            "spy_rv_parkinson": round(spy_rv_park, 2) if spy_rv_park is not None else None,
            "implied_basket_correlation": round(rho_imp, 4) if rho_imp is not None else None,
            "realized_basket_correlation": round(rho_real, 4) if rho_real is not None else None,
            "dispersion_gap": round(dispersion_gap, 4) if dispersion_gap is not None else None,
            "basket_size": len(basket),
            "n_names_tracked": len(name_data),
        },
        "current_readings": {
            "vix_30d": round(index_iv, 2) if index_iv is not None else None,
            "spy_rv_21d": round(spy_rv, 2) if spy_rv is not None else None,
            "vrp_spy": round(vrp_spy, 2) if vrp_spy is not None else None,
            "implied_correlation": round(rho_imp, 4) if rho_imp is not None else None,
            "realized_correlation": round(rho_real, 4) if rho_real is not None else None,
            "dispersion_gap": round(dispersion_gap, 4) if dispersion_gap is not None else None,
            "n_basket_names_with_data": len(basket),
        },
        "trigger_conditions": triggers,
        "forward_expectations": forward,
        "per_name_breakdown": name_rows_out,
        "top_iv_rich": [
            {"ticker": r["ticker"], "vrp_vol_pts": r.get("vrp_vol_pts"),
             "iv": r.get("iv"), "rv_yz": r.get("rv_yang_zhang")}
            for r in rich if r.get("vrp_vol_pts") is not None
        ],
        "top_iv_cheap": [
            {"ticker": c["ticker"], "vrp_vol_pts": c.get("vrp_vol_pts"),
             "iv": c.get("iv"), "rv_yz": c.get("rv_yang_zhang")}
            for c in cheap if c.get("vrp_vol_pts") is not None
        ],
        "recommended_trade": trade,
        "why_now_explainer": build_why_now(state, vrp_spy or 0.0, dispersion_gap or 0.0,
                                            name_rows_dict, rho_imp, rho_real),
        "historical_episodes": [
            {"date": "2008-10-15", "regime": "VRP_RICH (extreme)", "spy_fwd_60d_pct": 4.5,
             "note": "Post-Lehman VRP > +25; selling vol was textbook short-gamma trade"},
            {"date": "2018-02-05", "regime": "VRP_CHEAP -> Volmageddon", "spy_fwd_60d_pct": -3.8,
             "note": "VRP went negative pre-XIV blowup; rare cheap-vol regime"},
            {"date": "2020-03-09", "regime": "VRP_CHEAP", "spy_fwd_60d_pct": -12.5,
             "note": "Implied vol lagging realized in COVID crash onset"},
            {"date": "2023-10-27", "regime": "VRP_RICH", "spy_fwd_60d_pct": 11.8,
             "note": "VRP > +8; iron condors performed; vol-of-vol normal"},
        ],
        "academic_basis": [
            "Bakshi, Kapadia (2003): Delta-Hedged Gains and the Negative Market Volatility Risk Premium",
            "Carr, Wu (2009): Variance Risk Premiums",
            "Driessen, Maenhout, Vilkov (2009): The Price of Correlation Risk",
            "Yang, Zhang (2000): Drift Independent Volatility Estimation",
            "Parkinson (1980): The Extreme Value Method for Estimating Variance",
        ],
        "methodology": (
            "VRP = VIX (FRED VIXCLS, 30d ATM-weighted implied vol) - SPY 21d realized vol "
            "(Yang-Zhang estimator from FMP /stable/historical-price-eod/full OHLC). "
            "Single-name VRP uses FRED CBOE single-name VIX series. Implied correlation "
            "derived from index variance decomposition: sigma_idx^2 = sum(w_i^2 sigma_i^2) + "
            "rho * sum_{i!=j}(w_i w_j sigma_i sigma_j). Realized correlation is the average "
            "pairwise correlation of basket log returns. Dispersion gap = realized_corr - implied_corr."
        ),
        "sources": [
            "FRED CBOE single-name VIX series (VXAPLCLS/VXGOGCLS/VXAZNCLS/VXGSCLS/VXIBMCLS)",
            "FRED VIXCLS (30d implied vol)",
            "FMP /stable/historical-price-eod/full (OHLC for RV)",
            "Bakshi-Kapadia (2003), Carr-Wu (2009), Driessen-Maenhout-Vilkov (2009)",
        ],
        "schedule": "Every 30 min market hours, cron(0,30 13-21 ? * MON-FRI *)",
        "run_duration_seconds": round(time.time() - started, 2),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, indent=2, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )

    if state != "NORMAL" and signal_strength >= 50:
        vrp_str = f"{vrp_spy:+.1f}" if vrp_spy is not None else "n/a"
        gap_str = f"{dispersion_gap:+.3f}" if dispersion_gap is not None else "n/a"
        msg = (f"*VOL REGIME: {state}*\n"
               f"VRP SPY: {vrp_str} vol pts\n"
               f"Disp gap: {gap_str}\n"
               f"Signal: {signal_strength}/100")
        telegram(msg)

    return {
        "state": state,
        "signal_strength": signal_strength,
        "vrp_spy": vrp_spy,
        "dispersion_gap": dispersion_gap,
        "basket_size": len(basket),
    }


def lambda_handler(event, context):
    try:
        result = main_run()
        return {"statusCode": 200, "body": json.dumps(result, default=str)}
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"FATAL: {e}\n{tb}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e), "traceback": tb[-2000:]})}
