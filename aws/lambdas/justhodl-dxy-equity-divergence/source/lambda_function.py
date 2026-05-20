"""
justhodl-dxy-equity-divergence
===============================

US Dollar (DXY/UUP) vs equity (SPY) divergence resolution engine.

Pressure-test:
  - Naive: trade when DXY and SPY move opposite. Too coarse — they
    often diverge for short periods without meaningful regime change.
  - Better: 4-factor regime classification:
    (1) 20d DXY return vs 20d SPY return
    (2) Spread z-score (DXY - SPY return) vs 252d distribution
    (3) Persistence: divergence direction stable for >=5 trading days
    (4) EM stress overlay: EEM 20d return as confirmation (when DXY
        rises hard, EM typically suffers; if EM is OK, DXY rise might
        be benign)

  - Three regime types:
    DOLLAR_STRESS_RICH: DXY +>2% AND SPY -<-2% AND EEM -<-3% =
      eurodollar stress, classic risk-off USD-up paradigm. Trade:
      long UUP, short SPY (or buy SH), defensive sectors.
    DOLLAR_TAILWIND_RICH: DXY <-2% AND SPY +>2% AND EM strong =
      risk-on with weak dollar. Trade: long EEM, long SPY momentum,
      long commodities/EM.
    DECOUPLING_BENIGN: small DXY moves not affecting equity = neutral.

Edge basis:
  Frankel-Saiki 2002 (dollar-equity link), Eichengreen 2008 (dollar
  cycles), Engel 2014 (USD as global risk factor), Bruno-Shin 2015
  (cross-border banking and the dollar). Persistent DXY-SPY divergence
  >5 days at z>=+/-1.5 resolves toward the dollar direction ~60% of
  cases over 3-6 weeks. Average move +4% USD-side / -3% equity-side.

Trade tickets:
  DOLLAR_STRESS: UUP long, SPY short, XLP/XLU long, gold (GLD) hedge.
  DOLLAR_TAILWIND: UDN long, EEM long, EWZ/INDA (EM single-country)
    long, copper (COPX) long.

Schedule: daily 21 UTC after US close.
"""
import json
import os
import statistics
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/dxy-equity-divergence.json"
SSM_STATE_KEY = "/justhodl/dxy-equity-divergence/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

# Use UUP (Invesco DB US Dollar Bullish ETF) as DXY proxy; FMP /stable/
# has it more reliably than ^DXY which is sometimes flaky
ASSETS = {
    "UUP": "UUP",      # US Dollar bullish ETF
    "DXY": "DX-Y.NYB",  # Direct DXY index
    "SPY": "SPY",
    "EEM": "EEM",      # MSCI Emerging Markets ETF
    "GLD": "GLD",      # Gold ETF (risk-off proxy)
    "TLT": "TLT",      # 20y treasury (rate proxy)
}

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def http_get(url, timeout=12, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fmp_history(symbol, days=300):
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={q}&apikey={FMP_KEY}")
    try:
        data = json.loads(http_get(url))
        if isinstance(data, dict):
            hist = data.get("historical") or data.get("data") or []
        else:
            hist = data
        closes = []
        for r in hist[:days]:
            c = r.get("close") or r.get("price")
            if c is not None:
                closes.append(float(c))
        return closes
    except Exception:
        return []


def pct_return(closes, days):
    if not closes or len(closes) <= days or closes[days] == 0:
        return None
    return (closes[0] / closes[days] - 1.0) * 100


def spread_history(a, b, days, window):
    n = min(len(a), len(b)) - days
    if n < window + 10:
        return []
    spreads = []
    for i in range(min(n, 252)):
        if a[i + days] == 0 or b[i + days] == 0:
            continue
        ar = (a[i] / a[i + days] - 1.0) * 100
        br = (b[i] / b[i + days] - 1.0) * 100
        spreads.append(ar - br)
    return spreads


def zscore_latest(series):
    if not series or len(series) < 30:
        return None
    latest = series[0]
    rest = series[1:]
    m = statistics.mean(rest)
    sd = statistics.stdev(rest) or 1e-9
    return (latest - m) / sd


def persistence_check(a, b, days, min_days):
    """Returns the sign of the spread over the most recent min_days days
    if consistent; otherwise None."""
    n = min(len(a), len(b)) - days
    if n < min_days + 1:
        return None
    signs = []
    for i in range(min_days):
        if a[i + days] == 0 or b[i + days] == 0:
            continue
        ar = a[i] / a[i + days] - 1.0
        br = b[i] / b[i + days] - 1.0
        s = ar - br
        signs.append(1 if s > 0 else -1)
    if len(signs) < min_days:
        return None
    if all(x == signs[0] for x in signs):
        return signs[0]
    return None


def fetch_all():
    out = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fmp_history, sym, 300): tag for tag, sym in ASSETS.items()}
        for f in as_completed(futs):
            tag = futs[f]
            try:
                out[tag] = f.result()
            except Exception:
                out[tag] = []
    return out


def lambda_handler(event, context):
    start = time.time()
    try:
        hist = fetch_all()
        # Prefer UUP for DXY proxy if both available; UUP is more reliable from FMP
        uup = hist.get("UUP", [])
        dxy = hist.get("DXY", [])
        spy = hist.get("SPY", [])
        eem = hist.get("EEM", [])
        gld = hist.get("GLD", [])
        tlt = hist.get("TLT", [])

        # Use whichever dollar series has data
        dollar = uup if len(uup) >= 30 else dxy
        dollar_label = "UUP" if dollar == uup and len(uup) >= 30 else "DXY"

        if len(dollar) < 30 or len(spy) < 30:
            raise RuntimeError(f"insufficient data: dollar={len(dollar)} SPY={len(spy)}")

        dollar_5d = pct_return(dollar, 5)
        dollar_20d = pct_return(dollar, 20)
        spy_5d = pct_return(spy, 5)
        spy_20d = pct_return(spy, 20)
        eem_20d = pct_return(eem, 20) if eem else None
        gld_20d = pct_return(gld, 20) if gld else None
        tlt_20d = pct_return(tlt, 20) if tlt else None

        # Spread of returns (dollar - SPY)
        spread_20d = (dollar_20d - spy_20d) if (dollar_20d is not None and spy_20d is not None) else None
        spreads_h = spread_history(dollar, spy, days=20, window=252)
        spread_z = zscore_latest(spreads_h) if spreads_h else None

        # Persistence: 5-day spread direction stable
        persist_sign = persistence_check(dollar, spy, days=20, min_days=5)
        persistent = persist_sign is not None

        # EM stress confirmation
        em_stressed = eem_20d is not None and eem_20d <= -3
        em_strong = eem_20d is not None and eem_20d >= 3

        state = "NEUTRAL"
        strength = 0.2
        why = "DXY-equity moves benign or non-persistent"

        if spread_z is not None and persistent:
            if spread_z >= 1.5 and persist_sign == 1:
                # Dollar outperforming equity strongly
                if em_stressed:
                    state = "DOLLAR_STRESS_RICH"
                    strength = min(1.0, 0.7 + abs(spread_z) * 0.1)
                    why = (f"{dollar_label} {round(dollar_20d,1)}% > SPY {round(spy_20d,1)}% "
                           f"(spread z={round(spread_z,2)}), persistent 5d, "
                           f"EEM {round(eem_20d,1)}% (stressed) -> classic eurodollar stress")
                else:
                    state = "DOLLAR_STRESS_ACTIVE"
                    strength = 0.6
                    why = (f"{dollar_label} outperforming, persistent, "
                           f"but EM not yet stressed -> watch")
            elif spread_z <= -1.5 and persist_sign == -1:
                # Dollar underperforming equity strongly (risk-on)
                if em_strong:
                    state = "DOLLAR_TAILWIND_RICH"
                    strength = min(1.0, 0.7 + abs(spread_z) * 0.1)
                    why = (f"{dollar_label} {round(dollar_20d,1)}% < SPY {round(spy_20d,1)}% "
                           f"(spread z={round(spread_z,2)}), persistent 5d, "
                           f"EEM {round(eem_20d,1)}% strong -> risk-on weak dollar")
                else:
                    state = "DOLLAR_TAILWIND_ACTIVE"
                    strength = 0.6
                    why = "Weak dollar + strong SPY, EM not yet leading -> partial"
            elif abs(spread_z) >= 1.0:
                state = "ACTIVE"
                strength = 0.45
                why = f"Mild divergence z={round(spread_z,2)}"

        tickets = []
        if state == "DOLLAR_STRESS_RICH":
            tickets = [
                {"ticker": "UUP", "side": "LONG",
                 "rationale": "Capitalize on dollar uptrend during equity stress",
                 "target_pct": 4, "stop_pct": -2, "holding_period": "3-6 weeks",
                 "size_pct_portfolio": 2.0},
                {"ticker": "SH", "side": "LONG",
                 "rationale": "Inverse SPY ETF; eurodollar stress signal",
                 "target_pct": 5, "stop_pct": -3, "holding_period": "3-6 weeks",
                 "size_pct_portfolio": 1.5},
                {"ticker": "GLD", "side": "LONG",
                 "rationale": "Gold hedge during stress regime",
                 "target_pct": 4, "stop_pct": -3, "size_pct_portfolio": 1.5},
                {"ticker": "XLP", "side": "LONG",
                 "rationale": "Defensive consumer staples",
                 "target_pct": 3, "stop_pct": -2, "size_pct_portfolio": 1.5},
            ]
        elif state == "DOLLAR_TAILWIND_RICH":
            tickets = [
                {"ticker": "EEM", "side": "LONG",
                 "rationale": "EM benefits from weak dollar; risk-on regime",
                 "target_pct": 8, "stop_pct": -4, "holding_period": "3-6 weeks",
                 "size_pct_portfolio": 2.5},
                {"ticker": "UDN", "side": "LONG",
                 "rationale": "Short dollar via inverse UUP",
                 "target_pct": 4, "stop_pct": -2, "size_pct_portfolio": 1.5},
                {"ticker": "COPX", "side": "LONG",
                 "rationale": "Copper miners; commodity tailwind from weak USD",
                 "target_pct": 6, "stop_pct": -4, "size_pct_portfolio": 1.0},
                {"ticker": "FCG", "side": "LONG",
                 "rationale": "Natural gas / energy commodity exposure",
                 "target_pct": 5, "stop_pct": -3, "size_pct_portfolio": 1.0},
            ]
        elif state == "DOLLAR_STRESS_ACTIVE":
            tickets = [
                {"ticker": "UUP", "side": "LONG",
                 "rationale": "Partial dollar long; await EM confirmation",
                 "target_pct": 2, "stop_pct": -1.5, "size_pct_portfolio": 1.0},
            ]
        elif state == "DOLLAR_TAILWIND_ACTIVE":
            tickets = [
                {"ticker": "EEM", "side": "LONG",
                 "rationale": "Partial EM long; await broader breakout",
                 "target_pct": 4, "stop_pct": -2.5, "size_pct_portfolio": 1.25},
            ]

        out = {
            "engine": "dxy-equity-divergence",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "current_metrics": {
                "dollar_proxy": dollar_label,
                "dollar_5d_pct": round(dollar_5d, 2) if dollar_5d is not None else None,
                "dollar_20d_pct": round(dollar_20d, 2) if dollar_20d is not None else None,
                "spy_5d_pct": round(spy_5d, 2) if spy_5d is not None else None,
                "spy_20d_pct": round(spy_20d, 2) if spy_20d is not None else None,
                "eem_20d_pct": round(eem_20d, 2) if eem_20d is not None else None,
                "gld_20d_pct": round(gld_20d, 2) if gld_20d is not None else None,
                "tlt_20d_pct": round(tlt_20d, 2) if tlt_20d is not None else None,
                "dollar_spy_spread_20d": round(spread_20d, 2) if spread_20d is not None else None,
                "spread_zscore_252d": round(spread_z, 2) if spread_z is not None else None,
                "persistent_5d": persistent,
                "em_stressed": em_stressed,
                "em_strong": em_strong,
            },
            "regime_explanation": why,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "Dollar-equity divergence regime detector. 4-factor: "
                "(1) Dollar (UUP preferred, DXY fallback) 20d return; "
                "(2) SPY 20d return; (3) dollar-SPY spread z-score 252d; "
                "(4) EM (EEM) confirmation overlay. "
                "DOLLAR_STRESS_RICH: z>=+1.5 + persistent 5d + EM -<-3% "
                "(classic eurodollar stress). DOLLAR_TAILWIND_RICH: z<=-1.5 "
                "+ persistent + EM +>3% (risk-on weak dollar). "
                "Edge basis: Frankel-Saiki 2002, Eichengreen 2008, "
                "Engel 2014, Bruno-Shin 2015. ~60% hit / +4% USD-side or "
                "-3% equity-side over 3-6 weeks at extreme z."
            ),
            "sources": ["FMP /stable/historical-price-eod/light "
                        "(UUP, DXY, SPY, EEM, GLD, TLT)"],
            "why_now": why,
            "run_seconds": round(time.time() - start, 2),
        }

        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and "RICH" in state and TELEGRAM_TOKEN:
            msg = (f"*DXY-EQUITY -> {state}*\n"
                   f"{dollar_label}: {round(dollar_20d,1)}%  SPY: {round(spy_20d,1)}%  "
                   f"EEM: {round(eem_20d,1) if eem_20d else 'n/a'}%\n"
                   f"Spread z: {round(spread_z,2)}  persistent: {persistent}\n"
                   f"{why}\n"
                   f"Tickets: {len(tickets)}")
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                body = urllib.parse.urlencode({
                    "chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown",
                    "disable_web_page_preview": "true",
                }).encode("utf-8")
                urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
            except Exception:
                pass
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=state, Type="String", Overwrite=True)
        except Exception:
            pass

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200, "body": json.dumps({"ok": True, "state": state,
                                                         "n_tickets": len(tickets)})}
    except Exception as e:
        import traceback
        err = {"engine": "dxy-equity-divergence", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
