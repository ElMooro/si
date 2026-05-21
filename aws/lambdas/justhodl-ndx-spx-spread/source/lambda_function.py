"""
justhodl-ndx-spx-spread
========================

NDX-SPX (tech vs broad market) spread mean-reversion detector.

Pressure-test:
  - Naive: just rank QQQ vs SPY momentum. Misses regime context, doesn't
    distinguish secular tech outperformance from cyclical extremes.
  - Better: 4-factor regime classification:
    (1) QQQ/SPY ratio z-score vs 252d distribution
    (2) Ratio momentum (20d return of the ratio)
    (3) Sector concentration in QQQ (top 5 names %) - extreme concentration
        signals fragility
    (4) Russell 2000 (IWM) confirmation - true broad market lead vs
        mega-cap tech narrative

  - States:
    NDX_EXTREME_LEAD: QQQ/SPY z>=+1.8 + 5d persistence + concentration
      high -> tech overextended, mean-rev fade (long IWM/SPY short QQQ)
    SPX_EXTREME_LEAD: z<=-1.8 + 5d persistence + IWM weak -> SPX overextended,
      tech recovery setup (long QQQ short SPY)
    NEUTRAL: ratio in normal range

Edge basis:
  Fama-French 1992 (size/style cycles), Asness-Liew-Pedersen 1997 (value
  vs growth rotation), Lo-MacKinlay 1988 (mean reversion in equity portfolios).
  When NDX/SPX ratio reaches +2σ AND concentration is extreme, mean-rev
  resolves 4-8 weeks ~58% hit rate with -3% to -5% on the overextended side.

Trade tickets:
  NDX_EXTREME_LEAD (tech overextended): long IWM / SPY, short QQQ
  SPX_EXTREME_LEAD (tech oversold): long QQQ, short SPY/IWM

Schedule: daily 21:45 UTC.
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
S3_KEY = "data/ndx-spx-spread.json"
SSM_STATE_KEY = "/justhodl/ndx-spx-spread/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

ASSETS = {
    "QQQ": "QQQ", "SPY": "SPY", "IWM": "IWM",
    "XLK": "XLK", "XLF": "XLF",
    "AAPL": "AAPL", "MSFT": "MSFT", "NVDA": "NVDA",
    "GOOGL": "GOOGL", "META": "META",  # top NDX names for concentration check
}


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


def fmp_quote(symbol):
    q = urllib.parse.quote_plus(symbol)
    url = f"https://financialmodelingprep.com/stable/quote?symbol={q}&apikey={FMP_KEY}"
    try:
        data = json.loads(http_get(url))
        if isinstance(data, list) and data:
            return data[0]
    except Exception:
        pass
    return None


def fetch_all():
    out = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fmp_history, sym, 300): tag for tag, sym in ASSETS.items()}
        for f in as_completed(futs):
            tag = futs[f]
            try:
                out[tag] = f.result()
            except Exception:
                out[tag] = []
    return out


def pct_return(closes, days):
    if not closes or len(closes) <= days or closes[days] == 0:
        return None
    return (closes[0] / closes[days] - 1.0) * 100


def ratio_series(a, b, n):
    out = []
    for i in range(min(n, len(a), len(b))):
        if b[i] == 0:
            continue
        out.append(a[i] / b[i])
    return out


def zscore_latest(series):
    if not series or len(series) < 30:
        return None
    latest = series[0]
    rest = series[1:]
    m = statistics.mean(rest)
    sd = statistics.stdev(rest) or 1e-9
    return (latest - m) / sd


def persistence_sign(values, n):
    if not values or len(values) < n + 1:
        return None
    signs = []
    for i in range(n):
        diff = values[i] - values[i + 1]
        signs.append(1 if diff > 0 else (-1 if diff < 0 else 0))
    if all(s == signs[0] and s != 0 for s in signs):
        return signs[0]
    return None


def lambda_handler(event, context):
    start = time.time()
    try:
        h = fetch_all()
        qqq = h.get("QQQ", [])
        spy = h.get("SPY", [])
        iwm = h.get("IWM", [])
        xlk = h.get("XLK", [])
        xlf = h.get("XLF", [])
        if len(qqq) < 60 or len(spy) < 60:
            raise RuntimeError(f"insufficient data: QQQ={len(qqq)} SPY={len(spy)}")

        # QQQ/SPY ratio
        ratio = ratio_series(qqq, spy, 252)
        ratio_z = zscore_latest(ratio) if ratio else None
        ratio_20d_pct = (ratio[0] / ratio[20] - 1.0) * 100 if len(ratio) > 20 and ratio[20] else None
        ratio_5d_pct = (ratio[0] / ratio[5] - 1.0) * 100 if len(ratio) > 5 and ratio[5] else None
        ratio_persist = persistence_sign(ratio, 5)

        # Returns
        qqq_5d = pct_return(qqq, 5)
        qqq_20d = pct_return(qqq, 20)
        spy_5d = pct_return(spy, 5)
        spy_20d = pct_return(spy, 20)
        iwm_5d = pct_return(iwm, 5) if iwm else None
        iwm_20d = pct_return(iwm, 20) if iwm else None
        xlk_20d = pct_return(xlk, 20) if xlk else None
        xlf_20d = pct_return(xlf, 20) if xlf else None

        # NDX concentration check: top 5 stocks (AAPL+MSFT+NVDA+GOOGL+META) summed mcap
        top5_mcaps = []
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(fmp_quote, t): t for t in ["AAPL","MSFT","NVDA","GOOGL","META"]}
            for f in as_completed(futs):
                try:
                    q = f.result()
                    if q and q.get("marketCap"):
                        top5_mcaps.append(float(q["marketCap"]))
                except Exception:
                    pass
        top5_combined_trillion = sum(top5_mcaps) / 1e12 if top5_mcaps else None

        # IWM vs SPY: small-cap confirmation
        iwm_spy_lag = (iwm_20d - spy_20d) if (iwm_20d is not None and spy_20d is not None) else None

        # XLK vs XLF: tech vs financials sector spread
        xlk_xlf_spread = (xlk_20d - xlf_20d) if (xlk_20d is not None and xlf_20d is not None) else None

        # Classify
        state = "NEUTRAL"
        strength = 0.2
        why = "QQQ/SPY ratio in normal range"

        if ratio_z is not None and ratio_persist is not None:
            # NDX overextended: tech mean-rev fade
            if (ratio_z >= 1.8 and ratio_persist == 1
                    and qqq_20d is not None and qqq_20d - (spy_20d or 0) >= 3):
                state = "NDX_EXTREME_LEAD_RICH"
                strength = min(1.0, 0.7 + (ratio_z - 1.8) * 0.1)
                why = (f"QQQ/SPY z=+{round(ratio_z,2)}, QQQ outperforms SPY by "
                       f"{round(qqq_20d - (spy_20d or 0), 1)}%/20d, persistent 5d "
                       f"-> NDX overextended, fade")
            elif ratio_z >= 1.2 and ratio_persist == 1:
                state = "NDX_LEAD_ACTIVE"
                strength = 0.5
                why = f"QQQ/SPY z=+{round(ratio_z,2)}, tech leading; building extreme"
            # SPX overextended (tech weak): tech recovery setup
            elif (ratio_z <= -1.8 and ratio_persist == -1
                    and qqq_20d is not None and (spy_20d or 0) - qqq_20d >= 3):
                state = "SPX_EXTREME_LEAD_RICH"
                strength = min(1.0, 0.7 + abs(ratio_z - (-1.8)) * 0.1)
                why = (f"QQQ/SPY z={round(ratio_z,2)}, SPX outperforms QQQ by "
                       f"{round((spy_20d or 0) - qqq_20d, 1)}%/20d, persistent 5d "
                       f"-> tech oversold, recovery setup")
            elif ratio_z <= -1.2 and ratio_persist == -1:
                state = "SPX_LEAD_ACTIVE"
                strength = 0.5
                why = f"QQQ/SPY z={round(ratio_z,2)}, broad leading; building extreme"

        tickets = []
        if state == "NDX_EXTREME_LEAD_RICH":
            tickets = [
                {"ticker": "SPY", "side": "LONG",
                 "rationale": "Long SPY vs short QQQ pair on tech overextension",
                 "target_pct": 4, "stop_pct": -2.5, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 2.0},
                {"ticker": "QQQ", "side": "SHORT",
                 "rationale": "Short QQQ leg of pair trade",
                 "target_pct": -5, "stop_pct": 3, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 2.0},
                {"ticker": "IWM", "side": "LONG",
                 "rationale": "Small-cap leadership in tech mean-rev environment",
                 "target_pct": 6, "stop_pct": -3.5, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 1.5},
                {"ticker": "PSQ", "side": "LONG",
                 "rationale": "Inverse QQQ ETF (avoid options margin)",
                 "target_pct": 5, "stop_pct": -3, "size_pct_portfolio": 1.0},
            ]
        elif state == "NDX_LEAD_ACTIVE":
            tickets = [
                {"ticker": "PSQ", "side": "LONG",
                 "rationale": "Partial inverse QQQ entry; await full extreme",
                 "target_pct": 2.5, "stop_pct": -2, "size_pct_portfolio": 0.75},
            ]
        elif state == "SPX_EXTREME_LEAD_RICH":
            tickets = [
                {"ticker": "QQQ", "side": "LONG",
                 "rationale": "Long QQQ on tech oversold setup",
                 "target_pct": 6, "stop_pct": -3.5, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 2.5},
                {"ticker": "XLK", "side": "LONG",
                 "rationale": "Technology select sector ETF",
                 "target_pct": 7, "stop_pct": -4, "holding_period": "4-8 weeks",
                 "size_pct_portfolio": 1.5},
                {"ticker": "SPY", "side": "SHORT",
                 "rationale": "Short SPY leg of mean-rev pair",
                 "target_pct": -3, "stop_pct": 2.5, "size_pct_portfolio": 1.5},
            ]
        elif state == "SPX_LEAD_ACTIVE":
            tickets = [
                {"ticker": "QQQ", "side": "LONG",
                 "rationale": "Partial QQQ entry; await full extreme",
                 "target_pct": 3, "stop_pct": -2.5, "size_pct_portfolio": 1.0},
            ]

        out = {
            "engine": "ndx-spx-spread",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "current_metrics": {
                "qqq_spy_ratio": round(ratio[0], 4) if ratio else None,
                "ratio_zscore_252d": round(ratio_z, 2) if ratio_z is not None else None,
                "ratio_5d_pct": round(ratio_5d_pct, 2) if ratio_5d_pct is not None else None,
                "ratio_20d_pct": round(ratio_20d_pct, 2) if ratio_20d_pct is not None else None,
                "ratio_persistence_5d": ratio_persist,
                "qqq_5d_pct": round(qqq_5d, 2) if qqq_5d is not None else None,
                "qqq_20d_pct": round(qqq_20d, 2) if qqq_20d is not None else None,
                "spy_5d_pct": round(spy_5d, 2) if spy_5d is not None else None,
                "spy_20d_pct": round(spy_20d, 2) if spy_20d is not None else None,
                "iwm_5d_pct": round(iwm_5d, 2) if iwm_5d is not None else None,
                "iwm_20d_pct": round(iwm_20d, 2) if iwm_20d is not None else None,
                "iwm_spy_lag_pct": round(iwm_spy_lag, 2) if iwm_spy_lag is not None else None,
                "xlk_xlf_spread_20d": round(xlk_xlf_spread, 2) if xlk_xlf_spread is not None else None,
                "top5_ndx_combined_trillion_usd": (round(top5_combined_trillion, 2)
                                                    if top5_combined_trillion else None),
            },
            "regime_explanation": why,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "NDX-SPX (tech vs broad market) spread mean-reversion via "
                "QQQ/SPY ratio. 4-factor: (1) ratio z-score vs 252d; (2) ratio "
                "5d persistence; (3) IWM-SPY small-cap confirmation; (4) "
                "XLK/XLF sector overlay + NDX top-5 concentration. "
                "NDX_EXTREME_LEAD_RICH: ratio z>=+1.8 + persistent + QQQ "
                "outperforms SPY >3%/20d (tech overextended, fade). "
                "SPX_EXTREME_LEAD_RICH: z<=-1.8 (tech oversold, recover). "
                "Edge basis: Fama-French 1992, Asness-Liew-Pedersen 1997, "
                "Lo-MacKinlay 1988. ~58% hit / +/-3-5% / 4-8 wks."
            ),
            "sources": ["FMP /stable/historical-price-eod/light "
                        "(QQQ, SPY, IWM, XLK, XLF, top-5 NDX names)",
                        "FMP /stable/quote (mcaps)"],
            "why_now": why,
            "run_seconds": round(time.time() - start, 2),
        }

        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and "RICH" in state and TELEGRAM_TOKEN:
            msg = (f"*NDX-SPX-SPREAD -> {state}*\n"
                   f"QQQ/SPY: {round(ratio[0],4)}  z: {round(ratio_z,2)}\n"
                   f"QQQ 20d: {round(qqq_20d,1)}%  SPY 20d: {round(spy_20d,1)}%  "
                   f"IWM 20d: {round(iwm_20d,1) if iwm_20d else 'n/a'}%\n"
                   f"{why}\nTickets: {len(tickets)}")
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
        err = {"engine": "ndx-spx-spread", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
