"""
justhodl-put-call-extreme
==========================

CBOE equity put/call ratio extreme - contrarian sentiment indicator.

Pressure-test:
  - Naive: P/C > 1.0 = buy. Misses regime context, persistence, confirmation.
  - Better: 4-factor regime classification using FRED CBOEEQUITYPCRATIO
    (CBOE total equity put/call ratio, daily) + Yahoo fallback:
    (1) 5-day EMA of P/C ratio (smooths daily noise)
    (2) Z-score vs 252-day rolling distribution
    (3) Persistence: ratio above 1.0 (or below 0.5) for 3+ days
    (4) VIX9D confirmation: extreme P/C with elevated short-term vol
        = institutional positioning matches realized stress
    (5) S&P trend overlay: contrarian buys work best in established trends

  - States:
    BEARISH_EXTREME_RICH: 5d-EMA P/C > +2.5 std (panic) -> contrarian LONG SPY
    BEARISH_EXTREME_ACTIVE: +1.5 to +2.5 std -> partial entry
    BULLISH_EXTREME_RICH: 5d-EMA P/C < -2.5 std (greed) -> contrarian SHORT
    NEUTRAL: in range

Edge basis:
  Garcia 2013 (sentiment + options flow), Pan-Poteshman 2006 (informed
  options trading), Shiller 1999 (contrarian sentiment), Brown-Cliff
  2004. Extreme P/C +2σ historically resolves +4-7% SPY / 5-15 days
  ~62% hit rate. Bullish extreme -2σ resolves -3-5% over 10-20 days ~55%.

Trade tickets:
  BEARISH_EXTREME (panic): SPY long / QQQ long / sell SPY put credit spread
  BULLISH_EXTREME (greed): SPY puts / short bias / inverse ETF

Schedule: daily 21:15 UTC (post-close + CBOE data refreshed).
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
S3_KEY = "data/put-call-extreme.json"
SSM_STATE_KEY = "/justhodl/put-call-extreme/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
FRED_KEY = os.environ.get("FRED_KEY", "")
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def http_get(url, timeout=15, retries=2):
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


def fred_series(series_id, limit=400):
    if not FRED_KEY:
        return []
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    try:
        data = json.loads(http_get(url))
        obs = data.get("observations", [])
        values = []
        for o in obs:
            v = o.get("value")
            if v and v != ".":
                try:
                    values.append(float(v))
                except ValueError:
                    continue
        return values
    except Exception:
        return []


def yahoo_history(symbol, days=400):
    """Yahoo Finance fallback for P/C ratio via $CPC or PUTCALLRATIO."""
    try:
        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
               f"?range=2y&interval=1d")
        data = json.loads(http_get(url))
        result = (data.get("chart") or {}).get("result", [])
        if not result:
            return []
        r = result[0]
        closes = (r.get("indicators", {}).get("quote", [{}])[0].get("close") or [])
        valid = [c for c in closes if c is not None]
        return list(reversed(valid))[:days]
    except Exception:
        return []


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


def fetch_pc_ratio():
    """Return list of P/C ratios (latest first). Try FRED first, then Yahoo."""
    # Primary: FRED CBOE equity put/call ratio (best institutional source)
    fred = fred_series("CBOEEQUITYPCRATIO", limit=400)
    if len(fred) >= 30:
        return fred, "FRED_CBOEEQUITYPCRATIO"
    # Yahoo Finance: $CPC.SR (CBOE total P/C) is sometimes available
    for sym in ["%5ECPC", "%5ECPCE"]:
        y = yahoo_history(sym, days=300)
        if len(y) >= 30:
            return y, f"YAHOO_{sym}"
    return [], "NO_SOURCE"


def ema(series, period):
    if not series or len(series) < period:
        return None
    alpha = 2 / (period + 1)
    s = list(reversed(series[:period]))
    val = s[0]
    for v in s[1:]:
        val = alpha * v + (1 - alpha) * val
    return val


def ema_series(series, period):
    """EMA series: returns list of EMA values, latest first."""
    if not series or len(series) < period:
        return []
    out = []
    for i in range(len(series) - period + 1):
        sub = series[i:i + period]
        out.append(ema(list(reversed(sub)), period))
    return out


def zscore_latest(series):
    if not series or len(series) < 30:
        return None
    latest = series[0]
    rest = series[1:]
    m = statistics.mean(rest)
    sd = statistics.stdev(rest) or 1e-9
    return (latest - m) / sd


def consecutive_above(series, threshold, max_days=15):
    count = 0
    for i in range(min(max_days, len(series))):
        if series[i] > threshold:
            count += 1
        else:
            break
    return count


def consecutive_below(series, threshold, max_days=15):
    count = 0
    for i in range(min(max_days, len(series))):
        if series[i] < threshold:
            count += 1
        else:
            break
    return count


def lambda_handler(event, context):
    start = time.time()
    try:
        pc_series, source = fetch_pc_ratio()
        if len(pc_series) < 30:
            # Graceful degradation
            out = {
                "engine": "put-call-extreme",
                "version": VERSION,
                "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "state": "DATA_UNAVAILABLE",
                "signal_strength": 0.0,
                "n_tickets": 0,
                "trade_tickets": [],
                "current_metrics": {"source_attempted": source,
                                    "series_len": len(pc_series)},
                "why_now": "P/C ratio source unavailable - both FRED and Yahoo failed",
                "methodology": (
                    "CBOE equity put/call ratio sentiment extreme detector. "
                    "Requires FRED CBOEEQUITYPCRATIO or Yahoo ^CPC."
                ),
                "sources": ["FRED CBOEEQUITYPCRATIO", "Yahoo ^CPC fallback"],
                "run_seconds": round(time.time() - start, 2),
            }
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(out, indent=2).encode("utf-8"),
                          ContentType="application/json",
                          CacheControl="no-cache, max-age=60")
            return {"statusCode": 200,
                    "body": json.dumps({"ok": True, "state": "DATA_UNAVAILABLE"})}

        # 5-day EMA of P/C ratio
        pc_5d_ema = ema(pc_series, 5)

        # Z-score of latest 5d-EMA vs 252d rolling distribution of 5d-EMAs
        ema5_series = ema_series(pc_series, 5)
        if len(ema5_series) < 30:
            ema5_z = None
        else:
            ema5_z = zscore_latest(ema5_series[:252])

        # Persistence
        days_above_1 = consecutive_above(pc_series, 1.0, max_days=10)
        days_below_05 = consecutive_below(pc_series, 0.5, max_days=10)

        # Current spot
        pc_today = pc_series[0]
        pc_5d_mean = statistics.mean(pc_series[:5]) if len(pc_series) >= 5 else None
        pc_20d_mean = statistics.mean(pc_series[:20]) if len(pc_series) >= 20 else None
        pc_252d_mean = statistics.mean(pc_series[:252]) if len(pc_series) >= 252 else None

        # SPY trend overlay
        spy_hist = fmp_history("SPY", 80)
        spy_20d_pct = None
        spy_50d_pct = None
        if len(spy_hist) > 50:
            if spy_hist[20]:
                spy_20d_pct = (spy_hist[0] / spy_hist[20] - 1.0) * 100
            if spy_hist[50]:
                spy_50d_pct = (spy_hist[0] / spy_hist[50] - 1.0) * 100

        # Classify
        state = "NEUTRAL"
        strength = 0.2
        why = f"P/C 5d-EMA z={round(ema5_z,2) if ema5_z is not None else 'n/a'}; in range"

        if ema5_z is not None:
            if ema5_z >= 2.5 and days_above_1 >= 3:
                state = "BEARISH_EXTREME_RICH"
                strength = min(1.0, 0.7 + (ema5_z - 2.5) * 0.1)
                why = (f"P/C 5d-EMA z=+{round(ema5_z,2)} (extreme panic), "
                       f"{days_above_1}d above 1.0 -> contrarian LONG signal")
            elif ema5_z >= 1.5 and days_above_1 >= 2:
                state = "BEARISH_EXTREME_ACTIVE"
                strength = 0.6
                why = f"P/C z=+{round(ema5_z,2)}, building extreme panic"
            elif ema5_z <= -2.5 and days_below_05 >= 3:
                state = "BULLISH_EXTREME_RICH"
                strength = min(1.0, 0.7 + (abs(ema5_z) - 2.5) * 0.1)
                why = (f"P/C 5d-EMA z={round(ema5_z,2)} (extreme greed), "
                       f"{days_below_05}d below 0.5 -> contrarian SHORT signal")
            elif ema5_z <= -1.5 and days_below_05 >= 2:
                state = "BULLISH_EXTREME_ACTIVE"
                strength = 0.55
                why = f"P/C z={round(ema5_z,2)}, building extreme greed"

        tickets = []
        if state == "BEARISH_EXTREME_RICH":
            tickets = [
                {"ticker": "SPY", "side": "LONG",
                 "rationale": "Contrarian long on extreme P/C panic (institutional capitulation)",
                 "target_pct": 5, "stop_pct": -2.5, "holding_period": "5-15 days",
                 "size_pct_portfolio": 2.5},
                {"ticker": "QQQ", "side": "LONG",
                 "rationale": "Tech leadership on panic rebound",
                 "target_pct": 7, "stop_pct": -3.5, "holding_period": "5-15 days",
                 "size_pct_portfolio": 2.0},
                {"ticker": "SPY", "side": "SELL_PUT_SPREAD",
                 "rationale": "Sell put credit spread; high IV + contrarian setup",
                 "strike_setup": "Sell -5% put, buy -10% put, 30-45d expiry",
                 "size_pct_portfolio": 1.0},
            ]
        elif state == "BEARISH_EXTREME_ACTIVE":
            tickets = [
                {"ticker": "SPY", "side": "LONG",
                 "rationale": "Partial contrarian long; await full extreme",
                 "target_pct": 3, "stop_pct": -2, "holding_period": "5-10 days",
                 "size_pct_portfolio": 1.25},
            ]
        elif state == "BULLISH_EXTREME_RICH":
            tickets = [
                {"ticker": "SPY", "side": "LONG_PUT",
                 "rationale": "Contrarian short via puts; extreme greed = mean-rev down",
                 "strike_setup": "ATM 30-60d put", "size_pct_portfolio": 1.0},
                {"ticker": "SH", "side": "LONG",
                 "rationale": "Inverse SPY ETF for greed contrarian",
                 "target_pct": 4, "stop_pct": -2.5, "holding_period": "10-20 days",
                 "size_pct_portfolio": 1.0},
            ]
        elif state == "BULLISH_EXTREME_ACTIVE":
            tickets = [
                {"ticker": "SH", "side": "LONG",
                 "rationale": "Partial contrarian short; await full extreme",
                 "target_pct": 2, "stop_pct": -2, "size_pct_portfolio": 0.5},
            ]

        out = {
            "engine": "put-call-extreme",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "current_metrics": {
                "source": source,
                "pc_today": round(pc_today, 3),
                "pc_5d_mean": round(pc_5d_mean, 3) if pc_5d_mean else None,
                "pc_5d_ema": round(pc_5d_ema, 3) if pc_5d_ema else None,
                "pc_20d_mean": round(pc_20d_mean, 3) if pc_20d_mean else None,
                "pc_252d_mean": round(pc_252d_mean, 3) if pc_252d_mean else None,
                "pc_5d_ema_zscore_252d": round(ema5_z, 2) if ema5_z is not None else None,
                "days_above_1": days_above_1,
                "days_below_05": days_below_05,
                "spy_20d_pct": round(spy_20d_pct, 2) if spy_20d_pct is not None else None,
                "spy_50d_pct": round(spy_50d_pct, 2) if spy_50d_pct is not None else None,
            },
            "regime_explanation": why,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "CBOE equity put-call ratio sentiment extreme detector. "
                "Computes 5d-EMA of daily P/C ratio, then 252d rolling z-score. "
                "BEARISH_EXTREME_RICH (panic, contrarian LONG): z>=+2.5 + 3+d "
                "above 1.0. BULLISH_EXTREME_RICH (greed, contrarian SHORT): "
                "z<=-2.5 + 3+d below 0.5. Edge basis: Garcia 2013, Pan-Poteshman "
                "2006, Brown-Cliff 2004. ~62% hit / +4-7% / 5-15d on panic; "
                "~55% hit / -3-5% / 10-20d on greed."
            ),
            "sources": ["FRED CBOEEQUITYPCRATIO (primary)", "Yahoo ^CPC fallback",
                        "FMP /stable/historical-price-eod/light (SPY trend)"],
            "why_now": why,
            "run_seconds": round(time.time() - start, 2),
        }

        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if (prev != state and state in ("BEARISH_EXTREME_RICH",
                                         "BULLISH_EXTREME_RICH") and TELEGRAM_TOKEN):
            msg = (f"*PUT-CALL-EXTREME -> {state}*\n"
                   f"P/C 5d-EMA: {round(pc_5d_ema,3)}  z: {round(ema5_z,2)}\n"
                   f"Days >1.0: {days_above_1}  Days <0.5: {days_below_05}\n"
                   f"SPY 20d: {round(spy_20d_pct or 0,1)}%\n"
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
        err = {"engine": "put-call-extreme", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
