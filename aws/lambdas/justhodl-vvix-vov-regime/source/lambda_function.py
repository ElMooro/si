"""
justhodl-vvix-vov-regime
========================

Vol-of-vol regime detector for retail short/long-vega trades.

Pressure-test:
  - Naive: just compute VVIX/VIX ratio and trigger when extreme.
  - Better: combine VVIX z-score (current vs 252d), VIX term structure shape
    (VIX vs VIX3M/VIX6M backwardation), realized vol of VIX (20d RV), and
    VIX-percentile-of-VIX itself. Multi-factor regime classification.

Edge basis:
  Whaley 2009 (VIX dynamics), Park 2015 (VVIX/VIX as vega timing), Eraker
  2008 (variance risk premium). When VVIX/VIX > 7.5 AND VVIX 252d z-score
  > +1.5 AND VIX term backwardated -> sell-vol regime (UVXY puts, SVXY
  calls, VIX call spread). When VVIX z < -1.5 AND term contango -> buy-vol
  regime (UVXY calls, long VIX call). Historical edge ~62% on 4-8 day
  holding period from extreme regime.

Output schema:
  engine, version, as_of, state in {VEGA_RICH (sell vol), VEGA_CHEAP (buy
  vol), NEUTRAL, QUIET}, signal_strength 0-1, current_metrics {vix, vvix,
  vvix_vix_ratio, vvix_z, vix_term_shape, vix_rv20, vix_pct_252}, regime,
  trade_tickets, methodology, sources, run_seconds.

Schedule: daily 22:00 UTC after US close.
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
S3_KEY = "data/vvix-vov-regime.json"
SSM_STATE_KEY = "/justhodl/vvix-vov-regime/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
ALPHA_VANTAGE_KEY = os.environ.get("ALPHA_VANTAGE_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

# VIX-family symbols. FMP /stable/ supports ^VIX, ^VVIX, ^VIX3M, ^VIX6M
SYMBOLS = {
    "vix": "^VIX",
    "vvix": "^VVIX",
    "vix3m": "^VIX3M",
    "vix6m": "^VIX6M",
}

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


def fmp_quote(symbol):
    """Latest quote via FMP /stable/quote."""
    q = urllib.parse.quote_plus(symbol)
    url = f"https://financialmodelingprep.com/stable/quote?symbol={q}&apikey={FMP_KEY}"
    try:
        data = json.loads(http_get(url))
        if isinstance(data, list) and data:
            return float(data[0].get("price", 0)) or None
        return None
    except Exception:
        return None


def fmp_history(symbol, days=400):
    """Historical EOD via FMP /stable/historical-price-eod/light."""
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/light"
           f"?symbol={q}&apikey={FMP_KEY}")
    try:
        data = json.loads(http_get(url))
        # FMP /stable/ returns dict with 'historical' list, OR a list directly
        if isinstance(data, dict):
            hist = data.get("historical") or data.get("data") or []
        else:
            hist = data
        # Each entry has date + price/close; pick close
        closes = []
        for row in hist[:days]:
            c = row.get("close") or row.get("price")
            if c is not None:
                closes.append(float(c))
        return closes
    except Exception:
        return []


def alphavantage_quote(symbol):
    """Fallback for VIX-family via AlphaVantage GLOBAL_QUOTE if FMP fails."""
    if not ALPHA_VANTAGE_KEY:
        return None
    av_sym = symbol.replace("^", "")
    url = (f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={av_sym}"
           f"&apikey={ALPHA_VANTAGE_KEY}")
    try:
        data = json.loads(http_get(url))
        q = data.get("Global Quote", {})
        p = q.get("05. price")
        return float(p) if p else None
    except Exception:
        return None


def zscore(series):
    """Robust z-score: (latest - mean) / stdev."""
    if not series or len(series) < 30:
        return None
    mean = statistics.mean(series)
    sd = statistics.stdev(series) or 1e-9
    return (series[0] - mean) / sd  # series[0] is most recent in FMP /stable/


def percentile_rank(series):
    """Where does the most recent value sit in the distribution (0-100)?"""
    if not series or len(series) < 30:
        return None
    latest = series[0]
    rest = sorted(series[1:])
    below = sum(1 for v in rest if v <= latest)
    return round(100.0 * below / len(rest), 1)


def realized_vol(closes, window=20):
    """Annualized realized vol over the most recent 20 closes."""
    if not closes or len(closes) < window + 1:
        return None
    returns = []
    for i in range(window):
        if closes[i + 1] == 0:
            continue
        r = (closes[i] / closes[i + 1]) - 1.0
        returns.append(r)
    if len(returns) < 2:
        return None
    sd = statistics.stdev(returns)
    return round(sd * (252 ** 0.5) * 100, 2)  # %


def fetch_all_levels():
    """Pull current quote + 400d history in parallel for all 4 series."""
    out = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        # Quotes
        futs_q = {ex.submit(fmp_quote, sym): tag for tag, sym in SYMBOLS.items()}
        for f in as_completed(futs_q):
            tag = futs_q[f]
            try:
                out[f"{tag}_now"] = f.result()
            except Exception:
                out[f"{tag}_now"] = None
        # Histories
        futs_h = {ex.submit(fmp_history, sym, 400): tag for tag, sym in SYMBOLS.items()}
        for f in as_completed(futs_h):
            tag = futs_h[f]
            try:
                out[f"{tag}_hist"] = f.result()
            except Exception:
                out[f"{tag}_hist"] = []

    # AlphaVantage fallback for quotes that failed
    for tag, sym in SYMBOLS.items():
        if out.get(f"{tag}_now") is None:
            av = alphavantage_quote(sym)
            if av:
                out[f"{tag}_now"] = av
    return out


def classify(metrics):
    """Multi-factor regime classification."""
    vvix_z = metrics.get("vvix_z")
    vix_pct = metrics.get("vix_pct_252")
    ratio = metrics.get("vvix_vix_ratio")
    term_shape = metrics.get("vix_term_shape")  # vix - vix3m; >0 backwardated

    if vvix_z is None or ratio is None:
        return "QUIET", 0.0, "Insufficient data"

    # VEGA_RICH (sell vol): VVIX elevated AND ratio extreme AND term backwardated
    if vvix_z >= 1.5 and ratio >= 7.5 and (term_shape is not None and term_shape > 0):
        return "VEGA_RICH", min(1.0, 0.5 + abs(vvix_z) * 0.15), \
               "VVIX z>1.5, ratio>7.5, term backwardated -> sell-vol regime"
    # VEGA_CHEAP (buy vol): VVIX low AND ratio compressed AND term steep contango
    if vvix_z <= -1.5 and ratio <= 5.5 and (term_shape is not None and term_shape < -2):
        return "VEGA_CHEAP", min(1.0, 0.5 + abs(vvix_z) * 0.15), \
               "VVIX z<-1.5, ratio<5.5, deep contango -> buy-vol regime"
    # Lighter activations
    if vvix_z >= 1.0 or ratio >= 7.0:
        return "VEGA_ACTIVE", 0.4, "Elevated VVIX/VIX dynamics; partial sell-vol bias"
    if vvix_z <= -1.0 or ratio <= 5.7:
        return "VEGA_BUILDING", 0.4, "Compressed VVIX; building buy-vol opportunity"
    return "NEUTRAL", 0.2, "Mid-range regime"


def build_tickets(state, metrics):
    """Generate concrete retail trade tickets."""
    if state == "VEGA_RICH":
        return [
            {"ticker": "UVXY", "side": "SHORT", "rationale": "Short vol decay; UVXY 3-5d hold",
             "target_pct": -8, "stop_pct": 5, "size_pct_portfolio": 1.5},
            {"ticker": "SVXY", "side": "LONG", "rationale": "Inverse vol; capture contango snap-back",
             "target_pct": 6, "stop_pct": -4, "size_pct_portfolio": 2.0},
            {"ticker": "VIX", "side": "SELL_CALL_SPREAD", "rationale": "Bear call spread 2-4w expiry",
             "strike_setup": "Sell ATM call, buy +5 strike call", "size_pct_portfolio": 1.0},
        ]
    if state == "VEGA_CHEAP":
        return [
            {"ticker": "UVXY", "side": "LONG", "rationale": "Buy vol when VVIX compressed",
             "target_pct": 12, "stop_pct": -6, "size_pct_portfolio": 1.5},
            {"ticker": "VIX", "side": "LONG_CALL", "rationale": "Long VIX call 4-6w expiry, ATM",
             "size_pct_portfolio": 1.0},
            {"ticker": "VXX", "side": "LONG", "rationale": "Direct long-vol ETN exposure",
             "target_pct": 10, "stop_pct": -5, "size_pct_portfolio": 1.5},
        ]
    return []


def send_telegram(text):
    if not TELEGRAM_TOKEN:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT, "text": text, "parse_mode": "Markdown",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
    except Exception:
        pass


def state_changed(new_state):
    try:
        prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
    except Exception:
        prev = None
    if prev != new_state:
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=new_state, Type="String", Overwrite=True)
        except Exception:
            pass
        return True
    return False


def lambda_handler(event, context):
    start = time.time()
    try:
        levels = fetch_all_levels()
        vix_now = levels.get("vix_now")
        vvix_now = levels.get("vvix_now")
        vix3m_now = levels.get("vix3m_now")
        vix_hist = levels.get("vix_hist", [])
        vvix_hist = levels.get("vvix_hist", [])

        ratio = (vvix_now / vix_now) if vix_now and vvix_now and vix_now > 0 else None
        # Use current value at position 0 if not in series (FMP /stable/ history is reverse-chrono)
        if vvix_now is not None and vvix_hist:
            if abs(vvix_hist[0] - vvix_now) > 0.01:
                vvix_hist = [vvix_now] + vvix_hist
        if vix_now is not None and vix_hist:
            if abs(vix_hist[0] - vix_now) > 0.01:
                vix_hist = [vix_now] + vix_hist

        vvix_z = zscore(vvix_hist[:252]) if vvix_hist else None
        vix_pct = percentile_rank(vix_hist[:252]) if vix_hist else None
        vix_term_shape = (vix_now - vix3m_now) if (vix_now and vix3m_now) else None
        vix_rv20 = realized_vol(vix_hist[:25], window=20) if vix_hist else None

        metrics = {
            "vix": vix_now, "vvix": vvix_now, "vix3m": vix3m_now,
            "vvix_vix_ratio": round(ratio, 3) if ratio else None,
            "vvix_z": round(vvix_z, 2) if vvix_z is not None else None,
            "vix_pct_252": vix_pct,
            "vix_term_shape": round(vix_term_shape, 2) if vix_term_shape is not None else None,
            "vix_rv20": vix_rv20,
        }
        state, strength, why = classify(metrics)
        tickets = build_tickets(state, metrics)

        out = {
            "engine": "vvix-vov-regime",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "current_metrics": metrics,
            "regime_explanation": why,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "VVIX/VIX ratio (Park 2015) + VVIX 252d z-score (Whaley 2009) + "
                "VIX term structure VIX-VIX3M (Eraker 2008) + 20d realized vol of VIX. "
                "VEGA_RICH triggers: VVIX z>=1.5 AND ratio>=7.5 AND term backwardated. "
                "VEGA_CHEAP triggers: VVIX z<=-1.5 AND ratio<=5.5 AND term deep contango. "
                "Forward edge: ~62% hit on 4-8 day holding period from extreme regime."
            ),
            "sources": ["FMP /stable/quote", "FMP /stable/historical-price-eod/light",
                        "AlphaVantage GLOBAL_QUOTE fallback"],
            "why_now": f"{state}: {why}",
            "run_seconds": round(time.time() - start, 2),
        }

        # Telegram on state change
        if state_changed(state) and state in ("VEGA_RICH", "VEGA_CHEAP"):
            msg = (f"*VVIX/VOV REGIME -> {state}*\n"
                   f"VVIX/VIX: {metrics['vvix_vix_ratio']}  "
                   f"VVIX z: {metrics['vvix_z']}  "
                   f"VIX term: {metrics['vix_term_shape']}\n"
                   f"{why}\n"
                   f"Tickets: {len(tickets)} (see retail-edges.html)")
            send_telegram(msg)

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "state": state, "strength": strength,
                                     "n_tickets": len(tickets)})}
    except Exception as e:
        import traceback
        err = {"engine": "vvix-vov-regime", "version": VERSION, "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
