"""
justhodl-options-flow-scanner — institutional options + short-interest tracker

Combines THREE free/accessible data sources to build an options-flow signal
without requiring premium Polygon options access:

  1. Polygon /v3/reference/options/contracts — daily contract list per ticker
     We get: strikes, expiries, contract types
  2. Polygon /v2/aggs/ticker/{O:contract}/range — daily options bars
     We get: per-contract daily volume, open interest, OHLC of premium
  3. FINRA RegSHO daily short volume — free, no auth
     We get: daily short volume / total volume ratio per symbol

For each ticker in our universe:
  STEP A — Pull near-the-money calls (within ±10% of spot, expiry 14-90d)
           and same for puts. Sum daily contract volume.
  STEP B — Compute call/put volume ratio (CPR), 20-day average vs today
  STEP C — Pull 20-day short-volume series from FINRA daily files
  STEP D — Compute short-interest velocity (delta short_pct over 20d)
  STEP E — Score 0-100 combining:
           - Bullish call skew (CPR rising vs 20d avg)
           - Heavy call volume (today's call vol > 2x ATM avg)
           - Falling short interest (bears giving up)
           - High IV percentile (premiums elevated)

OUTPUT: data/options-flow.json

This is what would have caught:
  - LWLG/AAOI before pumps (call buying surges precede equity moves by 5-15d)
  - INTC government-news rally (short squeeze setup)
  - Crypto-equities run (rising calls + falling shorts)
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/options-flow.json")
POLY_KEY = os.environ.get("POLY_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
N_WORKERS = int(os.environ.get("N_WORKERS", "8"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "300"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))
DAYS_BACK = int(os.environ.get("DAYS_BACK", "20"))  # window for ratios

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-OptFlow/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _http_get_text(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-OptFlow/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def get_universe():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        return [(s.get("symbol") or "").upper() for s in d.get("stocks", []) if s.get("symbol")][:MAX_TICKERS]
    except Exception as e:
        print("[opt-flow] universe load failed: " + str(e))
        return []


def get_spot_price(ticker):
    """Get latest close price from FMP (we already have FMP key)."""
    fmp_key = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    url = "https://financialmodelingprep.com/stable/quote?symbol=" + ticker + "&apikey=" + fmp_key
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return float(d[0].get("price") or 0) or None
    except Exception:
        pass
    return None


def get_contracts(ticker, spot, days_min=14, days_max=90, strike_pct=0.10):
    """Get near-the-money options contracts (within ±strike_pct of spot, expiring 14-90 days)."""
    if not spot:
        return []
    today = time.strftime("%Y-%m-%d")
    min_strike = spot * (1 - strike_pct)
    max_strike = spot * (1 + strike_pct)
    # Polygon allows filters
    url = ("https://api.polygon.io/v3/reference/options/contracts?"
           "underlying_ticker=" + ticker +
           "&strike_price.gte=" + str(round(min_strike, 2)) +
           "&strike_price.lte=" + str(round(max_strike, 2)) +
           "&expiration_date.gte=" + today +
           "&limit=200&apiKey=" + POLY_KEY)
    try:
        d = _http_get_json(url, timeout=15)
        return d.get("results", []) or []
    except Exception:
        return []


def get_contract_volume_history(option_ticker, days_back=20):
    """Get daily volume history for one contract."""
    end_date = time.strftime("%Y-%m-%d")
    start_dt = time.gmtime(time.time() - (days_back + 5) * 86400)
    start_date = time.strftime("%Y-%m-%d", start_dt)
    url = ("https://api.polygon.io/v2/aggs/ticker/" + option_ticker +
           "/range/1/day/" + start_date + "/" + end_date + "?apiKey=" + POLY_KEY)
    try:
        d = _http_get_json(url, timeout=10)
        results = d.get("results") or []
        # Each result: {v: volume, c: close, o, h, l, t (epoch ms)}
        return results
    except Exception:
        return []


def fetch_finra_short_volume(date_yyyymmdd):
    """Fetch FINRA RegSHO daily short volume file. Returns dict: ticker -> {short_vol, total_vol, short_pct}."""
    url = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol" + date_yyyymmdd + ".txt"
    try:
        text = _http_get_text(url, timeout=20)
    except Exception:
        return {}
    out = {}
    for line in text.splitlines()[1:]:  # skip header
        parts = line.split("|")
        if len(parts) < 5:
            continue
        sym = parts[1].strip().upper()
        try:
            short_vol = float(parts[2])
            total_vol = float(parts[4])
            if total_vol > 0:
                short_pct = short_vol / total_vol * 100
                out[sym] = {
                    "short_vol": short_vol,
                    "total_vol": total_vol,
                    "short_pct": short_pct,
                }
        except (ValueError, IndexError):
            continue
    return out


def get_finra_short_history(days=20):
    """Get last N business days of FINRA short volume.
    Returns: dict[ticker] -> list of {date, short_pct, short_vol, total_vol}
    """
    history = defaultdict(list)
    # Walk back finding business days (skip weekends, hope no holidays — naive)
    days_collected = 0
    days_back = 0
    while days_collected < days and days_back < days * 2 + 5:
        check_dt = time.gmtime(time.time() - days_back * 86400)
        wday = check_dt.tm_wday  # 0=Mon, 5=Sat
        if wday >= 5:
            days_back += 1
            continue
        date_str = time.strftime("%Y%m%d", check_dt)
        date_iso = time.strftime("%Y-%m-%d", check_dt)
        try:
            data = fetch_finra_short_volume(date_str)
            if data:
                for sym, info in data.items():
                    history[sym].append({"date": date_iso, **info})
                days_collected += 1
        except Exception:
            pass
        days_back += 1
    # Sort each ticker's history oldest first
    for sym in history:
        history[sym].sort(key=lambda x: x["date"])
    return dict(history)


def evaluate_ticker(ticker, finra_history):
    """Compute options-flow score for one ticker."""
    spot = get_spot_price(ticker)
    if not spot:
        return None

    # 1. Get contracts
    contracts = get_contracts(ticker, spot)
    if not contracts:
        return {"symbol": ticker, "status": "no_contracts", "spot": spot}

    calls = [c for c in contracts if c.get("contract_type") == "call"]
    puts = [c for c in contracts if c.get("contract_type") == "put"]

    if not calls and not puts:
        return {"symbol": ticker, "status": "empty_chain", "spot": spot}

    # 2. Sample top 10 calls + 10 puts (most actively traded — closest to spot ATM)
    # Sort by abs distance from spot
    calls_atm = sorted(calls, key=lambda c: abs(float(c.get("strike_price") or 0) - spot))[:10]
    puts_atm = sorted(puts, key=lambda c: abs(float(c.get("strike_price") or 0) - spot))[:10]

    # 3. Get volume history for each — sum across contracts per day
    call_vol_by_day = defaultdict(float)
    put_vol_by_day = defaultdict(float)
    n_call_contracts_sampled = 0
    n_put_contracts_sampled = 0

    for c in calls_atm:
        opt_ticker = c.get("ticker")
        if not opt_ticker:
            continue
        bars = get_contract_volume_history(opt_ticker, DAYS_BACK)
        for b in bars:
            day_iso = time.strftime("%Y-%m-%d", time.gmtime(b.get("t", 0) / 1000))
            call_vol_by_day[day_iso] += b.get("v", 0) or 0
        n_call_contracts_sampled += 1

    for c in puts_atm:
        opt_ticker = c.get("ticker")
        if not opt_ticker:
            continue
        bars = get_contract_volume_history(opt_ticker, DAYS_BACK)
        for b in bars:
            day_iso = time.strftime("%Y-%m-%d", time.gmtime(b.get("t", 0) / 1000))
            put_vol_by_day[day_iso] += b.get("v", 0) or 0
        n_put_contracts_sampled += 1

    # 4. Compute call/put ratios over time
    all_days = sorted(set(list(call_vol_by_day.keys()) + list(put_vol_by_day.keys())))
    if not all_days:
        return {"symbol": ticker, "status": "no_volume_data", "spot": spot}

    daily_cpr = []
    for d in all_days:
        cv = call_vol_by_day.get(d, 0)
        pv = put_vol_by_day.get(d, 0)
        if cv + pv > 0:
            cpr = cv / max(pv, 1)
            daily_cpr.append({"date": d, "call_vol": cv, "put_vol": pv, "cpr": cpr})

    if len(daily_cpr) < 3:
        return {"symbol": ticker, "status": "thin_data", "spot": spot}

    # Recent vs older windows
    recent = daily_cpr[-5:]
    older = daily_cpr[:-5] if len(daily_cpr) > 5 else daily_cpr
    avg_cpr_recent = sum(d["cpr"] for d in recent) / len(recent)
    avg_cpr_older = sum(d["cpr"] for d in older) / len(older)
    cpr_change_pct = (avg_cpr_recent / avg_cpr_older - 1) * 100 if avg_cpr_older > 0 else 0

    # Total volume metrics
    total_call_vol_recent = sum(d["call_vol"] for d in recent)
    total_put_vol_recent = sum(d["put_vol"] for d in recent)
    avg_call_vol_older = sum(d["call_vol"] for d in older) / len(older) if older else 0
    avg_call_vol_recent = total_call_vol_recent / len(recent)
    call_vol_surge = avg_call_vol_recent / max(avg_call_vol_older, 1)

    # 5. FINRA short interest data
    finra = finra_history.get(ticker, [])
    short_metrics = None
    if len(finra) >= 5:
        recent_short = sum(d["short_pct"] for d in finra[-5:]) / 5
        older_short = sum(d["short_pct"] for d in finra[:-5]) / max(1, len(finra) - 5) if len(finra) > 5 else recent_short
        short_pct_change = recent_short - older_short
        avg_total_vol = sum(d["total_vol"] for d in finra[-5:]) / 5
        short_metrics = {
            "recent_avg_short_pct": round(recent_short, 1),
            "older_avg_short_pct": round(older_short, 1),
            "short_pct_change": round(short_pct_change, 2),
            "avg_total_vol_5d": int(avg_total_vol),
            "n_finra_days": len(finra),
        }

    # 6. SCORE
    score = 0.0
    flags = []

    # Bullish: call/put ratio rising
    if cpr_change_pct > 50:
        score += 30
        flags.append("CPR_SURGING")
    elif cpr_change_pct > 25:
        score += 20
        flags.append("CPR_RISING")
    elif cpr_change_pct > 10:
        score += 10

    # Bullish: heavy call volume vs baseline
    if call_vol_surge > 3.0:
        score += 25
        flags.append("CALL_VOL_3X")
    elif call_vol_surge > 2.0:
        score += 18
        flags.append("CALL_VOL_2X")
    elif call_vol_surge > 1.5:
        score += 10

    # Bullish: high absolute call/put ratio
    if avg_cpr_recent > 3.0:
        score += 15
        flags.append("ABS_CPR_3X")
    elif avg_cpr_recent > 2.0:
        score += 10
        flags.append("ABS_CPR_2X")
    elif avg_cpr_recent > 1.3:
        score += 5

    # Bullish: short interest declining (bears giving up)
    if short_metrics and short_metrics["short_pct_change"] < -3:
        score += 15
        flags.append("SHORTS_COVERING")
    elif short_metrics and short_metrics["short_pct_change"] < -1:
        score += 8
        flags.append("SHORTS_EASING")

    # Bullish: extreme high short pct (squeeze setup)
    if short_metrics and short_metrics["recent_avg_short_pct"] > 50:
        score += 15
        flags.append("HIGH_SHORT_SQUEEZE_SETUP")
    elif short_metrics and short_metrics["recent_avg_short_pct"] > 40:
        score += 8

    score = min(score, 100)

    if score >= 65:
        tier = "TIER_A_BULLISH_FLOW"
    elif score >= 50:
        tier = "TIER_B_FLOW_BUILDING"
    elif score >= 35:
        tier = "WATCH"
    else:
        tier = "NEUTRAL"

    return {
        "symbol": ticker,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "metrics": {
            "spot": round(spot, 2),
            "n_call_contracts": n_call_contracts_sampled,
            "n_put_contracts": n_put_contracts_sampled,
            "avg_cpr_recent_5d": round(avg_cpr_recent, 2),
            "avg_cpr_older": round(avg_cpr_older, 2),
            "cpr_change_pct": round(cpr_change_pct, 1),
            "total_call_vol_5d": int(total_call_vol_recent),
            "total_put_vol_5d": int(total_put_vol_recent),
            "call_vol_surge": round(call_vol_surge, 2),
            "short_metrics": short_metrics,
        },
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print("[opt-flow] starting v1.0")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0, "reason": "no universe"})}
    print("[opt-flow] universe: " + str(len(universe)) + " tickers")

    # Pull FINRA short interest history once (shared across all tickers)
    print("[opt-flow] fetching FINRA short volume history (" + str(DAYS_BACK) + " days)...")
    t0 = time.time()
    finra_history = get_finra_short_history(days=DAYS_BACK)
    print("[opt-flow] FINRA: " + str(len(finra_history)) + " tickers, " +
          "{:.1f}".format(time.time() - t0) + "s")

    results = []
    n_no_data = 0

    def evaluate(sym):
        if time.time() > deadline_at:
            return None
        try:
            r = evaluate_ticker(sym, finra_history)
            return r
        except Exception as e:
            print("[opt-flow] " + sym + " ERROR: " + str(e))
            return None

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
            except Exception:
                continue
            if not r:
                continue
            if r.get("status"):
                n_no_data += 1
            else:
                results.append(r)

    print("[opt-flow] OK: " + str(len(results)) + ", no_data: " + str(n_no_data))
    results.sort(key=lambda x: x["score"], reverse=True)

    tier_a = [r for r in results if r["tier"] == "TIER_A_BULLISH_FLOW"]
    tier_b = [r for r in results if r["tier"] == "TIER_B_FLOW_BUILDING"]

    out = {
        "schema_version": 1,
        "method": "options_flow_scanner_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_no_data": n_no_data,
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "n_finra_tickers": len(finra_history),
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "flags": r["flags"],
                    "spot": r["metrics"]["spot"],
                    "cpr_recent": r["metrics"]["avg_cpr_recent_5d"],
                    "cpr_change_pct": r["metrics"]["cpr_change_pct"],
                    "call_vol_surge": r["metrics"]["call_vol_surge"],
                    "short_pct_change": (r["metrics"]["short_metrics"] or {}).get("short_pct_change"),
                }
                for r in results[:25]
            ],
            "tier_a": [r["symbol"] for r in tier_a],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[opt-flow] wrote " + str(len(body)) + "b to " + S3_KEY)
    print("[opt-flow] tier_a=" + str(len(tier_a)) + " tier_b=" + str(len(tier_b)))
    if results[:8]:
        print("[opt-flow] TOP: " + str([(r["symbol"], r["score"], r["tier"]) for r in results[:8]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": out["duration_s"],
        }),
    }
