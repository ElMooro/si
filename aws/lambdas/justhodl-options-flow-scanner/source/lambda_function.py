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

OUTPUT: data/options-flow-scanner.json

This is what would have caught:
  - LWLG/AAOI before pumps (call buying surges precede equity moves by 5-15d)
  - INTC government-news rally (short squeeze setup)
  - Crypto-equities run (rising calls + falling shorts)
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/options-flow-scanner.json"  # fixed ownership; ignore obsolete legacy S3_KEY override
POLY_KEY = managed_secret(('POLY_KEY', 'POLYGON_API_KEY', 'POLYGON_KEY'), ("/justhodl/polygon/api-key",))
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
    fmp_key = managed_secret(('fmp_key', 'FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
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


def _legacy_lambda_handler(event=None, context=None):
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

# Complete predecessor is retained above. The source-backed handler below is active.
from pathlib import Path
from datetime import datetime,timezone,timedelta
from urllib.parse import quote_plus
import hashlib,threading
import offexchange_measurements as offexchange
from flow_observations import (CONTRACT,strict,clock,number,symbol,envelope,content,original,
    source_ref,validate_ref,universe,finra_files,dossier,spot,initial_url,cursor_url,provider_rows,contracts,bars_url)


def _option_source_identity():
    directory=Path(__file__).resolve().parent
    paths={name:directory/name for name in ('lambda_function.py','flow_observations.py')}
    paths['offexchange_measurements.py']=Path(offexchange.__file__)
    return {name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()} for name,path in paths.items() for raw in [path.read_bytes()]}


def _option_object(key,bound):
    obj=S3.get_object(Bucket=BUCKET,Key=key)
    try:raw=obj['Body'].read(bound+1)
    finally:obj['Body'].close()
    if len(raw)>bound or obj.get('ContentLength')!=len(raw) or not obj.get('ETag'):raise ValueError('Whole declared object required')
    return raw,obj['ETag']


def _option_immutable(key,raw,kind):
    try:S3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType=kind,CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    back,_=_option_object(key,len(raw))
    if back!=raw:raise ValueError('Whole immutable readback differs')


def _option_archive(raw):
    key='data/options-flow-scanner/history/'+hashlib.sha256(raw).hexdigest()+'.json'
    _option_immutable(key,raw,'application/json')
    return {'key':key,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}


class _OptionSources:
    """One invocation's bounded, exact public provider originals; no private data."""
    def __init__(self):self.raw={};self.lock=threading.Lock();self.bytes=0;self.stop=threading.Event()
    def capture(self,raw,endpoint,kind='json'):
        if len(raw)>8*1024*1024:raise ValueError('Whole provider response exceeds bound')
        ref=source_ref(raw,kind);a=envelope(raw,endpoint,datetime.now(timezone.utc).isoformat(),kind)
        with self.lock:
            if ref['key'] not in self.raw:
                # A launched four-worker window can retain two 8 MiB responses
                # per worker beyond the 80 MiB launch threshold (144 MiB total).
                if self.bytes+len(raw)>160*1024*1024:raise ValueError('Source retention budget exceeded; preserve current publication')
                _option_immutable(ref['key'],raw,'application/json' if kind=='json' else 'text/plain; charset=utf-8')
                self.raw[ref['key']]=raw;self.bytes+=len(raw)
        return a


def _option_fetch(url,sources,remaining,kind='json'):
    from urllib.parse import urlsplit,quote
    import re
    p=urlsplit(url);headers={'User-Agent':'JustHodl-Option-Observations/1.1','Accept-Encoding':'identity'};secret=None;full=url
    if p.scheme!='https' or p.fragment:raise ValueError('Declared provider destination required')
    if p.netloc=='financialmodelingprep.com' and p.path=='/stable/quote':
        secret=managed_secret(('fmp_key','FMP_KEY','FMP_API_KEY'),('/justhodl/fmp/api-key',));headers['apikey']=secret or ''
    elif p.netloc in ('api.polygon.io','api.massive.com') and (p.path=='/v3/reference/options/contracts' or re.fullmatch(r'/v2/aggs/ticker/O:[A-Z0-9.]+\d{6}[CP]\d{8}/range/1/day/\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}',p.path)):
        secret=POLY_KEY;full=url+('&' if p.query else '?')+'apiKey='+quote(secret or '',safe='')
    elif p.netloc!='cdn.finra.org' or not re.fullmatch(r'/equity/regsho/daily/CNMSshvol\d{8}\.txt',p.path) or p.query:raise ValueError('Unreviewed provider destination')
    base={'endpoint':url,'status':'not_attempted_runtime_rate_or_size_limit'}
    if remaining()<30 or sources.bytes>=80*1024*1024 or sources.stop.is_set():return base
    if p.netloc!='cdn.finra.org' and not secret:return {**base,'status':'credential_unavailable'}
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):return None
    try:
        request=urllib.request.Request(full,headers=headers)
        try:response=urllib.request.build_opener(NoRedirect()).open(request,timeout=10)
        except urllib.error.HTTPError as exc:response=exc
        with response:status=response.status;raw=response.read(8*1024*1024+1)
    except Exception as exc:return {**base,'status':'transport_unavailable','error_type':type(exc).__name__}
    if status==429:sources.stop.set()
    if len(raw)>8*1024*1024:return {**base,'status':'response_exceeds_bound','http_status':status,'original_retained':False}
    if secret:
        reflected=secret.encode() in raw
        try:decoded=json.loads(raw)
        except (ValueError,UnicodeError,RecursionError):decoded=None
        pending=[decoded]
        while pending and not reflected:
            value=pending.pop()
            if isinstance(value,str):reflected=secret in value
            elif isinstance(value,dict):pending.extend(value.keys());pending.extend(value.values())
            elif isinstance(value,list):pending.extend(value)
        if reflected:return {**base,'status':'credential_echo_withheld','http_status':status,'original_retained':False}
    a=sources.capture(raw,url,kind);a['http_status']=status
    # Even denied/error originals survive. Their HTTP status prevents measurements.
    return a


def _option_company(member,sources,remaining,checked,days_back):
    ticker=member['ticker'];url='https://financialmodelingprep.com/stable/quote?symbol='+quote_plus(ticker or '')
    a=_option_fetch(url,sources,remaining) if ticker else {'endpoint':url,'status':'invalid_symbol_not_requested'}
    result={'quote':a,'contract_pages':[],'bar_requests':[]};price=spot(a,sources.raw,ticker)
    if a.get('http_status')!=200 or price is None:return result
    url=initial_url(ticker,price,checked);seen=set()
    for i in range(10):
        if url in seen:break
        seen.add(url);a=_option_fetch(url,sources,remaining);result['contract_pages'].append(a)
        rows,p=provider_rows(a,sources.raw)
        if rows is None or not p.get('next_url'):break
        try:url=cursor_url(p['next_url'])
        except ValueError:break
    chain=contracts(ticker,price,result['contract_pages'],sources.raw,checked)
    for index in chain['selected_record_indices']:
        identity=chain['records'][index]['contract_id'];url=bars_url(identity,checked,days_back)
        result['bar_requests'].append(_option_fetch(url,sources,remaining))
    return result


def lambda_handler(event=None,context=None):
    started=time.monotonic();checked=datetime.now(timezone.utc);today=checked.date().isoformat()
    if S3_KEY!='data/options-flow-scanner.json':raise ValueError('Fixed output ownership required')
    if MAX_TICKERS<1 or N_WORKERS<1 or TIMEOUT_BUDGET_S<1 or not 1<=DAYS_BACK<=20:raise ValueError('Reviewed original acquisition bounds required')
    try:previous_raw,etag=_option_object(S3_KEY,64*1024*1024)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
        previous_raw=None;etag=None
    previous=strict(previous_raw) if previous_raw is not None else None
    if previous is not None and not isinstance(previous,dict):raise ValueError('Previous publication malformed')
    if previous and previous.get('measurement_contract')==CONTRACT:
        stamp=clock(previous.get('generated_at'))
        if stamp is None or stamp>=checked:raise ValueError('Previous generation clock invalid')
    sources=_OptionSources();raw,universe_etag=_option_object('data/universe.json',8*1024*1024)
    capture=sources.capture(raw,'data/universe.json');capture['etag']=universe_etag
    membership=universe(capture,min(MAX_TICKERS,300),sources.raw);selected=membership['selected']
    if not selected:raise ValueError('No selected universe; preserve previous publication')
    def remaining():
        own=min(TIMEOUT_BUDGET_S,260)-(time.monotonic()-started)
        return min(own,context.get_remaining_time_in_millis()/1000) if context and hasattr(context,'get_remaining_time_in_millis') else own
    finra=[];parsed_dates=0;looked_back=0;stopped=None
    for back in range(1,DAYS_BACK*2+5):
        if parsed_dates>=DAYS_BACK:stopped='original_file_target_reached';break
        if remaining()<90 or sources.bytes>=48*1024*1024:stopped='runtime_or_source_byte_reserve';break
        looked_back=back;stamp=checked.date()-timedelta(days=back)
        if stamp.weekday()>=5:continue
        url='https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+stamp.strftime('%Y%m%d')+'.txt'
        a=_option_fetch(url,sources,remaining,'txt');a['observation_date']=stamp.isoformat();finra.append(a)
        if a['status']=='received' and a.get('http_status')==200:
            try:rows=offexchange.cnms(content(a,sources.raw),stamp.isoformat())
            except (ValueError,UnicodeError):pass
            else:parsed_dates+=bool(rows)
        if sources.stop.is_set():stopped='rate_limited_no_retry';break
    flow=finra_files(finra,sources.raw,selected,today)
    captures=[{'quote':{'endpoint':'https://financialmodelingprep.com/stable/quote?symbol='+quote_plus(m['ticker'] or ''),'status':'not_attempted_runtime_rate_or_size_limit'},'contract_pages':[],'bar_requests':[]} for m in selected]
    workers=max(1,min(N_WORKERS,4))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0,len(selected),workers):
            if remaining()<40 or sources.bytes>=80*1024*1024 or sources.stop.is_set():break
            jobs=[(i,pool.submit(_option_company,selected[i],sources,remaining,today,DAYS_BACK)) for i in range(offset,min(offset+workers,len(selected)))]
            for i,future in jobs:captures[i]=future.result()
    records=[]
    for i,(member,group) in enumerate(zip(selected,captures)):
        row=dossier(member,group,sources.raw,flow,today,DAYS_BACK);row['request_index']=i;records.append(row)
    if not any(f['status']=='whole_cnms_file_parsed' for f in flow['files']) and not any(r['contract_population']['records'] for r in records):raise ValueError('All research populations unavailable; preserve last publication')
    prior_ref=_option_archive(previous_raw) if previous_raw is not None else None
    packet={'engine':'justhodl-options-flow-scanner','version':'1.1.0','schema_version':2,'measurement_contract':CONTRACT,
        'method':'current_contract_bar_observations_and_separate_finra_flows','status':'RESEARCH_ONLY','source_files':_option_source_identity(),
        'generated_at':datetime.now(timezone.utc).isoformat(),'acquisition_started_at':checked.isoformat(),'checked_as_of':today,'days_back':DAYS_BACK,
        'universe_acquisition':capture,'universe_membership':membership,'request_records':records,'previous_publication':prior_ref,
        'finra_acquisitions':finra,'finra_file_coverage':{k:v for k,v in flow.items() if k!='by_literal_symbol'},
        'finra_request_window':{'calendar_days_examined':looked_back,'maximum_calendar_days':DAYS_BACK*2+4,'target_nonempty_files':DAYS_BACK,'nonempty_parsed_files':parsed_dates,'stop_reason':stopped or 'original_lookback_exhausted'},
        'stats':{'n_universe':len(selected),'n_evaluated':None,'n_no_data':None,'n_tier_a':None,'n_tier_b':None,'n_finra_tickers':None},
        'summary':{'top_25_overall':[],'tier_a':[]},'all_qualifying':[],
        'call':None,'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,'independent_evidence_eligible':False,
        'private_state_read_or_written':False,'signals_logged':0,'notifications_sent':0,'retained_unique_source_bytes':sources.bytes,'duration_s':round(time.monotonic()-started,2),
        'caveats':['Current 14–90-day, ±10% strike contracts are a sampled current population, not a historical fixed universe or complete market flow.',
            'Each source response and every returned contract/bar occurrence is retained; unattempted, denied, incomplete and rate-limited requests are explicit.',
            'Call/put ratios require every selected call and put to have one valid same-date volume observation and complete returned reference pagination. Missing sessions are not zero.',
            'FINRA ShortVolume includes exempt trades and measures daily off-exchange trading flow, not outstanding short interest, covering or borrowing.',
            'No trade direction, opening/closing identity, spread legs, OI, IV, premium notional, investment tier, independent vote or sizing authority is supplied.',
            'Original release vintages, exchange calendars, historical contract membership, currency, adjustments, executable returns and forward performance remain unverified.'],
        'source_documentation':['https://massive.com/docs/rest/options/contracts/all-contracts','https://massive.com/docs/rest/options/aggregates/custom-bars','https://www.finra.org/sites/default/files/2020-12/short-sale-volume-user-guide.pdf']}
    raw=json.dumps(packet,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    if len(raw)>64*1024*1024 or remaining()<15:raise ValueError('Whole publication exceeds reserve; preserve current')
    archive=_option_archive(raw);precondition={'IfMatch':etag} if etag else {'IfNoneMatch':'*'}
    S3.put_object(Bucket=BUCKET,Key=S3_KEY,Body=raw,ContentType='application/json',CacheControl='public, max-age=900',**precondition)
    return {'statusCode':200,'body':json.dumps({'measurement_contract':CONTRACT,'request_occurrences':len(records),'archive':archive})}
