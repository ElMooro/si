"""
justhodl-microcap-float-squeeze — parabolic squeeze setup detector

Microcap stocks can go parabolic when 3 conditions align:
  1. Float exhaustion: daily volume / float gets very high (> 50%)
  2. High short interest: days-to-cover > 8
  3. Short interest velocity: rising or stable (bears didn't bail yet)
  4. Borrow rate spike: hard-to-borrow rate jumping

WHAT THIS TRACKS (per microcap stock):
  • Float share count (FMP)
  • 30-day average volume
  • Volume / float ratio (how much of float trades each day)
  • Short interest from FINRA (days short, %)
  • Days to cover = short_interest / avg_daily_volume
  • Short volume velocity from FINRA (rising/falling shorts)
  • Recent price level vs 60d high (squeeze setup vs already-squeezed)
  • Real revenue floor (not pure pump-and-dump)

FILTERING:
  • Only stocks with market cap $50M - $2B (microcap to small)
  • Daily $ volume > $500K (liquid enough to actually buy)
  • Has actual revenue > $20M (filters out pure pump shells)
  • Price > $1 (filters out penny stocks)

SCORE 0-100 combines:
  • Float exhaustion intensity
  • Days-to-cover magnitude
  • Short interest velocity (rising = better)
  • Recent base structure (not already pumped)
  • Liquidity adequacy
  • Optional: revenue growth bonus (ties into rev-accel)

OUTPUT: data/microcap-float-squeeze.json
"""
import io, json, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/microcap-float-squeeze.json")
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
N_WORKERS = int(os.environ.get("N_WORKERS", "10"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def get_universe():
    """Filter universe to nano/micro/small/mid caps for squeeze detection."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        all_stocks = d.get("stocks", [])
        target_buckets = {"nano", "micro", "small", "mid"}
        filtered = [s for s in all_stocks if s.get("cap_bucket") in target_buckets]
        return filtered[:MAX_TICKERS]
    except Exception as e:
        print("[float-sq] universe load failed: " + str(e))
        return []


def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-FloatSq/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def fetch_quote(symbol):
    try:
        text = fetch_url("https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY)
        d = json.loads(text)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_profile(symbol):
    """Get share float, shares outstanding."""
    try:
        text = fetch_url("https://financialmodelingprep.com/stable/profile?symbol=" + symbol + "&apikey=" + FMP_KEY)
        d = json.loads(text)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_history(symbol, days=90):
    """Daily price + volume for last 90 days."""
    try:
        text = fetch_url("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY, timeout=15)
        d = json.loads(text)
        if not isinstance(d, list):
            return None
        out = []
        for x in d[:days]:
            if x.get("close") and x.get("date"):
                out.append({
                    "date": x["date"],
                    "close": float(x["close"]),
                    "volume": float(x.get("volume") or 0),
                })
        out.sort(key=lambda r: r["date"])
        return out
    except Exception:
        return None


def _f(v):
    """Float coercion; None for missing/unparseable (fail-soft)."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def load_finra_short_volume_map(s3_client, bucket):
    """Daily Reg SHO short-sale *volume* from the fleet pipeline.

    justhodl-finra-short publishes data/finra-short.json; this replaces
    the direct cdn.finra.org scrape. Returns {} on any failure.
    """
    try:
        doc = json.loads(s3_client.get_object(
            Bucket=bucket, Key="data/finra-short.json")["Body"].read())
    except Exception:
        return {}
    return doc if isinstance(doc, dict) else {}


def load_si_positions(s3_client, bucket):
    """Settlement *positions* per ticker (Bloomberg parity 3/10)."""
    import short_interest_tickers
    try:
        return short_interest_tickers.load_tickers(
            s3_client, bucket).get("by_ticker", {}) or {}
    except Exception:
        return {}


def _vol_entry(short_vol_map, sym):
    """Per-ticker entry from the finra-short.json volume feed.

    Merges the `tickers` dict entry (takes precedence) with the richer
    `squeeze_candidates` entry, which fills in keys the tickers entry
    lacks (e.g. momentum_pct). Returns {} when the ticker is absent.
    """
    if not isinstance(short_vol_map, dict):
        return {}
    entry = {}
    tickers = short_vol_map.get("tickers") or {}
    if isinstance(tickers, dict):
        e = tickers.get(sym)
        if isinstance(e, dict):
            entry = dict(e)
    for c in (short_vol_map.get("squeeze_candidates") or []):
        if not isinstance(c, dict):
            continue
        if (c.get("symbol") or c.get("ticker") or "").upper() == sym:
            for k, v in c.items():
                if k not in entry and v not in (None, ""):
                    entry[k] = v
            break
    return entry


def evaluate_ticker(stock, short_vol_map, si_positions):
    sym = (stock.get("symbol") or "").upper()
    sector = stock.get("sector", "?")
    industry = stock.get("industry", "?")

    # Use universe-supplied data first to avoid extra API calls
    market_cap = stock.get("market_cap") or 0
    price = stock.get("price") or 0
    
    # If universe data is missing, fall back to live quote
    if not (market_cap and price):
        quote = fetch_quote(sym)
        if not quote:
            return None
        market_cap = quote.get("marketCap") or 0
        price = quote.get("price") or 0

    # Filter: $50M - $5B mcap (loosened upper to capture small inflection plays)
    if not (50_000_000 <= market_cap < 5_000_000_000):
        return None
    if price < 1.0:
        return None

    # Derive shares outstanding from market_cap / price (always works,
    # avoids need for now-broken /share-float endpoint)
    if price <= 0:
        return None
    shares_out = market_cap / price
    
    # Float estimate: 80% of shares outstanding (insiders/restricted typically 10-25%)
    float_shares = shares_out * 0.80
    if float_shares <= 0:
        return None

    # History
    history = fetch_history(sym, days=90)
    if not history or len(history) < 30:
        return None

    closes = [h["close"] for h in history]
    volumes = [h["volume"] for h in history]
    n = len(closes)
    today = closes[-1]

    avg_dollar_vol_30 = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dollar_vol_30 < 200_000:
        return None

    avg_vol_30 = sum(volumes[-30:]) / min(30, n)
    avg_vol_60 = sum(volumes[-60:]) / min(60, n) if n >= 60 else avg_vol_30

    # Float exhaustion: daily volume / float
    float_turnover_30d = avg_vol_30 / float_shares * 100  # in %

    # FINRA short data: pipeline artifacts (no direct HTTP).
    # DTC and position size come from real settlement positions
    # (data/short-interest-tickers.json); volume velocity comes from the
    # fleet's Reg SHO volume feed (data/finra-short.json). This fixes the
    # flow-vs-positions conflation: DTC is positions, velocity is volume.
    si = si_positions.get(sym, {}) if isinstance(si_positions, dict) else {}
    days_to_cover = _f(si.get("dtc_effective")) or _f(si.get("days_to_cover"))
    short_interest_shares = _f(si.get("short_interest"))
    si_change_pct = _f(si.get("change_pct"))
    si_settlement = si.get("settlement_date")

    vol = _vol_entry(short_vol_map, sym)
    short_pct_recent = _f(vol.get("si_pct")) or _f(vol.get("short_pct"))
    short_pct_change = _f(vol.get("momentum_pct"))
    short_velocity = short_pct_change

    # Returns / range context
    ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0
    ret_30d = (today / closes[-31] - 1) * 100 if n >= 31 else 0
    ret_60d = (today / closes[-61] - 1) * 100 if n >= 61 else 0
    range_high_60 = max(closes[-60:]) if n >= 60 else max(closes)
    range_low_60 = min(closes[-60:]) if n >= 60 else min(closes)
    range_pos = (today - range_low_60) / (range_high_60 - range_low_60) * 100 if range_high_60 > range_low_60 else 50

    # Liquidity vs float pre-conditions
    # An "easy squeeze" candidate is one where vol/float is high enough that
    # if the stock breaks out, normal-sized institutional buying alone can't
    # be filled without pushing price up materially.

    # ─── SCORING ───
    score = 0.0
    flags = []

    # 1. Float exhaustion
    if float_turnover_30d > 5:
        score += 25
        flags.append("FLOAT_HEAVY_TURNOVER")
    elif float_turnover_30d > 2:
        score += 15
        flags.append("FLOAT_MOD_TURNOVER")
    elif float_turnover_30d > 1:
        score += 8

    # 2. Short interest absolute level
    if short_pct_recent is not None:
        if short_pct_recent > 50:
            score += 25
            flags.append("SHORT_PCT_50%+")
        elif short_pct_recent > 40:
            score += 18
            flags.append("SHORT_PCT_40%+")
        elif short_pct_recent > 30:
            score += 10
            flags.append("SHORT_PCT_30%+")

    # 3. Days-to-cover
    if days_to_cover is not None:
        if days_to_cover > 10:
            score += 15
            flags.append("DAYS_TO_COVER_10+")
        elif days_to_cover > 5:
            score += 10
            flags.append("DAYS_TO_COVER_5+")

    # 4. Short velocity — rising shorts = bears piling in (squeeze fuel)
    if short_velocity is not None:
        if short_velocity > 5:
            score += 12
            flags.append("SHORT_RISING_5PP+")
        elif short_velocity > 2:
            score += 6
            flags.append("SHORT_RISING")
        elif short_velocity < -5:
            score -= 5
            flags.append("SHORTS_COVERED")  # already squeezed

    # 5. Setup vs already-pumped
    if range_pos < 60 and abs(ret_5d) < 10:
        score += 10
        flags.append("BASE_FORMING_SETUP")
    elif range_pos > 90 and ret_5d > 15:
        score -= 15
        flags.append("ALREADY_RUNNING")  # don't chase

    # 6. Volume not yet exploded (still under the radar)
    vol_surge = avg_vol_30 / avg_vol_60 if avg_vol_60 > 0 else 1.0
    if vol_surge > 2.5 and range_pos < 80:
        score += 8
        flags.append("VOLUME_BUILDING_QUIETLY")

    # 7. Liquidity sanity
    if avg_dollar_vol_30 > 5_000_000:
        score += 5
    elif avg_dollar_vol_30 > 2_000_000:
        score += 3

    score = max(0, min(score, 100))

    # Tier
    if score >= 70:
        tier = "TIER_S_PARABOLIC_SETUP"
    elif score >= 55:
        tier = "TIER_A_SQUEEZE_BREWING"
    elif score >= 40:
        tier = "TIER_B_WATCH"
    else:
        tier = "QUIET"

    return {
        "symbol": sym,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "metrics": {
            "price": round(price, 2),
            "market_cap": int(market_cap),
            "float_shares": int(float_shares),
            "shares_outstanding": int(shares_out),
            "avg_vol_30d": int(avg_vol_30),
            "avg_dollar_vol_30d": int(avg_dollar_vol_30),
            "float_turnover_30d_pct": round(float_turnover_30d, 2),
            "short_pct_recent": round(short_pct_recent, 1) if short_pct_recent is not None else None,
            "short_pct_change": round(short_pct_change, 2) if short_pct_change is not None else None,
            "days_to_cover_proxy": round(days_to_cover, 1) if days_to_cover is not None else None,
            "si_shares": int(short_interest_shares) if short_interest_shares is not None else None,
            "si_change_pct": round(si_change_pct, 2) if si_change_pct is not None else None,
            "si_settlement_date": si_settlement,
            "ret_5d": round(ret_5d, 1),
            "ret_30d": round(ret_30d, 1),
            "ret_60d": round(ret_60d, 1),
            "range_pos_60d": round(range_pos, 1),
            "vol_surge_30v60": round(vol_surge, 2),
            "sector": sector,
            "industry": industry,
        },
    }


def _legacy_lambda_handler(event=None, context=None):
    started = time.time()
    deadline = started + TIMEOUT_BUDGET_S
    print("[float-sq] starting v1.0")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0})}
    print("[float-sq] universe: " + str(len(universe)) + " stocks")

    print("[float-sq] loading FINRA volume feed + SI positions from pipeline...")
    short_vol_map = load_finra_short_volume_map(S3, BUCKET)
    si_positions = load_si_positions(S3, BUCKET)
    print("[float-sq] FINRA volume tickers: " + str(len((short_vol_map.get("tickers") or {}))) +
          ", SI position tickers: " + str(len(si_positions)))

    results = []
    n_no_data = 0
    n_filtered_out = 0

    def evaluate(stock):
        if time.time() > deadline:
            return None
        try:
            return evaluate_ticker(stock, short_vol_map, si_positions)
        except Exception as e:
            print("[float-sq] " + (stock.get("symbol") or "?") + " err: " + str(e))
            return None

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
                else:
                    n_filtered_out += 1
            except Exception:
                n_no_data += 1

    print("[float-sq] OK: " + str(len(results)) + ", filtered_out: " + str(n_filtered_out))
    results.sort(key=lambda x: -x["score"])

    by_tier = {
        "tier_s": [r for r in results if r["tier"] == "TIER_S_PARABOLIC_SETUP"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_SQUEEZE_BREWING"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_WATCH"],
    }

    out = {
        "schema_version": 1,
        "method": "microcap_float_squeeze_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_filtered_out": n_filtered_out,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "n_short_vol_tickers": len(short_vol_map.get("tickers") or {}),
            "n_si_tickers": len(si_positions),
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "flags": r["flags"],
                    "price": r["metrics"]["price"],
                    "market_cap": r["metrics"]["market_cap"],
                    "float_turnover": r["metrics"]["float_turnover_30d_pct"],
                    "short_pct": r["metrics"]["short_pct_recent"],
                    "days_to_cover": r["metrics"]["days_to_cover_proxy"],
                    "short_change": r["metrics"]["short_pct_change"],
                    "range_pos": r["metrics"]["range_pos_60d"],
                    "vol_surge": r["metrics"]["vol_surge_30v60"],
                    "ret_5d": r["metrics"]["ret_5d"],
                }
                for r in results[:25]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[float-sq] wrote " + str(len(body)) + "b")
    print("[float-sq] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "duration_s": out["duration_s"],
        }),
    }

# Complete predecessor is retained above. The source-backed handler below is active.
from pathlib import Path
from datetime import datetime,timezone,timedelta
from urllib.parse import quote_plus
import hashlib,threading
import offexchange_measurements as offexchange
from float_observations import (CONTRACT,strict,clock,number,symbol,envelope,content,original,
    source_ref,validate_ref,universe,finra_files,price_evidence,dossier)


def _float_source_identity():
    directory=Path(__file__).resolve().parent
    paths={name:directory/name for name in ('lambda_function.py','float_observations.py')}
    paths['offexchange_measurements.py']=Path(offexchange.__file__)
    return {name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()} for name,path in paths.items() for raw in [path.read_bytes()]}


def _float_object(key,bound):
    obj=S3.get_object(Bucket=BUCKET,Key=key)
    try:raw=obj['Body'].read(bound+1)
    finally:obj['Body'].close()
    if len(raw)>bound or obj.get('ContentLength')!=len(raw) or not obj.get('ETag'):raise ValueError('Whole declared object required')
    return raw,obj['ETag']


def _float_immutable(key,raw,kind):
    try:S3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType=kind,CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    back,_=_float_object(key,len(raw))
    if back!=raw:raise ValueError('Whole immutable readback differs')


def _float_archive(raw):
    key='data/microcap-float-squeeze/history/'+hashlib.sha256(raw).hexdigest()+'.json'
    _float_immutable(key,raw,'application/json')
    return {'key':key,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}


class _FloatSources:
    """One invocation's bounded, exact public provider originals; no private data."""
    def __init__(self):self.raw={};self.lock=threading.Lock();self.bytes=0
    def capture(self,raw,endpoint,kind='json'):
        if len(raw)>8*1024*1024:raise ValueError('Whole provider response exceeds bound')
        ref=source_ref(raw,kind);a=envelope(raw,endpoint,datetime.now(timezone.utc).isoformat(),kind)
        with self.lock:
            if ref['key'] not in self.raw:
                # A launched four-worker window can retain two 8 MiB responses
                # per worker beyond the 80 MiB launch threshold (144 MiB total).
                if self.bytes+len(raw)>160*1024*1024:raise ValueError('Source retention budget exceeded; preserve current publication')
                _float_immutable(ref['key'],raw,'application/json' if kind=='json' else 'text/plain; charset=utf-8')
                self.raw[ref['key']]=raw;self.bytes+=len(raw)
        return a


def _float_fetch(ticker,endpoint,sources):
    if endpoint not in ('quote','historical-price-eod/full'):raise ValueError('Undeclared FMP endpoint')
    if not ticker or symbol(ticker)!=ticker:return {'endpoint':endpoint,'status':'invalid_symbol_not_requested'}
    if not FMP_KEY:return {'endpoint':endpoint,'status':'credential_unavailable'}
    url='https://financialmodelingprep.com/stable/'+endpoint+'?symbol='+quote_plus(ticker)
    request=urllib.request.Request(url,headers={'apikey':FMP_KEY,'User-Agent':'JustHodl-Flow-Observations/1.1'})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):return None
    try:
        with urllib.request.build_opener(NoRedirect()).open(request,timeout=10) as response:raw=response.read(8*1024*1024+1)
    except Exception as exc:return {'endpoint':endpoint,'status':'rate_limited' if getattr(exc,'code',None)==429 else 'unavailable','http_status':getattr(exc,'code',None),'error_type':type(exc).__name__}
    if len(raw)>8*1024*1024:return {'endpoint':endpoint,'status':'response_exceeds_bound','original_retained':False}
    if FMP_KEY.encode() in raw:return {'endpoint':endpoint,'status':'credential_echo_withheld','original_retained':False}
    # Retention failures propagate: a received response cannot silently disappear.
    return sources.capture(raw,endpoint)


def _float_company(member,sources):
    ticker=member['ticker'];row=member['raw'];captures=[]
    cap=row.get('market_cap') or 0;price=row.get('price') or 0
    if not(cap and price):
        capture=_float_fetch(ticker,'quote',sources);captures.append(capture);quotes=original(capture,sources.raw)
        first=quotes[0] if isinstance(quotes,list) and quotes and isinstance(quotes[0],dict) else {}
        cap=first.get('marketCap');price=first.get('price')
    else:captures.append({'endpoint':'quote','status':'not_requested_original_universe_gate'})
    # Preserve only the predecessor's request budget. These nominal thresholds do
    # not prove USD currency, tradable liquidity, reported float or issuer identity.
    if number(cap) is None or number(price) is None or not(50_000_000<=cap<5_000_000_000 and price>=1):
        captures.append({'endpoint':'historical-price-eod/full','status':'not_requested_original_nominal_gate'})
    else:captures.append(_float_fetch(ticker,'historical-price-eod/full',sources))
    return captures


def _float_finra_day(stamp,sources):
    url='https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+stamp.strftime('%Y%m%d')+'.txt'
    request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-Flow-Observations/1.1'})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):return None
    try:
        with urllib.request.build_opener(NoRedirect()).open(request,timeout=10) as response:raw=response.read(8*1024*1024+1)
    except Exception as exc:a={'endpoint':url,'status':'rate_limited' if getattr(exc,'code',None)==429 else 'unavailable','http_status':getattr(exc,'code',None),'error_type':type(exc).__name__}
    else:a=sources.capture(raw,url,'txt') if len(raw)<=8*1024*1024 else {'endpoint':url,'status':'response_exceeds_bound','original_retained':False}
    a['observation_date']=stamp.isoformat();return a


def lambda_handler(event=None,context=None):
    started=time.monotonic();checked=datetime.now(timezone.utc);today=checked.date().isoformat()
    if S3_KEY!='data/microcap-float-squeeze.json':raise ValueError('Declared research output required')
    if MAX_TICKERS<1 or N_WORKERS<1 or TIMEOUT_BUDGET_S<1:raise ValueError('Original acquisition bounds required')
    try:previous_raw,etag=_float_object(S3_KEY,64*1024*1024)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
        previous_raw=None;etag=None
    previous=strict(previous_raw) if previous_raw is not None else None
    if previous is not None and not isinstance(previous,dict):raise ValueError('Previous publication malformed')
    if previous and previous.get('measurement_contract')==CONTRACT:
        stamp=clock(previous.get('generated_at'))
        if stamp is None or stamp>=checked:raise ValueError('Previous generation clock invalid')
    sources=_FloatSources();raw,universe_etag=_float_object('data/universe.json',8*1024*1024)
    capture=sources.capture(raw,'data/universe.json');capture['etag']=universe_etag
    membership=universe(capture,min(MAX_TICKERS,600),sources.raw);selected=membership['selected']
    if not selected:raise ValueError('No selected universe; preserve previous publication')
    def remaining():
        own=min(TIMEOUT_BUDGET_S,260)-(time.monotonic()-started)
        return min(own,context.get_remaining_time_in_millis()/1000) if context and hasattr(context,'get_remaining_time_in_millis') else own
    finra=[];parsed_dates=0;looked_back=0;stopped=None
    for back in range(1,45):
        if parsed_dates>=20:stopped='original_twenty_file_target_reached';break
        if remaining()<90 or sources.bytes>=48*1024*1024:stopped='runtime_or_source_byte_reserve';break
        looked_back=back;stamp=checked.date()-timedelta(days=back)
        if stamp.weekday()>=5:continue
        a=_float_finra_day(stamp,sources);finra.append(a)
        if a['status']=='received':
            try:rows=offexchange.cnms(content(a,sources.raw),stamp.isoformat())
            except (ValueError,UnicodeError):pass
            else:parsed_dates+=bool(rows)
        if a['status']=='rate_limited':stopped='rate_limited_no_retry';break
    flow=finra_files(finra,sources.raw,selected,today)
    captures=[[{'endpoint':'historical-price-eod/full','status':'not_attempted_runtime_rate_or_size_limit'}] for _ in selected]
    workers=max(1,min(N_WORKERS,4))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0,len(selected),workers):
            if remaining()<60 or sources.bytes>=80*1024*1024:break
            jobs=[(i,pool.submit(_float_company,selected[i],sources)) for i in range(offset,min(offset+workers,len(selected)))]
            for i,future in jobs:captures[i]=future.result()
            if any(a.get('status')=='rate_limited' for i,_ in jobs for a in captures[i]):break
    if not any(f['status']=='whole_cnms_file_parsed' for f in flow['files']) and not any(a['status']=='received' and isinstance(original(a,sources.raw),list) for group in captures for a in group):
        raise ValueError('All research sources unavailable; preserve previous publication')
    records=[]
    for i,(member,group) in enumerate(zip(selected,captures)):
        row=dossier(member,group,sources.raw,flow,today);row['request_index']=i;records.append(row)
    prior_ref=_float_archive(previous_raw) if previous_raw is not None else None
    packet={'engine':'justhodl-microcap-float-squeeze','version':'1.1.0','schema_version':2,'measurement_contract':CONTRACT,
        'method':'reported_finra_flow_and_market_source_observations','status':'RESEARCH_ONLY','source_files':_float_source_identity(),
        'generated_at':datetime.now(timezone.utc).isoformat(),'acquisition_started_at':checked.isoformat(),'checked_as_of':today,
        'universe_acquisition':capture,'universe_membership':membership,'request_records':records,'previous_publication':prior_ref,
        'finra_acquisitions':finra,'finra_file_coverage':{k:v for k,v in flow.items() if k!='by_literal_symbol'},
        'finra_request_window':{'calendar_days_examined':looked_back,'maximum_calendar_days':44,'target_nonempty_files':20,'nonempty_parsed_files':parsed_dates,'stop_reason':stopped or 'original_lookback_exhausted'},
        'n_finra_observations':sum(len(r['finra_observations']) for r in records),'n_price_records':sum(r['price_evidence'].get('records') or 0 for r in records),
        'stats':{'n_universe':len(selected),'n_evaluated':None,'n_filtered_out':None,'n_tier_s':None,'n_tier_a':None,'n_tier_b':None,'n_finra_tickers':None},
        'summary':{'top_25_overall':[],'tier_s':[]},'all_qualifying':[],
        'call':None,'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,'independent_evidence_eligible':False,
        'private_state_read_or_written':False,'signals_logged':0,'notifications_sent':0,
        'source_documentation':['https://www.finra.org/sites/default/files/2020-12/short-sale-volume-user-guide.pdf','https://syndication.finra.org/content/short-interest-what-it-what-it-not'],
        'caveats':['Whole original source bytes are retained under immutable public content identities. No source truncation or rank-list projection is presented as complete evidence.',
            'FINRA ShortVolume includes exempt trades. It is daily reported trading flow, not outstanding short interest, days-to-cover, covering or a squeeze probability.',
            'CNMS excludes exchange executions. Literal symbol joins do not prove security continuity, ownership or an independent investment vote.',
            'Missing files, invalid files, absent symbol rows and zero denominators remain explicit. Neither a weekday request nor a source row proves an exchange session calendar.',
            'Reported market cap and price do not establish float. No float, borrow rate, revenue floor or dollar-volume qualification is invented.',
            'Volume means use explicitly dated source rows in provider volume units. Currency, corporate actions, historical membership and executable returns remain unverified.'],
        'retained_unique_source_bytes':sources.bytes,'duration_s':round(time.monotonic()-started,2)}
    raw=json.dumps(packet,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
    if len(raw)>64*1024*1024 or remaining()<15:raise ValueError('Whole publication exceeds reserve; preserve previous current packet')
    archive=_float_archive(raw);precondition={'IfMatch':etag} if etag else {'IfNoneMatch':'*'}
    S3.put_object(Bucket=BUCKET,Key=S3_KEY,Body=raw,ContentType='application/json',CacheControl='public, max-age=900',**precondition)
    return {'statusCode':200,'body':json.dumps({'measurement_contract':CONTRACT,'request_occurrences':len(records),'archive':archive})}
