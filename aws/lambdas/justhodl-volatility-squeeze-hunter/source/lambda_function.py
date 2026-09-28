"""
justhodl-volatility-squeeze-hunter — coiled-spring detector

Detects stocks where multiple "energy compression" signals are firing
simultaneously — these are the highest-conviction breakout setups in
technical analysis. Mark Minervini, William O'Neil, John Carter all
made fortunes on this single concept: long bases break out with
explosive moves.

WHAT THIS DETECTS — 6 distinct compression signals:

  1. BOLLINGER BAND SQUEEZE — BB width in lowest 10th percentile of 252-day
     history. The classic "BB squeeze" — volatility crushed = imminent
     expansion. Fires when bb_width_percentile <= 10.

  2. TTM SQUEEZE — Bollinger Bands INSIDE Keltner Channels.
     This is John Carter's signature setup. Energy compressed because
     volatility has dropped below the trend channel. When BBs exit Keltner
     to the upside, that's the breakout signal.

  3. NR7 CLUSTER — count of "narrowest range in 7 days" in last 30 days.
     3+ NR7 days in 30 = serious compression. NR7 means today's high-low
     range is the smallest of the last 7 days.

  4. VCP (Volatility Contraction Pattern) — Minervini's signature.
     Pullbacks getting smaller (each correction shallower than the last)
     while volume contracts. We approximate by detecting:
       - 3+ corrections in last 6 months, each progressively smaller
       - Volume on each correction LOWER than prior correction

  5. ATR COMPRESSION — 20-day ATR in lowest 20th percentile of 252-day
     history. ATR is true range averaged — a clean volatility measure.

  6. INSIDE-DAY DENSITY — % of days in last 30 that were "inside days"
     (range fully contained within prior day). Highly compressed bases
     often have 30%+ inside days.

SCORE 0-100 combines all 6 + adds:
  - LONG-BASE bonus: stock has been in tight range for 60+ days
  - VOLUME-DRY bonus: volume in lowest quartile during compression
  - PRICE-STABLE bonus: range_pos between 30-70% (not at extremes)

ALERT TRIGGERS:
  - TIER_S: 5+ of 6 signals firing simultaneously (rare, exceptional)
  - TIER_A: 4 of 6 firing (strong setup)
  - TIER_B: 3 of 6 firing (worth watching)

OUTPUT: data/volatility-squeeze.json
"""
import io, json, os, time, urllib.request, statistics, math
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/volatility-squeeze.json")
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal fallback
FMP_KEY = managed_secret(("FMP_KEY", "FMP_API_KEY"), ("/justhodl/fmp/api-key",))
N_WORKERS = int(os.environ.get("N_WORKERS", "12"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def get_universe():
    """Use multi-cap universe — squeeze setups across all sizes except nano."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        all_stocks = d.get("stocks", [])
        # Skip nano — too noisy for reliable BB squeeze detection
        target_buckets = {"micro", "small", "mid", "large", "mega"}
        filtered = [s for s in all_stocks if s.get("cap_bucket") in target_buckets]
        return [(s.get("symbol") or "").upper() for s in filtered if s.get("symbol")][:MAX_TICKERS]
    except Exception as e:
        print("[squeeze] universe load failed: " + str(e))
        return []


def fetch_history(symbol, days=300):
    url = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Squeeze/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            if not isinstance(d, list):
                return None
            out = []
            for x in d[:days]:
                if x.get("close") and x.get("date"):
                    out.append({
                        "date": x.get("date"),
                        "close": float(x.get("close")),
                        "high": float(x.get("high") or x.get("close")),
                        "low": float(x.get("low") or x.get("close")),
                        "volume": float(x.get("volume") or 0),
                    })
            out.sort(key=lambda r: r["date"])
            return out
    except Exception:
        return None


def compute_squeeze_signals(symbol, history):
    if not history or len(history) < 200:
        return None

    closes = [h["close"] for h in history]
    highs = [h["high"] for h in history]
    lows = [h["low"] for h in history]
    volumes = [h["volume"] for h in history]
    n = len(closes)
    today = closes[-1]

    # Liquidity check
    avg_dollar_vol = sum(c * v for c, v in zip(closes[-30:], volumes[-30:])) / min(30, n)
    if avg_dollar_vol < 1000000:
        return None

    # ─── Signal 1: Bollinger Band squeeze ───
    # BB width = (upper - lower) / middle = 2 * 2σ / mean
    bb_widths = []
    for i in range(20, n):
        window = closes[i-20:i]
        m = sum(window) / 20
        sq_dev = sum((x - m) ** 2 for x in window) / 20
        sd = math.sqrt(sq_dev)
        if m > 0:
            bb_widths.append((highs[i] - lows[i]))  # placeholder; use 4σ/mean
            bb_widths[-1] = 4 * sd / m  # in %
    if len(bb_widths) < 100:
        return None
    cur_bb = bb_widths[-1]
    sorted_bbw = sorted(bb_widths[-252:] if len(bb_widths) >= 252 else bb_widths)
    bb_percentile = sum(1 for x in sorted_bbw if x <= cur_bb) / len(sorted_bbw) * 100

    # ─── Signal 2: TTM Squeeze (BB inside Keltner) ───
    # Keltner: middle = EMA20, upper = middle + 1.5 * ATR20
    # Approximation using simple MA20 + ATR
    atr_20 = sum(max(highs[i] - lows[i],
                       abs(highs[i] - closes[i-1]),
                       abs(lows[i] - closes[i-1])) for i in range(n-20, n)) / 20
    sma_20 = sum(closes[-20:]) / 20
    keltner_upper = sma_20 + 1.5 * atr_20
    keltner_lower = sma_20 - 1.5 * atr_20
    # Compute current BB upper/lower
    sd_20 = math.sqrt(sum((x - sma_20) ** 2 for x in closes[-20:]) / 20)
    bb_upper = sma_20 + 2 * sd_20
    bb_lower = sma_20 - 2 * sd_20
    ttm_squeezing = (bb_upper < keltner_upper and bb_lower > keltner_lower)

    # ─── Signal 3: NR7 cluster (last 30 days) ───
    nr7_count = 0
    for i in range(max(7, n-30), n):
        cur_range = highs[i] - lows[i]
        ranges_7 = [highs[j] - lows[j] for j in range(i-6, i+1)]
        if cur_range == min(ranges_7) and cur_range > 0:
            nr7_count += 1

    # ─── Signal 4: VCP pattern ───
    # Find local highs and the subsequent corrections.
    # Simpler proxy: check if 3-month max drawdown < 6-month max drawdown < 12-month
    if n >= 252:
        max_3m = max(highs[-66:])
        min_3m = min(lows[-66:])
        max_6m = max(highs[-126:])
        min_6m = min(lows[-126:])
        max_12m = max(highs[-252:])
        min_12m = min(lows[-252:])
        dd_3m = (max_3m - min_3m) / max_3m * 100 if max_3m > 0 else 0
        dd_6m = (max_6m - min_6m) / max_6m * 100 if max_6m > 0 else 0
        dd_12m = (max_12m - min_12m) / max_12m * 100 if max_12m > 0 else 0
        vcp_qualifies = (dd_3m < dd_6m and dd_6m < dd_12m and dd_3m < 15)
    else:
        vcp_qualifies = False
        dd_3m = dd_6m = dd_12m = 0

    # Volume contraction during recent base
    vol_60_avg = sum(volumes[-60:]) / 60 if n >= 60 else 0
    vol_180_avg = sum(volumes[-180:]) / 180 if n >= 180 else vol_60_avg
    vol_contraction = vol_60_avg / vol_180_avg if vol_180_avg > 0 else 1.0

    # ─── Signal 5: ATR compression ───
    atr_history = []
    for i in range(20, n):
        atr = sum(max(highs[j] - lows[j],
                       abs(highs[j] - closes[j-1]),
                       abs(lows[j] - closes[j-1])) for j in range(i-19, i+1)) / 20
        if closes[i] > 0:
            atr_history.append(atr / closes[i] * 100)  # as % of price
    if not atr_history:
        return None
    cur_atr = atr_history[-1]
    sorted_atr = sorted(atr_history[-252:] if len(atr_history) >= 252 else atr_history)
    atr_percentile = sum(1 for x in sorted_atr if x <= cur_atr) / len(sorted_atr) * 100

    # ─── Signal 6: Inside-day density ───
    inside_days = 0
    for i in range(max(1, n-30), n):
        if highs[i] <= highs[i-1] and lows[i] >= lows[i-1]:
            inside_days += 1
    inside_pct = inside_days / 30 * 100

    # ─── Long-base detector ───
    # Stock has been within ±15% of current price for 60+ days
    base_days = 0
    for i in range(n - 1, max(0, n - 200), -1):
        if abs(closes[i] / today - 1) > 0.15:
            break
        base_days += 1

    # ─── Range position (60d) ───
    range_high_60 = max(closes[-60:]) if n >= 60 else max(closes)
    range_low_60 = min(closes[-60:]) if n >= 60 else min(closes)
    range_pos_60 = (today - range_low_60) / (range_high_60 - range_low_60) * 100 if range_high_60 > range_low_60 else 50

    # ─── Returns context ───
    ret_5d = (today / closes[-6] - 1) * 100 if n >= 6 else 0
    ret_30d = (today / closes[-31] - 1) * 100 if n >= 31 else 0
    ret_90d = (today / closes[-91] - 1) * 100 if n >= 91 else 0

    # ─── Signal scoring ───
    n_signals_firing = 0
    flags = []

    # 1. BB squeeze
    bb_squeeze = bb_percentile <= 10
    if bb_squeeze:
        n_signals_firing += 1
        flags.append("BB_SQUEEZE_TIGHT")
    elif bb_percentile <= 20:
        flags.append("BB_NARROWING")

    # 2. TTM squeeze
    if ttm_squeezing:
        n_signals_firing += 1
        flags.append("TTM_SQUEEZE")

    # 3. NR7
    if nr7_count >= 3:
        n_signals_firing += 1
        flags.append("NR7_CLUSTER")

    # 4. VCP
    if vcp_qualifies and vol_contraction < 0.85:
        n_signals_firing += 1
        flags.append("VCP_PATTERN")
    elif vcp_qualifies:
        flags.append("VCP_PARTIAL")

    # 5. ATR compression
    if atr_percentile <= 20:
        n_signals_firing += 1
        flags.append("ATR_COMPRESSED")

    # 6. Inside day density
    if inside_pct >= 30:
        n_signals_firing += 1
        flags.append("INSIDE_DAY_DENSE")

    # ─── Score ───
    score = 0.0
    score += n_signals_firing * 12  # max 72

    # Long-base bonus
    if base_days >= 90:
        score += 15
        flags.append("LONG_BASE_90D")
    elif base_days >= 60:
        score += 10
        flags.append("BASE_60D")
    elif base_days >= 30:
        score += 5

    # Volume contraction during compression
    if vol_contraction < 0.75:
        score += 8
        flags.append("VOL_DRYING")
    elif vol_contraction < 0.90:
        score += 4

    # Range position in middle = base, not blow-off
    if 30 <= range_pos_60 <= 70:
        score += 5
        flags.append("MID_RANGE")

    # Penalty if recent surge (chasing past breakout already)
    if abs(ret_5d) > 8:
        score *= 0.8

    score = min(score, 100)

    # Tier
    if n_signals_firing >= 5:
        tier = "TIER_S_EXCEPTIONAL"
    elif n_signals_firing >= 4:
        tier = "TIER_A_STRONG_SQUEEZE"
    elif n_signals_firing >= 3:
        tier = "TIER_B_BUILDING"
    elif n_signals_firing >= 2:
        tier = "WATCH"
    else:
        tier = "QUIET"

    return {
        "symbol": symbol,
        "score": round(score, 1),
        "tier": tier,
        "n_signals_firing": n_signals_firing,
        "flags": flags,
        "metrics": {
            "today_close": round(today, 2),
            "bb_width_percentile": round(bb_percentile, 1),
            "ttm_squeezing": ttm_squeezing,
            "nr7_count_30d": nr7_count,
            "vcp_qualifies": vcp_qualifies,
            "vol_contraction": round(vol_contraction, 2),
            "atr_percentile": round(atr_percentile, 1),
            "inside_day_pct_30d": round(inside_pct, 1),
            "base_days": base_days,
            "range_pos_60d": round(range_pos_60, 1),
            "dd_3m": round(dd_3m, 1),
            "dd_6m": round(dd_6m, 1),
            "dd_12m": round(dd_12m, 1),
            "ret_5d": round(ret_5d, 1),
            "ret_30d": round(ret_30d, 1),
            "ret_90d": round(ret_90d, 1),
            "avg_dollar_vol_30d": round(avg_dollar_vol, 0),
        },
    }


def _legacy_lambda_handler(event=None, context=None):
    started = time.time()
    deadline = started + TIMEOUT_BUDGET_S
    print("[squeeze] starting v1.0")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0})}
    print("[squeeze] universe: " + str(len(universe)) + " tickers")

    results = []
    n_no_data = 0

    def evaluate(sym):
        if time.time() > deadline:
            return None
        h = fetch_history(sym, days=300)
        if not h:
            return None
        return compute_squeeze_signals(sym, h)

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate, s): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
                if r:
                    results.append(r)
                else:
                    n_no_data += 1
            except Exception:
                n_no_data += 1

    print("[squeeze] OK: " + str(len(results)) + ", no_data: " + str(n_no_data))
    results.sort(key=lambda x: -x["score"])

    by_tier = {
        "tier_s": [r for r in results if r["tier"] == "TIER_S_EXCEPTIONAL"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_STRONG_SQUEEZE"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_BUILDING"],
        "watch":  [r for r in results if r["tier"] == "WATCH"],
    }

    out = {
        "schema_version": 1,
        "method": "volatility_squeeze_hunter_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_no_data": n_no_data,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "n_watch":  len(by_tier["watch"]),
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "n_signals": r["n_signals_firing"],
                    "flags": r["flags"],
                    "bb_pct": r["metrics"]["bb_width_percentile"],
                    "atr_pct": r["metrics"]["atr_percentile"],
                    "nr7": r["metrics"]["nr7_count_30d"],
                    "inside_pct": r["metrics"]["inside_day_pct_30d"],
                    "base_days": r["metrics"]["base_days"],
                    "vol_contraction": r["metrics"]["vol_contraction"],
                    "dd_3m": r["metrics"]["dd_3m"],
                    "dd_6m": r["metrics"]["dd_6m"],
                    "dd_12m": r["metrics"]["dd_12m"],
                }
                for r in results[:25]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]],
            "tier_a": [r["symbol"] for r in by_tier["tier_a"]],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[squeeze] wrote " + str(len(body)) + "b")
    print("[squeeze] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])) +
          " tier_b=" + str(len(by_tier["tier_b"])))
    if results[:5]:
        print("[squeeze] top: " + str([(r["symbol"], r["score"], r["n_signals_firing"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "duration_s": out["duration_s"],
        }),
    }

# Complete predecessor remains above; only this source-backed handler is active.
from pathlib import Path
from datetime import datetime, timezone
import threading
from price_observations import (CONTRACT, HEAD, FLAGS, sha, encode, strict, clock,
    symbol, endpoint, source_ref, validate_ref, content, universe, history, build)


def _price_source_identity():
    root=Path(__file__).resolve().parent
    return {name:{'bytes':len(raw),'sha256':sha(raw)} for name in ('lambda_function.py','price_observations.py') for raw in [(root/name).read_bytes()]}


def _price_object(key,bound):
    obj=S3.get_object(Bucket=BUCKET,Key=key)
    try:raw=obj['Body'].read(bound+1)
    finally:obj['Body'].close()
    if len(raw)>bound or obj.get('ContentLength')!=len(raw) or not obj.get('ETag'):raise ValueError('Whole versioned declared object required')
    return raw,obj['ETag']


def _price_immutable(key,raw):
    try:S3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/json',CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    if _price_object(key,len(raw))[0]!=raw:raise ValueError('Whole immutable readback differs')


def _price_archive(raw):
    ref={'key':'data/volatility-squeeze/history/'+sha(raw)+'.json','bytes':len(raw),'sha256':sha(raw)}
    _price_immutable(ref['key'],raw);return ref


class _PriceSources:
    def __init__(self):self.raw={};self.bytes=0;self.lock=threading.Lock();self.stop=threading.Event()
    def capture(self,raw,url,requested_at):
        ref=source_ref(raw)
        with self.lock:
            if ref['key'] not in self.raw:
                if self.bytes+len(raw)>160*1024*1024:raise ValueError('Whole source retention budget exceeded; preserve current')
                _price_immutable(ref['key'],raw);self.raw[ref['key']]=raw;self.bytes+=len(raw)
        return {'status':'received','endpoint':url,'requested_at':requested_at,
            'received_at':datetime.now(timezone.utc).isoformat(),'original_ref':ref}


def _price_fetch(ticker,sources,remaining):
    if symbol(ticker) is None:return {'status':'invalid_symbol_not_requested'}
    url=endpoint(ticker);base={'endpoint':url,'status':'not_attempted_runtime_rate_or_size_limit'}
    if remaining()<60 or sources.bytes>=96*1024*1024 or sources.stop.is_set():return base
    if not FMP_KEY:return {**base,'status':'credential_unavailable'}
    requested=datetime.now(timezone.utc).isoformat()
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):return None
    try:
        req=urllib.request.Request(url,headers={'apikey':FMP_KEY,'User-Agent':'JustHodl-Price-Observations/1.0','Accept-Encoding':'identity'})
        try:response=urllib.request.build_opener(NoRedirect()).open(req,timeout=15)
        except urllib.error.HTTPError as exc:response=exc
        with response:status=response.status;raw=response.read(8*1024*1024+1)
    except Exception as exc:
        return {**base,'status':'transport_unavailable','requested_at':requested,'received_at':datetime.now(timezone.utc).isoformat(),'error_type':type(exc).__name__}
    if status in (401,403,429):sources.stop.set()
    result={**base,'http_status':status,'requested_at':requested,'received_at':datetime.now(timezone.utc).isoformat()}
    if len(raw)>8*1024*1024:return {**result,'status':'response_exceeds_bound','original_retained':False}
    reflected=FMP_KEY.encode() in raw
    try:decoded=json.loads(raw)
    except (ValueError,UnicodeError,RecursionError):decoded=None
    pending=[decoded]
    while pending and not reflected:
        value=pending.pop()
        if isinstance(value,str):reflected=FMP_KEY in value
        elif isinstance(value,dict):pending.extend(value.keys());pending.extend(value.values())
        elif isinstance(value,list):pending.extend(value)
    if reflected:return {**result,'status':'credential_echo_withheld','original_retained':False}
    # Whole retention failures propagate; they cannot become a successful empty response.
    out=sources.capture(raw,url,requested);out['http_status']=status;return out


def lambda_handler(event=None,context=None):
    started=time.monotonic();checked=datetime.now(timezone.utc)
    if S3_KEY!=HEAD or not all(type(v) is int and 1<=v<=100000 for v in (MAX_TICKERS,N_WORKERS,TIMEOUT_BUDGET_S)):
        raise ValueError('Fixed output and original configured request limits required')
    try:previous_raw,etag=_price_object(HEAD,64*1024*1024)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
        previous_raw=None;etag=None
    previous=strict(previous_raw) if previous_raw is not None else None
    if previous is not None and not isinstance(previous,dict):raise ValueError('Malformed previous publication')
    if previous and previous.get('measurement_contract')==CONTRACT:
        stamp=clock(previous.get('generated_at'))
        if stamp is None or stamp>=checked:raise ValueError('Previous publication clock invalid')
    def remaining():
        own=TIMEOUT_BUDGET_S-(time.monotonic()-started)
        return min(own,context.get_remaining_time_in_millis()/1000) if context and hasattr(context,'get_remaining_time_in_millis') else own
    sources=_PriceSources();requested=datetime.now(timezone.utc).isoformat()
    raw,universe_etag=_price_object('data/universe.json',8*1024*1024)
    capture=sources.capture(raw,'data/universe.json',requested);capture['etag']=universe_etag
    membership=universe(capture,sources.raw,MAX_TICKERS);selected=membership['selected']
    if not selected:raise ValueError('No original-scope membership; preserve current')
    attempts=[{'status':'invalid_symbol_not_requested'} if symbol(m['ticker']) is None else {'endpoint':endpoint(m['ticker']),'status':'not_attempted_runtime_rate_or_size_limit'} for m in selected]
    # Bounded in-flight work; all selected occurrences remain in coverage. The
    # actual configured 1,500-name population and 550-second budget are retained.
    workers=min(N_WORKERS,4)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0,len(selected),workers):
            if remaining()<60 or sources.bytes>=96*1024*1024 or sources.stop.is_set():break
            jobs=[(i,pool.submit(_price_fetch,selected[i]['ticker'],sources,remaining)) for i in range(offset,min(offset+workers,len(selected)))]
            for i,future in jobs:attempts[i]=future.result()
    finished=datetime.now(timezone.utc).isoformat()
    research=build(capture,attempts,sources.raw,finished,MAX_TICKERS)
    if not any(r['observations']['status'] in ('parsed_completed_observations','reported_empty_history','unresolved_identity_or_window','no_completed_observations') for r in research['request_records']):
        raise ValueError('No received market observations; preserve current')
    prior_ref=_price_archive(previous_raw) if previous_raw is not None else None
    packet={'engine':'justhodl-volatility-squeeze-hunter','version':'2.0.0','schema_version':2,
        'measurement_contract':CONTRACT,'method':'dated_price_compression_observations','status':'RESEARCH_ONLY',
        'generated_at':finished,'acquisition_started_at':checked.isoformat(),'source_files':_price_source_identity(),
        'universe_acquisition':capture,'acquisition_limits':{'MAX_TICKERS':MAX_TICKERS,'N_WORKERS':N_WORKERS,'TIMEOUT_BUDGET_S':TIMEOUT_BUDGET_S,'active_workers':workers},
        'previous_publication':prior_ref,'retained_unique_source_bytes':sources.bytes,**research,
        'stats':{'n_universe':len(selected),'n_evaluated':None,'n_no_data':None,'n_tier_s':None,'n_tier_a':None,'n_tier_b':None,'n_watch':None},
        'summary':{'top_25_overall':[],'tier_s':[],'tier_a':[]},'all_qualifying':[],
        'call':None,**FLAGS,'signals_logged':0,'notifications_sent':0,'duration_s':round(time.monotonic()-started,2),
        'source_documentation':['https://site.financialmodelingprep.com/developer/docs/stable/historical-price-eod-full'],
        'caveats':['Complete original responses, original row coordinates, selected windows and exclusion reasons are retained; failed, empty and unattempted requests differ.',
            'The latest 300 completed-date reported occurrences are selected before validation. Bad window members cannot disappear into a clean sample.',
            'Every measurement is descriptive. Related OHLCV transforms are one evidence root; nested ranges are not a VCP correction sequence.',
            'Observation windows are not verified exchange sessions. Currency, corporate actions, historical security membership, source first-release vintages and executable returns remain unverified.',
            'No score, tier, investment call, portfolio size or trading authority is produced. The original nominal liquidity threshold is displayed as context, not a verified dollar-volume gate.']}
    raw=encode(packet)
    if len(raw)>64*1024*1024 or remaining()<15:raise ValueError('Whole publication exceeds reserve; preserve current')
    archive=_price_archive(raw);precondition={'IfMatch':etag} if etag else {'IfNoneMatch':'*'}
    S3.put_object(Bucket=BUCKET,Key=HEAD,Body=raw,ContentType='application/json',CacheControl='public, max-age=900',**precondition)
    return {'statusCode':200,'body':json.dumps({'measurement_contract':CONTRACT,'request_occurrences':len(selected),'archive':archive})}
