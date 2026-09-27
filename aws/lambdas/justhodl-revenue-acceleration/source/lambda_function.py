"""Preserved v1 description. The active final handler emits reported statements
and explicitly comparable calendar periods, without ranks or predictions.

justhodl-revenue-acceleration — fundamental coiled-spring detector

Catches names where revenue growth is INFLECTING — the rate of growth
itself is accelerating quarter-over-quarter. This is the most powerful
fundamental signal for institutional accumulation. Examples:
  • NVDA Q3 2023: rev growth went from 100% to 200% to 300% in 3 quarters
  • AVGO 2024: 50% → 70% → 90% YoY acceleration
  • CRWD 2020: subscription rev growth from 80% → 90% → 100%
  • SMCI 2023: rev growth from 30% → 60% → 90%
  • LLY GLP-1: 40% → 60% → 80% revenue acceleration

WHAT THIS COMPUTES:

  PER-TICKER:
    1. YoY revenue growth for last 4-8 quarters (FMP /income-statement quarterly)
    2. Sequential growth rate (Q3-Q2)
    3. Acceleration metric: growth_rate(t) - growth_rate(t-1)
         POSITIVE = accelerating, NEGATIVE = decelerating
    4. 2nd-derivative: acceleration_rate(t) - acceleration_rate(t-1)
         positive 2nd-derivative = inflection up
    5. Beat magnitude: actual revenue vs analyst estimate (FMP)
    6. EPS acceleration alongside revenue
    7. Operating leverage: rev growth vs opex growth
    8. Gross margin trend (last 4 quarters)
    9. Free cash flow inflection (going from negative to positive)

  SCORE 0-100:
    • Revenue growth acceleration (last 2 Qs): max 30 pts
    • Sustained acceleration (3+ Qs in a row): max 25 pts
    • Latest quarter beat magnitude: max 15 pts
    • Gross margin expansion: max 15 pts
    • Operating leverage: max 10 pts
    • EPS acceleration confirming: max 5 pts

  TIERS:
    TIER_S_INFLECTION: 4+ quarters accelerating + beating, score >= 80
    TIER_A_ACCELERATING: 2-3 quarters accelerating, score >= 60
    TIER_B_BUILDING: 1+ quarter accelerating, score >= 45

Special microcap focus:
  • Names with revenue < $500M but growth > 50% AND accelerating
  • Names with first $100M annualized revenue (institutional threshold)
  • Names with first quarter of profitability after years of losses

OUTPUT: data/revenue-acceleration.json
"""
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/revenue-acceleration.json")
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
N_WORKERS = int(os.environ.get("N_WORKERS", "6"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "1200"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

S3 = boto3.client("s3", region_name=REGION)


def get_universe():
    """Use multi-cap universe. Revenue acceleration is most powerful in small/micro
    where growth from $50M to $200M revenue produces 5x stock moves."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        d = json.loads(obj["Body"].read())
        all_stocks = d.get("stocks", [])
        # All caps participate — but most powerful in small/micro
        target_buckets = {"micro", "small", "mid", "large", "mega"}
        filtered = [s for s in all_stocks if s.get("cap_bucket") in target_buckets]
        return filtered[:MAX_TICKERS]
    except Exception as e:
        print("[rev-accel] universe load failed: " + str(e))
        return []


def fetch_quarterly_income(symbol, limit=8, max_retries=2):
    """Get last 8 quarters of income statement from FMP. Retries on 429."""
    url = "https://financialmodelingprep.com/stable/income-statement?symbol=" + symbol + "&period=quarter&limit=" + str(limit) + "&apikey=" + FMP_KEY
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-RevAccel/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                d = json.loads(r.read())
                if isinstance(d, list):
                    return d
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < max_retries:
                time.sleep(1.0 + attempt)
                continue
            last_err = "HTTP " + str(e.code)
            break
        except Exception as e:
            last_err = str(e)[:80]
            break
    if last_err:
        # Throttled error logging — only first ~5 errors per run to avoid log flood
        global _err_count
        try:
            _err_count = (_err_count + 1) if "_err_count" in globals() else 1
        except Exception:
            _err_count = 1
        if _err_count <= 5:
            print("[rev-accel] fetch err " + symbol + ": " + last_err)
    return None


def fetch_market_cap(symbol, stock_data=None):
    """Get current market cap. Prefer universe-supplied data to skip API call."""
    if stock_data and stock_data.get("market_cap"):
        return stock_data["market_cap"]
    url = "https://financialmodelingprep.com/stable/quote?symbol=" + symbol + "&apikey=" + FMP_KEY
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-RevAccel/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
            if isinstance(d, list) and d:
                return float(d[0].get("marketCap") or 0) or None
    except Exception:
        pass
    return None


def evaluate_ticker(stock):
    sym = (stock.get("symbol") or "").upper()
    sector = stock.get("sector", "?")
    industry = stock.get("industry", "?")

    quarters = fetch_quarterly_income(sym, limit=8)
    if not quarters or len(quarters) < 5:
        return None

    # Sort newest first (FMP usually does this already, but be safe)
    quarters_sorted = sorted(quarters, key=lambda q: q.get("date", ""), reverse=True)
    
    # Build revenue series — need 4+ quarters paired with year-ago
    if len(quarters_sorted) < 5:
        return None

    # Compute YoY growth for each quarter where we have year-ago comparison
    yoy_growth = []
    for i in range(min(4, len(quarters_sorted) - 4)):
        cur_rev = quarters_sorted[i].get("revenue") or 0
        ago_rev = quarters_sorted[i + 4].get("revenue") or 0
        if ago_rev > 0:
            growth = (cur_rev - ago_rev) / abs(ago_rev) * 100
            yoy_growth.append({
                "quarter_end": quarters_sorted[i].get("date"),
                "revenue": cur_rev,
                "ago_revenue": ago_rev,
                "yoy_pct": growth,
            })

    if len(yoy_growth) < 2:
        return None

    # yoy_growth is now newest first; reverse so oldest first for trend analysis
    yoy_growth.reverse()

    # Acceleration: growth_rate change between consecutive quarters
    acceleration = []
    for i in range(1, len(yoy_growth)):
        delta = yoy_growth[i]["yoy_pct"] - yoy_growth[i-1]["yoy_pct"]
        acceleration.append({
            "quarter_end": yoy_growth[i]["quarter_end"],
            "growth_delta": delta,
            "growth_then": yoy_growth[i-1]["yoy_pct"],
            "growth_now": yoy_growth[i]["yoy_pct"],
        })

    # Most recent acceleration
    latest_growth = yoy_growth[-1]["yoy_pct"] if yoy_growth else 0
    latest_acceleration = acceleration[-1]["growth_delta"] if acceleration else 0
    
    # Count consecutive accelerating quarters
    consec_accel = 0
    for a in reversed(acceleration):
        if a["growth_delta"] > 0:
            consec_accel += 1
        else:
            break

    # Sequential growth (latest q vs prior q)
    seq_growth = None
    if len(quarters_sorted) >= 2:
        cur_rev = quarters_sorted[0].get("revenue") or 0
        prior_rev = quarters_sorted[1].get("revenue") or 0
        if prior_rev > 0:
            seq_growth = (cur_rev - prior_rev) / abs(prior_rev) * 100

    # Gross margin trend
    gross_margins = []
    for q in quarters_sorted[:4]:
        rev = q.get("revenue") or 0
        gp = q.get("grossProfit") or 0
        if rev > 0:
            gross_margins.append({
                "quarter_end": q.get("date"),
                "gm_pct": gp / rev * 100,
            })
    gross_margins.reverse()
    gm_trend = None
    if len(gross_margins) >= 2:
        gm_trend = gross_margins[-1]["gm_pct"] - gross_margins[0]["gm_pct"]

    # Operating leverage: rev growth vs opex growth (latest quarter)
    op_leverage = None
    if len(quarters_sorted) >= 5:
        cur_rev = quarters_sorted[0].get("revenue") or 0
        ago_rev = quarters_sorted[4].get("revenue") or 0
        cur_opex = (quarters_sorted[0].get("operatingExpenses") or 0)
        ago_opex = (quarters_sorted[4].get("operatingExpenses") or 0)
        if ago_rev > 0 and ago_opex > 0:
            rev_growth_pct = (cur_rev - ago_rev) / abs(ago_rev) * 100
            opex_growth_pct = (cur_opex - ago_opex) / abs(ago_opex) * 100
            op_leverage = rev_growth_pct - opex_growth_pct

    # EPS acceleration
    eps_growth = []
    for i in range(min(4, len(quarters_sorted) - 4)):
        cur_eps = quarters_sorted[i].get("epsdiluted") or quarters_sorted[i].get("eps") or 0
        ago_eps = quarters_sorted[i + 4].get("epsdiluted") or quarters_sorted[i + 4].get("eps") or 0
        if abs(ago_eps) > 0.01:
            eps_growth.append((cur_eps - ago_eps) / abs(ago_eps) * 100)
    eps_accelerating = (len(eps_growth) >= 2 and eps_growth[0] > eps_growth[-1])

    # Microcap focus
    market_cap = fetch_market_cap(sym, stock_data=stock)
    is_microcap = market_cap and market_cap < 500_000_000
    is_smallcap = market_cap and 500_000_000 <= market_cap < 2_000_000_000
    
    cur_rev_annualized = (quarters_sorted[0].get("revenue") or 0) * 4
    crossed_100m = cur_rev_annualized >= 100_000_000 and quarters_sorted[4].get("revenue", 0) * 4 < 100_000_000

    # ─── SCORING ───
    score = 0.0
    flags = []

    # 1. Revenue growth acceleration (latest)
    if latest_acceleration > 30:
        score += 30
        flags.append("ACCEL_30PP+")
    elif latest_acceleration > 15:
        score += 22
        flags.append("ACCEL_15PP+")
    elif latest_acceleration > 5:
        score += 12
        flags.append("ACCEL_5PP+")
    elif latest_acceleration > 0:
        score += 5

    # 2. Consecutive accelerating quarters
    if consec_accel >= 4:
        score += 25
        flags.append("ACCEL_4Q_STREAK")
    elif consec_accel >= 3:
        score += 18
        flags.append("ACCEL_3Q_STREAK")
    elif consec_accel >= 2:
        score += 10
        flags.append("ACCEL_2Q_STREAK")

    # 3. Latest growth rate (absolute level)
    if latest_growth > 100:
        score += 15
        flags.append("GROWTH_100%+")
    elif latest_growth > 50:
        score += 12
        flags.append("GROWTH_50%+")
    elif latest_growth > 25:
        score += 8
        flags.append("GROWTH_25%+")
    elif latest_growth > 10:
        score += 5

    # 4. Gross margin expansion
    if gm_trend is not None:
        if gm_trend > 5:
            score += 15
            flags.append("GM_EXPAND_5PP+")
        elif gm_trend > 2:
            score += 10
            flags.append("GM_EXPAND_2PP+")
        elif gm_trend > 0.5:
            score += 5

    # 5. Operating leverage
    if op_leverage is not None:
        if op_leverage > 20:
            score += 10
            flags.append("OP_LEVERAGE_20PP+")
        elif op_leverage > 10:
            score += 6
            flags.append("OP_LEVERAGE_10PP+")
        elif op_leverage > 5:
            score += 3

    # 6. EPS acceleration
    if eps_accelerating:
        score += 5
        flags.append("EPS_ACCELERATING")

    # 7. Special situations
    if is_microcap and latest_growth > 50 and latest_acceleration > 0:
        score += 10
        flags.append("MICROCAP_GROWTH_INFLECTION")
    if is_smallcap and latest_growth > 30 and latest_acceleration > 0:
        score += 5
        flags.append("SMALLCAP_GROWTH_INFLECTION")
    if crossed_100m:
        score += 8
        flags.append("CROSSED_$100M_REVENUE")

    score = min(score, 100)

    # Tier
    if score >= 80 and consec_accel >= 4:
        tier = "TIER_S_INFLECTION"
    elif score >= 60 and consec_accel >= 2:
        tier = "TIER_A_ACCELERATING"
    elif score >= 45:
        tier = "TIER_B_BUILDING"
    else:
        tier = "WATCH"

    return {
        "symbol": sym,
        "score": round(score, 1),
        "tier": tier,
        "flags": flags,
        "consec_accel_quarters": consec_accel,
        "metrics": {
            "latest_yoy_growth_pct": round(latest_growth, 1),
            "latest_acceleration_pp": round(latest_acceleration, 1),
            "seq_growth_pct": round(seq_growth, 1) if seq_growth else None,
            "gm_trend_pp": round(gm_trend, 1) if gm_trend is not None else None,
            "op_leverage_pp": round(op_leverage, 1) if op_leverage is not None else None,
            "eps_accelerating": eps_accelerating,
            "market_cap": int(market_cap) if market_cap else None,
            "is_microcap": is_microcap,
            "is_smallcap": is_smallcap,
            "annualized_revenue": int(cur_rev_annualized) if cur_rev_annualized else None,
            "sector": sector,
            "industry": industry,
        },
        "yoy_growth_history": yoy_growth,
        "acceleration_history": acceleration,
    }


def _legacy_lambda_handler(event=None, context=None):
    started = time.time()
    deadline = started + TIMEOUT_BUDGET_S
    print("[rev-accel] starting v1.0")

    universe = get_universe()
    if not universe:
        return {"statusCode": 200, "body": json.dumps({"n": 0})}
    print("[rev-accel] universe: " + str(len(universe)) + " stocks")

    results = []
    n_no_data = 0

    def evaluate(stock):
        if time.time() > deadline:
            return None
        try:
            return evaluate_ticker(stock)
        except Exception as e:
            print("[rev-accel] " + (stock.get("symbol") or "?") + " err: " + str(e))
            return None

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

    print("[rev-accel] OK: " + str(len(results)) + ", no_data: " + str(n_no_data))
    results.sort(key=lambda x: -x["score"])

    by_tier = {
        "tier_s": [r for r in results if r["tier"] == "TIER_S_INFLECTION"],
        "tier_a": [r for r in results if r["tier"] == "TIER_A_ACCELERATING"],
        "tier_b": [r for r in results if r["tier"] == "TIER_B_BUILDING"],
    }
    
    # Microcap special list
    microcap_picks = [r for r in results
                       if r["metrics"]["is_microcap"]
                       and r["metrics"]["latest_yoy_growth_pct"] > 30
                       and r["consec_accel_quarters"] >= 2][:25]

    out = {
        "schema_version": 1,
        "method": "revenue_acceleration_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_evaluated": len(results),
            "n_no_data": n_no_data,
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_tier_b": len(by_tier["tier_b"]),
            "n_microcap_picks": len(microcap_picks),
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "tier": r["tier"],
                    "consec_accel": r["consec_accel_quarters"],
                    "flags": r["flags"],
                    "growth": r["metrics"]["latest_yoy_growth_pct"],
                    "acceleration": r["metrics"]["latest_acceleration_pp"],
                    "gm_trend": r["metrics"]["gm_trend_pp"],
                    "op_leverage": r["metrics"]["op_leverage_pp"],
                    "is_microcap": r["metrics"]["is_microcap"],
                    "annualized_rev": r["metrics"]["annualized_revenue"],
                    "sector": r["metrics"]["sector"],
                }
                for r in results[:25]
            ],
            "tier_s": [r["symbol"] for r in by_tier["tier_s"]],
            "microcap_picks": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "growth": r["metrics"]["latest_yoy_growth_pct"],
                    "acceleration": r["metrics"]["latest_acceleration_pp"],
                    "consec_accel": r["consec_accel_quarters"],
                    "annualized_rev": r["metrics"]["annualized_revenue"],
                    "market_cap": r["metrics"]["market_cap"],
                }
                for r in microcap_picks
            ],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print("[rev-accel] wrote " + str(len(body)) + "b")
    print("[rev-accel] tier_s=" + str(len(by_tier["tier_s"])) + " tier_a=" + str(len(by_tier["tier_a"])))
    if results[:5]:
        print("[rev-accel] top: " + str([(r["symbol"], r["score"], r["consec_accel_quarters"]) for r in results[:5]]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_evaluated": len(results),
            "n_tier_s": len(by_tier["tier_s"]),
            "n_tier_a": len(by_tier["tier_a"]),
            "n_microcap_picks": len(microcap_picks),
            "duration_s": out["duration_s"],
        }),
    }


# Original producer retained above; only the following final handler is active.
from pathlib import Path
from datetime import datetime,timezone
from urllib.parse import quote_plus
import hashlib
from revenue_observations import CONTRACT,strict,clock,number,symbol,envelope,original,universe,dossier


def _revenue_source_identity():
    directory=Path(__file__).resolve().parent
    return {name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
            for name in ('lambda_function.py','revenue_observations.py') for raw in [(directory/name).read_bytes()]}


def _revenue_fetch(ticker,endpoint):
    if not ticker or symbol(ticker)!=ticker:return {'endpoint':endpoint,'status':'invalid_symbol_not_requested'}
    if not FMP_KEY:return {'endpoint':endpoint,'status':'credential_unavailable'}
    url='https://financialmodelingprep.com/stable/'+endpoint+'?symbol='+quote_plus(ticker)
    if endpoint=='income-statement':url+='&period=quarter&limit=8'
    req=urllib.request.Request(url,headers={'apikey':FMP_KEY,'User-Agent':'JustHodl-Revenue-Observations/1.1'})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,req,fp,code,msg,headers,newurl):return None
    try:
        with urllib.request.build_opener(NoRedirect()).open(req,timeout=10) as response:raw=response.read(512*1024+1)
        if len(raw)>512*1024:return {'endpoint':endpoint,'status':'response_exceeds_bound','original_retained':False}
        if FMP_KEY.encode() in raw:return {'endpoint':endpoint,'status':'credential_echo_withheld','original_retained':False}
        return envelope(raw,endpoint,datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        return {'endpoint':endpoint,'status':'rate_limited' if getattr(exc,'code',None)==429 else 'unavailable',
                'http_status':getattr(exc,'code',None),'error_type':type(exc).__name__}


def _revenue_company(member):
    ticker=member['ticker'];captures=[_revenue_fetch(ticker,'income-statement')];values=original(captures[0])
    # Maintain the original quote-acquisition budget gate, without using array
    # offsets to publish a growth measurement or treating cap currency as USD.
    rows=sorted(values,key=lambda r:r.get('date','') if isinstance(r,dict) and isinstance(r.get('date'),str) else '',reverse=True) if isinstance(values,list) else []
    pairs=sum(1 for i in range(min(4,max(0,len(rows)-4))) if isinstance(rows[i+4],dict) and number(rows[i+4].get('revenue')) is not None and rows[i+4]['revenue']>0)
    if pairs<2:
        captures.append({'endpoint':'quote','status':'not_requested_original_statement_gate'})
    elif member['raw'].get('market_cap'):
        captures.append({'endpoint':'quote','status':'not_requested_original_universe_cap'})
    else:captures.append(_revenue_fetch(ticker,'quote'))
    return captures


def _revenue_object(key,bound):
    obj=S3.get_object(Bucket=BUCKET,Key=key)
    try:raw=obj['Body'].read(bound+1)
    finally:obj['Body'].close()
    if len(raw)>bound or obj.get('ContentLength')!=len(raw) or not obj.get('ETag'):raise ValueError('Whole declared object required')
    return raw,obj['ETag']


def _revenue_previous():
    try:raw,etag=_revenue_object(S3_KEY,64*1024*1024)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey'):return None,None,None
        raise
    p=strict(raw)
    if not isinstance(p,dict):raise ValueError('Previous publication malformed')
    return p,raw,etag


def _revenue_archive(raw):
    key='data/revenue-acceleration/history/'+hashlib.sha256(raw).hexdigest()+'.json'
    try:S3.put_object(Bucket=BUCKET,Key=key,Body=raw,IfNoneMatch='*',ContentType='application/json',CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','PreconditionFailed','ConditionalRequestConflict'):raise
    back,_=_revenue_object(key,len(raw))
    if back!=raw:raise ValueError('Whole immutable readback differs')
    return {'key':key,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}


def lambda_handler(event=None,context=None):
    started=time.monotonic();checked=datetime.now(timezone.utc);today=checked.date().isoformat()
    if S3_KEY!='data/revenue-acceleration.json':raise ValueError('Declared research output required')
    if MAX_TICKERS<1 or N_WORKERS<1 or TIMEOUT_BUDGET_S<1:raise ValueError('Original acquisition bounds required')
    previous,previous_raw,etag=_revenue_previous()
    if previous and previous.get('measurement_contract')==CONTRACT:
        stamp=clock(previous.get('generated_at'))
        if stamp is None or stamp>=checked:raise ValueError('Previous generation clock invalid')
    raw,source_etag=_revenue_object('data/universe.json',8*1024*1024)
    capture=envelope(raw,'data/universe.json',datetime.now(timezone.utc).isoformat());capture['etag']=source_etag
    membership=universe(capture,min(MAX_TICKERS,1200));selected=membership['selected']
    if not selected:raise ValueError('No selected universe; preserve previous publication')
    captures=[[{'endpoint':'income-statement','status':'not_attempted_runtime_rate_or_size_limit'}] for _ in selected]
    workers=max(1,min(N_WORKERS,6));total_bytes=0
    def remaining():
        own=min(TIMEOUT_BUDGET_S,600)-(time.monotonic()-started)
        return min(own,context.get_remaining_time_in_millis()/1000) if context and hasattr(context,'get_remaining_time_in_millis') else own
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0,len(selected),workers):
            if remaining()<50 or total_bytes>=16*1024*1024:break
            jobs=[(i,pool.submit(_revenue_company,selected[i])) for i in range(offset,min(offset+workers,len(selected)))]
            for i,future in jobs:
                captures[i]=future.result();total_bytes+=sum(a.get('original_bytes',0) for a in captures[i])
            if any(a.get('status')=='rate_limited' for i,_ in jobs for a in captures[i]):break
    if not any(a.get('endpoint')=='income-statement' and a.get('status')=='received' and isinstance(original(a),list) for group in captures for a in group):
        raise ValueError('All statement populations unavailable; preserve previous publication')
    records=[]
    for i,(member,group) in enumerate(zip(selected,captures)):
        row=dossier(member,group,today);row['request_index']=i;records.append(row)
    prior_ref=_revenue_archive(previous_raw) if previous_raw is not None else None
    packet={'engine':'justhodl-revenue-acceleration','version':'1.1.0','schema_version':2,'measurement_contract':CONTRACT,
        'method':'reported_statements_and_explicit_calendar_quarter_comparisons','status':'RESEARCH_ONLY','source_files':_revenue_source_identity(),
        'generated_at':datetime.now(timezone.utc).isoformat(),'acquisition_started_at':checked.isoformat(),'checked_as_of':today,
        'universe_acquisition':capture,'universe_membership':membership,'request_records':records,'previous_publication':prior_ref,
        'n_statement_observations':sum(len(r['statement_observations']) for r in records),
        'stats':{'n_universe':len(selected),'n_evaluated':None,'n_no_data':None,'n_tier_s':None,'n_tier_a':None,'n_tier_b':None,'n_microcap_picks':None},
        'summary':{'top_25_overall':[],'tier_s':[],'microcap_picks':[]},'all_qualifying':[],
        'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False,'independent_evidence_eligible':False,
        'call':None,'signals_logged':0,'notifications_sent':0,'private_state_read_or_written':False,
        'source_documentation':['https://site.financialmodelingprep.com/developer/docs/stable/income-statement','https://site.financialmodelingprep.com/developer/docs/cycle-times'],
        'caveats':['Every returned statement and selected/unattempted request is retained. Invalid records never become measured zero.',
            'Changes require unique explicit comparable issuer/currency/calendar-quarter periods. Fiscal-week calendars remain unresolved.',
            'Acquired normalized statements do not recover original first-release vintages, acquisitions, FX or corporate-action adjustments.',
            'Revenue-growth acceleration is the change between comparable adjacent annual-growth rates, in percentage points; it is not a forecast.',
            'Trailing revenue requires four contiguous quarters. One quarter multiplied by four is not reported as trailing-year revenue.',
            'No earnings-beat or free-cash-flow source is acquired by this engine. No stock tiers, accumulation or sizing claims are made.',
            'FMP income statements also feed other fundamentals engines; repeated source evidence cannot count as independent agreement.'],
        'duration_s':round(time.monotonic()-started,2)}
    raw=json.dumps(packet,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode('utf-8')
    if len(raw)>64*1024*1024 or remaining()<15:raise ValueError('Whole publication exceeds reserve; preserve previous current packet')
    archive=_revenue_archive(raw);precondition={'IfMatch':etag} if etag else {'IfNoneMatch':'*'}
    S3.put_object(Bucket=BUCKET,Key=S3_KEY,Body=raw,ContentType='application/json',CacheControl='public, max-age=900',**precondition)
    return {'statusCode':200,'body':json.dumps({'measurement_contract':CONTRACT,'request_occurrences':len(records),'archive':archive})}
