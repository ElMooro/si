"""Dated yen funding, FX and CFTC futures measurements; no calibrated unwind call."""
import json
import math
import statistics
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/yen-carry.json"
FRED_KEY = managed_secret(('FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",))

# FRED carries the official BOJ statistics and the US side of the carry.
FRED_SERIES = {
    "boj_assets":  "JPNASSETS",         # BOJ total assets (100m yen)
    "jp_rate_3m":  "IR3TIB01JPM156N",   # Japan 3M interbank — BOJ policy proxy
    "jgb_10y":     "IRLTLT01JPM156N",   # Japan 10y government bond yield, %
    "usdjpy":      "DEXJPUS",           # USD/JPY spot, daily
    "us_2y":       "DGS2",              # US 2y treasury, %
    "us_10y":      "DGS10",             # US 10y treasury, %
    "fed_funds":   "DFF",               # US fed funds effective, %
}


# ───────────────────────── data fetchers ─────────────────────────
def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-yen-carry/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    raise last or RuntimeError(f"fetch failed: {url}")


def fred(series_id, limit=100000):
    """FRED observations -> newest-first [(date, float)]."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}&observation_start=2000-01-01")
    d = json.loads(_get(url))
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o["date"], float(v)))
        except (TypeError, ValueError):
            continue
    return out


def read_existing(key):
    """Defensively read an existing S3 JSON output (for cross-reference)."""
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


# ───────────────────────── helpers ─────────────────────────
def _d(s):
    return datetime.strptime(s, "%Y-%m-%d")


MONTHLY={'boj_assets','jp_rate_3m','jgb_10y'}
METHOD='yen-dated-measurements.v2'


def clean_rows(rows):
    result={}
    for d,v in rows:
        datetime.strptime(d,'%Y-%m-%d')
        if not isinstance(v,(int,float)) or not math.isfinite(v):raise ValueError('nonfinite observation')
        if d in result and result[d]!=v:raise ValueError('conflicting date')
        result[d]=v
    return sorted(result.items(),reverse=True)


def observation_quality(name,rows,today=None):
    today=today or datetime.now(timezone.utc).date()
    d=rows[0][0] if rows else None
    age=(today-datetime.strptime(d,'%Y-%m-%d').date()).days if d else None
    sla=100 if name in MONTHLY else 7
    return {'status':'unavailable' if age is None else 'invalid' if age<0 else 'stale' if age>sla else 'fresh',
            'observation_date':d,'age_days':age,'max_age_days':sla,'frequency':'monthly' if name in MONTHLY else 'daily'}


def monthly_change(rows,months,percent=False):
    if not rows:return None
    y,m=map(int,rows[0][0][:7].split('-'));target=y*12+m-1-months
    previous=next((v for d,v in rows if d[:7]==f'{target//12:04d}-{target%12+1:02d}'),None)
    if previous is None or percent and previous==0:return None
    return (rows[0][1]/previous-1)*100 if percent else rows[0][1]-previous


def daily_change(rows,days):
    if not rows:return None
    target=datetime.strptime(rows[0][0],'%Y-%m-%d')-timedelta(days=days)
    previous=next((v for d,v in rows if 0<=(target-datetime.strptime(d,'%Y-%m-%d')).days<=4),None)
    return (rows[0][1]/previous-1)*100 if previous and rows[0][1]>0 else None


def daily_vol(rows,n):
    if len(rows)<n+1:return None
    sample=rows[:n+1]
    if any(v<=0 for _,v in sample) or any(not 1<=(_d(sample[i][0])-_d(sample[i+1][0])).days<=4 for i in range(n)):return None
    returns=[math.log(sample[i][1]/sample[i+1][1]) for i in range(n)]
    return statistics.stdev(returns)*math.sqrt(252)*100


def matched_monthly_gap(us_daily,jp_monthly,today=None):
    today=today or datetime.now(timezone.utc).date()
    us={}
    for d,v in us_daily:
        if d[:7]<today.strftime('%Y-%m'):us.setdefault(d[:7],[]).append(v)
    jp={d[:7]:v for d,v in jp_monthly if d[:7]<today.strftime('%Y-%m')}
    common=sorted(m for m in jp if len(us.get(m,[]))>=15)
    if not common:return {'status':'unavailable','spread_pp':None}
    month=common[-1];date=month+'-01';age=(today-datetime.strptime(date,'%Y-%m-%d').date()).days
    if age>100:return {'status':'stale','spread_pp':None,'observation_month':month}
    usd=statistics.mean(us[month]);jpy=jp[month]
    return {'status':'fresh','spread_pp':round(usd-jpy,4),'us_rate_pct':round(usd,5),'jp_rate_pct':jpy,
            'observation_month':month,'us_observations':len(us[month]),'basis':'monthly averages, differing instrument definitions; indicative comparison only'}


def cftc_measurement(records,today=None):
    today=today or datetime.now(timezone.utc).date();rows={};invalid=0
    for r in records:
        if str(r.get('cftc_contract_market_code'))!='097741':continue
        try:
            d=str(r['report_date_as_yyyy_mm_dd'])[:10];datetime.strptime(d,'%Y-%m-%d')
            def number(key):
                v=r.get(key+'_all',r.get(key));v=float(v)
                if not math.isfinite(v) or v<0:raise ValueError('invalid contract count')
                return v
            long=number('lev_money_positions_long');short=number('lev_money_positions_short');oi=number('open_interest')
            if oi<=0 or max(long,short)>oi:raise ValueError('invalid open interest reconciliation')
            row={'report_date':d,'leveraged_funds_long':long,'leveraged_funds_short':short,'net_contracts':long-short,'open_interest':oi,'net_pct_open_interest':(long-short)/oi*100}
            if d in rows and rows[d]!=row:raise ValueError('duplicate report')
            rows[d]=row
        except (ValueError,TypeError,KeyError):invalid+=1
    history=[rows[d] for d in sorted(rows,reverse=True)];cur=history[0] if history else None
    age=(today-datetime.strptime(cur['report_date'],'%Y-%m-%d').date()).days if cur else None
    status='invalid' if invalid or age is not None and age<0 else 'unavailable' if cur is None else 'stale' if age>14 else 'fresh'
    baseline=history[1:261];z=percentile=None
    if status=='fresh' and len(baseline)>=156:
        values=[x['net_pct_open_interest'] for x in baseline];sd=statistics.stdev(values)
        if sd:z=(cur['net_pct_open_interest']-statistics.mean(values))/sd
        percentile=100*sum(v<=cur['net_pct_open_interest'] for v in values)/len(values)
    return {'quality':{'status':status,'observation_date':cur['report_date'] if cur else None,'age_days':age,'max_age_days':14,'invalid_rows':invalid},
            'current':cur if status=='fresh' else None,'history':history,'history_weeks':len(history),'baseline_weeks':len(baseline),
            'net_pct_oi_zscore':round(z,3) if z is not None else None,'net_pct_oi_percentile':round(percentile,2) if percentile is not None else None,
            'minimum_baseline_weeks':156,'source':'CFTC TFF futures-only, 097741, gpe5-46if',
            'source_url':'https://publicreporting.cftc.gov/resource/gpe5-46if.json',
            'scope':'Leveraged funds in reported JPY futures only; excludes OTC swaps, bank loans and most global carry positions.',
            'net_speculator':None,'crowded_short':None,'extreme':None,'reversal_risk':None,
            'whole_carry_trade_size':None,'score_contribution':0}


def fetch_cftc():
    query=urllib.parse.urlencode({'cftc_contract_market_code':'097741','$order':'report_date_as_yyyy_mm_dd DESC','$limit':600})
    return json.loads(_get('https://publicreporting.cftc.gov/resource/gpe5-46if.json?'+query))


def build_measurements(series,positioning,today=None):
    today=today or datetime.now(timezone.utc).date();observations={};clean={}
    for name,sid in FRED_SERIES.items():
        try:rows=clean_rows(series.get(name,[]));q=observation_quality(name,rows,today)
        except (ValueError,TypeError):rows=[];q={'status':'invalid','observation_date':None}
        clean[name]=rows if q['status']=='fresh' else []
        observations[name]={'series_id':sid,'source_url':'https://fred.stlouisfed.org/series/'+sid,'quality':q,
                            'latest':rows[0][1] if q['status']=='fresh' else None,'history_start':rows[-1][0] if rows else None,
                            'history_observations':len(rows),'unit':'100_million_JPY' if name=='boj_assets' else 'JPY_per_USD' if name=='usdjpy' else 'percent_per_annum'}
    front=matched_monthly_gap(clean['fed_funds'],clean['jp_rate_3m'],today)
    duration=matched_monthly_gap(clean['us_10y'],clean['jgb_10y'],today)
    fx=clean['usdjpy'];spot=fx[0][1] if fx else None
    rv20,rv60=daily_vol(fx,20),daily_vol(fx,60)
    chg=daily_change(fx,30)
    assets=clean['boj_assets'];jp=clean['jp_rate_3m'];jgb=clean['jgb_10y']
    breakeven=None
    if front.get('status')=='fresh':breakeven=100*(1-(1+front['jp_rate_pct']/1200)/(1+front['us_rate_pct']/1200))
    fresh=sum(v['quality']['status']=='fresh' for v in observations.values())
    return {'schema_version':'2.0','methodology_version':METHOD,'generated_at':datetime.now(timezone.utc).isoformat(),
            'ok':fresh>0,'quality':{'status':'fresh' if fresh==len(FRED_SERIES) and positioning['quality']['status']=='fresh' else 'partial' if fresh else 'unavailable','fresh_series':fresh,'required_series':len(FRED_SERIES)},
            'observations':observations,'headline':'Dated yen funding and FX measurements; no calibrated unwind forecast.',
            'carry_regime':'NOT_CALIBRATED','unwind_risk_score':None,'unwind_risk_label':'NOT_CALIBRATED','unwind_risk_components':{},
            'boj_injection_score':None,'boj_stance_label':'NOT_ATTRIBUTED','carry_attractiveness':'NOT_CALIBRATED',
            'call':None,'decisive_call':None,'execution_eligible':False,
            'boj_funding_leg':{'boj_assets_jpy_trillion':assets[0][1]/10000 if assets else None,
                'boj_balance_sheet_chg_6m_pct':monthly_change(assets,6,True),'boj_balance_sheet_chg_12m_pct':monthly_change(assets,12,True),
                'jp_short_rate_pct':jp[0][1] if jp else None,'jp_short_rate_chg_6m_pp':monthly_change(jp,6),
                'rate_definition':'OECD monthly Japan three-month interbank rate, not the BOJ policy rate or a borrowing quote',
                'policy_direction':'NOT_MEASURED','note':'Total-asset changes require component attribution before describing QE or QT.'},
            'carry_width':{'front_end_carry_pp':None,'duration_carry_pp':None,'front_end_proxy':front,'ten_year_yield_gap':duration,
                'hedged_carry_return':None,'executable_carry':None,'missing':['actual borrowing spread','matched investment instrument','FX forward points','cross-currency basis','transaction costs','margin and leverage']},
            'funding_scenario':{'status':'illustrative' if breakeven is not None else 'unavailable',
                'break_even_usdjpy_decline_1m_pct':breakeven,'assumptions':'Unhedged USD asset; one month of simple interest at the matched-month overnight USD / 3M JPY proxy rates; zero fees, borrowing spread and mark-to-market losses. Not an executable quote.'},
            'fx_detonator':{'usdjpy':spot,'usdjpy_chg_1m_pct':chg,'usdjpy_chg_3m_pct':daily_change(fx,91),'usdjpy_chg_6m_pct':daily_change(fx,182),
                'realized_vol_20d_pct':rv20,'realized_vol_60d_pct':rv60,'vol_regime':'NOT_CALIBRATED' if rv20 is not None else 'UNAVAILABLE',
                'yen_direction':'UNAVAILABLE' if chg is None else 'STRENGTHENING' if chg<0 else 'WEAKENING' if chg>0 else 'UNCHANGED',
                'basis':'Exact calendar lookbacks within four days; annualized log-return sample volatility from 20/60 complete daily returns.'},
            'jgb_long_end':{'jgb_10y_pct':jgb[0][1] if jgb else None,'jgb_10y_chg_6m_pp':monthly_change(jgb,6),'jgb_10y_chg_12m_pp':monthly_change(jgb,12),'stress':'NOT_CALIBRATED'},
            'positioning':positioning,'history_basis':'FRED current-vintage histories since 2000; CFTC up to 600 weekly reports. Future daily archives retain publication vintages.',
            'triggers':[],'eurodollar_read':None,'cross_reference':{}}


def lambda_handler(event,context):
    started=time.monotonic();series={};errors=[]
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures={pool.submit(fred,sid):name for name,sid in FRED_SERIES.items()};pos_future=pool.submit(fetch_cftc)
        for future in as_completed(futures):
            name=futures[future]
            try:series[name]=future.result()
            except Exception as exc:errors.append({'series':name,'error':type(exc).__name__})
        try:positioning=cftc_measurement(pos_future.result())
        except Exception as exc:positioning=cftc_measurement([]);errors.append({'series':'CFTC','error':type(exc).__name__})
    out=build_measurements(series,positioning);out['errors']=errors;out['elapsed_s']=round(time.monotonic()-started,2)
    body=json.dumps(out,allow_nan=False).encode()
    for key in (f"data/yen-carry/measurements/{out['generated_at'][:10]}.json",OUT_KEY):
        s3.put_object(Bucket=S3_BUCKET,Key=key,Body=body,ContentType='application/json',CacheControl='public, max-age=3600')
    return {'statusCode':200,'ok':out['ok'],'quality':out['quality']}
