"""Dated central-bank component stocks and rates; no unidentifiable policy-flow score."""
import math
import calendar
import csv
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
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
OUT_KEY = "data/cb-injection.json"
FRED_KEY = managed_secret(('FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",))

# FRED carries the official ECB / BOJ / Fed statistics
SERIES = {
    "FED_SECURITIES":"WSHOSHO",
    "FED_PRIMARY_CREDIT":"WLCFLPCL",
    "FED_SWAPS":"SWPT",
    "ECB_BS":   "ECBASSETSW",        # ECB total assets, weekly, EUR mn
    "ECB_RATE": "ECBDFR",            # ECB deposit facility rate, %
    "BOJ_BS":   "JPNASSETS",         # BOJ total assets
    "BOJ_RATE": "IR3TIB01JPM156N",   # Japan 3M interbank — BOJ policy proxy
    "FED_BS":   "WALCL",             # Fed total assets, USD mn
    "FED_RATE": "DFEDTARU",          # Fed funds target rate, upper bound
    "SNB_RATE": "IR3TIB01CHM156N",   # Swiss 3M interbank — SNB policy proxy
    "JPY":      "DEXJPUS",           # yen per USD (up = weak yen)
    "CHF":      "DEXSZUS",           # franc per USD (up = weak franc)
    "EUR":      "DEXUSEU",           # USD per euro (up = strong euro)
}


def fred(series_id, limit=800):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&sort_order=desc&limit={limit}")
    last_err, d = None, None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-cb-injection/1.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            break
        except Exception as e:
            last_err = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    if d is None:
        raise last_err or RuntimeError(f"FRED fetch failed: {series_id}")
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o["date"], float(v)))
        except (TypeError, ValueError):
            continue
    return out  # newest-first [(date, value)]


def read_existing(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


METHOD='cb-component-measurements.v2'
SLA={'daily':7,'weekly':21,'monthly':100}


def clean(rows):
    result={}
    for d,v in rows:
        datetime.strptime(d,'%Y-%m-%d')
        if not isinstance(v,(int,float)) or not math.isfinite(v):raise ValueError('nonfinite')
        if d in result and result[d]!=v:raise ValueError('conflicting duplicate')
        result[d]=v
    return sorted(result.items(),reverse=True)


def quality(rows,frequency,today):
    d=rows[0][0] if rows else None;age=(today-datetime.strptime(d,'%Y-%m-%d').date()).days if d else None
    return {'status':'unavailable' if age is None else 'invalid' if age<0 else 'stale' if age>SLA[frequency] else 'fresh',
            'observation_date':d,'age_days':age,'max_age_days':SLA[frequency],'frequency':frequency}


def offset_month(d,n):
    stamp=datetime.strptime(d,'%Y-%m-%d');ix=stamp.year*12+stamp.month-1-n;y,m=divmod(ix,12);m+=1
    return datetime(y,m,min(stamp.day,calendar.monthrange(y,m)[1]))


def previous(rows,n,frequency):
    if not rows:return None
    target=offset_month(rows[0][0],n)
    if frequency=='monthly':return next(((d,v) for d,v in rows if d[:7]==target.strftime('%Y-%m')),None)
    tolerance=8 if frequency=='weekly' else 4
    return next(((d,v) for d,v in rows if 0<=(target-datetime.strptime(d,'%Y-%m-%d')).days<=tolerance),None)


def measure(rows,frequency,unit,source,scale=1,today=None):
    today=today or datetime.now(timezone.utc).date()
    try:rows=clean(rows);q=quality(rows,frequency,today)
    except (ValueError,TypeError):rows=[];q={'status':'invalid','observation_date':None}
    live=q['status']=='fresh';latest=rows[0][1]*scale if live else None;changes={}
    for n in (1,6,12):
        old=previous(rows,n,frequency) if live else None
        changes[str(n)]={'start_date':old[0] if old else None,'end_date':rows[0][0] if rows else None,
                          'level_change':(rows[0][1]-old[1])*scale if old else None,
                          'percent_change':(rows[0][1]/old[1]-1)*100 if old and old[1]!=0 else None}
    return {'latest':latest,'unit':unit,'source_id':source,'quality':q,'changes':changes,
            'history_start':rows[-1][0] if rows else None,'history_observations':len(rows)}


def decompose(series,unit,sources,today=None):
    today=today or datetime.now(timezone.utc).date();normalized={k:clean(series.get(k,[])) for k in sources}
    out={'status':'incomplete','unit':unit,'observation_date':None,'components':{},'stock_change_1m':None,
         'other_assets_and_adjustments_change_1m':None,'fx_valuation_change_1m':None,'policy_purchase_transactions_1m':None,
         'net_injection_estimate':None,'interpretation':'Stock reconciliation only. Maturities, sales, valuation and transactions require separate accounting; rate changes are not added to asset flows.'}
    for k,rows in normalized.items():out['components'][k]=measure(rows,'weekly',unit,sources[k],today=today)
    if not normalized or any(quality(rows,'weekly',today)['status']!='fresh' for rows in normalized.values()):return out
    common=set.intersection(*(set(d for d,v in rows) for rows in normalized.values()))
    if not common:return out
    date=max(common)
    if (today-datetime.strptime(date,'%Y-%m-%d').date()).days>21:return out
    lookups={k:dict(rows) for k,rows in normalized.items()};total_rows=[(d,lookups['total_assets'][d]) for d in sorted(common,reverse=True)]
    old=previous(total_rows,1,'weekly')
    out['observation_date']=date
    for k in normalized:
        m=out['components'][k];m['aligned_value']=lookups[k][date]
        m['aligned_change_1m']=lookups[k][date]-lookups[k][old[0]] if old else None
    residual=lookups['total_assets'][date]-sum(lookups[k][date] for k in normalized if k!='total_assets')
    out['other_assets_and_adjustments_level']=residual
    if residual < -0.01:out['status']='invalid_components_exceed_total';return out
    if old:
        change=lookups['total_assets'][date]-old[1]
        attributed=sum(out['components'][k]['aligned_change_1m'] for k in normalized if k!='total_assets')
        prior_residual=old[1]-sum(lookups[k][old[0]] for k in normalized if k!='total_assets')
        if prior_residual < -0.01:
            out['status']='invalid_components_exceed_total';return out
        residual_change=residual-prior_residual
        out.update(status='partial_attribution',start_date=old[0],stock_change_1m=change,
                   other_assets_and_adjustments_change_1m=residual_change,
                   reconciliation_residual=change-attributed-residual_change)
    return out


def ecb_portfolio():
    key='ILM/W.U2.C.A070100.U2.EUR'
    request=urllib.request.Request('https://data-api.ecb.europa.eu/service/data/'+key+'?format=csvdata&startPeriod=2000-01-01',headers={'User-Agent':'Mozilla/5.0'})
    with urllib.request.urlopen(request,timeout=30) as response:rows=list(csv.DictReader(io.StringIO(response.read().decode('utf-8-sig'))))
    out=[]
    for row in rows:
        if not row.get('OBS_VALUE'):continue
        if row.get('UNIT')!='EUR' or row.get('UNIT_MULT') not in ('6','9'):raise ValueError('ECB unit metadata unsupported')
        if row.get('OBS_STATUS','A') not in ('A','E','P'):continue
        y,w=map(int,row['TIME_PERIOD'].split('-W'));d=datetime.fromisocalendar(y,w,5).date().isoformat()
        out.append((d,float(row['OBS_VALUE'])*10**(int(row['UNIT_MULT'])-9)))
    return clean(out)


def ecb_archive(sid):
    doc=read_existing('data/ecb-hist/'+sid+'.json') or {}
    if doc.get('methodology_version')!='ecb-dated-units.v2' or doc.get('unit')!='EUR_bn':return []
    return clean(doc.get('points') or [])


def build_measurements(data,ecb,today=None):
    today=today or datetime.now(timezone.utc).date();banks=[]
    fed={k:[(d,v/1000) for d,v in data.get(tag,[])] for k,tag in [('total_assets','FED_BS'),('securities_outright','FED_SECURITIES'),('primary_credit','FED_PRIMARY_CREDIT'),('central_bank_swaps','FED_SWAPS')]}
    fed_sources={'total_assets':'WALCL','securities_outright':'WSHOSHO','primary_credit':'WLCFLPCL','central_bank_swaps':'SWPT'}
    ecb_sources={'total_assets':'ILM.W.U2.C.T000000.Z5.Z01','monetary_policy_securities':'ILM.W.U2.C.A070100.U2.EUR','monetary_policy_lending':'ILM.W.U2.C.A050000.U2.EUR'}
    specs=[('Fed','USD','FED_BS','FED_RATE','weekly',1/1000,True),('ECB','EUR','ECB_BS','ECB_RATE','weekly',1/1000,True),
           ('BOJ','JPY','BOJ_BS','BOJ_RATE','monthly',0.1,False),('SNB','CHF',None,'SNB_RATE','monthly',1,False)]
    for bank,ccy,bs,rate,freq,scale,is_policy in specs:
        total=measure(data.get(bs,[]) if bs else [],freq,ccy+'_bn',SERIES.get(bs,'not configured'),scale,today)
        rate_obs=measure(data.get(rate,[]),'daily' if is_policy else 'monthly','percent_per_annum',SERIES[rate],today=today)
        decomposition=(decompose(fed,'USD_bn',fed_sources,today) if bank=='Fed' else decompose(ecb,'EUR_bn',ecb_sources,today) if bank=='ECB' else
                       {'status':'unavailable','net_injection_estimate':None,'missing':['securities purchases and maturities','lending','FX and gold valuation changes']})
        banks.append({'cb':bank,'currency':ccy,'balance_sheet':total,'rate':rate_obs,
                      'rate_definition':'Fed target upper bound' if bank=='Fed' else 'ECB deposit facility rate' if bank=='ECB' else 'OECD monthly 3M interbank rate; not central-bank policy rate',
                      'policy_rate_pct':rate_obs['latest'] if is_policy else None,'interbank_proxy_pct':rate_obs['latest'] if not is_policy else None,
                      'decomposition':decomposition,'injection_stance':None,'stance_label':'NOT_ATTRIBUTED',
                      'quality':{'status':'partial' if total['latest'] is not None or rate_obs['latest'] is not None else 'unavailable'}})
    fx={name:measure(data.get(tag,[]),'daily','JPY_per_USD' if tag=='JPY' else 'CHF_per_USD' if tag=='CHF' else 'USD_per_EUR',SERIES[tag],today=today) for name,tag in [('JPY','JPY'),('CHF','CHF'),('EUR','EUR')]}
    return {'schema_version':'2.0','methodology_version':METHOD,'generated_at':datetime.now(timezone.utc).isoformat(),
            'ok':any(b['quality']['status']!='unavailable' for b in banks),
            'quality':{'status':'partial' if any(b['quality']['status']!='unavailable' for b in banks) else 'unavailable',
                       'reason':'Transaction and valuation attribution incomplete; see individual component availability'},
            'headline':'Central-bank stocks, rates and component changes; net policy injection is not yet identified.',
            'central_banks':banks,'fx_context':fx,'global_injection_impulse':{'score':None,'label':'NOT_ATTRIBUTED','reason':'No mixed-currency sum or rate-cut-to-flow conversion'},
            'carry_trade':{'carry_conditions':'NOT_CALIBRATED','unwind_risk_score':None,'unwind_risk_label':'NOT_CALIBRATED'},
            'call':None,'execution_eligible':False,'eurodollar_read':None,'cross_reference':{},
            'note':'Securities stocks, lending and swaps are shown separately on matching dates. Changes are in native-currency billions. '
                   'Residual assets and valuation effects remain explicit unknowns; balance-sheet growth does not establish QE, and a rate cut is not a measured cash injection.'}


def lambda_handler(event,context):
    started=time.monotonic();data={};ecb={};errors=[]
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures={pool.submit(fred,sid):('fred',name) for name,sid in SERIES.items()}
        futures[pool.submit(ecb_portfolio)]=('ecb','monetary_policy_securities')
        futures[pool.submit(ecb_archive,'total_assets')]=('ecb','total_assets')
        futures[pool.submit(ecb_archive,'ilm_mp_lending')]=('ecb','monetary_policy_lending')
        for future in as_completed(futures):
            kind,name=futures[future]
            try:rows=clean(future.result())
            except Exception as exc:rows=[];errors.append({'series':name,'error':type(exc).__name__})
            (data if kind=='fred' else ecb)[name]=rows
    out=build_measurements(data,ecb);out.update(errors=errors,elapsed_s=round(time.monotonic()-started,2))
    body=json.dumps(out,allow_nan=False).encode()
    for key in (f"data/cb-injection/measurements/{out['generated_at'][:10]}.json",OUT_KEY):
        s3.put_object(Bucket=S3_BUCKET,Key=key,Body=body,ContentType='application/json',CacheControl='public, max-age=3600')
    return {'statusCode':200,'body':json.dumps({'quality':out['quality'],'ok':out['ok']})}
