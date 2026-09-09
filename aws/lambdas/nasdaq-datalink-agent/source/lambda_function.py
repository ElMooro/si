from managed_secret import managed_secret
import json, urllib.parse, os, sys, math, time
from datetime import date, datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from public_provider_json import ProviderError, error_metadata, get_json

# Bundle api_auth.py alongside lambda_function.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from api_auth import authorize

API_KEY = managed_secret(('NASDAQ_API_KEY', 'NASDAQ_DATALINK_API_KEY', 'NASDAQ_DATALINK_KEY', 'QUANDL_API_KEY'), ('/justhodl/nasdaq-datalink/api-key',))

FRED_KEY = managed_secret(('FRED_API_KEY','FRED_KEY'),('/justhodl/fred/api-key',))
_CACHE=None
_CACHE_AT=0

# Allowed origins — nasdaq-datalink.html on justhodl.ai calls this directly
ALLOWED_ORIGINS = [
    "https://justhodl.ai",
    "https://www.justhodl.ai",
]

# Requested datasets; provider access and actual availability are checked per response.
DATASETS = {
    "market_indices": {
        "NASDAQOMX/COMP-NASDAQ": "NASDAQ Composite",
        "NASDAQOMX/NDX-NASDAQ": "NASDAQ-100",
        "NASDAQOMX/XQC-NASDAQ": "NASDAQ Financial-100",
    },
    "economic_indicators": {
        "FRED/GDP": "US GDP",
        "FRED/UNRATE": "Unemployment Rate",
        "FRED/CPIAUCSL": "CPI All Urban",
        "FRED/FEDFUNDS": "Fed Funds Rate",
        "FRED/DGS10": "10Y Treasury Yield",
        "FRED/DGS2": "2Y Treasury Yield",
        "FRED/T10Y2Y": "10Y-2Y Spread",
        "FRED/T10Y3M": "10Y-3M Spread",
        "FRED/VIXCLS": "VIX Close",
        "FRED/BAMLH0A0HYM2": "HY OAS Spread",
        "FRED/UMCSENT": "Consumer Sentiment",
        "FRED/M2SL": "M2 Money Supply",
        "FRED/WALCL": "Fed Balance Sheet",
        "FRED/DTWEXBGS": "Trade-Weighted Dollar",
        "FRED/DCOILWTICO": "WTI Crude Oil",
    },
    "housing": {
        "FRED/CSUSHPINSA": "Case-Shiller Home Price",
        "FRED/MORTGAGE30US": "30Y Mortgage Rate",
        "FRED/HOUST": "Housing Starts",
    },
    "labor": {
        "FRED/PAYEMS": "Nonfarm Payrolls",
        "FRED/ICSA": "Initial Jobless Claims",
        "FRED/JTSJOL": "JOLTS Job Openings",
    },
}

def fetch_nasdaq(code, limit=24):
    try:
        url = f"https://data.nasdaq.com/api/v3/datasets/{code}/data.json?" + urllib.parse.urlencode({'api_key':API_KEY,'limit':limit,'order':'desc'})
        d = get_json(url)
        ds = d.get('dataset_data', {})
        rows, cols = ds.get('data', []), ds.get('column_names', [])
        if not rows:return {"error":"EMPTY_DATASET"}
        if not isinstance(rows,list) or not isinstance(cols,list) or not cols or len(cols)!=len(set(cols)) or any(not isinstance(c,str) for c in cols) or any(not isinstance(row,list) or len(row)!=len(cols) for row in rows):
            return {"error":"PROVIDER_SCHEMA_INVALID"}
        latest = dict(zip(cols, rows[0]))
        previous = dict(zip(cols, rows[1])) if len(rows) > 1 else {}
        val_col = cols[1] if len(cols)>1 else None
        def numeric(value):
            try:
                result=float(value)
                return result if not isinstance(value,bool) and math.isfinite(result) else None
            except (TypeError,ValueError):return None
        cur_val,prev_val=numeric(latest.get(val_col)),numeric(previous.get(val_col))
        change=round((cur_val-prev_val)/abs(prev_val)*100,2) if cur_val is not None and prev_val not in (None,0) else None
        if change is not None and not math.isfinite(change):change=None
        return {"columns":cols,"latest":latest,"previous":previous,"value":cur_val,
                "change_pct":change,"change_scope":"latest versus immediately preceding provider observation",
                "history":[dict(zip(cols,row)) for row in rows],"count":len(rows),"requested_history_rows":limit,
                **({"error":"LATEST_VALUE_UNAVAILABLE"} if cur_val is None else {})}
    except ProviderError as e:
        return error_metadata(e)
    except Exception:
        return {"error":"PROVIDER_SCHEMA_INVALID"}

def fetch_fred(code,limit=24):
    if not FRED_KEY:return {'error':'FRED_CREDENTIAL_UNAVAILABLE'}
    try:
        series=code.split('/',1)[1]
        params={'api_key':FRED_KEY,'series_id':series,'file_type':'json','sort_order':'desc','limit':limit,'observation_end':datetime.now(timezone.utc).date().isoformat()}
        data=get_json('https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(params))
        observations=data.get('observations')
        if not isinstance(observations,list) or not observations:return {'error':'FRED_OBSERVATIONS_UNAVAILABLE'}
        history=[]
        for observation in observations:
            if not isinstance(observation,dict):raise ValueError()
            stamp=observation.get('date');date.fromisoformat(stamp)
            raw=observation.get('value')
            value=None if raw in (None,'.','') else float(raw)
            if isinstance(raw,bool) or value is not None and not math.isfinite(value):raise ValueError()
            history.append({'Date':stamp,'Value':value})
        if any(history[i]['Date']<=history[i+1]['Date'] for i in range(len(history)-1)):raise ValueError()
        latest=history[0];previous=history[1] if len(history)>1 else {};current=latest['Value'];prior=previous.get('Value')
        change=round((current-prior)/abs(prior)*100,2) if current is not None and prior not in (None,0) else None
        if change is not None and not math.isfinite(change):change=None
        return {'columns':['Date','Value'],'latest':latest,'previous':previous,'value':current,'change_pct':change,
                'change_scope':'latest versus immediately preceding provider observation','history':history,'count':len(history),'requested_history_rows':limit,
                'provider':'FRED_DIRECT','source_series':series,'provider_observations':observations,
                'provider_metadata':{key:data.get(key) for key in ('realtime_start','realtime_end','observation_start','observation_end','units','output_type','order_by','sort_order','count','offset','limit')},
                'vintage_scope':'Current FRED vintage; not point-in-time research data',
                **({'error':'LATEST_VALUE_UNAVAILABLE'} if current is None else {})}
    except ProviderError as exc:return error_metadata(exc)
    except Exception:return {'error':'FRED_SCHEMA_INVALID'}

def fetch(code,limit=24):
    primary=fetch_nasdaq(code,limit)
    if 'error' not in primary:return {**primary,'provider':'NASDAQ_DATA_LINK'}
    if code.startswith('FRED/'):
        fallback=fetch_fred(code,limit)
        if fallback.get('history'):
            return {**fallback,'primary_provider_status':{key:primary[key] for key in ('error','http_status') if key in primary},'fallback_used':True}
        primary['fallback_status']={key:fallback[key] for key in ('error','http_status') if key in fallback}
    return {**primary,'provider':'NASDAQ_DATA_LINK','fallback_used':False}

def lambda_handler(event, context):
    global _CACHE,_CACHE_AT
    # Function URL configuration owns CORS; duplicate response headers break browsers.
    h = {'Content-Type': 'application/json', 'Cache-Control':'no-store'}
    if isinstance(event, dict) and event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': h, 'body': '{}'}

    # Auth gate also enforces the shared bounded anonymous-access contract.
    key_meta, err = authorize(event, allowed_origins=ALLOWED_ORIGINS)
    if err:
        err['headers']={k:v for k,v in err.get('headers',{}).items() if not k.lower().startswith('access-control-')}
        return err

    path = event.get('rawPath', '') if isinstance(event, dict) else ''
    if path == '/health':
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'status': 'HANDLER_READY', 'provider_data_verified':False, 'agent': 'nasdaq-datalink-agent', 'datasets': sum(len(v) for v in DATASETS.values())})}
    if path not in ('','/'):
        return {'statusCode':404,'headers':h,'body':json.dumps({'error':'unknown_route'})}
    if _CACHE is not None and time.monotonic()-_CACHE_AT<300:
        return {'statusCode':200,'headers':h,'body':json.dumps({**_CACHE,'served_from_warm_cache':True},allow_nan=False)}
    try:
        stamp=datetime.now(timezone.utc).isoformat()
        result = {"agent": "nasdaq-datalink-agent", "ts":stamp, "generated_at":stamp, "categories": {},
                  "execution_eligible":False}
        codes=[code for datasets in DATASETS.values() for code in datasets]
        with ThreadPoolExecutor(max_workers=6) as pool:
            responses=dict(zip(codes,pool.map(fetch,codes)))
        ok = err = 0
        for cat, datasets in DATASETS.items():
            cr = {}
            for code, name in datasets.items():
                d = responses[code]
                cr[code] = {"name": name, **d}
                if 'error' in d:
                    err += 1
                else:
                    ok += 1
            result["categories"][cat] = cr
        result["metrics_ok"] = ok
        result["metrics_err"] = err
        result['ts']=result['generated_at']=datetime.now(timezone.utc).isoformat()
        result['served_from_warm_cache']=False
        result['fallback_datasets']=sum(bool(row.get('fallback_used')) for group in result['categories'].values() for row in group.values())
        result['status']='READY' if ok and not err else 'PARTIAL' if ok else 'UNAVAILABLE'
        _CACHE=result;_CACHE_AT=time.monotonic()
        return {'statusCode': 200, 'headers': h, 'body': json.dumps(result,allow_nan=False)}
    except Exception:
        return {'statusCode': 503, 'headers': h, 'body': json.dumps({'agent':'nasdaq-datalink-agent','status':'UNAVAILABLE','error':'snapshot_unavailable'})}
