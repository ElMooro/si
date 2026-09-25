"""Retain Gold Rotation originals and inspect date/adjustment definitions.

Runner-only bounded public-market reads using the engine's existing FMP
credential. No legacy invocation, account read, alert, schedule or public write.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from collections import Counter
from datetime import date,datetime,timedelta,timezone
import hashlib,json,re,subprocess,sys,time,urllib.request,urllib.error,urllib.parse
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from managed_secret import managed_secret
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-gold-equity-rotation'
PRIVATE='audit-private/20260909-originals/gold-rotation-research/'
CURRENT='data/gold-equity-rotation.json';MAX=8*1024*1024
SYMBOLS=('GLD','SPY','GDX','SLV','UUP','TLT','VNQ','REM')
KINDS=('profile','light','full','dividend-adjusted')
REQUEST='chatgpt-gold-rotation-source-preflight-6031'
STATUS=PRIVATE+'requests/'+hashlib.sha256(REQUEST.encode()).hexdigest()+'.json'

def now():return datetime.now(timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False,ensure_ascii=False).encode()
def missing(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey')
def bounded(stream):
    try:raw=stream.read(MAX+1)
    finally:stream.close()
    if not 0<len(raw)<=MAX:raise ValueError('Whole source byte bound')
    return raw
def get(s3,key):return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
def retain(s3,raw):
    assert isinstance(raw,bytes) and 0<len(raw)<=MAX
    key=PRIVATE+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    assert get(s3,key)==raw
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def original(s3,ref):
    assert re.fullmatch('[a-f0-9]{64}',ref['sha256']) and ref['key']==PRIVATE+ref['sha256']+'.bin'
    raw=get(s3,ref['key']);assert len(raw)==ref['bytes'] and sha(raw)==ref['sha256'];return raw
def status(s3,record,claim=False):
    raw=encoded(record);s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert get(s3,STATUS)==raw
def url(symbol,kind,start,end):
    assert symbol in SYMBOLS and kind in KINDS,'Reviewed symbol and endpoint required'
    assert date.fromisoformat(start)<date.fromisoformat(end)<datetime.now(timezone.utc).date()
    path='profile' if kind=='profile' else 'historical-price-eod/'+kind
    params={'symbol':symbol}
    if kind!='profile':params.update({'from':start,'to':end})
    return 'https://financialmodelingprep.com/stable/'+path+'?'+urllib.parse.urlencode(params)
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Provider redirect refused')
def strict(raw):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate JSON key')
            result[key]=value
        return result
    def invalid(_):raise ValueError('Nonfinite provider JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
def describe(raw,symbol,kind):
    doc=strict(raw)
    rows=doc if isinstance(doc,list) else doc.get('historical',doc.get('data')) if isinstance(doc,dict) else None
    if not isinstance(rows,list):return {'root_type':type(doc).__name__,'rows':None,'review_status':'unexpected_shape'}
    valid=[r for r in rows if isinstance(r,dict)];dates=[];bad_dates=0
    for row in valid:
        value=row.get('date')
        try:
            if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError()
            date.fromisoformat(value);dates.append(value)
        except ValueError:bad_dates+=1
    counts=Counter(dates);fields=sorted({key for row in valid for key in row})
    result={'root_type':type(doc).__name__,'rows':len(rows),'object_rows':len(valid),'fields':fields,
        'first_date':min(dates) if dates else None,'last_date':max(dates) if dates else None,
        'invalid_or_missing_dates':bad_dates,'duplicate_dates':sum(n-1 for n in counts.values()),
        'symbols_reported':sorted({str(r['symbol']) for r in valid if 'symbol' in r}),
        'symbol_mismatch_rows':sum('symbol' in r and r['symbol']!=symbol for r in valid),
        'source_order':'descending' if dates and dates==sorted(dates,reverse=True) else 'ascending' if dates and dates==sorted(dates) else 'unordered_or_unavailable',
        'review_status':'inventory_only_not_qualified'}
    if kind=='profile':result['identity']=[{k:r.get(k) for k in ('symbol','companyName','currency','exchange','isEtf','isin','cusip')} for r in valid]
    else:
        price_fields=[k for k in ('price','open','high','low','close','adjOpen','adjHigh','adjLow','adjClose','volume','adjVolume') if k in fields]
        result['value_coverage']={k:{'present':sum(k in r for r in valid),'null':sum(r.get(k) is None for r in valid)} for k in price_fields}
        result['boundary_rows']=[{k:r.get(k) for k in ('symbol','date',*price_fields)} for r in (valid[:1]+valid[-1:] if valid else [])]
    return result
def fetch(s3,symbol,kind,start,end,credential,opener,deadline):
    source=url(symbol,kind,start,end);received=None;http=None;raw=None
    if time.monotonic()>=deadline:raise TimeoutError('Source audit budget exhausted')
    request=urllib.request.Request(source,headers={'User-Agent':'JustHodl-gold-source-audit/1.0','apikey':credential})
    try:
        response=opener.open(request,timeout=max(1,min(15,deadline-time.monotonic())))
        http=response.status;raw=bounded(response);received=now()
    except urllib.error.HTTPError as exc:http=exc.code;raw=bounded(exc);received=now()
    except (TimeoutError,OSError):return {'symbol':symbol,'kind':kind,'source_url':source,'received_at':now(),'status':'transport_unavailable','original':None}
    if credential.encode() in raw:raise ValueError('Credential echo refused before retention')
    ref=retain(s3,raw)
    try:inventory=describe(raw,symbol,kind)
    except (ValueError,TypeError):inventory={'review_status':'invalid_json_or_shape'}
    return {'symbol':symbol,'kind':kind,'source_url':source,'received_at':received,'http_status':http,
        'status':'response_retained' if http==200 else 'provider_error_retained','original':ref,'inventory':inventory}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6031_gold_rotation_source_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_gold_rotation_source_preflight.py')],cwd=ROOT,check=True)
        try:prior=strict(get(s3,STATUS))
        except Exception as exc:
            if not missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect retained incomplete request; never silently recollect'
            ref=prior['manifest'];manifest=strict(original(s3,ref));r.kv(adopted_completed_request=True)
        else:
            status(s3,{'request_id':REQUEST,'status':'claimed','started_at':now()},True)
            actual=runtime(lam,s3,events,scheduler,FUNCTION)
            predecessor=retain(s3,get(s3,CURRENT));legacy=strict(original(s3,predecessor))
            assert legacy.get('engine')=='gold-equity-rotation'
            env=lam.get_function_configuration(FunctionName=FUNCTION).get('Environment',{}).get('Variables',{})
            credential=env.get('FMP_KEY') or env.get('FMP_API_KEY') or managed_secret((),('/justhodl/fmp/api-key','/justhodl/fmp-api-key'))
            assert credential,'Existing managed FMP credential unavailable'
            today=datetime.now(timezone.utc).date();start=str(today-timedelta(days=900));end=str(today-timedelta(days=1))
            deadline=time.monotonic()+180;captures={}
            with ThreadPoolExecutor(max_workers=4) as pool:
                jobs={pool.submit(fetch,s3,symbol,kind,start,end,credential,urllib.request.build_opener(NoRedirect()),deadline):(symbol,kind) for symbol in SYMBOLS for kind in KINDS}
                for job in as_completed(jobs):
                    symbol,kind=jobs[job];captures[symbol+':'+kind]=job.result()
                    status(s3,{'request_id':REQUEST,'status':'capturing','predecessor':predecessor,'captures':captures})
            assert len(captures)==len(SYMBOLS)*len(KINDS)
            total=sum((v['original'] or {}).get('bytes',0) for v in captures.values());assert total<=64*1024*1024
            assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
            manifest={'contract':'gold-rotation-source-preflight.v1','generated_at':now(),'runtime':actual,
                'predecessor':predecessor,'predecessor_clock':legacy.get('as_of'),'range':{'from':start,'to':end},
                'captures':captures,'retained_provider_bytes':total,'forecasts_validated':False,
                'request_definitions':{'light':'Legacy endpoint retained for reconciliation; adjustment basis not inferred.',
                    'full':'Provider full OHLCV response; basis requires explicit review.',
                    'dividend-adjusted':'Provider dividend-adjusted OHLCV; investor realized returns not established.',
                    'profile':'Instrument identity and quote currency.'},
                'definitions':['https://site.financialmodelingprep.com/developer/docs/stable/historical-price-eod-light',
                    'https://site.financialmodelingprep.com/developer/docs/stable/historical-price-eod-full',
                    'https://site.financialmodelingprep.com/developer/docs/stable/historical-price-eod-dividend-adjusted']}
            ref=retain(s3,encoded(manifest));status(s3,{'request_id':REQUEST,'status':'complete','manifest':ref})
        protected={STATUS,ref['key'],manifest['predecessor']['key'],*(v['original']['key'] for v in manifest['captures'].values() if v['original'])}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(retained_manifest=ref,runtime=manifest['runtime'],predecessor=manifest['predecessor'],predecessor_clock=manifest['predecessor_clock'],
            provider_inventory=manifest['captures'],retained_provider_bytes=manifest['retained_provider_bytes'],
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,provider_requests_this_run=0 if prior else 32,
            engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,public_head_writes=0,schedules_changed=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
