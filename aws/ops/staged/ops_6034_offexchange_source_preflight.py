"""Bounded original-source inventory; no legacy invocation or public-head write.

Weekly/monthly probes inspect grain and pagination headers, not market coverage.
The complete CNMS file and existing publications stay separate and protected.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from collections import Counter
from datetime import date,datetime,timedelta,timezone
import hashlib,json,re,subprocess,sys,time,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-dark-pool'
PRIVATE='audit-private/20260909-originals/offexchange-research/'
PACKETS=('data/dark-pool.json','data/finra-short.json','data/finra-short-history.json','data/dix.json','data/history/dark-pool-dix.json','data/config/finra-monthly-spec.json')
REQUEST='chatgpt-offexchange-source-preflight-6034';STATUS=PRIVATE+'requests/'+hashlib.sha256(REQUEST.encode()).hexdigest()+'.json'
MAX=64*1024*1024;SOURCE_MAX=8*1024*1024
HEADERS=('record-total','record-offset','record-limit','total-records-on-page','record-max-limit','finra-api-request-id','content-type')
def now():return datetime.now(timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def missing(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('404','NoSuchKey')
def bounded(stream,limit=MAX):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    if not 0<len(raw)<=limit:raise ValueError('Whole source byte bound')
    return raw
def get(s3,key):
    if key not in PACKETS and not re.fullmatch(re.escape(PRIVATE)+r'(?:[a-f0-9]{64}\.bin|requests/[a-f0-9]{64}\.json)',key):raise ValueError('Reviewed research read required')
    return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
def retain(s3,raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded original required')
    key=PRIVATE+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if get(s3,key)!=raw:raise ValueError('Original readback differs')
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def original(s3,ref):
    if not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or ref['key']!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Protected identity required')
    raw=get(s3,ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Retained original differs')
    return raw
def strict(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def invalid(_):raise ValueError('Nonfinite JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
def status(s3,doc,claim=False):
    raw=encoded(doc);s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if get(s3,STATUS)!=raw:raise ValueError('Request readback differs')
def plan(today,daily_date):
    end=today-timedelta(days=1);start=end-timedelta(days=59)
    if not isinstance(daily_date,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',daily_date) or not today-timedelta(days=14)<=date.fromisoformat(daily_date)<today:raise ValueError('Recent explicit CNMS observation date required')
    requests={}
    for dataset in ('weeklySummary','monthlySummary','blocksSummary'):
        requests['metadata:'+dataset]={'url':'https://api.finra.org/metadata/group/otcMarket/name/'+dataset,'body':None,'kind':'metadata'}
    for dataset,codes,field,first in [('weeklySummary',('ATS_W_SMBL','OTC_W_SMBL'),'weekStartDate',start),('monthlySummary',('ATS_M_SMBL_FIRM','OTC_M_SMBL_FIRM'),'monthStartDate',end-timedelta(days=180))]:
        for code in codes:
            body={'limit':2000,'offset':0,'compareFilters':[{'fieldName':'summaryTypeCode','compareType':'EQUAL','fieldValue':code}],
                'dateRangeFilters':[{'fieldName':field,'startDate':str(first),'endDate':str(end)}]}
            requests['probe:'+code]={'url':'https://api.finra.org/data/group/otcMarket/name/'+dataset,'body':body,'kind':'probe'}
    requests['daily:CNMS']={'url':'https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+daily_date.replace('-','')+'.txt','body':None,'kind':'daily_file'}
    return requests
def describe(raw,kind,headers):
    if kind=='daily_file':
        lines=raw.decode('utf-8-sig').splitlines();rows=[line.split('|') for line in lines[1:] if '|' in line];fields=lines[0].split('|') if lines else []
        return {'kind':kind,'fields':fields,'rows':len(rows),'row_width_errors':sum(len(row)!=len(fields) for row in rows),
            'dates':sorted({row[0] for row in rows}),'trailer':lines[-1] if lines else None,'review_status':'inventory_only_not_qualified'}
    doc=strict(raw)
    if kind=='metadata':return {'kind':kind,'root_type':type(doc).__name__,'schema':doc,'review_status':'inventory_only_not_qualified'}
    if not isinstance(doc,list):return {'kind':kind,'root_type':type(doc).__name__,'review_status':'unexpected_shape','coverage_complete':False}
    rows=[v for v in doc if isinstance(v,dict)];fields=sorted({k for row in rows for k in row});dates={}
    for name in ('weekStartDate','monthStartDate','summaryStartDate','initialPublishedDate','lastUpdateDate'):
        counts=Counter(str(v[name]) for v in rows if name in v);dates[name]=dict(sorted(counts.items()))
    keys=[tuple(str(row.get(k)) for k in ('summaryTypeCode','weekStartDate','monthStartDate','tierIdentifier','issueSymbolIdentifier','marketParticipantIdentifier','firmCrdNumber')) for row in rows]
    return {'kind':kind,'rows':len(doc),'object_rows':len(rows),'fields':fields,'date_counts':dates,
        'codes':sorted({str(row.get('summaryTypeCode')) for row in rows}),'tiers':sorted({str(row.get('tierIdentifier')) for row in rows}),
        'candidate_key_duplicates':sum(n-1 for n in Counter(keys).values()),'reported_total':headers.get('record-total'),
        'coverage_complete':False,'review_status':'first_page_grain_probe_not_qualified','boundary_rows':rows[:1]+rows[-1:]}
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Unreviewed redirect refused')
def fetch(s3,spec,opener,deadline):
    allowed=re.fullmatch(r'https://api\.finra\.org/(?:metadata|data)/group/otcMarket/name/(?:weeklySummary|monthlySummary|blocksSummary)',spec['url']) or re.fullmatch(r'https://cdn\.finra\.org/equity/regsho/daily/CNMSshvol\d{8}\.txt',spec['url'])
    if not allowed:raise ValueError('Reviewed FINRA destination required')
    if time.monotonic()>=deadline:raise TimeoutError('Source audit deadline')
    req=urllib.request.Request(spec['url'],data=encoded(spec['body']) if spec['body'] is not None else None,headers={'User-Agent':'JustHodl-offexchange-source-audit/1.0','Accept':'text/plain' if spec['kind']=='daily_file' else 'application/json','Content-Type':'application/json'})
    try:
        response=opener.open(req,timeout=max(1,min(40,deadline-time.monotonic())));code=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in HEADERS};raw=bounded(response,SOURCE_MAX)
    except urllib.error.HTTPError as exc:code=exc.code;headers={k.lower():v for k,v in exc.headers.items() if k.lower() in HEADERS};raw=bounded(exc,SOURCE_MAX)
    except (OSError,TimeoutError):return {**spec,'received_at':now(),'status':'transport_unavailable','original':None,'coverage_complete':False}
    ref=retain(s3,raw)
    try:inventory=describe(raw,spec['kind'],headers)
    except (ValueError,TypeError,UnicodeError):inventory={'review_status':'invalid_source_shape','coverage_complete':False}
    return {**spec,'received_at':now(),'http_status':code,'headers':headers,'original':ref,'inventory':inventory,
        'status':'response_retained' if code==200 else 'provider_error_retained'}
def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6034_offexchange_source_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_offexchange_source_preflight.py')],cwd=ROOT,check=True)
        try:prior=strict(get(s3,STATUS))
        except Exception as exc:
            if not missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect retained incomplete request; never recollect silently'
            ref=prior['manifest'];manifest=strict(original(s3,ref));r.kv(adopted_completed_request=True)
        else:
            status(s3,{'request_id':REQUEST,'status':'claimed','started_at':now()},True)
            actual=runtime(lam,s3,events,scheduler,FUNCTION);parents={};daily=None
            for key in PACKETS:
                try:raw=get(s3,key)
                except Exception as exc:
                    if not missing(exc):raise
                    parents[key]={'status':'missing','original':None};continue
                doc=strict(raw);parents[key]={'status':'retained','original':retain(s3,raw),'generated_at':doc.get('generated_at'),'data_date':doc.get('data_date'),'latest_week':doc.get('latest_week')}
                if key=='data/finra-short.json':daily=doc.get('data_date')
            assert parents[PACKETS[0]]['original'],'Whole predecessor required'
            requests=plan(datetime.now(timezone.utc).date(),daily);captures={};deadline=time.monotonic()+160
            with ThreadPoolExecutor(max_workers=3) as pool:
                jobs={pool.submit(fetch,s3,spec,urllib.request.build_opener(NoRedirect()),deadline):name for name,spec in requests.items()}
                for job in as_completed(jobs):
                    captures[jobs[job]]=job.result();status(s3,{'request_id':REQUEST,'status':'capturing','parents':parents,'captures':captures})
            assert len(captures)==8
            total=sum((row['original'] or {}).get('bytes',0) for row in captures.values());assert total<=64*1024*1024
            assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
            manifest={'contract':'offexchange-source-preflight.v1','generated_at':now(),'runtime':actual,'parents':parents,'captures':captures,'provider_bytes':total,
                'coverage_complete':False,'forecast_qualified':False,'scope':'Metadata, four first-page grain probes and one complete CNMS response. No market-coverage, buying-pressure, inventory, block-size or forecast qualification.'}
            ref=retain(s3,encoded(manifest));status(s3,{'request_id':REQUEST,'status':'complete','manifest':ref})
        protected={STATUS,ref['key'],*(v['original']['key'] for v in manifest['parents'].values() if v['original']),*(v['original']['key'] for v in manifest['captures'].values() if v['original'])}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(manifest=ref,runtime=manifest['runtime'],parents=manifest['parents'],captures=manifest['captures'],provider_bytes=manifest['provider_bytes'],
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,coverage_complete=False,provider_requests_this_run=0 if prior else 8,
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
