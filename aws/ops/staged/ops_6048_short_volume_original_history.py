"""Capture 61 FINRA-published CNMS originals from retained monthly indexes.

No engine invocation or public-head change. Every accepted HTTP attempt is
journalled once; incomplete campaigns require inspection under a new request.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone,date,timedelta
from fractions import Fraction
from urllib.parse import urlsplit,parse_qs
import hashlib,re,sys,time,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6046_short_volume_baseline as base
import short_volume_source_index as index
import offexchange_measurements as measures
BUCKET=base.BUCKET;PRIVATE=base.PRIVATE;MAX=8*1024*1024
REQUEST='chatgpt-short-volume-original-history-6048'
BASELINE={'key':PRIVATE+'494c3166776891a534b1bf020969c32a2986b71ce582f4ff5ad9582d2d360faf.bin','sha256':'494c3166776891a534b1bf020969c32a2986b71ce582f4ff5ad9582d2d360faf','bytes':5922}
def now():return datetime.now(timezone.utc).isoformat()
def key(label):return PRIVATE+'requests/'+hashlib.sha256((REQUEST+':'+label).encode()).hexdigest()+'.json'
STATUS=key('campaign')

def journal(s3,label,value,claim=False):
    raw=base.retained.encoded(value);path=key(label)
    s3.put_object(Bucket=BUCKET,Key=path,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    assert base.read(s3,path)==raw

def response_bytes(stream,limit=MAX):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    if len(raw)>limit:raise ValueError('Whole source response exceeds bound')
    return raw

def protect(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>MAX:raise ValueError('Whole bounded source response required')
    digest=base.retained.sha(raw);path=PRIVATE+digest+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=path,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    assert response_bytes(s3.get_object(Bucket=BUCKET,Key=path)['Body'])==raw
    return {'key':path,'sha256':digest,'bytes':len(raw)}

def original(s3,ref):
    if not re.fullmatch(r'[a-f0-9]{64}',ref['sha256']) or ref['key']!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Source identity differs')
    raw=response_bytes(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or base.retained.sha(raw)!=ref['sha256']:raise ValueError('Source bytes differ')
    return raw

def reviewed(url):
    if index.FILE.fullmatch(url):return url
    parsed=urlsplit(url)
    if parsed.scheme!='https' or parsed.netloc!='www.finra.org' or parsed.path!=index.PATH or parsed.fragment:raise ValueError('Unreviewed public-source URL')
    if not parsed.query:return url
    query=parse_qs(parsed.query,keep_blank_values=True,strict_parsing=True)
    if set(query)!={index.MONTH,index.YEAR} or any(len(v)!=1 for v in query.values()):raise ValueError('Reviewed source filter required')
    if query[index.MONTH][0] not in [f'{v:02d}' for v in range(1,13)] or not re.fullmatch(r'\d{1,3}',query[index.YEAR][0]):raise ValueError('Invalid public-source filter')
    return url

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Unreviewed provider redirect refused')

def fetch(s3,label,url,deadline,transport=None):
    reviewed(url);journal(s3,label,{'request_id':REQUEST,'status':'claimed','url':url},True);started=now()
    try:
        if time.monotonic()>=deadline:raise TimeoutError('Source campaign deadline')
        request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-ShortVolumeResearch/2.0','Accept':'text/plain' if index.FILE.fullmatch(url) else 'text/html'})
        try:response=(transport or urllib.request.build_opener(NoRedirect()).open)(request,timeout=max(1,min(25,deadline-time.monotonic())))
        except urllib.error.HTTPError as exc:response=exc
        code=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in ('content-type','content-length','last-modified','etag','date')}
        raw=response_bytes(response);ref=protect(s3,raw)
        result={'url':url,'requested_at':started,'received_at':now(),'http_status':code,'headers':headers,'original':ref,
            'status':'response_retained' if code==200 and raw else 'empty_http_response' if not raw else 'provider_error_retained'}
        journal(s3,label,{'request_id':REQUEST,'status':'complete','capture':result});return result
    except Exception as exc:
        journal(s3,label,{'request_id':REQUEST,'status':'failed','url':url,'error_type':type(exc).__name__});raise

def successful(s3,capture):
    if capture['status']!='response_retained' or capture['http_status']!=200:raise ValueError('Published file is unavailable; no fabricated zero or retry')
    return original(s3,capture['original'])

def audit_daily(raw,stamp):
    rows=measures.cnms(raw,stamp);lines=raw.decode('utf-8-sig').splitlines();totals=[Fraction(0),Fraction(0),Fraction(0)]
    if not rows:raise ValueError('Listed CNMS file contains no observations')
    for row,line in zip(rows,lines[1:-1]):
        fields=line.split('|');values=[Fraction(v) for v in fields[2:5]]
        for n in range(3):totals[n]+=values[n]
        if values[2]:
            exact=values[0]*100/values[2]
            assert abs(Fraction(row['short_volume_pct'])-exact)<=Fraction(1,2*10**12)
        else:assert row['short_volume_pct'] is None
        assert row['short_includes_exempt'] is True
    assert len(rows)==int(lines[-1]) and len({r['symbol'] for r in rows})==len(rows)
    return {'observation_date':stamp,'rows':len(rows),'trailer_records':int(lines[-1]),'ratio_checks':len(rows),
        'zero_volume_rows':sum(r['total_volume_shares']=='0' for r in rows),
        'exact_share_sums_as_rationals':dict(zip(('short_including_exempt','short_exempt_subset','reported_total'),map(str,totals))),
        'scope':'FINRA disseminated regular-session NMS TRF/ADF volume','short_interest_available':False,'direction_inferred':False}

def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6048_short_volume_original_history') as r:
        try:prior=base.retained.strict(base.read(s3,STATUS))
        except Exception as exc:
            if not base.retained.missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect incomplete source campaign; never repeat it'
            ref=prior['manifest'];manifest=base.retained.strict(original(s3,ref))
        else:
            assert base.retained.strict(base.checked(s3,BASELINE))['contract']=='short-volume-baseline.v1'
            journal(s3,'campaign',{'request_id':REQUEST,'status':'claimed','started_at':now()},True)
            progress={'captures':{},'errors':{}};deadline=time.monotonic()+240;today=datetime.now(timezone.utc).date()
            try:
                seed=fetch(s3,'index:discovery',index.URL,deadline);progress['captures']['index:discovery']=seed
                discovery=index.parse(successful(s3,seed),today);plan=index.month_requests(today,discovery);indexes=[]
                for spec in plan:
                    label='index:'+spec['period'];capture=fetch(s3,label,spec['url'],deadline);progress['captures'][label]=capture
                    parsed=index.parse(successful(s3,capture),today,spec['period']);indexes.append(parsed)
                    journal(s3,'campaign',{'request_id':REQUEST,'status':'capturing',**progress})
                selected=index.selected_files(indexes,today);assert date.fromisoformat(selected[-1]['observation_date'])>=today-timedelta(days=7),'Latest published file is unexpectedly old'
                progress.update(month_plan=plan,selected_files=selected,daily_audits={})
                def task(row):
                    stamp=row['observation_date'];capture=fetch(s3,'daily:'+stamp,row['url'],deadline)
                    return capture,audit_daily(successful(s3,capture),stamp)
                with ThreadPoolExecutor(max_workers=3) as pool:
                    jobs={pool.submit(task,row):row['observation_date'] for row in selected}
                    for future in as_completed(jobs):
                        stamp=jobs[future]
                        try:progress['captures']['daily:'+stamp],progress['daily_audits'][stamp]=future.result()
                        except Exception as exc:progress['errors'][stamp]=type(exc).__name__
                        journal(s3,'campaign',{'request_id':REQUEST,'status':'capturing',**progress})
                assert not progress['errors'],progress['errors'];assert len(progress['daily_audits'])==61
                parser_paths=('aws/shared/short_volume_source_index.py','aws/shared/offexchange_measurements.py')
                manifest={'contract':'short-volume-original-source-inventory.v1','generated_at':now(),'request_id':REQUEST,'selection_cutoff':today.isoformat(),
                    'baseline':BASELINE,**progress,'source_modules':{p:base.retained.sha((ROOT/p).read_bytes()) for p in parser_paths},
                    'rows_checked':sum(v['rows'] for v in progress['daily_audits'].values()),'historical_availability_verified':False,
                    'security_identity_continuity_verified':False,'research_compiler_qualified':False,
                    'provider_requests':len(progress['captures']),'engine_invocations':0,'public_head_writes':0,
                    'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
                ref=protect(s3,base.retained.encoded(manifest));journal(s3,'campaign',{'request_id':REQUEST,'status':'complete','manifest':ref})
            except Exception as exc:
                journal(s3,'campaign',{'request_id':REQUEST,'status':'failed','error_type':type(exc).__name__,**progress});raise
        protected={STATUS,ref['key'],BASELINE['key']}
        for label,capture in manifest['captures'].items():protected.update((key(label),capture['original']['key']))
        def deny(path):assert denied_with_retry('https://justhodl.ai/'+path) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+path)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(manifest=ref,files=len(manifest['daily_audits']),rows_checked=manifest['rows_checked'],daily_audits=manifest['daily_audits'],
            source_modules=manifest['source_modules'],protected_artifacts_checked=len(protected),provider_requests=manifest['provider_requests'],
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
