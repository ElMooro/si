"""Preserve the ICI predecessor and probe two official public releases once.

No producer invocation, publication, account read, notification or schedule edit.
Full code and responses stay under the existing protected research prefix.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,json,subprocess,sys,urllib.request,urllib.error
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import bounded,schedule_evidence,verified_alias
from release_package_evidence import shared_imports
from ops_6141_signal_board_original_baseline import package_inventory
import retained_access_evidence as access
import liquidity_agent_triggers as triggers

FUNCTION='justhodl-ici-flows';BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/ici-research/'
REQUEST='chatgpt-ici-original-baseline-6165'
SOURCES={'mmf':'https://www.ici.org/research/stats/mmf',
         'combined_flows':'https://www.ici.org/research/stats/combined_flows'}
KEYS=('data/ici-flows.json','data/history/ici-mmf.json','data/history/ici-flows.json')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
now=lambda:datetime.now(timezone.utc).isoformat()
STATUS=PRIVATE+'requests/'+sha(REQUEST.encode())+'.json'

def code(exc):return str(getattr(exc,'response',{}).get('Error',{}).get('Code',type(exc).__name__))

def retain(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>64*1024*1024:raise ValueError('Whole bounded bytes required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])!=raw:raise ValueError('Retained baseline differs')
    return ref

def journal(s3,value,claim=False):
    raw=encoded(value)
    s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
    if bounded(s3.get_object(Bucket=BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Baseline journal differs')

def public_response(name,transport=None):
    if name not in SOURCES:raise ValueError('Reviewed official release required')
    req=urllib.request.Request(SOURCES[name],headers={'User-Agent':'JustHodl-research/1.0','Accept':'text/html'})
    opener=transport or urllib.request.build_opener(access.NoRedirect()).open
    try:response=opener(req,timeout=30)
    except urllib.error.HTTPError as exc:response=exc
    status=response.status
    headers={k.lower():v for k,v in response.headers.items() if k.lower() in ('content-type','content-length','etag','date','last-modified')}
    raw=bounded(response,8*1024*1024)
    if 'content-length' in headers and int(headers['content-length'])!=len(raw):raise ValueError('Incomplete source response')
    return raw,status,headers

def summary(raw):
    try:value=json.loads(raw)
    except (ValueError,UnicodeError):return {'status':'whole_unparsed_bytes','source_qualified':False}
    return {'type':type(value).__name__,'top_level_count':len(value) if isinstance(value,(dict,list)) else None,
            'source_qualified':False,'original_vintages_verified':False}

def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_ici_original_baseline.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(x,region_name='us-east-1') for x in ('s3','lambda','events','scheduler'))
    with report('ops_6165_ici_original_baseline') as r:
        progress={'contract':'ici-original-baseline.v1','request_id':REQUEST,'started_at':now(),'status':'claimed',
                  'provider_attempts':0,'captures':{},'sources':{},'repository_predecessors':{},'snapshot_atomic':False}
        journal(s3,progress,True)
        try:
            fn=lam.get_function(FunctionName=FUNCTION);cfg=fn['Configuration']
            if cfg['State']!='Active' or cfg['LastUpdateStatus']!='Successful':raise ValueError('Stable predecessor required')
            raw=bounded(urllib.request.urlopen(fn['Code']['Location'],timeout=40))
            if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=cfg['CodeSha256']:raise ValueError('Actual code hash differs')
            source=ROOT/'aws/lambdas'/FUNCTION/'source'
            paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
            expected={p.relative_to(source).as_posix():p.read_bytes() for p in paths}
            expected.update({p.name:p.read_bytes() for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
            if not expected:raise ValueError('Repository predecessor required')
            conf=json.loads((source.parent/'config.json').read_bytes());alias=verified_alias(lam,FUNCTION,cfg,conf)
            schedules=schedule_evidence(events,scheduler,FUNCTION,cfg['FunctionArn'],conf,alias)
            runtime={k:cfg.get(k) for k in ('FunctionName','CodeSha256','LastModified','Runtime','Handler','Timeout','MemorySize','Architectures')}
            progress['predecessor']={'runtime':runtime,'schedules':schedules,'zip':retain(s3,raw),
                'inventory':package_inventory(raw,expected),'triggers':triggers.collect(lam,scheduler,s3,cfg['FunctionArn'],BUCKET)}
            for key in (*KEYS,'data/ops/releases/'+FUNCTION+'.json'):
                try:obj=s3.get_object(Bucket=BUCKET,Key=key)
                except Exception as exc:
                    if code(exc) not in ('404','NoSuchKey'):raise
                    progress['captures'][key]={'status':'missing','source_key':key};continue
                body=bounded(obj['Body']);progress['captures'][key]={'source_key':key,'status':'whole_object_retained',
                    'original':retain(s3,body),'etag':obj['ETag'],'last_modified':obj['LastModified'].isoformat(),'summary':summary(body)}
            originals=paths+[source.parent/'config.json',ROOT/'ici-flows.html',ROOT/'assets/signal-board-registry.json']
            for path in originals:progress['repository_predecessors'][path.relative_to(ROOT).as_posix()]=retain(s3,path.read_bytes())
            for name,url in SOURCES.items():
                item={'url':url,'status':'attempt_recorded','requested_at':now()};progress['sources'][name]=item
                progress['provider_attempts']+=1;journal(s3,progress)
                try:body,status,headers=public_response(name)
                except (urllib.error.URLError,ConnectionError,TimeoutError) as exc:item.update(status='transport_unavailable',error_type=type(exc).__name__)
                else:item.update(status='whole_http_response_retained',http_status=status,headers=headers,original=retain(s3,body),source_qualified=False)
                item['received_at']=now();journal(s3,progress)
            after=lam.get_function_configuration(FunctionName=FUNCTION)
            if any(after.get(k)!=cfg.get(k) for k in runtime) or schedules!=schedule_evidence(events,scheduler,FUNCTION,cfg['FunctionArn'],conf,alias):raise ValueError('Predecessor changed during baseline')
            progress.update(status='complete',finished_at=now());manifest=retain(s3,encoded(progress));journal(s3,progress)
            protected={STATUS,manifest['key'],progress['predecessor']['zip']['key']}
            protected.update(v['key'] for v in progress['repository_predecessors'].values())
            protected.update(v['original']['key'] for v in [*progress['captures'].values(),*progress['sources'].values()] if 'original' in v)
            outcomes=[access.check(key) for key in sorted(protected)];privacy=access.summarize(outcomes)
            access_ref=retain(s3,encoded({'contract':'ici-baseline-access.v1','outcomes':outcomes}))
            outcomes.append(access.check(access_ref['key']));privacy=access.summarize(outcomes)
            r.kv(manifest=manifest,access_evidence=access_ref,**privacy,
                 predecessor_runtime=runtime,code_matches_repository=progress['predecessor']['inventory']['code_matches_repository'],
                 schedules=schedules,capture_status={k:v['status'] for k,v in progress['captures'].items()},
                 provider_results={k:{f:v.get(f) for f in ('status','http_status','error_type','original')} for k,v in progress['sources'].items()},
                 provider_attempts=progress['provider_attempts'],native_invocations=0,public_writes=0,history_writes=0,
                 account_reads=0,notifications_sent=0,schedules_changed=0,source_qualification=False,
                 scope='Whole protected predecessor baseline and two one-attempt official requests; no output is seeded or produced.')
            if not privacy['all_denied']:raise ValueError('Protected evidence must be denied anonymously')
        except Exception as exc:
            progress.update(status='failed',error_type=type(exc).__name__,finished_at=now());journal(s3,progress);raise

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
