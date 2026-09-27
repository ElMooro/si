"""Preserve complete PortWatch code, output, context and compressed history before repair.

No ArcGIS/provider requests, native invocations, account reads, notifications,
public/history mutations or schedule changes. Retention does not qualify shipping signals.
"""
from pathlib import Path
from datetime import datetime,timezone
import ast,base64,gzip,hashlib,json,subprocess,sys,urllib.request
from io import BytesIO
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded,schedule_evidence,verified_alias
from release_package_evidence import shared_imports
from ops_6141_signal_board_original_baseline import package_inventory
import retained_access_evidence as access
FN='justhodl-portwatch';BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/portwatch-research/'
SOURCE_SHA='165f5b7853eb33a17ab7e21bc576fc68239f499dead0a1b2ea75a263f7d7abd5'
INPUTS={'data/warm/portwatch/history/daily-rows.json.gz','data/import-canary.json'}
KEYS=(*sorted(INPUTS),'data/portwatch.json','data/ops/releases/'+FN+'.json')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')


def source_reads(raw):
    if sha(raw)!=SOURCE_SHA:raise ValueError('Complete reviewed source identity required')
    tree=ast.parse(raw.decode('utf-8'));names={}
    for n in tree.body:
        if isinstance(n,ast.Assign) and isinstance(n.value,ast.Constant):
            names.update({t.id:n.value.value for t in n.targets if isinstance(t,ast.Name)})
    reads=set()
    for n in ast.walk(tree):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='get_object':
            keys=[k.value for k in n.keywords if k.arg=='Key']
            if len(keys)!=1:raise ValueError('One explicit source key required')
            key=names.get(keys[0].id) if isinstance(keys[0],ast.Name) else ast.literal_eval(keys[0])
            if key not in INPUTS:raise ValueError('Unreviewed source read')
            reads.add(key)
    if reads!=INPUTS:raise ValueError('Complete source population required')
    return sorted(reads)


def decoded_metadata(raw,key):
    try:
        decoded=bounded(gzip.GzipFile(fileobj=BytesIO(raw))) if key.endswith('.gz') else raw
        p=json.loads(decoded)
    except (ValueError,UnicodeError,OSError,EOFError):
        return {'decoded_type':'invalid','original_provider_verified':False}
    meta={'decoded_type':type(p).__name__,'original_provider_verified':False,'decoded_bytes':len(decoded)}
    if not isinstance(p,dict):return meta
    meta.update(generated_at=p.get('generated_at'),version=p.get('version'),contract=p.get('contract'))
    if key=='data/portwatch.json':
        meta.update(chokepoint_rows=len(p.get('chokepoints') or []),port_rows=len(p.get('ports') or []),
                    exporter_rows=len(p.get('exporters') or []),history_through=p.get('history_through'),
                    source_metric=p.get('metric_field'),ports_metric=p.get('ports_metric'),requests=p.get('requests'),errors=p.get('errors'))
    if key.endswith('daily-rows.json.gz'):
        families={}
        for name in ('choke','choke_fallback','ports'):
            rows=p.get(name)
            if rows is None:families[name]={'status':'absent'};continue
            if not isinstance(rows,dict):families[name]={'status':'malformed'};continue
            dates=[];ids=set();bad=0
            for ident,row in rows.items():
                parts=ident.split('|',1)
                if len(parts)!=2 or not isinstance(row,dict):bad+=1;continue
                ids.add(parts[0]);dates.append(parts[1])
            families[name]={'status':'stored','rows':len(rows),'distinct_ids':len(ids),'malformed_rows':bad,
                            'first_date':min(dates,default=None),'last_date':max(dates,default=None)}
        meta['families']=families
        meta['stored_through']=p.get('through')
    return meta


def retain(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>64*1024*1024:raise ValueError('Complete bounded bytes required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);back=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(back) or back!=raw:raise ValueError('Whole retained readback differs')
    return ref


def capture(s3,key):
    if key not in KEYS:raise ValueError('Only reviewed public research/history objects allowed')
    try:obj=s3.get_object(Bucket=BUCKET,Key=key)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) in ('NoSuchKey','404'):return {'status':'missing'}
        raise
    raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):
        raise ValueError('Whole versioned stored object required')
    ref=retain(s3,raw)
    meta=decoded_metadata(raw,key)
    return {'status':'whole_object_retained','original':ref,'etag':obj['ETag'],
            'last_modified':obj['LastModified'].isoformat(),'metadata':meta}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_portwatch_original_baseline.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    source=ROOT/'aws/lambdas'/FN/'source';config=source.parent/'config.json'
    with report('ops_6200_portwatch_original_baseline') as r:
        reads=source_reads((source/'lambda_function.py').read_bytes())
        deployed=lam.get_function(FunctionName=FN);cfg=deployed['Configuration']
        if cfg['State']!='Active' or cfg['LastUpdateStatus']!='Successful':raise ValueError('Stable actual producer required')
        raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40))
        if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=cfg['CodeSha256']:raise ValueError('Actual deployment package differs')
        paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
        shared=shared_imports(ROOT,paths)
        expected={p.relative_to(source).as_posix():p.read_bytes() for p in paths}
        expected.update({p.name:p.read_bytes() for p in shared if not (source/p.name).exists()})
        inventory=package_inventory(raw,expected)
        conf=json.loads(config.read_bytes()) if config.exists() else {}
        alias=verified_alias(lam,FN,cfg,conf)
        schedules=schedule_evidence(events,scheduler,FN,cfg['FunctionArn'],conf,alias)
        fields=('FunctionName','CodeSha256','LastModified','Runtime','Handler','Timeout','MemorySize','Architectures','Role','EphemeralStorage')
        runtime={k:cfg.get(k) for k in fields}
        baseline={'contract':'portwatch-original-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                  'snapshot_atomic':False,'runtime':runtime,'schedules':schedules,'whole_zip':retain(s3,raw),
                  'inventory':inventory,'source_reads':reads,'config_present':config.exists(),
                  'repository_sources':{p.relative_to(ROOT).as_posix():retain(s3,p.read_bytes()) for p in [*paths,*shared,*([config] if config.exists() else [])]},
                  'captures':{key:capture(s3,key) for key in KEYS}}
        after=lam.get_function_configuration(FunctionName=FN)
        if any(after.get(k)!=v for k,v in runtime.items()) or schedule_evidence(events,scheduler,FN,cfg['FunctionArn'],conf,alias)!=schedules:
            raise ValueError('Runtime changed during baseline')
        ref=retain(s3,encode(baseline))
        protected={ref['key'],baseline['whole_zip']['key']}
        protected.update(v['key'] for v in baseline['repository_sources'].values())
        protected.update(v['original']['key'] for v in baseline['captures'].values() if 'original' in v)
        privacy=access.summarize([access.check(k) for k in sorted(protected)])
        r.kv(baseline=ref,actual_runtime=runtime,schedules=schedules,config_present=config.exists(),
             source_reads=reads,source_files_checked=inventory['source_files_checked'],code_matches_repository=inventory['code_matches_repository'],
             source_difference_names=sorted(inventory['source_differences']),
             captures={k:{x:y for x,y in v.items() if x in ('status','original','metadata')} for k,v in baseline['captures'].items()},**privacy,
             native_invocations=0,provider_requests=0,account_reads=0,notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Whole code/current/import-context/compressed-history baseline. Original ArcGIS responses, query completeness, measurement definitions, comparable calendar windows and forecasting permission remain unverified.')
        if not privacy['all_denied']:raise ValueError('Retained baseline must remain private')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
