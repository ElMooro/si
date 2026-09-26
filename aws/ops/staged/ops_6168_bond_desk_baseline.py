"""Retain the complete Bond Desk predecessor and every declared S3 input.

Research baseline only: no provider requests, producer invocation, account read,
public output, history modification, notification or cadence change.
"""
from pathlib import Path
from datetime import datetime,timezone
import ast,base64,hashlib,json,subprocess,sys,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded,schedule_evidence,verified_alias
from release_package_evidence import shared_imports
from ops_6141_signal_board_original_baseline import package_inventory
import retained_access_evidence as access

FUNCTION='justhodl-bond-desk';BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/bond-desk-research/'
KEYS=('data/bond-desk.json','data/history/bond-desk.json','data/history/bond-crisis-analogs.json',
      'etf-flows/daily.json','data/etf-true-flows.json','data/ici-flows.json','data/credit-stress.json',
      'data/credit-stress-history.json','data/bond-vol.json','data/auction-crisis.json','data/settlement-fails.json',
      'data/term-premium.json','data/dealer-survey.json','data/eurodollar-plumbing.json','data/euro-fragmentation.json',
      'data/systemic-stress.json','data/yen-carry.json','data/hkma.json','data/ops/releases/justhodl-bond-desk.json')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
now=lambda:datetime.now(timezone.utc).isoformat()
STATUS=PRIVATE+'requests/'+sha(b'chatgpt-bond-desk-original-baseline-6168')+'.json'


def source_reads(source):
    """Require coverage of every current literal/name-bound S3 read, no import."""
    tree=ast.parse(source);names={}
    for node in tree.body:
        if isinstance(node,ast.Assign):
            for target in node.targets:
                if isinstance(target,ast.Name) and isinstance(node.value,ast.Constant):names[target.id]=node.value.value
    keys=set()
    for node in ast.walk(tree):
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='_s3json':
            arg=node.args[0];key=names.get(arg.id) if isinstance(arg,ast.Name) else ast.literal_eval(arg)
            if key not in KEYS:raise ValueError('Unreviewed or private S3 input')
            keys.add(key)
    if not keys:raise ValueError('Whole source read inventory required')
    return sorted(keys)


def retain(s3,raw):
    if not isinstance(raw,bytes) or len(raw)>64*1024*1024:raise ValueError('Whole bounded predecessor required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])!=raw:raise ValueError('Whole retained predecessor differs')
    return ref


def capture(s3,key):
    if key not in KEYS:raise ValueError('Only declared non-account inputs may be read')
    try:obj=s3.get_object(Bucket=BUCKET,Key=key)
    except Exception as exc:
        code=str(getattr(exc,'response',{}).get('Error',{}).get('Code'))
        if code in ('NoSuchKey','404'):return {'status':'missing','captured_at':now()}
        raise
    raw=bounded(obj['Body']);ref=retain(s3,raw)
    try:packet=json.loads(raw)
    except (ValueError,UnicodeError):packet=None
    metadata={'decoded_type':type(packet).__name__,'original_provider_verified':False,'forecast_qualified':False}
    if isinstance(packet,dict):metadata.update(top_level_keys=sorted(packet),generated_at=packet.get('generated_at'),contract=packet.get('contract'),
        permission_declarations={k:packet.get(k) for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')})
    return {'status':'whole_object_retained','original':ref,'captured_at':now(),'etag':obj['ETag'],
        'last_modified':obj['LastModified'].isoformat(),'metadata':metadata}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_bond_desk_baseline.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    source=ROOT/'aws/lambdas'/FUNCTION/'source';reads=source_reads((source/'lambda_function.py').read_text(encoding='utf-8'))
    with report('ops_6168_bond_desk_baseline') as r:
        progress={'contract':'bond-desk-baseline.v1','started_at':now(),'status':'claimed','snapshot_atomic':False,'source_reads':reads,'captures':{}}
        def journal(claim=False):
            raw=encode(progress);s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
            if bounded(s3.get_object(Bucket=BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Baseline journal differs')
        journal(True)
        try:
            fn=lam.get_function(FunctionName=FUNCTION);cfg=fn['Configuration']
            if cfg['State']!='Active' or cfg['LastUpdateStatus']!='Successful':raise ValueError('Stable predecessor required')
            raw=bounded(urllib.request.urlopen(fn['Code']['Location'],timeout=40))
            if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=cfg['CodeSha256']:raise ValueError('Actual package identity differs')
            paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
            expected={p.relative_to(source).as_posix():p.read_bytes() for p in paths}
            expected.update({p.name:p.read_bytes() for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
            conf=json.loads((source.parent/'config.json').read_bytes());alias=verified_alias(lam,FUNCTION,cfg,conf)
            schedules=schedule_evidence(events,scheduler,FUNCTION,cfg['FunctionArn'],conf,alias)
            fields=('FunctionName','CodeSha256','LastModified','Runtime','Handler','Timeout','MemorySize','Architectures')
            runtime={key:cfg.get(key) for key in fields}
            progress['predecessor']={'runtime':runtime,'schedules':schedules,'zip':retain(s3,raw),'inventory':package_inventory(raw,expected)}
            progress['repository_sources']={p.relative_to(ROOT).as_posix():retain(s3,p.read_bytes()) for p in [*paths,source.parent/'config.json',ROOT/'bond-desk.html']}
            for key in KEYS:progress['captures'][key]=capture(s3,key);journal()
            after=lam.get_function_configuration(FunctionName=FUNCTION)
            if any(after.get(k)!=v for k,v in runtime.items()) or schedule_evidence(events,scheduler,FUNCTION,cfg['FunctionArn'],conf,alias)!=schedules:
                raise ValueError('Runtime changed during baseline')
            progress.update(status='complete',finished_at=now());ref=retain(s3,encode(progress));journal()
            protected={STATUS,ref['key'],progress['predecessor']['zip']['key']}
            protected.update(v['key'] for v in progress['repository_sources'].values())
            protected.update(v['original']['key'] for v in progress['captures'].values() if 'original' in v)
            privacy=access.summarize([access.check(key) for key in sorted(protected)])
            r.kv(baseline=ref,actual_runtime=runtime,schedules=schedules,source_inventory=reads,
                source_files_checked=progress['predecessor']['inventory']['source_files_checked'],
                code_matches_repository=progress['predecessor']['inventory']['code_matches_repository'],
                source_difference_names=sorted(progress['predecessor']['inventory']['source_differences']),
                capture_status={key:value['status'] for key,value in progress['captures'].items()},**privacy,
                native_invocations=0,provider_requests=0,public_writes=0,history_writes=0,account_reads=0,notifications_sent=0,schedules_changed=0,
                scope='Complete predecessor and derived S3 input baseline. Source arithmetic, historical vintages, investment and portfolio qualification remain unverified.')
            if not privacy['all_denied']:raise ValueError('Retained predecessor must remain private')
        except Exception as exc:progress.update(status='failed',error_type=type(exc).__name__);journal();raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
