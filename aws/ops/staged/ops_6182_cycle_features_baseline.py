"""Retain the complete Cycle Features predecessor and every declared S3 input.

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

FUNCTION='justhodl-cycle-features';BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/cycle-feature-research/'
OUTPUTS=('data/cycle/features.json.gz','data/cycle/features-manifest.json','data/ops/releases/justhodl-cycle-features.json')
INPUTS=(
    'data/warm/oecd/cycle/DF_CLI.csv.gz','data/warm/oecd/cycle/DF_KEI.csv.gz',
    'data/warm/oecd/cycle/DF_BTS.csv.gz','data/warm/oecd/cycle/DF_CS.csv.gz','data/warm/oecd/cycle/DF_FINMARK.csv.gz',
    'data/warm/bis/cycle/WS_EER_M.csv.gz','data/warm/bis/cycle/WS_CBPOL_M.csv.gz',
    'data/warm/oecd/data/DSD_KEI@DF_KEI.dat.gz','data/warm/oecd/data/DSD_LFS@DF_IALFS_UNE_M.dat.gz',
    'data/warm/bis/data/WS_TC.dat.gz','data/warm/bis/data/WS_SPP.dat.gz','data/warm/bis/data/WS_EER.dat.gz',
    'data/warm/bis/data/WS_CREDIT_GAP.dat.gz','data/warm/bis/data/WS_DSR.dat.gz',
    'data/warm/eurostat/data/EI_BSCO_M.dat.gz','data/warm/eurostat/data/EI_BSSI_M_R2.dat.gz',
    'data/warm/eurostat/data/EI_BSIN_M_R2.dat.gz','data/warm/eurostat/data/EI_LMHR_M.dat.gz',
    'data/asia-leads.json','data/global-sovereign.json')
KEYS=INPUTS+OUTPUTS
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
now=lambda:datetime.now(timezone.utc).isoformat()
STATUS=PRIVATE+'requests/'+sha(b'chatgpt-cycle-feature-original-baseline-6182')+'.json'


def source_reads(source):
    """Audit literal inputs and reviewed parameter families without importing AWS code."""
    tree=ast.parse(source);keys=set();delegates=[]
    assignments={t.id:n.value for n in tree.body if isinstance(n,ast.Assign) for t in n.targets if isinstance(t,ast.Name)}
    keys.add(ast.literal_eval(assignments['CLI_CACHE_KEY']))
    lanes=assignments['LANES']
    if not isinstance(lanes,ast.Dict):raise ValueError('Explicit source lanes required')
    for lane in lanes.values:
        if not isinstance(lane,ast.Tuple) or len(lane.elts)!=3:raise ValueError('Unreviewed lane shape')
        for node in lane.elts[1:]:
            key=ast.literal_eval(node)
            if key is not None:keys.add(key)
    for node in ast.walk(tree):
        if not isinstance(node,ast.Call):continue
        if isinstance(node.func,ast.Attribute) and node.func.attr=='get_object':
            delegates.append(ast.unparse(node))
        if not isinstance(node.func,ast.Name):continue
        name=node.func.id
        if name not in ('get_bytes','get_json','read','eurostat_tsv'):continue
        if len(node.args)!=1 or node.keywords:raise ValueError('Unreviewed source signature')
        arg=node.args[0]
        if name in ('get_bytes','get_json') and isinstance(arg,ast.Constant):keys.add(ast.literal_eval(arg))
        elif name=='read':
            value=ast.literal_eval(arg)
            if value not in ('WS_TC','WS_SPP','WS_EER','WS_CREDIT_GAP','WS_DSR'):raise ValueError('Unreviewed BIS input')
            keys.add('data/warm/bis/data/'+value+'.dat.gz')
        elif name=='get_bytes' and isinstance(arg,ast.Name) and arg.id in ('key','cache_key','fallback','CLI_CACHE_KEY'):pass
        elif name=='get_bytes' and ast.unparse(arg) in ("f'data/warm/bis/data/{fid}.dat.gz'","f'data/warm/eurostat/data/{flow}.dat.gz'"):pass
        elif name=='eurostat_tsv' and isinstance(arg,ast.Name) and arg.id=='flow':pass
        else:raise ValueError('Unreviewed dynamic source')
    if delegates!=['S3.get_object(Bucket=BUCKET, Key=key)']:raise ValueError('Unreviewed S3 read delegation')
    eu=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='load_eurostat')
    spec=next(n.value for n in eu.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='spec' for t in n.targets))
    for row in ast.literal_eval(spec):keys.add('data/warm/eurostat/data/'+row[0]+'.dat.gz')
    if keys!=set(INPUTS):raise ValueError('Whole source allowlist differs')
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
    raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):raise ValueError('Whole source ContentLength differs')
    ref=retain(s3,raw)
    try:packet=json.loads(raw)
    except (ValueError,UnicodeError):packet=None
    metadata={'decoded_type':type(packet).__name__,'original_provider_verified':False,'forecast_qualified':False}
    if isinstance(packet,dict):metadata.update(top_level_keys=sorted(packet),generated_at=packet.get('generated_at'),contract=packet.get('contract'),
        permission_declarations={k:packet.get(k) for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')})
    return {'status':'whole_object_retained','original':ref,'captured_at':now(),'etag':obj['ETag'],
        'last_modified':obj['LastModified'].isoformat(),'metadata':metadata}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_cycle_features_baseline.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    source=ROOT/'aws/lambdas'/FUNCTION/'source';reads=source_reads((source/'lambda_function.py').read_text(encoding='utf-8'))
    with report('ops_6182_cycle_features_baseline') as r:
        progress={'contract':'cycle-feature-baseline.v1','started_at':now(),'status':'claimed','snapshot_atomic':False,'source_reads':reads,'captures':{}}
        def journal(claim=False):
            raw=encode(progress);s3.put_object(Bucket=BUCKET,Key=STATUS,Body=raw,ContentType='application/json',CacheControl='no-store',**({'IfNoneMatch':'*'} if claim else {}))
            if bounded(s3.get_object(Bucket=BUCKET,Key=STATUS)['Body'])!=raw:raise ValueError('Baseline journal differs')
        journal(True)
        try:
            fn=lam.get_function(FunctionName=FUNCTION);cfg=fn['Configuration']
            if cfg.get('Environment',{}).get('Variables',{}).get('S3_BUCKET',BUCKET)!=BUCKET:raise ValueError('Unexpected source bucket')
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
            progress['repository_sources']={p.relative_to(ROOT).as_posix():retain(s3,p.read_bytes()) for p in [*paths,source.parent/'config.json',ROOT/'global-cycle.html']}
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
