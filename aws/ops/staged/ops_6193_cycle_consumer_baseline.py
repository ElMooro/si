"""Preserve actual cycle consumer code before changing decision boundaries.

Only the two public cycle research inputs are read. No consumer execution,
provider request, account/learning-log read, notification or public write.
"""
from pathlib import Path
from datetime import datetime, timezone
import ast, base64, hashlib, json, subprocess, sys, urllib.request
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import bounded, verified_alias, schedule_evidence
from release_package_evidence import shared_imports
from ops_6141_signal_board_original_baseline import package_inventory
import retained_access_evidence as access
BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/cycle-consumer-research/'
PINS={
    'justhodl-katlin':'bb872a9505e5704d1074755756b80cbb5fc5a3dc251ec19e0d9d4df1fafc9eb4',
    'justhodl-allocator':'eedab991776cd74dd09f90e51de929507ec8ca0a725d393ceee3aff9acf840c6',
    'justhodl-morning-intelligence':'32c45231b62de0bd04076e1316a81169510f469992363c0ba23953b6a725ba5d',
}
KEYS=('data/global-business-cycle.json','data/global-recession.json')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def retain(client,raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=64*1024*1024:raise ValueError('Complete bounded original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:client.put_object(Bucket=BUCKET,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if bounded(client.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])!=raw:raise ValueError('Whole original readback differs')
    return ref


def capture(client,key):
    if key not in KEYS:raise ValueError('Only the two declared public cycle inputs may be read')
    obj=client.get_object(Bucket=BUCKET,Key=key);raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or not obj.get('ETag'):raise ValueError('Whole versioned input required')
    return {'original':retain(client,raw),'etag':obj['ETag'],'last_modified':obj['LastModified'].isoformat()}


def source_check(function,raw):
    if function not in PINS or sha(raw)!=PINS[function]:raise ValueError('Unreviewed cycle consumer source')
    tree=ast.parse(raw.decode('utf-8'))
    if not any(isinstance(n,ast.Constant) and n.value=='data/global-business-cycle.json' for n in ast.walk(tree)):
        raise ValueError('Expected cycle dependency missing')
    return {'bytes':len(raw),'sha256':sha(raw),'scope':'Complete source pinned without importing or executing its producer'}


def runtime(lam,s3,events,scheduler,function):
    source=ROOT/'aws/lambdas'/function/'source'
    try:deployed=lam.get_function(FunctionName=function)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code'))=='ResourceNotFoundException':
            return {'status':'function_not_deployed','function_name':function}
        raise
    cfg=deployed['Configuration']
    if cfg['State']!='Active' or cfg['LastUpdateStatus']!='Successful':raise ValueError('Stable deployed predecessor required')
    raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40))
    if base64.b64encode(hashlib.sha256(raw).digest()).decode()!=cfg['CodeSha256']:raise ValueError('Actual package hash differs')
    paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p.read_bytes() for p in paths}
    shared=shared_imports(ROOT,paths)
    expected.update({p.name:p.read_bytes() for p in shared if not (source/p.name).exists()})
    inventory=package_inventory(raw,expected)
    config=source.parent/'config.json';conf=json.loads(config.read_bytes()) if config.exists() else {}
    alias=verified_alias(lam,function,cfg,conf)
    schedules=schedule_evidence(events,scheduler,function,cfg['FunctionArn'],conf,alias)
    fields=('FunctionName','CodeSha256','Runtime','Handler','Timeout','MemorySize','Architectures','Role','EphemeralStorage','LastModified')
    result={'status':'whole_actual_package_retained','runtime':{k:cfg.get(k) for k in fields},
            'inventory':inventory,'whole_zip':retain(s3,raw),'schedules':schedules,'active_alias':alias,
            'repository_config_present':config.exists(),
            'repository_sources':{p.relative_to(ROOT).as_posix():retain(s3,p.read_bytes()) for p in [*paths,*shared,*([config] if config.exists() else [])]}}
    after=lam.get_function_configuration(FunctionName=function)
    if any(after.get(k)!=cfg.get(k) for k in fields):raise ValueError('Runtime changed during retention')
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_cycle_consumer_baseline.py')],cwd=ROOT,check=True)
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report('ops_6193_cycle_consumer_baseline') as r:
        baseline={'contract':'cycle-consumer-baseline.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                  'snapshot_atomic':False,'source_checks':{},'consumers':{},'inputs':{}}
        for function in PINS:
            raw=(ROOT/'aws/lambdas'/function/'source/lambda_function.py').read_bytes()
            baseline['source_checks'][function]=source_check(function,raw)
            baseline['consumers'][function]=runtime(clients['lambda'],clients['s3'],clients['events'],clients['scheduler'],function)
        for key in KEYS:baseline['inputs'][key]=capture(clients['s3'],key)
        baseline['status']='complete_for_declared_scope'
        ref=retain(clients['s3'],encode(baseline))
        protected=[ref['key'],*[r['whole_zip']['key'] for r in baseline['consumers'].values() if 'whole_zip' in r]]
        privacy=access.summarize([access.check(k) for k in protected])
        if not privacy['all_denied']:raise ValueError('Retained code must stay private')
        summaries={fn:{k:v for k,v in row.items() if k not in ('inventory','repository_sources')} for fn,row in baseline['consumers'].items()}
        for fn,row in baseline['consumers'].items():
            if 'inventory' in row:
                summaries[fn].update(code_matches_repository=row['inventory']['code_matches_repository'],
                    source_files_checked=row['inventory']['source_files_checked'],source_differences=row['inventory']['source_differences'])
        r.kv(baseline=ref,consumer_runtimes=summaries,whole_source_pins=baseline['source_checks'],
             input_bytes={key:row['original']['bytes'] for key,row in baseline['inputs'].items()},**privacy,
             native_invocations=0,provider_requests=0,account_reads=0,learning_log_reads=0,notifications_sent=0,
             public_writes=0,history_writes=0,schedules_changed=0,
             scope='Exact code and two cycle inputs only. Other market/warehouse acquisitions and historical research outputs are outside this baseline; no consumer performance or forecast is qualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
