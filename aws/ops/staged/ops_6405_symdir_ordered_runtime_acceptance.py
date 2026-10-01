"""Read-only post-repair native package/control acceptance; no engine data access."""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
FN='justhodl-symdir'
SOURCE_HASHES={'aws/lambdas/justhodl-symdir/source/lambda_function.py': '0ea1c8dfb3155a4ecac0785ae258e91fe7579beef0d8f328430522211bca5b74', 'aws/lambdas/justhodl-symdir/source/warehouse_routing.py': '64c20bbf47726dad25ad83ccacad0ad3f50b683a8f03676fdc21728d09e532b1', 'aws/shared/treasury_fiscal_model.py': 'aa7627c565fe088662bef508dde9cf993f59e3acdd8e855a8e75b2b42af25fa3', 'aws/lambdas/justhodl-symdir/source/directory_identity.py': '2022ec3d4067ac603e7e1eac5db0e8941c3f3af4beb47d973b4ebbfa9b87df51'}
EXPECTED_CONTROL={'timeout': 900, 'memory_mb': 6144, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-build', 'state': 'ENABLED', 'expression': 'cron(40 5 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-codelists', 'state': 'ENABLED', 'expression': 'rate(20 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-fredfresh', 'state': 'ENABLED', 'expression': 'rate(1 hour)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-fredupdates', 'state': 'ENABLED', 'expression': 'rate(15 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-titles', 'state': 'ENABLED', 'expression': 'rate(1 hour)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-ustbank', 'state': 'ENABLED', 'expression': 'cron(30 21 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-symdir-warm', 'state': 'ENABLED', 'expression': 'rate(5 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-symdir', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 2048}

def expected_commit():
    return subprocess.check_output(["git","log","-1","--format=%H","--",*SOURCE_HASHES],cwd=ROOT,text=True).strip()


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kw):
        if kw!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FN+'.json'}:raise ValueError('Exact public native receipt only')
        return self.client.get_object(**kw)


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def validate(value, commit):
    if value.get('function_name')!=FN or value.get('receipt')!={'status':'matched','commit':commit}:
        raise ValueError('Exact repaired function receipt required')
    for key in ('source_files_checked','handler_bytes','timeout','memory_mb','ephemeral_storage_mb'):
        if type(value.get(key)) is not int or value[key]<=0:raise ValueError('Positive exact runtime counts required')
    for key in ('code_sha256','runtime','handler','role'):
        if type(value.get(key)) is not str or not value[key]:raise ValueError('Native control identity required')
    if not isinstance(value.get('architectures'),list) or not value['architectures'] or not isinstance(value.get('schedules'),list):
        raise ValueError('Complete native control arrays required')
    if not all(type(x) is str and x for x in value['architectures']):raise ValueError('Architecture identity required')
    if value['source_files_checked'] != 4 or {k:value.get(k) for k in EXPECTED_CONTROL} != EXPECTED_CONTROL:
        raise ValueError('Native sources or original controls differ: '+','.join(['source_files_checked'] if value['source_files_checked'] != 4 else [k for k in EXPECTED_CONTROL if value.get(k)!=EXPECTED_CONTROL[k]]))
    for row in value['schedules']:
        if not isinstance(row,dict) or any(type(row.get(k)) is not str or not row[k] for k in ('kind','name','state','expression')):
            raise ValueError('Complete native schedule identity required')
        if type(row.get('native_targets')) is not int or row['native_targets']<1:raise ValueError('Bound native target required')


def normalized(value, commit):
    # Scheduler pagination has no ordering contract. Normalize only ordering;
    # preserve every entry, duplicate, binding, cadence and operating setting.
    schedules=value.get('schedules')
    if not isinstance(schedules,list) or not all(isinstance(row,dict) for row in schedules):
        raise ValueError('Complete native schedule rows required')
    for row in schedules:
        for key in ('kind','name','group'):
            if key in row and type(row[key]) is not str:
                raise ValueError('Typed native schedule identity required')
    value={**value,'schedules':sorted(schedules,key=lambda row:(row.get('kind',''),row.get('group','default'),row.get('name','')))}
    validate(value, commit)
    return value


def main():
    import boto3
    from ops_report import report
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed repaired source changed')
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    commit=expected_commit()
    with report('ops_6405_symdir_ordered_runtime_acceptance') as out:
        before=normalized(runtime(*clients,FN),commit);after=normalized(runtime(*clients,FN),commit)
        if encoded(before)!=encoded(after):raise ValueError('Native producer changed during acceptance inspection')
        cfg=json.loads((ROOT/'aws/lambdas'/FN/'config.json').read_bytes())
        declared={'function_name':cfg['function_name'],'runtime':cfg['runtime'],'handler':cfg['handler'],
                  'timeout':cfg['timeout'],'memory_mb':cfg['memory'],'role':cfg['role']}
        matches=encoded({k:before[k] for k in declared})==encoded(declared)
        enabled=bool(before['schedules']) and all(r['state']=='ENABLED' for r in before['schedules'])
        out.kv(evidence={'status':'exact_native_release_checked',
          'expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,'native_before':before,'native_after':after,
          'declared_settings':declared,'declared_settings_match':matches,'all_observed_schedules_enabled':enabled,
          'normal_publication_verified':False,'source_replay_verified':False,'investment_authority':False,
          'native_invocations':0,'provider_requests':0,'current_packet_reads':0,'private_reads':0,'account_reads':0,
          'consumer_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0,
          'scope':'Exact native package, public release receipt and selected resource/schedule controls only. No environment values, signed package URL, symbol directory, index, master, original archive, current series, account artifacts or logs are returned.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
