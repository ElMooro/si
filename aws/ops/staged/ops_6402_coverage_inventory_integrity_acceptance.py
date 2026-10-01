"""Read-only post-repair native package/control acceptance; no engine data access."""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
FN='justhodl-coverage-gap-report'
SOURCE_HASHES={'aws/lambdas/justhodl-coverage-gap-report/source/lambda_function.py': 'a6049b0395273e29d9b09989d202264c50c3d9e196af8c26f5d9eefbb4edb0dc', 'aws/lambdas/justhodl-coverage-gap-report/source/coverage_model.py': 'add5bc00054096e95609b8156e8743b72fed07aeb5b6cc1dd7ed11b1ec6c04fc', 'aws/lambdas/justhodl-coverage-gap-report/source/coverage_store.py': '993c7256ba409b2f19df7155dba6c263455eccd130ddeb1b3c46fcb9d9986f69'}
EXPECTED_CONTROL={'timeout': 120, 'memory_mb': 1024, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-coverage-gap-daily', 'state': 'ENABLED', 'expression': 'cron(45 6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-coverage-gap-report', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}

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
    if value['source_files_checked'] != 3 or {k:value.get(k) for k in EXPECTED_CONTROL} != EXPECTED_CONTROL:
        raise ValueError('Native sources or original controls differ')
    for row in value['schedules']:
        if not isinstance(row,dict) or any(type(row.get(k)) is not str or not row[k] for k in ('kind','name','state','expression')):
            raise ValueError('Complete native schedule identity required')
        if type(row.get('native_targets')) is not int or row['native_targets']<1:raise ValueError('Bound native target required')


def normalized(value, commit):
    validate(value, commit)
    return {**value,'schedules':sorted(value['schedules'],key=lambda r:(r['kind'],r.get('group','default'),r['name']))}


def main():
    import boto3
    from ops_report import report
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed repaired source changed')
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    commit=expected_commit()
    with report('ops_6402_coverage_inventory_integrity_acceptance') as out:
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
          'scope':'Exact native package, public release receipt and selected resource/schedule controls only. No environment values, signed package URL, source summaries, original archive, current report or logs are returned.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
