"""Read-only post-repair native package/control acceptance; no engine data access."""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
FN='justhodl-symbology-master'
SOURCE_HASHES={'aws/lambdas/justhodl-symbology-master/source/lambda_function.py': '1b1c01e82fe1afc59c942c4b44d1490de4179b92911c589db19ce29e8273aa3c', 'aws/lambdas/justhodl-symbology-master/source/bond_symbology.py': '6ca251a7c996592510a5cbda5eab38403729838ec5cd2364dd488e552706250d', 'aws/lambdas/justhodl-symbology-master/source/equity_identity.py': '73c93e15c6d195b05d792bc0ce1e6224ff6b2f36922f16c56150cb7c257499d3', 'aws/shared/openfigi.py': '4888f7598ac84440dc429afac4b7a0e53dd97be3550c6c54eedf0d6a20b855e5'}
EXPECTED_CONTROL={'timeout': 120, 'memory_mb': 1024, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-symbology-master-daily', 'state': 'ENABLED', 'expression': 'cron(15 5 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-symbology-master', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}

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
    if value['source_files_checked'] != 5 or {k:value.get(k) for k in EXPECTED_CONTROL} != EXPECTED_CONTROL:
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
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed predecessor source changed')
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptOnly(s3),events,scheduler)
    commit=expected_commit()
    with report('ops_6399_equity_identity_integrity_acceptance') as out:
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
          'scope':'Exact native package, public release receipt and selected resource/schedule controls only. No environment values, signed package URL, queue, master, original archive, data publication or logs are returned.'})


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
