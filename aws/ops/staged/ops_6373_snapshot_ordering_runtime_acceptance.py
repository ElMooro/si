"""Read-only acceptance of the complete ordered snapshot source closure.

No account/current/history/provider packet reads, producer invocation, native
writes or schedule changes. Only reviewed release receipts may be read from S3.
"""
from pathlib import Path
import json,subprocess,sys,time
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared','scripts')]
from market_runtime_evidence import runtime,BUCKET
from check_worker_prerequisite import read_receipt,source_identity,validate as validate_worker,check_snapshot_origin
FUNCTIONS={'justhodl-portfolio-snapshot':(6,512,180)}


class ReceiptOnly:
    def __init__(self,client,function):
        if function not in FUNCTIONS:raise ValueError('Unreviewed native function')
        self.client,self.function=client,function
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/'+self.function+'.json'}:raise ValueError('Only the selected exact release receipt may be read')
        return self.client.get_object(**request)


class WorkerReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/worker-justhodl-data-proxy.json'}:raise ValueError('Only the exact Worker receipt may be read')
        return self.client.get_object(**request)


def validate(actual,expected,baseline,function):
    if function not in FUNCTIONS:raise ValueError('Unreviewed native function')
    count,memory,timeout=FUNCTIONS[function]
    if actual.get('function_name')!=function or actual.get('receipt')!={'status':'matched','commit':expected}:raise ValueError('Exact intended function receipt required')
    if type(actual.get('source_files_checked')) is not int or actual['source_files_checked']!=count:raise ValueError('Complete reviewed source closure required')
    if (actual.get('memory_mb'),actual.get('timeout'))!=(memory,timeout):raise ValueError('Original resource limits required')
    for key in ('memory_mb','timeout','runtime','handler','architectures','role','ephemeral_storage_mb'):
        if actual.get(key)!=baseline[key]:raise ValueError('Original native setting differs: '+key)
    def order(rows):
        if type(rows) is not list or any(type(row) is not dict or type(row.get('kind')) is not str or type(row.get('name')) is not str for row in rows):raise ValueError('Whole original schedule inventory required')
        return sorted(rows,key=lambda row:(row['kind'],row.get('group','default'),row['name']))
    if order(actual.get('schedules'))!=order(baseline['schedules']):raise ValueError('Original schedule bindings differ')
    if function=='justhodl-portfolio-snapshot':
        alias=actual.get('active_alias',{})
        if alias.get('alias')!='live' or type(alias.get('version')) is not str or not alias['version'].isdigit() or int(alias['version'])<1 or alias.get('code_sha256')!=actual.get('code_sha256'):raise ValueError('Exact promoted snapshot alias required')


def verify_runtime(result):
    import boto3
    baseline=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-snapshot-byte-publication.json').read_bytes())
    paths=['aws/lambdas/justhodl-portfolio-snapshot/source']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',*paths],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    function='justhodl-portfolio-snapshot'
    prior=baseline['native_acceptance']['runtimes'][function]
    clients=(lam,ReceiptOnly(s3,function),events,scheduler)
    actual=runtime(*clients,function);validate(actual,expected,prior,function)
    if runtime(*clients,function)!=actual:raise ValueError('Native package or settings changed during acceptance')
    worker_expected=source_identity(function=function)
    if worker_expected['commit']!='f3e77dcb361941e7d9bbf298c1f2ed93275bc0c4':raise ValueError('Reviewed ordered snapshot Worker prerequisite differs')
    worker=validate_worker(read_receipt(WorkerReceiptOnly(s3),time.monotonic()+20),worker_expected,function=function)
    if not check_snapshot_origin(lam):raise ValueError('Native snapshot origin differs')
    result.kv(expected_commit=expected,runtime=actual,exact_native_package_verified=True,worker_prerequisite=worker,reviewed_snapshot_origin_verified=True,
        all_original_resources_and_schedules_preserved=True,normal_private_publication_verified=False,
        native_invocations=0,current_packet_reads=0,archive_history_reads=0,private_reads=0,provider_requests=0,schedule_changes=0)
    return actual


def main():
    from ops_report import report
    with report('ops_6373_snapshot_ordering_runtime_acceptance') as result:verify_runtime(result)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
