"""Read-only exact admin package/receipt and unchanged native settings.

No account, current, provider or history reads; no producer invocation or writes.
The predecessor was already verified by operation 6361 / 36683289126.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET
FUNCTION='justhodl-portfolio-admin'


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FUNCTION+'.json'}:raise ValueError('Only the exact admin release receipt may be read')
        return self.client.get_object(**request)


def validate(actual,expected,baseline):
    if actual.get('function_name')!=FUNCTION or actual.get('receipt')!={'status':'matched','commit':expected}:raise ValueError('Exact intended admin receipt required')
    if actual.get('source_files_checked')!=1 or type(actual['source_files_checked']) is not int:raise ValueError('Complete reviewed admin source closure required')
    for key in ('function_name','source_files_checked','memory_mb','timeout','runtime','handler','architectures','role','ephemeral_storage_mb','schedules'):
        if actual.get(key)!=baseline[key]:raise ValueError('Original native setting differs: '+key)
    if (actual['memory_mb'],actual['timeout'],actual['handler'])!=(256,30,'lambda_function.lambda_handler') or actual['schedules']:
        raise ValueError('Original unscheduled admin resources required')


def main():
    import boto3
    from ops_report import report
    baseline=json.loads((ROOT/'docs/audit/2026-09-30/watchlist-admin-integrity.json').read_bytes())['predecessor_runtime']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    with report('ops_6362_watchlist_admin_runtime_acceptance') as result:
        clients=(lam,ReceiptOnly(s3),events,scheduler)
        actual=runtime(*clients,FUNCTION);result.kv(actual_runtime=actual,expected_commit=expected)
        validate(actual,expected,baseline)
        if runtime(*clients,FUNCTION)!=actual:raise ValueError('Native runtime changed during acceptance')
        result.kv(exact_native_package_verified=True,all_original_resources_and_schedules_preserved=True,
                  normal_private_mutations_verified=False,native_invocations=0,current_packet_reads=0,archive_history_reads=0,
                  private_reads=0,provider_requests=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
