"""Read-only exact snapshot source/receipt, alias and original runtime bindings.

No private/current/provider/history reads, producer invocation or resource writes.
The predecessor snapshot was accepted by operation 6360 / 36678709268.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET
FUNCTION='justhodl-portfolio-snapshot'


class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if request!={'Bucket':BUCKET,'Key':'data/ops/releases/'+FUNCTION+'.json'}:raise ValueError('Only the exact snapshot release receipt may be read')
        return self.client.get_object(**request)


def validate(actual,expected,baseline):
    if actual.get('function_name')!=FUNCTION or actual.get('receipt')!={'status':'matched','commit':expected}:raise ValueError('Exact intended snapshot receipt required')
    if type(actual.get('source_files_checked')) is not int or actual['source_files_checked']!=3:raise ValueError('Complete reviewed snapshot source closure required')
    for key in ('function_name','source_files_checked','memory_mb','timeout','runtime','handler','architectures','role','ephemeral_storage_mb'):
        if actual.get(key)!=baseline[key]:raise ValueError('Original native setting differs: '+key)
    def order(rows):
        if not isinstance(rows,list) or any(not isinstance(row,dict) or not isinstance(row.get('kind'),str) or not isinstance(row.get('name'),str) for row in rows):
            raise ValueError('Complete original schedule metadata required')
        return sorted(rows,key=lambda row:(row['kind'],row.get('group','default'),row['name']))
    if order(actual.get('schedules',[]))!=order(baseline['schedules']):raise ValueError('Original schedule bindings differ')
    if (actual['memory_mb'],actual['timeout'],actual['handler'])!=(512,180,'lambda_function.lambda_handler'):raise ValueError('Original snapshot resources required')
    alias=actual.get('active_alias',{})
    if alias.get('alias')!='live' or not isinstance(alias.get('version'),str) or not alias['version'].isdigit() or int(alias['version'])<1 or alias.get('code_sha256')!=actual.get('code_sha256'):
        raise ValueError('One exact promoted live alias required')


def main():
    import boto3
    from ops_report import report
    baseline=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-book-read-integrity.json').read_bytes())['predecessor_runtime']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FUNCTION+'/source'],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    with report('ops_6363_portfolio_book_read_runtime_acceptance') as result:
        clients=(lam,ReceiptOnly(s3),events,scheduler)
        actual=runtime(*clients,FUNCTION);result.kv(actual_runtime=actual,expected_commit=expected)
        validate(actual,expected,baseline)
        if runtime(*clients,FUNCTION)!=actual:raise ValueError('Native runtime changed during acceptance')
        result.kv(exact_native_package_verified=True,all_original_resources_and_schedules_preserved=True,
                  normal_private_publication_verified=False,native_invocations=0,current_packet_reads=0,archive_history_reads=0,
                  private_reads=0,provider_requests=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
