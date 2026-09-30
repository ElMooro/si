"""Read-only snapshot/admin native source, receipts and original runtime bindings.

No private/current/history/provider packets, producer invocations, account writes
or resource/schedule changes. The admin package is included because its existing
shared accounting tests participate in the next related source batch.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET
FUNCTIONS={'justhodl-portfolio-snapshot':(512,180),'justhodl-portfolio-admin':(256,30)}
BASELINE_PATH=ROOT/'docs/audit/2026-09-30/portfolio-accounting-integrity.json'


class ReceiptsOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        allowed={'data/ops/releases/'+fn+'.json' for fn in FUNCTIONS}
        if set(request)!={'Bucket','Key'} or request['Bucket']!=BUCKET or request['Key'] not in allowed:
            raise ValueError('Only the two reviewed portfolio release receipts may be read')
        return self.client.get_object(**request)


def validate(actual,expected=None,baseline=None):
    function=actual.get('function_name')
    if function not in FUNCTIONS or actual.get('receipt',{}).get('status')!='matched':raise ValueError('Exact reviewed native function and receipt required')
    if type(actual.get('source_files_checked')) is not int or actual['source_files_checked']<1:raise ValueError('Complete native source closure required')
    if (actual.get('memory_mb'),actual.get('timeout'))!=FUNCTIONS[function] or actual.get('handler')!='lambda_function.lambda_handler':raise ValueError('Original declared resources differ')
    if expected is not None and actual['receipt'].get('commit')!=expected:raise ValueError('Exact intended source batch required')
    if baseline is not None:
        for key in ('function_name','source_files_checked','memory_mb','timeout','runtime','handler','architectures','role','ephemeral_storage_mb'):
            if actual.get(key)!=baseline[key]:raise ValueError('Original native setting differs: '+key)
        order=lambda rows:sorted(rows,key=lambda row:(row['kind'],row.get('group','default'),row['name']))
        if order(actual.get('schedules',[]))!=order(baseline['schedules']):raise ValueError('Original schedule bindings differ')
    if function=='justhodl-portfolio-snapshot':
        primary=[row for row in actual.get('schedules',[]) if (row['kind'],row['name'],row['expression'])==('EventBridge rule','justhodl-portfolio-snapshot-hourly','cron(40 * * * ? *)')]
        if len(primary)!=1 or primary[0]['state']!='ENABLED' or primary[0]['native_targets']!=1:raise ValueError('Original active snapshot binding required')


def main():
    import boto3
    from ops_report import report
    baseline=json.loads(BASELINE_PATH.read_bytes()).get('predecessor_runtime') if BASELINE_PATH.exists() else None
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-portfolio-snapshot/source'],cwd=ROOT,text=True).strip() if baseline else None
    lam,s3,events,scheduler=[boto3.client(service,region_name='us-east-1') for service in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptsOnly(s3),events,scheduler)
    with report('ops_6359_portfolio_accounting_runtime_acceptance') as r:
        actual={}
        for function in FUNCTIONS:
            before=runtime(*clients,function);r.kv(function=function,actual_runtime=before,expected_commit=expected)
            validate(before,expected,baseline[function] if baseline else None)
            if runtime(*clients,function)!=before:raise ValueError('Native runtime changed during acceptance')
            actual[function]=before
        r.kv(actual_runtimes=actual,phase='post_change' if baseline else 'predecessor_baseline',exact_native_packages_verified=True,
             all_original_resources_and_schedules_preserved=True if baseline else None,normal_private_publication_verified=False,
             native_invocations=0,current_packet_reads=0,archive_history_reads=0,private_reads=0,provider_requests=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
