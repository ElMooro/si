"""Read only portfolio risk release, complete native package and original runtime/schedules.

Never invokes the producer, reads current/history/research packets, probes a
provider, or changes resources, schedules or portfolio state.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET
FUNCTION='justhodl-portfolio-risk'
BASELINE_PATH=ROOT/'docs/audit/2026-09-30/portfolio-native-ordering.json'


class ReceiptsOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if set(request)!={'Bucket','Key'} or request['Bucket']!=BUCKET or request['Key']!='data/ops/releases/'+FUNCTION+'.json':
            raise ValueError('Only the reviewed portfolio risk release receipt may be read')
        return self.client.get_object(**request)


def validate(actual,expected=None,baseline=None):
    if actual.get('function_name')!=FUNCTION or actual.get('receipt',{}).get('status')!='matched':
        raise ValueError('Exact native function and matched release receipt required')
    if type(actual.get('source_files_checked')) is not int or actual['source_files_checked']<1:
        raise ValueError('Whole native source closure required')
    if actual.get('memory_mb')!=1024 or actual.get('timeout')!=300 or actual.get('handler')!='lambda_function.lambda_handler':
        raise ValueError('Original portfolio risk resources/handler differ')
    if expected is not None and actual['receipt'].get('commit')!=expected:
        raise ValueError('Exact intended source commit required')
    if baseline is not None:
        if baseline['source_files_checked']!=7 or actual['source_files_checked']!=7:raise ValueError('Expected the unchanged seven-member source closure')
        for key in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb'):
            if actual.get(key)!=baseline[key]:raise ValueError('Original runtime differs: '+key)
        def ordered(rows):return sorted(rows,key=lambda v:(v['kind'],v.get('group','default'),v['name']))
        if ordered(actual.get('schedules',[]))!=ordered(baseline['schedules']):raise ValueError('Complete original schedule bindings differ')
    selected=[row for row in actual.get('schedules',[]) if (row['name'],row['kind'],row['expression'])==('justhodl-portfolio-risk-hourly','EventBridge rule','cron(43 * * * ? *)')]
    if len(selected)!=1 or selected[0]['state']!='ENABLED' or selected[0]['native_targets']!=1:raise ValueError('Original active portfolio risk schedule required')


def main():
    import boto3
    from ops_report import report
    baseline=json.loads(BASELINE_PATH.read_bytes())['native_acceptance']['actual_runtime']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-portfolio-risk/source'],cwd=ROOT,text=True).strip() if baseline else None
    lam,s3,events,scheduler=[boto3.client(s,region_name='us-east-1') for s in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptsOnly(s3),events,scheduler)
    with report('ops_6357_portfolio_calculation_runtime_acceptance') as r:
        before=runtime(*clients,FUNCTION)
        r.kv(actual_runtime=before,expected_commit=expected,phase='post_change' if baseline else 'predecessor_baseline')
        validate(before,expected,baseline)
        after=runtime(*clients,FUNCTION)
        if before!=after:raise ValueError('Native runtime changed during acceptance')
        r.kv(exact_native_package_verified=True,original_resources_and_bindings_recorded=True,
            all_original_resources_and_schedules_preserved=True if baseline else None,
            native_publication_verified=False,native_invocations=0,current_packet_reads=0,
            archive_history_reads=0,private_reads=0,provider_requests=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
