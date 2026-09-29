"""Exact auction release packages, receipts, original resources and all bindings.

Read-only: excludes packets, histories, accounts, outcomes, providers and secrets.
No native invocation or schedule change. Ordinary publication stays separate.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET

FUNCTIONS=('justhodl-auction-desk',)
BASELINE_PATH=ROOT/'docs/audit/2026-09-29/auction-reaction-inputs-predecessor.json'


class ReceiptsOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if set(request)!={'Bucket','Key'} or request['Bucket']!=BUCKET or request['Key'] not in {
                'data/ops/releases/'+fn+'.json' for fn in FUNCTIONS}:
            raise ValueError('Only the reviewed auction desk release receipt may be read')
        return self.client.get_object(**request)


def validate(actual,fn,expected,baseline):
    if actual.get('function_name')!=fn or actual.get('receipt')!={'status':'matched','commit':expected}:
        raise ValueError('Exact function and source commit required')
    if type(actual.get('source_files_checked')) is not int or actual['source_files_checked']<baseline['source_files_checked']:
        raise ValueError('Whole source closure required')
    for key in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb'):
        if actual.get(key)!=baseline[key]:raise ValueError('Original native resource differs: '+key)
    def ordered(rows):return sorted(rows,key=lambda v:(v['kind'],v.get('group','default'),v['name']))
    if ordered(actual.get('schedules',[]))!=ordered(baseline['schedules']):
        raise ValueError('Complete original schedule bindings differ')


def main():
    import boto3
    from ops_report import report
    baseline=json.loads(BASELINE_PATH.read_bytes())['actual_runtimes']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-auction-desk/source/lambda_function.py','aws/shared/auction_reactions.py'],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=[boto3.client(s,region_name='us-east-1') for s in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptsOnly(s3),events,scheduler)
    with report('ops_6343_auction_reaction_inputs_runtime_acceptance') as r:
        before={fn:runtime(*clients,fn) for fn in FUNCTIONS}
        r.kv(actual_runtimes=before,expected_commit=expected)
        for fn,value in before.items():validate(value,fn,expected,baseline[fn])
        after={fn:runtime(*clients,fn) for fn in FUNCTIONS}
        if before!=after:raise ValueError('Auction runtime changed during read-only acceptance')
        r.kv(exact_native_packages_verified=True,all_original_resources_and_schedules_preserved=True,
            native_publication_verified=False,native_invocations=0,current_packet_reads=0,
            archive_history_reads=0,private_reads=0,provider_requests=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
