"""Receipt/package/resource acceptance for the existing Calls independent auditor only.

No producer invocation, current/history/protected packet read, provider call,
portfolio write or schedule change. Baseline and post-change evidence are separate.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,BUCKET
FUNCTIONS=('justhodl-calls-research-audit',)
RESOURCES={'justhodl-calls-research-audit':(1536,300)}
BASELINE_PATH=ROOT/'docs/audit/2026-09-29/calls-byte-proof-predecessor.json'



class ReceiptsOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**request):
        if set(request)!={'Bucket','Key'} or request['Bucket']!=BUCKET or request['Key'] not in {
                'data/ops/releases/'+fn+'.json' for fn in FUNCTIONS}:
            raise ValueError('Only the reviewed audit release receipt may be read')
        return self.client.get_object(**request)


def validate(actual,fn,expected=None,baseline=None):
    if actual.get('function_name')!=fn or fn not in FUNCTIONS or actual.get('receipt',{}).get('status')!='matched':
        raise ValueError('Exact native function and matched release receipt required')
    if type(actual.get('source_files_checked')) is not int or actual['source_files_checked']<1:
        raise ValueError('Whole native source closure required')
    if (actual.get('memory_mb'),actual.get('timeout'))!=RESOURCES[fn] or actual.get('handler')!='lambda_function.lambda_handler':
        raise ValueError('Original research resources/handler differ')
    if expected is not None and actual['receipt'].get('commit')!=expected:
        raise ValueError('Exact intended source commit required')
    if baseline is not None:
        if actual['source_files_checked']<baseline['source_files_checked']:raise ValueError('Source closure shrank')
        for key in ('timeout','memory_mb','runtime','handler','architectures','role','ephemeral_storage_mb'):
            if actual.get(key)!=baseline[key]:raise ValueError('Original runtime differs: '+key)
        def ordered(rows):return sorted(rows,key=lambda v:(v['kind'],v.get('group','default'),v['name']))
        if ordered(actual.get('schedules',[]))!=ordered(baseline['schedules']):raise ValueError('Complete original schedule bindings differ')
    schedules=actual.get('schedules',[])
    if not schedules or not any(row.get('state')=='ENABLED' and row.get('native_targets')==1 and row.get('expression') for row in schedules):
        raise ValueError('Existing active native schedule required')



def main():
    import boto3
    from ops_report import report
    baseline={'justhodl-calls-research-audit':json.loads(BASELINE_PATH.read_bytes())['native_acceptance']}
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-calls-research-audit/source/lambda_function.py'],cwd=ROOT,text=True).strip()
    lam,s3,events,scheduler=[boto3.client(s,region_name='us-east-1') for s in ('lambda','s3','events','scheduler')]
    clients=(lam,ReceiptsOnly(s3),events,scheduler)
    with report('ops_6349_calls_byte_proof_runtime_acceptance') as r:
        before={fn:runtime(*clients,fn) for fn in FUNCTIONS}
        r.kv(actual_runtimes=before,expected_commit=expected,phase='post_change' if baseline else 'predecessor_baseline')
        for fn,value in before.items():validate(value,fn,expected,baseline[fn] if baseline else None)
        after={fn:runtime(*clients,fn) for fn in FUNCTIONS}
        if before!=after:raise ValueError('Native runtime changed during acceptance')
        r.kv(exact_native_packages_verified=True,original_resources_and_bindings_recorded=True,
            all_original_resources_and_schedules_preserved=True if baseline else None,
            native_publication_verified=False,native_invocations=0,current_packet_reads=0,
            archive_history_reads=0,private_reads=0,provider_requests=0,schedule_changes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
