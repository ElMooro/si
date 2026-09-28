"""Exact new Genealogy package/resource envelope and unchanged original cadence.

Read-only package and configuration evidence. No legacy/private/current research
output, producer invocation, provider collection or actual artifact write.
"""
from pathlib import Path
import json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime
FN='justhodl-signal-genealogy'


def validate(actual,baseline,expected):
    if actual['receipt']!={'status':'matched','commit':expected} or actual['source_files_checked']!=17:
        raise ValueError('Exact complete native Genealogy package required')
    if (actual['memory_mb'],actual['timeout'])!=(2048,600):
        raise ValueError('Reviewed Genealogy runtime envelope differs')
    for field in ('function_name','runtime','handler','architectures','role','ephemeral_storage_mb','schedules'):
        if actual[field]!=baseline[field]:raise ValueError('Original operating setting changed: '+field)


def main():
    import boto3
    from ops_report import report
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/genealogy-calendar-audit.json').read_bytes())['runtime_baseline']['actual_runtime']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6321_genealogy_native_runtime_acceptance') as r:
        actual=runtime(*clients,FN);r.kv(actual_runtime=actual,expected_commit=expected)
        validate(actual,baseline,expected)
        if runtime(*clients,FN)!=actual:raise ValueError('Runtime changed during acceptance')
        r.kv(exact_package_accepted=True,original_cadence_preserved=True,
            next_original_schedule_utc='2026-09-29T06:40:00+00:00',
            native_publication_verified=False,actual_native_capacity_verified=False,
            native_invocations=0,provider_requests=0,learning_ledger_reads=0,private_account_reads=0,
            downstream_output_reads=0,research_head_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
            scope='Exact complete replacement package, reviewed 2048 MB/600-second envelope and original 06:40 schedule only. Actual native publication, durable transport and runtime capacity await the ordinary schedule. No old private-ledger-derived result is read.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
