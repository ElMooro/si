"""Exact repaired FI/FX package and preserved original schedule; no invocation."""
from pathlib import Path
import json
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from market_runtime_evidence import runtime,bounded

FN='justhodl-fifx-vol-migration'
BUCKET='justhodl-dashboard-live'


def main():
    import boto3
    from ops_report import report
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/fifx-normal-publication-acceptance.json').read_bytes())
    original=baseline['evidence']['actual_runtime']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/'+FN+'/source'],cwd=ROOT,text=True).strip()
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6316_fifx_concurrent_runtime') as r:
        actual=runtime(*clients,FN)
        if actual['receipt']!={'status':'matched','commit':expected}:
            raise ValueError('Exact repaired native receipt required')
        if {k:v for k,v in actual.items() if k not in ('receipt','code_sha256')} != {
            k:v for k,v in original.items() if k not in ('receipt','code_sha256')}:
            raise ValueError('Original FI/FX operating settings or complete package count changed')
        obj=clients[1].get_object(Bucket=BUCKET,Key='data/fifx-vol.json');raw=bounded(obj['Body'])
        if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):
            raise ValueError('Whole current publication required')
        import hashlib
        packet=json.loads(raw)
        r.kv(expected_commit=expected,actual_runtime=actual,
            current_publication={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),
                'generated_at':packet.get('generated_at'),'quality':packet.get('quality'),
                'matches_accepted_predecessor':hashlib.sha256(raw).hexdigest()==baseline['evidence']['native_publication']['sha256']},
            next_original_schedule_utc='2026-09-29T21:20:00+00:00',
            repaired_normal_publication_verified=False,native_invocations=0,provider_requests=0,
            private_journal_reads=0,learning_ledger_reads=0,downstream_live_head_reads=0,
            private_account_reads=0,public_writes=0,schedule_changes=0,
            scope='Actual repaired code and exact release only, with unchanged complete operating settings and original schedule. The current predecessor packet remains historical evidence; source recovery requires the next ordinary native run and complete replay.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
