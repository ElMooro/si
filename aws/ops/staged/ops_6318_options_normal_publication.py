"""Read-only normal Options publication with separately shipped consumer receipts.

Consumers were legitimately upgraded after the Options producer. Verify each
intended release and all actual bytes; do not require an obsolete common SHA.
No consumer output or private state is read, and no producer is invoked.
"""
from pathlib import Path
import json
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6258_options_flow_acceptance as acceptance

EXPECTED={
    'justhodl-options-flow-scanner':'f9b0cc066ecaca3dde599b98b426b9d6483f479d',
    'justhodl-best-ideas':'b7db9ef147732700a4817e156eca2d2d53171896',
    'justhodl-flow-confluence':'f9b0cc066ecaca3dde599b98b426b9d6483f479d',
    'justhodl-options-confluence':'b3dd1c2b5f1ee461471d83a4fd733932c1bf694b'}
COUNTS={'justhodl-options-flow-scanner':4,'justhodl-best-ideas':5,
        'justhodl-flow-confluence':10,'justhodl-options-confluence':3}
PUBLIC_SHA='bdfeb030e1b5f16ef9493de70026100f8fef11f838709e86ae8e6d9bafd1dbc9'


def check_packages(actual, baselines):
    if set(actual)!=set(EXPECTED):raise ValueError('Complete Options producer/consumer package set required')
    for fn,count in COUNTS.items():
        acceptance.check_runtime(actual[fn],baselines[fn],EXPECTED[fn],count)


def main():
    import boto3
    from ops_report import report
    for path in ('aws/lambdas/justhodl-options-flow-scanner/tests/run_tests.py',
                 'tests/test_option_scanner_boundary.py','tests/ops/test_options_flow_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
    baselines=json.loads((ROOT/'docs/audit/2026-09-28/options-flow-original-baseline.json').read_bytes())['actual_producers']
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6318_options_normal_publication') as r:
        before={fn:acceptance.runtime(*args,fn) for fn in EXPECTED};check_packages(before,baselines)
        route=acceptance.fanout(clients)
        obj=clients['s3'].get_object(Bucket=acceptance.BUCKET,Key=acceptance.KEY)
        raw=acceptance.bounded(obj['Body'],64*1024*1024)
        if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw) or acceptance.hashlib.sha256(raw).hexdigest()!=PUBLIC_SHA:
            raise ValueError('Whole reviewed 22:00 normal Options publication required')
        packet=acceptance.compiler().strict(raw)
        if acceptance.read_public_archive(clients['s3'],acceptance.archive_ref(raw))!=raw:
            raise ValueError('Current immutable public archive differs')
        prior=acceptance.read_public_archive(clients['s3'],packet['previous_publication']) if packet.get('previous_publication') else None
        sources=acceptance.read_sources(clients['s3'],packet)
        result=acceptance.publication(raw,prior,sources)
        if any(acceptance.runtime(*args,fn)!=before[fn] for fn in EXPECTED) or acceptance.fanout(clients)!=route:
            raise ValueError('Runtime or routing changed during acceptance')
        r.kv(expected_commits=EXPECTED,actual_packages=before,fanout_route=route,
            native_publication=result,current_archive_verified=True,native_invocations=0,
            provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,
            learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
            scope='Whole normal Options producer and its retained originals, exact intended receipts for four independently shipped packages, unchanged runtime/fanout. Successful replay does not establish source availability, first-release history, independent signals or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
