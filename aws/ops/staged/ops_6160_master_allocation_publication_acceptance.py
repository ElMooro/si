"""Accept a normal new allocator publication without invoking it or touching holdings."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from master_allocation_publication import validate
FUNCTION='justhodl-master-allocator'
EXPECTED='9ae85b37199793501b0bbc7af9bc8e6b939cacb0'
BUCKET='justhodl-dashboard-live'


def main():
    lam,s3,events,scheduler,ssm=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler','ssm'))
    with report('ops_6160_master_allocation_publication_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_master_allocation_publication.py')],cwd=ROOT,check=True)
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        if before['receipt']!={'status':'matched','commit':EXPECTED}:raise ValueError('Exact allocator source receipt required')
        response=s3.get_object(Bucket=BUCKET,Key='data/master-allocation.json');raw=bounded(response['Body'])
        packet=json.loads(raw)
        machine=json.loads(ssm.get_parameter(Name='/justhodl/master-allocation/target',WithDecryption=False)['Parameter']['Value'])
        proof=validate(packet,machine,'2026-09-26T15:20:00+00:00',datetime.now(timezone.utc).isoformat())
        request=urllib.request.Request('https://justhodl.ai/data/master-allocation.json?exact=1&nogen=1',
            headers={'User-Agent':'justhodl-verify-release/1.0','Cache-Control':'no-cache'})
        if bounded(urllib.request.urlopen(request,timeout=40))!=raw:raise ValueError('Anonymous public output differs')
        if runtime(lam,s3,events,scheduler,FUNCTION)!=before:raise ValueError('Package or schedule changed during acceptance')
        r.kv(expected_commit=EXPECTED,actual_runtime=before,public_and_machine_boundary=proof,
            public_sha256=hashlib.sha256(raw).hexdigest(),public_bytes=len(raw),public_head_last_modified=response['LastModified'].isoformat(),
            producer_invocations=0,consumer_invocations=0,provider_requests=0,public_writes=0,
            private_account_reads=0,machine_target_reads=1,notifications_sent=0,schedules_changed=0,
            scope='Complete publication and non-actionable machine-target binding; heuristic calculations and market sources remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
