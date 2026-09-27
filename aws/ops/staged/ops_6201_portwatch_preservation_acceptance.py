"""Read-only code, full fixture and original-schedule shipping research acceptance."""
from pathlib import Path
import subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-portwatch/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
import portwatch_store as store
import lambda_function as native
FN='justhodl-portwatch';BUCKET='justhodl-dashboard-live'
BASELINE='0c0642b96f7981e80e2199ccc85e18c18e2e012308ff5fa2bf24937c676c5167'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6201_portwatch_preservation_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        actual=runtime(lam,s3,events,scheduler,FN)
        if actual['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact engine release required')
        baseline=store.strict(store.retained(s3,BUCKET,{'key':store.PRIVATE+BASELINE+'.bin','sha256':BASELINE,'bytes':8723}))
        for old,new in {'Runtime':'runtime','Handler':'handler','MemorySize':'memory_mb','Timeout':'timeout','Architectures':'architectures','Role':'role'}.items():
            if baseline['runtime'][old]!=actual[new]:raise ValueError('Original runtime changed: '+old)
        if baseline['runtime']['EphemeralStorage']['Size']!=actual['ephemeral_storage_mb'] or baseline['schedules']!=actual['schedules']:
            raise ValueError('Original cadence or storage changed')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
        obj=s3.get_object(Bucket=BUCKET,Key=store.HEAD);raw=store.whole(obj['Body'],length=obj.get('ContentLength'))
        packet=store.strict(raw)
        publication={'status':'pending_original_daily_1120_publication','generated_at':packet.get('generated_at'),'version':packet.get('version')}
        if packet.get('contract')==store.CONTRACT:
            replay=store.replay(native,s3,BUCKET,packet)
            history=s3.get_object(Bucket=BUCKET,Key=store.HISTORY)
            actual_history=store.whole(history['Body'],length=history.get('ContentLength'))
            manifest=store.strict(store.retained(s3,BUCKET,packet['publication_context']['manifest']))
            planned=store.retained(s3,BUCKET,manifest['complete_native_outputs'][store.HISTORY])
            if actual_history!=planned:raise ValueError('Current complete history differs from retained plan')
            publication.update(status='complete_native_original_response_and_history_replayed',replay=replay)
        if runtime(lam,s3,events,scheduler,FN)!=actual:raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=expected,actual_runtime=actual,baseline_sha256=BASELINE,native_publication=publication,
             fixture_scope='Whole original compiler, five provider-query families and 4,800 synthetic retained history rows across eight entities; synthetic bodies do not substitute for complete native publication proof.',
             native_invocations=0,provider_requests=0,account_reads=0,notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Whole native ArcGIS HTTP responses and original deterministic calculations/history replay. Query population completeness, economic definitions, day windows, source revisions, historical vintages and portfolio consequences remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
