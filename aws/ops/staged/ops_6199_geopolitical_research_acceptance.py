"""Read-only code, full fixture and original-schedule news research acceptance."""
from pathlib import Path
import subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-geopolitical-risk/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
import geo_news_store as store
FN='justhodl-geopolitical-risk';BUCKET='justhodl-dashboard-live'
BASELINE='26fdf187025347feabe9af8f2cb0b257488283853952a39a277060dfaad64a6a'


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6199_geopolitical_research_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        actual=runtime(lam,s3,events,scheduler,FN)
        if actual['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact engine release required')
        baseline=store.strict(store.retained(s3,BUCKET,{'key':store.PRIVATE+BASELINE+'.bin','sha256':BASELINE,'bytes':28172}))
        for old,new in {'Runtime':'runtime','Handler':'handler','MemorySize':'memory_mb','Timeout':'timeout','Architectures':'architectures','Role':'role'}.items():
            if baseline['runtime'][old]!=actual[new]:raise ValueError('Original runtime changed: '+old)
        if baseline['runtime']['EphemeralStorage']['Size']!=actual['ephemeral_storage_mb'] or baseline['schedules']!=actual['schedules']:
            raise ValueError('Original cadence or storage changed')
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
        obj=s3.get_object(Bucket=BUCKET,Key=store.HEAD);raw=store.whole(obj['Body'],length=obj.get('ContentLength'))
        packet=store.strict(raw)
        native={'status':'pending_original_daily_1130_publication','generated_at':packet.get('generated_at'),'version':packet.get('version'),
                'bytes':len(raw),'sha256':store.sha(raw)}
        if packet.get('contract')==store.model.CONTRACT:
            replay=store.replay(s3,BUCKET,packet)
            history=s3.get_object(Bucket=BUCKET,Key=store.HISTORY)
            actual_history=store.whole(history['Body'],length=history.get('ContentLength'))
            planned=store.retained(s3,BUCKET,packet['publication_context']['planned_complete_history'])
            if actual_history!=planned:raise ValueError('Current complete history differs from retained plan')
            native.update(status='complete_native_original_response_and_history_replayed',replay=replay,
                          configured_feeds=packet['sources']['feeds_in_corpus'],attempted_feeds=packet['sources']['feeds_attempted'],
                          parsed_feeds=packet['sources']['feeds_responding'],quality=packet['quality']['status'])
        if runtime(lam,s3,events,scheduler,FN)!=actual:raise ValueError('Runtime changed during verification')
        r.kv(expected_commit=expected,actual_runtime=actual,baseline_sha256=BASELINE,native_publication=native,
             fixture_scope='Every actual configured feed (164) and country (22), all output fields and 120 retained legacy history dates; synthetic bodies do not substitute for native publication proof.',
             native_invocations=0,provider_requests=0,account_reads=0,notifications_sent=0,public_writes=0,history_writes=0,schedules_changed=0,
             scope='Whole observed RSS response and deterministic measurement/history replay. News truth, independence, historical vintages, predictive skill and portfolio permission are unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
