"""Read-only actual package, unchanged cadence and whole stored-history acceptance."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/lambdas/justhodl-boom-stage/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6194_cycle_consumer_acceptance import unchanged
import boom_history as history
import boom_measurements as measurements
FN='justhodl-boom-stage';BUCKET='justhodl-dashboard-live'
BASELINE='c835e015ac32b810ed943370b52b32b7500e627ef3515d431d9e98637d128b86'
BASELINE_KEY='audit-private/20260909-originals/shipping-input-research/'+BASELINE+'.bin'


def raw_read(s3,key):
    obj=s3.get_object(Bucket=BUCKET,Key=key);raw=bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength']!=len(raw):raise ValueError('Complete object required')
    return raw


def retained(s3,ref):
    if (not isinstance(ref,dict) or ref.get('key')!=history.PRIVATE+str(ref.get('sha256'))+'.bin'
            or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=history.LIMIT):raise ValueError('Exact retained identity required')
    raw=raw_read(s3,ref['key'])
    if len(raw)!=ref['bytes'] or history.sha(raw)!=ref['sha256']:raise ValueError('Retained original differs')
    return raw


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6208_boom_fred_calendar_acceptance') as r:
        raw=raw_read(s3,BASELINE_KEY)
        if len(raw)!=20814 or hashlib.sha256(raw).hexdigest()!=BASELINE:raise ValueError('Complete baseline differs')
        baseline=json.loads(raw)
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        actual=runtime(lam,s3,events,scheduler,FN)
        if actual['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact release receipt required')
        unchanged(baseline['consumers'][FN],actual)
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
        head_raw=raw_read(s3,history.HEAD);head=history.decode(head_raw)
        history_raw=raw_read(s3,history.HISTORY);stored=history.decode(history_raw)
        publication={'status':'pending_original_daily_1230_publication','generated_at':head.get('generated_at'),'version':head.get('version'),
                     'bytes':len(head_raw),'sha256':history.sha(head_raw)}
        review=head.get('history_preservation') or {}
        if review.get('contract')=='boom-whole-history.v1' and head.get('version')=='1.7.2':
            source=ROOT/'aws/lambdas'/FN/'source'
            compilers={n:history.sha((source/n).read_bytes()) for n in ('lambda_function.py','boom_history.py','boom_measurements.py')}
            compilers['managed_secret.py']=history.sha((ROOT/'aws/shared/managed_secret.py').read_bytes())
            if review.get('compiler_sha256')!=compilers:raise ValueError('Exact history compiler differs')
            if history_raw!=retained(s3,review['complete_planned_history']):raise ValueError('Complete published history differs')
            native=history.decode(retained(s3,review['complete_native_calculation']))
            if native.get('pairs')!=head.get('pairs'):raise ValueError('Published pair calculation differs')
            before=history.decode(retained(s3,review['predecessor_history'])) if review.get('predecessor_history') else {'days':{}}
            if any(stored['days'].get(k)!=v for k,v in before['days'].items()):raise ValueError('A previous research date changed')
            if len(stored['days'])!=len(before['days'])+1:raise ValueError('Unexpected history growth')
            if any(head.get(k) is not False for k in history.FLAGS):raise ValueError('Inherited model is not qualified')
            if native.get('fred_measurements')!=head.get('fred_measurements'):raise ValueError('Complete FRED output differs')
            fred=measurements.replay(head.get('fred_measurements'),lambda ref:retained(s3,ref))
            publication['fred_calendar_replay']=fred
            publication.update(status='complete_stored_history_and_fred_arithmetic_verified',previous_dates=len(before['days']),
                               current_dates=len(stored['days']),provider_original_replay=False)
        if runtime(lam,s3,events,scheduler,FN)!=actual:raise ValueError('Actual package changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=actual,baseline_sha256=BASELINE,native_publication=publication,
             stored_history={'bytes':len(history_raw),'sha256':history.sha(history_raw),'dates':len(stored['days']),
                             'pair_records':sum(len(v) for v in stored['days'].values())},
             native_invocations=0,provider_requests=0,account_reads=0,notifications_sent=0,
             public_writes=0,history_writes=0,schedules_changed=0,
             scope='Exact source and original runtime/cadence, seventeen isolated native/history/measurement cases, and retained stored calculation consistency. Whole FRED metadata and requested-window observations replayed only when a new native packet exists. Other provider acquisitions, original vintages and legacy model claims remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
