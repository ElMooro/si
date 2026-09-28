"""Replay the unchanged credit producer after observing both actual bindings.

The failed single-trigger assumption in 6297 remains rejected. This separately
checks exactly the two bindings retained by 6298, without accepting other drift.
No invocation, provider request, account read, schedule change or data write.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,runpy,sys

ROOT=Path(__file__).resolve().parents[3]
EXPECTED='9cb4a62b116865bbf9695493bcff820c87da9277'
FN='justhodl-credit-stress'
BINDINGS=(
    ('EventBridge rule','justhodl-credit-stress-cadence','cron(10 22 ? * MON-FRI *)','UTC','default'),
    ('EventBridge Scheduler','credit-stress-sched','cron(0 20 ? * MON-FRI *)','UTC','default'),
)


def validate_observed_runtime(value):
    if value['receipt']!={'status':'matched','commit':EXPECTED}:raise ValueError('Exact predecessor receipt required')
    if value['function_name']!=FN or value['timeout']!=300 or value['memory_mb']!=512:
        raise ValueError('Observed credit resources differ')
    actual=[]
    for row in value['schedules']:
        if row['state']!='ENABLED' or row['native_targets']!=1:raise ValueError('Observed binding state or target differs')
        actual.append((row['kind'],row['name'],row['expression'],row.get('timezone','UTC'),row.get('group','default')))
    if sorted(actual)!=sorted(BINDINGS):raise ValueError('Exact observed two-binding inventory required')


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-credit-stress/source','aws/shared','scripts')]
    from ops_report import report
    from market_runtime_evidence import runtime
    import credit_research_store as store
    from replay_credit_research import verify
    diagnosis=runpy.run_path(str(Path(__file__).with_name('ops_6297_credit_cadence_baseline.py')))['diagnosis']
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')]
    with report('ops_6299_credit_observed_baseline') as r:
        before=runtime(*args,FN);validate_observed_runtime(before)
        read=store.reader(clients['s3'],'justhodl-dashboard-live');raw=read(store.CURRENT);packet=json.loads(raw)
        reproduced=verify(packet,read);finding=diagnosis(packet,datetime.now(timezone.utc))
        if read(store.CURRENT)!=raw:raise ValueError('Public head changed during baseline')
        if runtime(*args,FN)!=before:raise ValueError('Runtime changed during baseline')
        r.kv(expected_commit=EXPECTED,actual_runtime=before,
             public_head={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},
             retained_original_replay=reproduced,cadence_diagnosis=finding,
             observed_two_binding_inventory_verified=True,original_single_binding_assumption_accepted=False,
             native_invocations=0,provider_requests=0,credential_reads=0,private_account_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact observed existing runtime and complete retained-source replay. No extension of expiry or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
