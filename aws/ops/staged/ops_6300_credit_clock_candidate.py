"""Read-only retained-source evaluation of a proposed Credit collection clock.

No publication, invocation, schedule change, provider request, credential lookup,
account read, notification or history write. Candidate output stays in memory.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,runpy,sys

ROOT=Path(__file__).resolve().parents[3]


def check_candidate(packet,proposed,clock,at):
    if 'replay' in proposed:raise ValueError('Candidate must not borrow a predecessor replay identity')
    omit={'version','freshness','replay'}
    if {k:v for k,v in packet.items() if k not in omit}!={k:v for k,v in proposed.items() if k not in omit}:
        raise ValueError('Candidate changed fields outside the declared clock policy')
    if any(proposed.get(k) is not False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')):
        raise ValueError('Candidate cannot gain investment permission')
    dates=[clock.stamp(row['source_valid_until']) for row in proposed['measurements'].values() if row.get('source_valid_until')]
    expected=min([clock.stamp(proposed['freshness']['pipeline_check_due_at']),*dates])
    if clock.stamp(proposed['freshness']['valid_until'])!=expected:raise ValueError('Observation deadline was not preserved')
    return {'old_collection_current':clock.collection_current(packet,at),
        'candidate_collection_current':clock.collection_current(proposed,at),
        'old_freshness':packet['freshness'],'candidate_freshness':proposed['freshness'],
        'unchanged_measurements':len(proposed['measurements']),'unchanged_comparisons':len(proposed['comparisons']),
        'evaluated_at':at,'candidate_published':False,'production_policy_changed':False,
        'scope':'Counterfactual collection-clock evaluation of the same original observations; not a current production reading.'}


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/lambdas/justhodl-credit-stress/source','aws/shared','scripts')]
    from ops_report import report
    from market_runtime_evidence import runtime
    import credit_research_store as store
    import credit_collection_clock as clock
    from replay_credit_research import verify
    baseline=runpy.run_path(str(Path(__file__).with_name('ops_6299_credit_observed_baseline.py')))
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')]
    with report('ops_6300_credit_clock_candidate') as r:
        before=runtime(*args,baseline['FN']);baseline['validate_observed_runtime'](before)
        read=store.reader(clients['s3'],'justhodl-dashboard-live');raw=read(store.CURRENT);packet=json.loads(raw)
        reproduced=verify(packet,read);proposed=clock.candidate(packet)
        result=check_candidate(packet,proposed,clock,datetime.now(timezone.utc).isoformat())
        if read(store.CURRENT)!=raw:raise ValueError('Public head changed during candidate evaluation')
        if runtime(*args,baseline['FN'])!=before:raise ValueError('Runtime changed during candidate evaluation')
        code=Path(clock.__file__).read_bytes()
        r.kv(actual_runtime=before,retained_original_replay=reproduced,candidate_evaluation=result,
             public_head={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},
             candidate_compiler={'bytes':len(code),'sha256':hashlib.sha256(code).hexdigest()},
             native_invocations=0,provider_requests=0,credential_reads=0,private_account_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,production_policy_changed=False)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
