"""Read-only acceptance of the first observed original-cadence Credit 2.1 run."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,sys
ROOT=Path(__file__).resolve().parents[3]
EXPECTED='8f48fc660a266e93b03e42b4a8a8826aa420a36f'
FN='justhodl-credit-stress'


def validate_new_publication(packet):
    if type(packet) is not dict or packet.get('version')!='2.1.0':raise ValueError('Original new-policy publication not yet present')
    at=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'))
    if at.tzinfo is None or at<datetime(2026,9,28,20,tzinfo=timezone.utc):raise ValueError('Publication precedes original collection opportunity')


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/lambdas/justhodl-credit-stress/source','aws/ops','aws/ops/checks','scripts')]
    from market_runtime_evidence import runtime
    from ops_report import report
    import credit_research_store as store,credit_collection_clock as clock
    from replay_credit_research import verify
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/credit-cadence-investigation.json').read_text(encoding='utf-8'))['observed_baseline']['actual_runtime']
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')]
    with report('ops_6309_credit_normal_cadence_acceptance') as r:
        before=runtime(*args,FN)
        if before['receipt']!={'status':'matched','commit':EXPECTED}:raise ValueError('Exact original native release differs')
        for field in ('timeout','memory_mb','schedules','runtime','handler','architectures','role','ephemeral_storage_mb'):
            if before[field]!=baseline[field]:raise ValueError('Original operating settings differ')
        read=store.reader(clients['s3'],'justhodl-dashboard-live');raw=read(store.CURRENT);packet=json.loads(raw)
        validate_new_publication(packet);replay=verify(packet,read)
        evaluated=datetime.now(timezone.utc).isoformat();current=clock.collection_current(packet,evaluated)
        if not current:raise ValueError('Observed new publication collection clock is no longer current')
        if read(store.CURRENT)!=raw or runtime(*args,FN)!=before:raise ValueError('Publication or runtime changed during acceptance')
        r.kv(expected_commit=EXPECTED,actual_runtime=before,public_head={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
             'generated_at':packet['generated_at'],'version':packet['version']},retained_original_replay=replay,
             evaluated_at=evaluated,freshness=packet['freshness'],collection_current=current,new_policy_publication_verified=True,
             native_invocations=0,provider_requests=0,downstream_output_reads=0,private_account_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact deployed Credit package, unchanged original schedules, and complete retained-original replay of the publication observed after the original 20:00 slot. No producer invocation or investment permission.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
