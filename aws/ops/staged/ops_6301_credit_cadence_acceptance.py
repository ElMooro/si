"""Read-only exact-release and retained-source acceptance for the cadence batch.

No native invocation, provider request, credential lookup, downstream/private
account output read, data write or schedule change. Old public runs stay old.
"""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,subprocess,sys

ROOT=Path(__file__).resolve().parents[3]
FUNCTIONS=tuple('justhodl-'+n for n in ('ai-chat','bond-desk','bottleneck-boom','capitulation','credit-composite',
    'credit-stress','cycle-clock','market-extremes','morning-intelligence','vol-radar'))
PATHS=('aws/shared/credit_collection_clock.py','aws/shared/credit_research.py',
    'aws/lambdas/justhodl-credit-stress/source','aws/lambdas/justhodl-bond-desk/source')


def original_operating_settings(actual,original):
    for field in ('function_name','timeout','memory_mb','schedules','runtime','handler','architectures','role','ephemeral_storage_mb'):
        if actual[field]!=original[field]:raise ValueError('Original operating setting differs: '+field)


def main():
    import boto3
    sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared','aws/lambdas/justhodl-credit-stress/source','scripts')]
    from ops_report import report
    from market_runtime_evidence import runtime
    import credit_collection_clock as clock,credit_research_store as store
    from replay_credit_research import verify
    # Later ops-report/docs commits do not change this exact native batch identity.
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',*PATHS],cwd=ROOT,text=True).strip()
    credit=json.loads((ROOT/'docs/audit/2026-09-28/credit-cadence-investigation.json').read_text(encoding='utf-8'))['observed_baseline']['actual_runtime']
    bond=json.loads((ROOT/'docs/audit/2026-09-28/bond-desk-normal-publication-acceptance.json').read_text(encoding='utf-8'))['runtime_acceptance']
    clients={name:boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')}
    args=[clients[name] for name in ('lambda','s3','events','scheduler')]
    with report('ops_6301_credit_cadence_acceptance') as r:
        before={name:runtime(*args,name) for name in FUNCTIONS}
        if any(row['receipt']!={'status':'matched','commit':expected} for row in before.values()):
            raise ValueError('Every affected native receipt must match the intended cadence batch')
        original_operating_settings(before['justhodl-credit-stress'],credit)
        original_operating_settings(before['justhodl-bond-desk'],bond)
        read=store.reader(clients['s3'],'justhodl-dashboard-live');raw=read(store.CURRENT);packet=json.loads(raw)
        replay=verify(packet,read);at=datetime.now(timezone.utc).isoformat()
        current=clock.collection_current(packet,at)
        if read(store.CURRENT)!=raw:raise ValueError('Public credit head changed during acceptance')
        if {name:runtime(*args,name) for name in FUNCTIONS}!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtimes=before,retained_original_replay=replay,
             public_head={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'generated_at':packet['generated_at'],'version':packet['version']},
             freshness=packet['freshness'],evaluated_at=at,collection_current=current,
             exact_code_release_verified=True,normal_new_policy_publication_verified=packet['version']=='2.1.0',
             native_invocations=0,provider_requests=0,credential_reads=0,downstream_output_reads=0,private_account_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact ten-function code/receipt acceptance and credit retained-source replay. A legacy public packet is not relabeled as a new-policy publication. No portfolio authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
