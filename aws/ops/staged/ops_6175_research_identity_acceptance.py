"""Read-only package/identity acceptance; do not run either research producer."""
from pathlib import Path
import hashlib,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from prospective_journal import read_record
from research_identity import record_identity_issue,identity_policy

BUCKET='justhodl-dashboard-live'
FUNCTIONS=('justhodl-signal-harvester','justhodl-prospective-evaluator')


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6175_research_identity_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/research_identity.py'],cwd=ROOT,text=True).strip()
        before={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
        for fn,evidence in before.items():
            if evidence['receipt']!={'status':'matched','commit':expected}:raise ValueError('Exact package receipt required')
            memory,timeout=(1024,900) if fn==FUNCTIONS[0] else (512,300)
            if (evidence['memory_mb'],evidence['timeout'])!=(memory,timeout):raise ValueError('Runtime differs')
            subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')],cwd=ROOT,check=True)
        def raw(key):return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],8*1024*1024)
        current=raw('data/prospective-research.json');head=json.loads(current);ref=head['capture']
        if not re.fullmatch(r'data/research-forecasts/captures/[0-9a-f]{64}\.json',ref['key']):raise ValueError('Invalid public capture reference')
        capture=raw(ref['key'])
        if hashlib.sha256(capture).hexdigest()!=ref['sha256']:raise ValueError('Capture bytes differ')
        doc=json.loads(capture);excluded=[]
        for entry in doc['records']:
            record=read_record(s3,BUCKET,entry)
            issue=record_identity_issue(record)
            if issue:excluded.append({'forecast_id':record['forecast_id'],'source_key':record['source']['source_key'],'reason':issue})
        outcomes=raw('data/prospective-outcomes.json');outcome_doc=json.loads(outcomes)
        after={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
        if after!=before:raise ValueError('Runtime or schedule changed during acceptance')
        r.kv(expected_commit=expected,actual_runtimes=before,identity_policy=identity_policy(),
            existing_capture={'key':ref['key'],'sha256':ref['sha256'],'records_checked':len(doc['records']),
                              'generated_at':head['generated_at'],'known_crypto_exclusions':excluded,
                              'new_identity_policy_published':head.get('identity_policy')==identity_policy()},
            existing_outcomes={'generated_at':outcome_doc['generated_at'],'sha256':hashlib.sha256(outcomes).hexdigest(),
                               'new_identity_policy_published':outcome_doc.get('identity_policy')==identity_policy()},
            provider_requests=0,native_invocations=0,public_writes=0,history_writes=0,account_reads=0,schedules_changed=0,
            scope='Exact deployed packages and offline regression checks; existing public records preserved. Normal publication reported separately.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
