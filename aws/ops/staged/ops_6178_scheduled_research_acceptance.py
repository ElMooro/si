"""Read complete scheduled research and source-size metadata; no producer work."""
from pathlib import Path
from collections import Counter
import hashlib,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from prospective_journal import read_record
from research_identity import record_identity_issue,identity_policy
from private_artifact import public_source_allowed
BUCKET='justhodl-dashboard-live'
FUNCTIONS=('justhodl-signal-harvester','justhodl-prospective-evaluator')


def reconcile(head,capture,outcome,batch,policy):
    if head.get('identity_policy')!=policy or capture.get('identity_policy')!=policy or outcome.get('identity_policy')!=policy:raise ValueError('New scheduled policy not published')
    if capture.get('contract')!='prospective-research-capture.v1' or head.get('schema_version')!='prospective-research-summary.v1':raise ValueError('Capture contract differs')
    for key in ('generated_at','coverage','sizing_eligible','promotion_eligible'):
        if capture.get(key)!=head.get(key):raise ValueError('Capture metadata differs')
    if capture['protocol_ref']!=head['protocol']:raise ValueError('Protocol differs')
    records,sources=capture['records'],capture['sources']
    expected={'records_in_capture':len(records),'new_records':sum(r['created'] is True for r in records),
        'rank_observations':sum(r['origin']=='rank_observation' for s in sources for r in s['observations']),
        'ineligible_sources':sum(bool(s['eligibility_reasons']) for s in sources),
        'unsupported_identity_count':sum(s['unsupported_identity_count'] for s in sources)}
    if any(type(head.get(k)) is not int or head[k]!=v for k,v in expected.items()):raise ValueError('Capture counts differ')
    if {k:v for k,v in outcome.items() if k!='batch'}!=batch:raise ValueError('Complete batch differs')
    if dict(Counter(r['status'] for r in batch['results']))!=batch['status_counts']:raise ValueError('Outcome counts differ')
    for doc in (head,capture,outcome):
        if any(doc.get(k) is not False for k in ('sizing_eligible','promotion_eligible')):raise ValueError('Research authority differs')
    if outcome.get('net_return_pct') is not None or outcome.get('portfolio_pnl') is not None:raise ValueError('Unexpected portfolio claim')
    return expected


def main():
    lam,s3,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler'))
    with report('ops_6178_scheduled_research_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/research_identity.py'],cwd=ROOT,text=True).strip()
        before={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
        if any(v['receipt']!={'status':'matched','commit':expected} for v in before.values()):raise ValueError('Exact deployed packages required')
        def raw(key):return bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],8*1024*1024)
        head=json.loads(raw('data/prospective-research.json'));outcome=json.loads(raw('data/prospective-outcomes.json'))
        def artifact(ref,kind):
            if not re.fullmatch(r'[a-f0-9]{64}',ref['sha256']) or ref['key']!='data/research-forecasts/'+kind+'/'+ref['sha256']+'.json':raise ValueError('Unapproved artifact path')
            body=raw(ref['key'])
            if hashlib.sha256(body).hexdigest()!=ref['sha256']:raise ValueError('Whole artifact bytes differ')
            return json.loads(body)
        capture=artifact(head['capture'],'captures');batch=artifact(outcome['batch'],'evaluation-runs')
        counts=reconcile(head,capture,outcome,batch,identity_policy())
        if outcome['selection_handler_sha256']!=hashlib.sha256((ROOT/'aws/lambdas/justhodl-prospective-evaluator/source/lambda_function.py').read_bytes()).hexdigest():raise ValueError('Scheduled evaluator handler differs')
        identities=Counter()
        for entry in capture['records']:
            record=read_record(s3,BUCKET,entry)
            if record_identity_issue(record):raise ValueError('Crypto source entered equity capture')
            identities[record['observation']['instrument']['asset_class']]+=1
        sizes=[]
        for gap in head['coverage']['source_read_failures']:
            key=gap['source_key']
            if not re.fullmatch(r'data/[a-zA-Z0-9_.-]+\.json',key) or not public_source_allowed(key):raise ValueError('Unapproved metadata target')
            try:
                obj=s3.head_object(Bucket=BUCKET,Key=key)
                sizes.append({'source_key':key,'bytes':obj['ContentLength'],'last_modified':obj['LastModified'].isoformat(),'capture_reason':gap['reason']})
            except Exception as exc:
                sizes.append({'source_key':key,'metadata_error':str(getattr(exc,'response',{}).get('Error',{}).get('Code','unknown')),'capture_reason':gap['reason']})
        if {fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtimes=before,identity_policy=identity_policy(),
            capture={'generated_at':head['generated_at'],'reference':head['capture'],'counts':counts,'records_and_storage_clocks_checked':len(capture['records']),'asset_classes':dict(identities),'coverage':head['coverage']},
            outcomes={'generated_at':outcome['generated_at'],'reference':outcome['batch'],'forecasts_checked':outcome['forecasts_checked'],'status_counts':outcome['status_counts'],'coverage':outcome['coverage'],'evidence_errors':outcome['evidence_errors']},
            skipped_source_metadata=sizes,native_invocations=0,provider_requests=0,public_writes=0,history_writes=0,account_reads=0,schedules_changed=0,
            scope='Complete normal scheduled captures and batch integrity, retained forecast identity/storage clocks, exact code and source-size metadata. Not upstream original replay or validated investment performance.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
