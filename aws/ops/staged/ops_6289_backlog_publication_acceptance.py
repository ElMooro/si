"""Read-only Backlog package, original cadence and whole publication history."""
from pathlib import Path
import importlib.util,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared','aws/lambdas/justhodl-backlog/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6235_backlog_measurements_acceptance as original
import backlog_store as store
FN=original.FN;KEY=original.KEY;BUCKET=original.BUCKET


def retained(s3,ref):
    if (not isinstance(ref,dict) or set(ref)!=set(('key','sha256','bytes')) or
        not isinstance(ref['sha256'],str) or not re.fullmatch(r'[a-f0-9]{64}',ref['sha256']) or
        type(ref['bytes']) is not int or not 0<=ref['bytes']<=store.BOUND or
        ref['key']!=store.PREFIX+ref['sha256']+'.json'):
        raise ValueError('Only complete declared public Backlog history allowed')
    raw=store.whole(s3.get_object(Bucket=BUCKET,Key=ref['key']))
    if store.reference(raw)!=ref:raise ValueError('Whole Backlog history differs')
    return raw


def publication(raw,prior_raw=None):
    p=store.decode(raw)
    if not isinstance(p,dict):raise ValueError('Whole public publication required')
    if p.get('publication_contract')!=store.CONTRACT or p.get('version')=='1.1.0':
        return {'status':'pending_original_schedule_archived_publication','generated_at':p.get('generated_at'),
                'version':p.get('version'),'bytes':len(raw),'sha256':store.reference(raw)['sha256'],
                'publication_history_verified':False,'investment_authority':False}
    if p.get('version') not in ('1.2.0','1.3.0') or not original.same(p.get('source_files'),store.source_identity()):
        raise ValueError('Exact publication compiler required')
    prior=store.decode(prior_raw) if prior_raw is not None else None
    if prior_raw is not None and (not isinstance(prior,dict) or not isinstance(prior.get('by_ticker'),dict)):
        raise ValueError('Whole previous ledger required')
    if not original.same(p.get('previous_publication'),(store.reference(prior_raw) if prior_raw is not None else None)):
        raise ValueError('Exact previous publication differs')
    result=original.publication(raw)
    if result['status']!='published_observation_arithmetic_reproduced':raise ValueError('Measurement arithmetic replay required')
    result.update(publication_history_verified=True,source_files_verified=len(store.source_identity()),
                  whole_provider_http_replay_verified=False,investment_authority=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')];s3=clients[1]
    with report('ops_6289_backlog_publication_acceptance') as r:
        expected=expected_commit(FN);before=runtime(*clients,FN);check_runtime(before,baseline,expected,5)
        raw=store.whole(s3.get_object(Bucket=BUCKET,Key=KEY));p=store.decode(raw);prior=None;archived=False
        if isinstance(p,dict) and p.get('publication_contract')==store.CONTRACT:
            if retained(s3,store.reference(raw))!=raw:raise ValueError('Current whole archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=retained(s3,p['previous_publication'])
        result=publication(raw,prior)
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,private_state_reads=0,account_reads=0,
             learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact public Backlog producer and original cadence; whole own current/prior public archives and published arithmetic. Complete original HTTP, first-release history and investment authority remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
