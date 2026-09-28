"""Read-only Buyback package, original schedule, own history and provider replay."""
from pathlib import Path
import json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared','aws/lambdas/justhodl-buyback-engine/source')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6236_buyback_measurements_acceptance as arithmetic
import buyback_store as store
import buyback_sources as sources
FN=arithmetic.FN;KEY=arithmetic.KEY;BUCKET=arithmetic.BUCKET


def retained(s3,ref):
    if (not isinstance(ref,dict) or set(ref)!={'key','bytes','sha256'} or not isinstance(ref['sha256'],str)
            or not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or type(ref['bytes']) is not int
            or not 0<=ref['bytes']<=store.BOUND or ref['key']!=store.PREFIX+ref['sha256']+'.json'):
        raise ValueError('Only whole declared public Buyback history allowed')
    raw=store.whole(s3.get_object(Bucket=BUCKET,Key=ref['key']))
    if not sources.same(store.reference(raw),ref):raise ValueError('Whole Buyback history differs')
    return raw


def publication(raw,s3):
    packet=store.decode(raw)
    if not isinstance(packet,dict):raise ValueError('Whole public Buyback packet required')
    if packet.get('version')!='1.3.0':
        result=arithmetic.publication(raw)
        result.update(source_capture_status='pending_original_schedule_source_capture',publication_history_verified=False,investment_authority=False)
        return result
    if packet.get('publication_contract')!=store.CONTRACT:raise ValueError('New native publication contract required')
    if not sources.same(packet.get('source_files'),store.source_identity()):raise ValueError('Exact native compiler sources required')
    if retained(s3,store.reference(raw))!=raw:raise ValueError('Current archive differs')
    if packet.get('previous_publication') is not None:
        prior=store.decode(retained(s3,packet['previous_publication']))
        if not isinstance(prior,dict) or not isinstance(prior.get('tickers'),dict):raise ValueError('Whole preceding Buyback ledger required')
    result=arithmetic.publication(raw)
    if result['status']!='published_statement_arithmetic_reproduced':raise ValueError('Complete published arithmetic required')
    replay=sources.replay(packet,lambda ref:sources.read_original(s3,BUCKET,ref))
    result.update(provider_replay=replay,publication_history_verified=True,source_files_verified=4,investment_authority=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6291_buyback_originals_acceptance') as r:
        expected=expected_commit(FN);before=runtime(*clients,FN);check_runtime(before,baseline,expected,6)
        raw=store.whole(clients[1].get_object(Bucket=BUCKET,Key=KEY));result=publication(raw,clients[1])
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,private_state_reads=0,account_reads=0,
             learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact public Buyback producer and original cadence; own whole public history and provider originals. Upstream selection/context, SEC first-release vintages and investment authority remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
