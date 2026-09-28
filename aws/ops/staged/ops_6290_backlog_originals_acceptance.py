"""Read-only native Backlog package, history and original provider response replay."""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared','aws/lambdas/justhodl-backlog/source')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6289_backlog_publication_acceptance as history
import backlog_sources as sources
FN=history.FN;KEY=history.KEY;BUCKET=history.BUCKET;store=history.store


def publication(raw,s3):
    packet=store.decode(raw)
    if not isinstance(packet,dict):raise ValueError('Whole public Backlog packet required')
    if packet.get('version')!='1.3.0':
        return {'status':'pending_original_schedule_source_capture','version':packet.get('version'),
                'generated_at':packet.get('generated_at'),'bytes':len(raw),'sha256':store.reference(raw)['sha256'],
                'whole_provider_http_replay_verified':False,'investment_authority':False}
    if history.retained(s3,store.reference(raw))!=raw:raise ValueError('Current archive differs')
    prior=history.retained(s3,packet['previous_publication']) if packet.get('previous_publication') is not None else None
    result=history.publication(raw,prior)
    result['provider_replay']=sources.replay(packet,lambda ref:sources.read_original(s3,BUCKET,ref))
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/accounting-original-baseline.json').read_bytes())['actual_producers'][FN]
    clients=[boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6290_backlog_originals_acceptance') as r:
        expected=expected_commit(FN);before=runtime(*clients,FN);check_runtime(before,baseline,expected,5)
        raw=store.whole(clients[1].get_object(Bucket=BUCKET,Key=KEY));result=publication(raw,clients[1])
        if runtime(*clients,FN)!=before:raise ValueError('Runtime changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,native_publication=result,
             native_invocations=0,provider_requests=0,consumer_output_reads=0,private_state_reads=0,account_reads=0,
             learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact public Backlog producer and original cadence; whole own public archives and retained provider originals. Private selection cache, first-release history and investment authority remain unqualified.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
