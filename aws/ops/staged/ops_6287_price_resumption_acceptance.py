"""Read-only exact Price Compression compiler, original cadence and queue replay."""
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
import ops_6262_price_compression_acceptance as original
from ops_6286_morning_producer_replay import accept


def publication(raw,prior_raw=None,sources=None):
    m=original.compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole public publication required')
    if p.get('measurement_contract')!=m.CONTRACT or p.get('version')=='2.0.0':
        return {'status':'pending_original_schedule_resumed_publication','version':p.get('version'),
                'generated_at':p.get('generated_at'),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
                'resumption_publication_verified':False,'investment_authority':False}
    if p.get('version')!='2.1.0':raise ValueError('Recognized resumption compiler required')
    out=original.publication(raw,prior_raw,sources)
    selected=p['universe_membership']['selected'];records=p['request_records'];progress=p.get('acquisition_progress')
    m.validate_acquisition_progress(selected,records,progress)
    plan=m.acquisition_plan(selected,m.strict(prior_raw) if prior_raw is not None else None)
    if progress!=m.acquisition_progress(plan,progress['visited_request_indices'],progress['stop_reason'],p['retained_unique_source_bytes']):
        raise ValueError('Resumption does not replay from whole previous publication')
    if progress['stop_reason']=='source_byte_budget' and progress['retained_source_bytes']<96*1024*1024:
        raise ValueError('Source dispatch budget was not reached')
    if progress['stop_reason']=='provider_denial_or_rate_limit' and not any(r['acquisition'].get('http_status') in (401,403,429) for r in records):
        raise ValueError('Provider stop outcome missing')
    out.update(resumption_publication_verified=True,visited_occurrences=progress['visited_occurrences'],
               pending_occurrences=progress['pending_occurrences'],complete_selected_visit_cycle=progress['cycle_complete'],
               stop_reason=progress['stop_reason'],historical_runtime_stop_independently_verified=False,
               whole_universe_current_coverage_verified=False,investment_authority=False)
    return out


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/original.FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    proxy=SimpleNamespace(**{k:getattr(original,k) for k in ('FN','KEY','BUCKET','compiler','fanout','producer_settings','archive_ref','read_public_archive','read_sources')},publication=publication)
    directory=ROOT/'docs/audit/2026-09-28'
    baseline=json.loads((directory/'volatility-squeeze-original-baseline.json').read_bytes())
    prior=json.loads((directory/'price-compression-code-acceptance.json').read_bytes())
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')}
    with report('ops_6287_price_resumption_acceptance') as r:
        result=accept(proxy,baseline,prior,clients)
        r.kv(producer=result,native_invocations=0,provider_requests=0,consumer_output_reads=0,private_state_reads=0,
             account_reads=0,learning_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Exact public Price Compression producer and original route only; complete own originals/history and selected request progress. No invocation or investment qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
