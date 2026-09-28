"""Read-only complete EPS package, original cadence and retained owned originals."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
from ops_6272_summary_context_acceptance import expected_commit
import ops_6283_eps_transport_acceptance as transport
original=transport.original;FN=original.FN;KEY=original.KEY;BUCKET=original.BUCKET


def source_reader(s3):
    m=original.compiler();cache={}
    def read(descriptor):
        m.validate_estimate_descriptor(descriptor);ref=descriptor['original_ref'];key=ref['key']
        if key not in cache:
            obj=s3.get_object(Bucket=BUCKET,Key=key)
            try:raw=obj['Body'].read(ref['bytes']+1)
            finally:obj['Body'].close()
            if obj.get('ContentLength')!=len(raw):raise ValueError('Whole owned estimate source required')
            m.estimate_source_envelope(descriptor,raw);cache[key]=raw
        m.estimate_source_envelope(descriptor,cache[key]);return cache[key]
    return read


def publication(raw,prior_raw=None,read_source=None):
    m=original.compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole public packet required')
    if p.get('measurement_contract')!=m.CONTRACT or p.get('version') in ('1.1.0','1.1.1'):
        return {'status':'pending_original_fanout_baseline_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_at':p.get('generated_at'),'version':p.get('version'),'current_compiler_publication_verified':False,'investment_authority':False}
    if p.get('version')!='1.2.0' or 'estimate_baselines' not in p:raise ValueError('Recognized retained-baseline compiler required')
    io=p.get('retained_source_io',{});workers=io.get('worker_limit')
    if type(workers) is not int or not 1<=workers<=10 or io!={'connect_timeout_seconds':3,'read_timeout_seconds':5,'total_attempts':1,
        'worker_limit':workers,'minimum_remaining_seconds':25,'source_bytes_limit':256*1024}:raise ValueError('Bounded owned-source transport differs')
    result=transport.publication(raw,prior_raw,read_source)
    descriptors=p['estimate_baselines']['entries']
    for descriptor in descriptors:
        if read_source is None:raise ValueError('Every retained annual original must be verified')
        m.estimate_source_envelope(descriptor,read_source(descriptor))
    result.update(status='published_eps_retained_originals_replayed',retained_baselines=len(descriptors),
        comparison_sources_received=sum(r['comparison_source']['status']=='received' for r in p['request_records']),
        historical_read_failures_independently_verified=False,first_release_history_verified=False,investment_authority=False)
    return result


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/eps-revision-velocity-original-baseline.json').read_bytes())['actual_producers'][FN]
    prior_route=json.loads((ROOT/'docs/audit/2026-09-27/eps-target-code-acceptance.json').read_bytes())['fanout_route']
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6284_eps_baselines_acceptance') as r:
        commit=expected_commit(FN);before=runtime(*args,FN);check_runtime(before,baseline,commit,3)
        route=original.fanout(clients)
        if any(route[k]!=prior_route[k] for k in ('matching_ticks','routes')):raise ValueError('Original EPS scheduled route differs')
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=original.compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==original.compiler().CONTRACT:
            if original.read_public_archive(clients['s3'],original.archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=original.read_public_archive(clients['s3'],p['previous_publication'])
        result=publication(raw,prior,source_reader(clients['s3']))
        if runtime(*args,FN)!=before or original.fanout(clients)!=route:raise ValueError('Runtime or original fanout changed during acceptance')
        r.kv(expected_commit=commit,actual_runtime=before,fanout_route=route,native_publication=result,current_archive_verified=archived,
            native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
            public_writes=0,history_writes=0,schedule_changes=0,scope='Complete three-file EPS compiler and original fanout. Declared public producer, own immutable prior packet and strictly owned hashed estimate originals only. No native/provider invocation, private/consumer reads or investment authority.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
