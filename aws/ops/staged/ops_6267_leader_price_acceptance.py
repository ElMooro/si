"""Read-only Leader Price package, original schedule and whole public-source replay.

No native/provider/consumer invocation, private output/account/learning reads,
schedule mutation, notification or publication write.
"""
from pathlib import Path
import hashlib,importlib.util,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
FN='justhodl-momentum-leaders';KEY='data/momentum-leaders.json';BUCKET='justhodl-dashboard-live'


def compiler():
    path=ROOT/'aws/lambdas'/FN/'source/leader_price_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_leader_price_observations',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def archive_ref(raw):
    return {'key':'data/momentum-leaders/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def read_public_archive(s3,ref):
    if not isinstance(ref,dict) or not re.fullmatch(r'data/momentum-leaders/history/[a-f0-9]{64}\.json',str(ref.get('key',''))):
        raise ValueError('Only declared public Leader Price history permitted')
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'],64*1024*1024)
    if obj.get('ContentLength')!=len(raw) or archive_ref(raw)!=ref:raise ValueError('Whole immutable archive differs')
    return raw


def source_acquisitions(p):
    yield from p['input_acquisitions'].values()
    yield p['benchmark']['acquisition']
    for r in p['request_records']:yield r['acquisition']


def read_sources(s3,p):
    m=compiler();sources={};total=0
    for a in source_acquisitions(p):
        if a.get('status')!='received':
            if 'original_ref' in a:raise ValueError('Unexpected original on unavailable attempt')
            continue
        ref=m.validate_ref(a.get('original_ref'))
        if ref['key'] in sources:continue
        total+=ref['bytes']
        if total>160*1024*1024:raise ValueError('Declared originals exceed bound')
        obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'],8*1024*1024)
        if obj.get('ContentLength')!=len(raw) or m.source_ref(raw)!=ref:raise ValueError('Whole original source differs')
        sources[ref['key']]=raw
    return sources


def publication(raw,prior_raw=None,sources=None):
    m=compiler();p=m.strict(raw);sources={} if sources is None else sources
    if not isinstance(p,dict):raise ValueError('Whole public packet required')
    out={'status':'pending_original_schedule_publication','bytes':len(raw),'sha256':m.sha(raw),'generated_at':p.get('generated_at'),'version':p.get('version')}
    if p.get('measurement_contract') not in (None,m.CONTRACT):raise ValueError('Unknown producer contract requires review')
    if p.get('measurement_contract')!=m.CONTRACT:return out
    paths={name:ROOT/'aws/lambdas'/FN/'source'/name for name in ('lambda_function.py','leader_price_observations.py')}
    paths['momentum_research_boundary.py']=ROOT/'aws/shared/momentum_research_boundary.py'
    identity={name:{'bytes':len(raw),'sha256':m.sha(raw)} for name,path in paths.items() for raw in [path.read_bytes()]}
    if p.get('source_files')!=identity:raise ValueError('Exact packet source identity differs')
    if any(p.get(k) is not False for k in m.FLAGS) or p.get('call') is not None:raise ValueError('Explicit research permissions required')
    if any(p.get(key)!=[] for key in ('leaders','pump_confirmed','all_scored')) or any(p.get(key) is not None for key in ('n_scored','n_leaders','n_pump_confirmed')) or p.get('signals_logged')!=0 or p.get('notifications_sent')!=0:raise ValueError('Legacy rankings and alerts cannot gain authority')
    stamp=m.clock(p.get('generated_at'));started=m.clock(p.get('acquisition_started_at'))
    if stamp is None or started is None or stamp<started:raise ValueError('Publication clocks invalid')
    if p.get('previous_publication')!=(archive_ref(prior_raw) if prior_raw is not None else None):raise ValueError('Whole prior public packet differs')
    previous=m.strict(prior_raw) if prior_raw else {}
    if previous.get('measurement_contract')==m.CONTRACT:
        prior_clock=m.clock(previous.get('generated_at'))
        if prior_clock is None or prior_clock>=started:raise ValueError('Previous publication clock invalid')
    for a in source_acquisitions(p):
        if 'requested_at' in a:
            requested=m.clock(a.get('requested_at'));received=m.clock(a.get('received_at'))
            if requested is None or received is None or not started<=requested<=received<=stamp:raise ValueError('Attempt outside acquisition clock')
    limits=p['acquisition_limits']
    expected_limits={'MAX_UNIVERSE':60,'LOOKBACK_DAYS':90,'request_calendar_span':290,'original_configured_workers':8,
                     'active_workers':4,'stock_acquisition_budget_seconds':120,'publication_budget_seconds':240}
    if limits!=expected_limits:raise ValueError('Original scope and bounded request controls differ')
    replay=m.build(p['input_acquisitions'],[r['acquisition'] for r in p['request_records']],sources,p['generated_at'],60,p['benchmark']['acquisition'])
    if any(p.get(k)!=v for k,v in replay.items()):raise ValueError('Complete price/source/coverage replay differs')
    if p.get('retained_unique_source_bytes')!=sum(len(v) for v in sources.values()):raise ValueError('Unique original byte count differs')
    out.update(status='published_leader_originals_replayed',source_blobs=len(sources),request_occurrences=len(p['request_records']),
        quality=p['quality'],first_release_history_verified=False,independent_evidence=False,investment_authority=False)
    return out



def main():
    for path in ('aws/lambdas/justhodl-momentum-leaders/tests/run_tests.py','tests/ops/test_leader_price_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6267_leader_price_acceptance') as r:
        before=runtime(*args,FN);r.kv(actual_package=before);check_runtime(before,baseline,expected,4)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'],64*1024*1024)
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==compiler().CONTRACT:
            if read_public_archive(clients['s3'],archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=read_public_archive(clients['s3'],p['previous_publication'])
        sources=read_sources(clients['s3'],p) if p.get('measurement_contract')==compiler().CONTRACT else {}
        result=publication(raw,prior,sources)
        if runtime(*args,FN)!=before:raise ValueError('Runtime or routing changed during acceptance')
        r.kv(expected_commit=expected,actual_package=before,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,downstream_consumer_qualification=False,
             scope='Exact native producer package and original runtime/schedule only; whole declared public producer packet and its own source/history graph. No consumer output read, native invocation or predictive qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
