"""Qualify captured option coefficients privately; no native producer changed."""
from pathlib import Path
from collections import Counter,defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from fractions import Fraction
import json,subprocess,sys,time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5975_etf_constituent_source_preflight import runtime,denied
from ops_5994_option_flow_native_acceptance import public
from ops_5993_option_flow_recovered_qualification import verify_records
import ops_5987_options_dependency_preflight as baseline
import option_population_model as model
import option_flow_research as upstream
import option_flow_store as store
import option_research_rows as codec
BUCKET='justhodl-dashboard-live'
SOURCE_RUN={'manifest_key':'data/option-flow-research/runs/766c98dc95802c0f51596df696b2ebe08605b451125f0d2b08516eed5fe97b73.json',
    'output_sha256':'563d4da3260257c37b517424f0ada8083231616286ce87fc881edb8d19ffa5f9'}
FUNCTION='justhodl-dealer-gex'


def independent(output,summary,source,read):
    """Use exact rational arithmetic on original provider values, not model terms."""
    originals={}
    for page in source['pages']:
        if page.get('status')=='received' and page.get('http_status')==200 and page.get('original'):
            doc=upstream.capture.decode(upstream.protected(page['original'],read))
            if isinstance(doc,dict) and isinstance(doc.get('results'),list):originals[page['page']]=doc['results']
    totals=defaultdict(lambda:[Fraction(0),0]);groups=defaultdict(lambda:defaultdict(lambda:[Fraction(0),0]))
    populations=Counter();group_populations=defaultdict(Counter);returned=eligible=0
    for part in summary['record_blocks']:
        for row in codec.unpack(upstream.checked(part['artifact'],read,'rows')):
            returned+=1
            if not row['identity_eligible']:continue
            eligible+=1;kind=row['contract_type'];populations[kind]+=1
            key=(row['expiration_date'],Fraction(Decimal(row['metrics']['strike']['value'])))
            group_populations[key][kind]+=1
            original=originals[row['evidence']['page']][row['evidence']['row_index']]
            values={};m=row['metrics']
            if m['open_interest']['value'] is not None:
                oi=Fraction(original['open_interest']);values['reported_open_interest']=oi
                assert Fraction(original['details']['shares_per_contract'])==100
                for field in ('gamma','delta'):
                    if m['vendor_'+field]['value'] is not None:
                        values[field+'_oi_shares']=Fraction(original['greeks'][field])*oi*100
            for field,value in values.items():
                for accumulator in (totals,groups[key]):
                    accumulator[kind,field][0]+=value;accumulator[kind,field][1]+=1
    def check(aggregate,expected,population):
        for kind in ('call','put'):
            for field in ('reported_open_interest','gamma_oi_shares','delta_oi_shares'):
                value,count=expected[kind,field];cell=aggregate['sides'][kind][field]
                assert cell['included_rows']==count and cell['identity_population_rows']==population[kind]
                assert cell['excluded_rows']==population[kind]-count
                assert cell['complete_field_coverage']==(population[kind]>0 and count==population[kind])
                assert cell['observation_time'] is None
                if count:assert Fraction(Decimal(cell['value']))==value,'Original coefficient differs'
                else:assert cell['value'] is None
    check(output['totals'],totals,populations)
    assert output['totals']['counts']['returned_rows']==returned
    assert output['totals']['counts']['identity_eligible_rows']==eligible
    assert len(output['by_expiry_strike'])==len(group_populations)
    seen=set()
    for group in output['by_expiry_strike']:
        key=(group['expiration_date'],Fraction(Decimal(group['strike_usd_per_share'])))
        assert key not in seen;seen.add(key)
        check(group,groups[key],group_populations[key])
    assert seen==set(group_populations)
    assert all(output[k] is False for k in model.PERMISSIONS)
    assert output['observed_dealer_inventory'] is None and output['zero_gamma_flip'] is None and output['dealer_hedging_flow'] is None
    return {'returned_rows':returned,'identity_eligible_rows':eligible,'expiry_strike_groups':len(groups),
        'verified_aggregate_cells':6*(len(groups)+1),'exact_rational_reconciliation':True}


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_5996_option_population_qualification') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_option_population_model.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'tests/test_option_population_qualification.py')],cwd=ROOT,check=True)
        proof=json.loads(public('data/option-flow-scheduled-verification.json'))
        assert proof['publication']['replay']==SOURCE_RUN
        actual=runtime(lam,s3,events,scheduler,FUNCTION)
        assert sum(x['state']=='ENABLED' for x in actual['schedules'])==2
        predecessors={}
        for key in ('data/dealer-gex.json','data/dealer-gex-history.json'):
            raw=store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            predecessors[key]={'original':baseline.retain(s3,raw),'shape':baseline.describe(key,json.loads(raw))}
        run=store.verified_run(SOURCE_RUN,read);inputs=upstream.checked(run['input'],read,'inputs')
        packet={**upstream.checked(run['output'],read,'outputs'),'replay':SOURCE_RUN}
        model.verified_packet(upstream.encoded(packet),read)
        results={};all_delivery={};protected={x['original']['key'] for x in predecessors.values()};started=time.monotonic()
        counts=Counter()
        for symbol in model.UNDERLYINGS:
            output=model.chain(packet,symbol,read)
            summary=upstream.checked(packet['chains'][symbol]['chain'],read,'chains')
            source=inputs['chains'][symbol]
            source_checks=verify_records(source,summary,read)
            checked=independent(output,summary,source,read)
            for page in source['pages']:
                if page.get('original'):protected.add(page['original']['key'])
            delivery={}
            def emit(key,raw):delivery[key]=baseline.retain(s3,raw)
            ref=model.deliver(output,emit)
            def read_delivery(key):return upstream.protected(delivery[key],read)
            assert model.restore(ref,read_delivery)==output
            all_delivery.update(delivery);protected.update(x['key'] for x in delivery.values())
            counts.update({k:v for k,v in checked.items() if type(v) is int})
            results[symbol]={'chain':ref,'expanded_sha256':upstream.sha(upstream.encoded(output)),
                'expanded_bytes':len(upstream.encoded(output)),'bounded_delivery_bytes':sum(x['bytes'] for x in delivery.values()),
                'largest_delivery_bytes':max(x['bytes'] for x in delivery.values()),'delivery_artifacts':len(delivery),
                'source_checks':source_checks,'coefficient_checks':checked,'source_chain':output['source_chain']}
            del output
        compiler=baseline.retain(s3,Path(model.__file__).read_bytes());protected.add(compiler['key'])
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        manifest={'contract':'option-population-qualification.v1','generated_at':store.now(),'source_run':SOURCE_RUN,
            'source_acceptance_sha256':upstream.sha(upstream.encoded(proof)),'compiler':compiler,'results':results,
            'private_delivery':all_delivery,'predecessors':predecessors,'predecessor_runtime':actual,
            'counts':dict(counts),'elapsed_s':round(time.monotonic()-started,3),
            'peak_runner_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'scope':'Captured populations only. No ownership inference, provider collection, native invocation or public head mutation.'}
        retained=baseline.retain(s3,upstream.encoded(manifest));protected.add(retained['key'])
        r.kv(retained_candidate=retained,counts=dict(counts),arithmetic_complete=True,privacy_acceptance_pending=True)
        def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        r.kv(retained_manifest=retained,source_run=SOURCE_RUN,results=results,counts=dict(counts),
            elapsed_s=manifest['elapsed_s'],peak_runner_rss_kib=manifest['peak_runner_rss_kib'],
            protected_artifacts_checked=len(protected),predecessor_runtime=actual,
            provider_requests=0,engine_invocations=0,public_head_writes=0,schedules_changed=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
