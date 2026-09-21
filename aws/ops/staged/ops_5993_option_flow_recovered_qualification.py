"""Recover the complete retained option candidate, without source recollection.

Runner IAM and existing managed provider configuration only. No producer,
account, decision consumer, paid AI, notification or portfolio invocation.
"""
from pathlib import Path
from collections import Counter,defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal,localcontext
import json,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import option_flow_store as store
import option_flow_research as model
import option_research_rows as codec
from ops_5975_etf_constituent_source_preflight import denied,runtime
BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-polygon-options-flow'
REQUEST='chatgpt-ops-5993-option-flow-recovered'
ORIGINAL_REQUEST='chatgpt-ops-5991-option-flow-candidate'
RECOVERY={'manifest_key':'data/option-flow-research/runs/f348b184d9b18526ff1caf224e0b3f6025e1cabe2af9269515ab6f9792645ac1.json','output_sha256':'3c0eafc805c5416c7452ab2fff2814de6ffc8b6d83b1947187e79397e01a6abf'}


def original_keys(value):
    found=set()
    if isinstance(value,dict):
        for child in value.values():found.update(original_keys(child))
    elif isinstance(value,list):
        for child in value:found.update(original_keys(child))
    elif isinstance(value,str) and value.startswith(model.PRIVATE) and value.endswith('.bin'):found.add(value)
    return found


def public_hash(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
            headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=35) as response:raw=store.bounded(response)
    return {'sha256':model.sha(raw),'bytes':len(raw)}


def verify_records(source_chain,summary,read):
    """Check individual source quantities and independent captured-population sums."""
    counts=Counter();rows=[];originals={}
    for page in source_chain['pages']:
        if page.get('status')=='received' and page.get('http_status')==200 and page.get('original'):
            raw=model.protected(page['original'],read);doc=model.capture.decode(raw)
            if isinstance(doc,dict) and isinstance(doc.get('results'),list):originals[page['page']]=(page,doc['results'])
    totals=defaultdict(list);dates=defaultdict(lambda:defaultdict(list));field_states=defaultdict(Counter)
    for part in summary['record_blocks']:
        block=model.checked(part['artifact'],read,'rows');decoded=codec.unpack(block)
        assert len(decoded)==part['rows'] and block['expanded_sha256']==part['expanded_sha256']
        for item in decoded:
            page,source_rows=originals[item['evidence']['page']];position=item['evidence']['row_index'];raw=source_rows[position]
            assert item['evidence']['original']==page['original'] and item['evidence']['row_pointer']=='/results/'+str(position)
            assert item['source_received_at']==model.clock(page['acquired_at']).isoformat()
            assert all(item[k] is False for k in model.PERMISSIONS)
            if not isinstance(raw,dict):
                assert item['identity_eligible'] is False;counts['malformed_retained_rows']+=1;continue
            for name,cell in item['metrics'].items():
                field_states[name][cell['state']]+=1;parent=raw
                for field in cell['source_field'].strip('/').split('/'):
                    if not isinstance(parent,dict) or field not in parent:parent=None;break
                    parent=parent[field]
                if 'reported_value' in cell:
                    assert not isinstance(parent,bool) and isinstance(parent,(int,Decimal))
                    assert Decimal(cell['reported_value'])==parent;counts['exact_reported_numbers']+=1
                if cell['value'] is not None:
                    assert Decimal(cell['value'])==parent;counts['qualified_numeric_values']+=1
                    counts['true_zero_values']+=int(Decimal(cell['value'])==0)
                if name=='vendor_gamma' and isinstance(parent,(int,Decimal)) and not isinstance(parent,bool) and parent<0:
                    assert cell['state']=='outside_domain' and cell['value'] is None;counts['negative_gamma_retained_and_excluded']+=1
            if item['identity_eligible']:
                kind=raw['details']['contract_type'];oi=item['metrics']['open_interest']['value']
                totals[kind].append(Decimal(raw['open_interest']) if oi is not None else None)
                stamp=item['clocks']['daily_bar_updated'].get('date_new_york')
                if stamp:
                    volume=item['metrics']['daily_volume']['value']
                    dates[stamp][kind].append(Decimal(raw['day']['volume']) if volume is not None else None)
                assert item['open_interest_date'] is None and item['greek_observation_date'] is None
            counts['retained_rows']+=1;rows.append((item['evidence']['page'],position))
    assert len(rows)==len(set(rows)), 'Each original position retained once'
    assert counts['retained_rows']+counts['malformed_retained_rows']==summary['coverage']['returned_rows']
    if summary['research_status']=='source_unavailable':return dict(counts)
    assert summary['coverage']['field_quality']=={k:dict(v) for k,v in field_states.items()}
    def check(cell,values):
        valid=[v for v in values if v is not None]
        assert cell['population_rows']==len(values) and cell['included_rows']==len(valid)
        assert cell['complete_field_coverage']==(bool(values) and len(values)==len(valid))
        assert cell['missing_or_invalid_rows']==len(values)-len(valid)
        with localcontext() as ctx:
            ctx.prec=180
            if valid:assert Decimal(cell['value'])==sum(valid,Decimal(0))
            else:assert cell['value'] is None
    for kind,label in (('call','calls'),('put','puts')):check(summary['reported_open_interest'][label],totals[kind])
    assert len(summary['daily_bar_update_groups'])==len(dates)
    for group in summary['daily_bar_update_groups']:
        pop=dates[group['daily_bar_update_date_new_york']]
        for kind,label in (('call','calls'),('put','puts')):check(group[label],pop[kind])
        assert group['market_session_completeness_verified'] is False
    assert summary['total_session_volume'] is None and summary['dealer_position'] is None
    counts['dated_bar_groups']=len(dates)
    return dict(counts)


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5993_option_flow_recovered_qualification') as r:
        for path in ('tests/test_option_flow_research.py','tests/test_option_flow_store.py','tests/test_option_flow_acceptance.py','tests/test_option_contract_research.py','tests/test_option_research_rows.py'):
            subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
        before_runtime=runtime(lam,s3,events,scheduler,FUNCTION);before_public=public_hash(model.LEGACY)
        try:s3.head_object(Bucket=BUCKET,Key=model.CURRENT)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise RuntimeError('Native option packet already exists; inspect before collecting again')
        try:s3.head_object(Bucket=BUCKET,Key=store.request_key(REQUEST))
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise RuntimeError('Candidate already attempted; use retained evidence without recollecting')
        original_status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=store.request_key(ORIGINAL_REQUEST))['Body']))
        assert original_status['candidate_replay']==RECOVERY
        started=time.monotonic()
        result=store.run(s3,BUCKET,REQUEST,'runner-ops-5993',recover_run=RECOVERY,remaining_seconds=900,publish_current=False)
        elapsed=round(time.monotonic()-started,3)
        assert result['status']=='complete' and result['published'] is False and result['compatibility_published'] is False
        read=store.reader(s3,BUCKET);run=store.verified_run(result['replay'],read)
        inputs=model.checked(run['input'],read,'inputs');output=model.checked(run['output'],read,'outputs')
        assert result['provider_requests_this_execution']==0
        assert model.sha(model.encoded(output))==RECOVERY['output_sha256']
        acquisition_upper_seconds=(model.clock(inputs['generated_at'])-model.clock(original_status['started_at'])).total_seconds()
        counts=Counter();coverage={};protected=original_keys(inputs)|{store.request_key(REQUEST),store.request_key(ORIGINAL_REQUEST)}
        for symbol,row in output['chains'].items():
            summary=model.checked(row['chain'],read,'chains')
            counts.update(verify_records(inputs['chains'][symbol],summary,read))
            coverage[symbol]={'research_status':row['research_status'],'returned_rows':row['coverage']['returned_rows'],
                'eligible_identity_rows':row['coverage']['eligible_identity_rows'],'captured_pages':row['acquisition']['captured_pages'],
                'compiled_pages':row['acquisition']['compiled_pages'],'stop':row['acquisition']['stop'],
                'record_blocks':row['record_blocks'],'dated_bar_groups':row['dated_bar_groups']}
            protected.add(store.request_key(ORIGINAL_REQUEST)[:-5]+'/chains/'+symbol+'.json')
        assert set(model.CONTINUITY)<=set(output['chains'])
        assert output['quality']['returned_rows']==counts['retained_rows']+counts['malformed_retained_rows']
        assert runtime(lam,s3,events,scheduler,FUNCTION)==before_runtime
        # A scheduled predecessor may run during a long qualification. Only a
        # retained identity match is asserted when its generation did not move.
        after_public=public_hash(model.LEGACY)
        assert result['publish_current'] is False
        try:s3.head_object(Bucket=BUCKET,Key=model.CURRENT)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise AssertionError('Candidate must not publish a mutable current packet')
        def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        evidence={'contract':'option-flow-candidate-qualification.v1','generated_at':output['generated_at'],'candidate_replay':result['replay'],
            'quality':output['quality'],'coverage':coverage,'independent_checks':dict(counts),
            'provider_requests':inputs['provider_requests'],'source_bytes':inputs['source_bytes'],
            'recovery_path_seconds':elapsed,'original_acquisition_upper_seconds':acquisition_upper_seconds,'production_path_upper_seconds':elapsed+acquisition_upper_seconds,'runner_peak_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'runtime_budget_pass':elapsed+acquisition_upper_seconds<780,'memory_budget_pass':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss<3*1024*1024,
            'protected_artifacts_checked':len(protected),'originals_anonymously_denied':True,
            'before_public':before_public,'after_public':after_public,'predecessor_runtime':before_runtime,
            'provider_requests_this_execution':0,'reproduced_first_candidate_output_exactly':True,'mutable_current_published':False,'engine_invocations':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'schedules_changed':0}
        retained=store.protect(s3,BUCKET,model.encoded(evidence),read)
        r.kv(**evidence,retained_qualification=retained)
        assert evidence['runtime_budget_pass'] and evidence['memory_budget_pass'],'Retained candidate needs runtime adaptation before native publication'


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
