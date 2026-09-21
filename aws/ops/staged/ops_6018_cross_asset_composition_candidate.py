"""Qualify v2 parent composition using the exact 6017 retained baseline.

No new source capture, producer invocation, schedule change or public head write.
"""
from pathlib import Path
from collections import defaultdict
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib,json,subprocess,sys,time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6003_massive_composite_candidate as old_audit
import ops_6017_cross_asset_composition_preflight as baseline
import massive_research_model_v2 as model
import massive_research_store_v2 as store
BUCKET=baseline.BUCKET
BASELINE={'key':'audit-private/20260909-originals/massive-research/5ecd98285a2ff997b550bc96dbdb45d9cc3046456d998333162559c7a3b756e7.bin',
    'sha256':'5ecd98285a2ff997b550bc96dbdb45d9cc3046456d998333162559c7a3b756e7','bytes':42794}
REQUEST='chatgpt-cross-asset-composition-candidate-6018'
STATUS=store.request_key(REQUEST)


def independent(output,read):
    """Independently check graph membership, every new pointer, identities/clocks.

    Parent measurement arithmetic remains the parent engine's assurance scope.
    """
    legacy=deepcopy(output);legacy['sources']={k:v for k,v in legacy['sources'].items() if k in model.legacy.SOURCES}
    legacy['instruments']={k:v for k,v in legacy['instruments'].items() if ':' not in k}
    legacy['dependency_graph']['nodes']={k:v for k,v in legacy['dependency_graph']['nodes'].items() if v['kind'] in model.legacy.SOURCES}
    legacy['dependency_graph']['measurement_families']=[f for f in legacy['dependency_graph']['measurement_families'] if f['family'] in model.legacy.FAMILIES]
    counts=old_audit.independent(legacy,read);expected={};groups=defaultdict(list);cutoff=datetime.fromisoformat(output['generated_at'])
    graph=output['dependency_graph'];identities=output['instrument_identities']
    assert set(output['sources'])==set(model.SOURCES)
    for symbol in legacy['instruments']:
        assert identities[symbol]=={'asset_class':'provider_security_symbol','symbol':symbol,
            'meaning':'Existing option/fund symbol scope; exchange, share class and economic equivalence are not inferred.'}
    for kind in ('fx','futures'):
        row=output['sources'][kind];assert row['node'] is not None
        node=graph['nodes'][row['node']];key,contract,prefix,run_contract,_=model.SOURCES[kind]
        raw=read(node['replay']['manifest_key']);run=json.loads(raw)
        assert node['replay']['manifest_key']==prefix+'runs/'+hashlib.sha256(raw).hexdigest()+'.json'
        assert run['contract']==run_contract
        raw=read(run['output']['key']);body=json.loads(raw);digest=hashlib.sha256(raw).hexdigest()
        assert node['output']==run['output']=={'key':prefix+'outputs/'+digest+'.json','sha256':digest,'bytes':len(raw)}
        assert node['replay']['output_sha256']==run['output_sha256']==digest and node['kind']==kind
        assert row['node']==kind+':'+digest and body['contract']==contract
        assert node['generated_at']==run['generated_at']==body['generated_at']==row['source_generated_at']
        assert all(body[k] is False for k in model.FLAGS)
        captured=read(row['capture']['original']['key']);packet=json.loads(captured)
        assert hashlib.sha256(captured).hexdigest()==row['capture']['original']['sha256'] and len(captured)==row['capture']['original']['bytes']
        assert {k:v for k,v in packet.items() if k!='replay'}==body and packet['replay']==node['replay']
        assert row['source_capture_completed_at']==body['source_capture_completed_at']
        local={};due=[]
        if kind=='fx':
            assert body['configured_pairs']==len(body['pairs'])
            for pair,item in body['pairs'].items():
                instrument='FX:MASSIVE:'+pair;pointer='/pairs/'+pair;definition=identities[instrument]
                assert definition=={'asset_class':'currency_quote','provider':'Massive/Polygon','pair':pair,
                    'base_code':item['base_code'],'quote_code':item['quote_code'],'provider_ticker':item['provider_ticker'],
                    'price_unit':item['price_unit'],'metal_base_quantity_unit_verified':item.get('metal_base_quantity_unit_verified')}
                dates={'source_capture_completed_at':item['source_capture_completed_at'],
                    'source_review_due_at':item['source_review_due_at'],
                    'observation_clock_role':'reported_aggregate_window_start_not_close_time',
                    'latest_reported_window_start_utc':(item.get('latest_reported_row') or {}).get('window_start_utc')}
                for leg in pair.split('_'):groups['currency:'+leg].append(instrument)
                if item['source_review_due_at']:due.append(datetime.fromisoformat(item['source_review_due_at']))
                local[instrument]=(pointer,dates);counts['fx_pairs']=counts.get('fx_pairs',0)+1
            clocks={k:v['source_capture_completed_at'] for k,v in body['pairs'].items()}
        else:
            for product,item in body['products'].items():
                for index,contract_row in enumerate(item['contracts']):
                    ticker=contract_row['ticker'];instrument='FUTURE:'+item['venue']+':'+product+':'+ticker
                    assert identities[instrument]=={'asset_class':'dated_future','provider':'Massive','venue':item['venue'],
                        'product':product,'ticker':ticker,'definition_date':body['definition_date'],
                        'definition_source':contract_row['definition_source'],'dataset':contract_row['dataset']}
                    dates={'source_capture_completed_at':body['datasets'][contract_row['dataset']]['source_capture_completed_at'],
                        'source_review_due_at':None,'observation_clock_role':'reported_session_label_not_synchronized_price',
                        'latest_reported_session_end_date':(contract_row.get('latest_reported_row') or {}).get('session_end_date'),
                        'bar_finality_independently_verified':False}
                    local[instrument]=('/products/'+product+'/contracts/'+str(index),dates)
                    groups['futures_product:'+item['venue']+':'+product].append(instrument)
                    counts['dated_futures_contracts']=counts.get('dated_futures_contracts',0)+1
            clocks={k:v['source_capture_completed_at'] for k,v in body['datasets'].items()}
        assert row['instrument_names']==sorted(local) and row['instrument_count']==len(local)
        assert row['measurement_capture_clocks']==clocks and row['source_definition_date']==body.get('definition_date')
        deadline=min(due,default=None)
        assert row['source_review_due_at']==(deadline.isoformat() if deadline else None)
        assert row['source_review_overdue']==(cutoff>=deadline if deadline else None)
        for instrument,(pointer,dates) in local.items():
            expected[instrument]=(kind,row['node'],pointer)
            refs=output['instruments'][instrument];assert len(refs)==1;entry=refs[0]
            assert (entry['source'],entry['node'],entry['pointer'])==expected[instrument]
            assert all(entry[k]==v for k,v in dates.items()) and entry['source_generated_at']==body['generated_at']
            assert entry['source_review_overdue']==(cutoff>=datetime.fromisoformat(dates['source_review_due_at']) if dates['source_review_due_at'] else None)
            value=body
            for part in pointer.split('/')[1:]:value=value[int(part)] if isinstance(value,list) else value[part]
            assert isinstance(value,dict)
        family='fx_quotes' if kind=='fx' else 'futures_prices';members=[f for f in graph['measurement_families'] if f['family']==family]
        assert len(members)==1 and members[0]['nodes']==[row['node']] and members[0]['independent_investment_votes']==0
    assert set(output['instruments'])==set(legacy['instruments'])|set(expected)==set(identities)
    assert len(graph['nodes'])==len(legacy['dependency_graph']['nodes'])+2
    actual=graph['shared_exposures'];assert len(actual)==len(groups)
    assert {g['key']:g['instruments'] for g in actual}=={k:sorted(v) for k,v in groups.items()}
    assert all(g['independent_investment_votes']==0 for g in actual)
    assert output['quality']['bound_native_sources']==output['quality']['declared_native_sources']==7
    assert output['verification']['original_provider_replay_performed_by_composite'] is False
    counts['parent_instrument_rows']+=len(expected);counts['shared_exposure_groups']=len(groups)
    return counts


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1');read=store.reader(s3,BUCKET)
    with report('ops_6018_cross_asset_composition_candidate') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas/justhodl-massive-signals/tests/run_tests.py')],cwd=ROOT,check=True)
        baseline_doc=model.strict(model.original(BASELINE,read))
        runtime=baseline.runtime(lam,s3,events,scheduler,'justhodl-massive-signals')
        assert runtime==baseline_doc['producer_runtimes']['justhodl-massive-signals']
        packages=baseline.check_packages(lam,ROOT,baseline.CONSUMERS);assert all(p['pass'] for p in packages)
        try:prior=model.strict(baseline.get(s3,STATUS))
        except Exception as exc:
            if not store.missing(exc):raise
            prior=None
        if prior:
            assert prior['status']=='complete','Inspect incomplete qualification; never rerun silently'
            candidate=model.strict(model.original(prior['candidate'],read));accepted=prior['candidate']
        else:
            status={'status':'claimed','request_id':REQUEST,'baseline':BASELINE,'generated_at':store.now()}
            store.status_write(s3,BUCKET,STATUS,status,IfNoneMatch='*')
            def checkpoint(**fields):status.update(fields);store.status_write(s3,BUCKET,STATUS,status)
            captures={k:{**v,'status':'retained'} for k,v in baseline_doc['captures'].items()}
            inputs={'contract':'massive-composite-inputs.v2','generated_at':store.now(),
                'sources':{k:captures[k] for k in model.CAPTURE_KEYS},'predecessors':{k:captures[k] for k in model.PREDECESSORS},
                'prior_composite':captures[model.CURRENT]}
            before={k:baseline.get(s3,k) for k in (model.CURRENT,*model.PREDECESSORS)}
            assert before[model.CURRENT]==model.original(inputs['prior_composite']['original'],read),'Prior composite advanced; review before migration'
            started=time.monotonic();out=model.build(inputs,read)
            identity=store.retain(s3,BUCKET,inputs,out,read,checkpoint);elapsed=time.monotonic()-started
            assert store.replay(identity,read)==out
            counts=independent(out,read);peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            assert elapsed<90 and peak<384*1024,'Inspect composition resource budget before native deployment'
            previous=model.strict(model.original(inputs['prior_composite']['original'],read))
            assert store.legacy_store.replay(previous['replay'],read)=={k:v for k,v in previous.items() if k!='replay'}
            assert {k:baseline.get(s3,k) for k in before}==before,'Public heads changed during qualification'
            assert baseline.runtime(lam,s3,events,scheduler,'justhodl-massive-signals')==runtime
            candidate={'contract':'cross-asset-composition-candidate.v1','generated_at':store.now(),'baseline':BASELINE,
                'candidate_replay':identity,'counts':counts,'quality':out['quality'],'predecessor_runtime':runtime,
                'consumer_packages':packages,'candidate_seconds':round(elapsed,3),'peak_runner_rss_kib':peak,
                'prior_composite_replay':previous['replay'],'prior_v1_replay_verified':True,'composition_replay_verified':True,
                'parent_outputs_independently_checked':True,'original_provider_replay_performed_by_this_audit':False}
            accepted=store.protect(s3,BUCKET,model.encoded(candidate),read)
            checkpoint(status='complete',candidate=accepted)
        protected={STATUS,BASELINE['key'],accepted['key'],*(v['original']['key'] for v in baseline_doc['captures'].values())}
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(accepted_candidate=accepted,candidate_replay=candidate['candidate_replay'],counts=candidate['counts'],quality=candidate['quality'],
            candidate_seconds=candidate['candidate_seconds'],peak_runner_rss_kib=candidate['peak_runner_rss_kib'],
            prior_v1_replay_verified=True,parent_outputs_independently_checked=True,composition_replay_verified=True,
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,provider_requests=0,engine_invocations=0,
            public_head_writes=0,schedules_changed=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
