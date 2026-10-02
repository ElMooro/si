"""Invented inputs, full producer with isolated transports, and exact predecessor.

No service credentials, current packets, providers or learning ledgers are read.
"""
from pathlib import Path
from unittest.mock import patch,Mock
from contextlib import ExitStack
import ast,copy,io,json,math,random,socket,sys,types,unittest
R=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(R/'aws/shared'),str(R/'aws/shared/tests')]
import ranker_numeric as n
from ciss_vintage_test_support import load
from ranker_numeric_test_support import prior_source,D

class Numbers(unittest.TestCase):
    def test_valid_legacy_arithmetic_and_rounding_are_identical(self):
        source=(D/'lambda-before.py.txt').read_text(encoding='utf-8')
        nodes=[v for v in ast.parse(source).body if isinstance(v,ast.FunctionDef) and v.name in ('normalize_signal_score','compute_conviction')]
        scope={'math':math,'engine_trust':None};exec(compile(ast.Module(body=nodes,type_ignores=[]),'<retained predecessor>','exec'),scope)
        rng=random.Random(560)
        for _ in range(200):
            systems={name:{'score':rng.choice([-5,0,1,20,99,100,300,'15.5']), 'n_systems':rng.choice([0,1,3])} for name in ('a','b','compound')}
            weights={name:rng.choice([0,.5,1,2]) for name in systems}
            with self.subTest(systems=systems,weights=weights):
                old=scope['compute_conviction'](systems,weights);new=n.evidence(systems,weights)
                self.assertEqual((new['score'],new['contributions']),old)
                self.assertEqual(new['status'],'usable');self.assertFalse(new['portfolio_qualified'])
    def test_invalid_scores_are_unavailable_not_zero_or_maximum(self):
        for value in (None,True,False,float('nan'),float('inf'),-float('inf'),'NaN','Infinity','',' ','unavailable',[],{},10**1000):
            with self.subTest(value=repr(value)[:30]):
                out=n.evidence({'a':{'score':value}},{});self.assertIsNone(out['score']);self.assertEqual(out['status'],'unavailable')
                json.dumps(out,allow_nan=False)
    def test_valid_zero_and_empty_are_distinct(self):
        out=n.evidence({'a':{'score':0}},{});self.assertEqual(out['score'],0);self.assertEqual(out['components'][0]['normalized'],0)
        self.assertEqual(out['legacy_active_components'],0);self.assertIsNone(n.evidence({}, {})['score'])
    def test_compound_count_is_not_coerced_and_missing_default_is_disclosed(self):
        for value in ('3',True,3.5,-1,None,float('inf')):
            for score in (0,20,200):
                self.assertIsNone(n.evidence({'compound':{'score':score,'n_systems':value}}, {})['score'])
        out=n.evidence({'compound':{'score':0}},{});self.assertEqual(out['score'],10)
        self.assertEqual(out['components'][0]['compound_count_origin'],'legacy_default')
    def test_invalid_weights_and_trust_never_become_flat_defaults(self):
        for value in (None,True,False,-1,'','bad',float('nan'),float('inf')):
            self.assertIsNone(n.evidence({'a':{'score':10}},{'a':value})['score'])
            self.assertIsNone(n.evidence({'a':{'score':10}},{},lambda _:value)['score'])
        self.assertEqual(n.evidence({'a':{'score':10}},{'a':0})['score'],0)
        self.assertEqual(n.evidence({'a':{'score':10}}, {})['components'][0]['calibration_origin'],'flat_default')
    def test_trust_exception_is_explicit_and_does_not_leak_error(self):
        out=n.evidence({'a':{'score':10}},{},Mock(side_effect=PermissionError('SECRET_FIXTURE')))
        self.assertIsNone(out['score']);self.assertNotIn('SECRET_FIXTURE',json.dumps(out))
    def test_arithmetic_overflow_cannot_publish_nonfinite_result(self):
        for systems,weights,trust in [({'a':{'score':100}},{'a':1e308},None),({'a':{'score':1}},{'a':1e308},lambda _:1e308),({'a':{'score':1},'b':{'score':1}},{'a':1e308,'b':1e308},None)]:
            out=n.evidence(systems,weights,trust);self.assertIsNone(out['score']);json.dumps(out,allow_nan=False)
    def test_underflow_cannot_masquerade_as_true_zero(self):
        for systems,weights in [({'a':{'score':'1e-400'}},{}),({'a':{'score':10}},{'a':'1e-400'}),({'a':{'score':.1}},{'a':5e-324})]:
            self.assertIsNone(n.evidence(systems,weights)['score'])
        self.assertEqual(n.evidence({'a':{'score':'0.000'}},{})['score'],0)
    def test_invalid_nested_details_are_isolated_not_replaced_with_null(self):
        for value in (float('nan'),float('inf'),object()):
            out=n.evidence({'a':{'score':10,'details':{'invented':value}}}, {})
            self.assertIsNone(out['score']);self.assertEqual(out['reasons'][0]['reason'],'component_details_not_json_safe');json.dumps(out,allow_nan=False)
    def test_replay_records_unrounded_components_and_every_adjustment(self):
        base=n.evidence({'a':{'score':12.345},'b':{'score':0}},{'a':.73},lambda _:1.17)
        self.assertEqual(base['score'],round(sum(c['contribution'] for c in base['components'])*base['convergence_multiplier']/base['denominator'],1))
        trace={'base':base,'adjustments':[]};score=base['score']
        for name,mult in [('a',.82),('b',.93)]:
            new=round(score*mult,1);self.assertEqual(n.adjustment(trace,name,(new,mult,None)),(new,mult,None));score=new
        self.assertEqual(trace['adjustments'][1]['before'],trace['adjustments'][0]['after'])
        with self.assertRaises(n.InvalidNumber):n.adjustment(trace,'bad',(float('inf'),1,None))
    def test_exact_transition_preserves_every_unedited_byte(self):
        source=(R/'aws/lambdas/justhodl-master-ranker/source/lambda_function.py').read_text(encoding='utf-8')
        self.assertEqual(prior_source(source),(D/'lambda-before.py.txt').read_text(encoding='utf-8'))

class Weights(unittest.TestCase):
    path='/invented/weights'
    def row(self,name,value):return {'Name':self.path+'/'+name,'Value':value}
    def run_pages(self,pages):
        client=Mock();client.get_parameters_by_path.side_effect=pages
        result=n.load_weights(client,self.path,lambda d:{k:v for k,v in d.items() if k!='excluded'})
        return result,client
    def test_every_page_and_true_zero_survive(self):
        out,client=self.run_pages([{'Parameters':[self.row('a','0')],'NextToken':'next'},{'Parameters':[self.row('b','0.5')]}])
        self.assertEqual(out,{'a':0,'b':.5});self.assertTrue(out.diagnostics['available']);self.assertEqual(out.diagnostics['pages_read'],2)
        self.assertEqual(client.get_parameters_by_path.call_args.kwargs,{'Path':self.path,'Recursive':True,'WithDecryption':False,'NextToken':'next'})
    def test_bad_present_weight_stays_null_and_never_becomes_missing(self):
        out,_=self.run_pages([{'Parameters':[self.row('a','bad'),self.row('b','0.5'),self.row('excluded','NaN')]}])
        self.assertEqual(out,{'a':None,'b':.5});self.assertIsNone(n.evidence({'a':{'score':50}},out)['score']);self.assertEqual(n.evidence({'b':{'score':50}},out)['score'],25)
    def test_duplicate_basename_is_not_last_write_wins(self):
        out,_=self.run_pages([{'Parameters':[self.row('a','1')],'NextToken':'next'},{'Parameters':[self.row('nested/a','0')]}])
        self.assertIsNone(out['a']);self.assertEqual(out.diagnostics['reasons'][0]['reason'],'duplicate_calibration_name')
    def test_late_service_failure_discards_incomplete_census(self):
        out,_=self.run_pages([{'Parameters':[self.row('a','1')],'NextToken':'next'},PermissionError('SECRET_FIXTURE')])
        self.assertFalse(out.diagnostics['available']);self.assertEqual(out,{});self.assertIsNone(n.evidence({'b':{'score':10}},out)['score']);self.assertNotIn('SECRET_FIXTURE',repr(out.diagnostics))
    def test_repeated_or_malformed_tokens_and_names_refuse_census(self):
        for pages in ([{'Parameters':[],'NextToken':'x'},{'Parameters':[],'NextToken':'x'}],[{'Parameters':[],'NextToken':True}],[{'Parameters':[{'Name':'/elsewhere/a','Value':'1'}]}],[{}],[{'Parameters':None}]):
            out,_=self.run_pages(pages);self.assertFalse(out.diagnostics['available'])
    def test_successful_empty_configuration_discloses_legacy_default(self):
        out,_=self.run_pages([{'Parameters':[]}]);self.assertTrue(out.diagnostics['available']);self.assertEqual(n.evidence({'a':{'score':10}},out)['score'],10)

class Handler(unittest.TestCase):
    def setUp(self):
        self.network=patch.object(socket.socket,'connect',side_effect=AssertionError('Network forbidden in fixture'));self.network.start();self.addCleanup(self.network.stop)
    def execute(self,idx,weights=None,packets=None,legacy=False,macro=None):
        m=load('justhodl-master-ranker')
        if legacy:
            with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None)}):
                exec(compile((D/'lambda-before.py.txt').read_bytes(),'<whole retained producer>','exec'),m.__dict__)
        writes={};reads=[];events=[]
        def get(**kw):
            reads.append(kw['Key']);self.assertEqual(kw['Key'],m.S3_KEY_OUT+'.prev');return {'Body':io.BytesIO(b'{"top_tickers":[]}')}
        def put(**kw):
            value=json.loads(kw['Body'],parse_constant=lambda _:(_ for _ in ()).throw(AssertionError('Nonstandard JSON')));writes[kw['Key']]=value
        packets=packets or {};feeds={'institutional_13f_context':{},'holdings_exclusions':[],**packets.get('feeds',{})}
        with ExitStack() as stack:
            for name,value in {'S3':types.SimpleNamespace(get_object=get,put_object=put),'engine_trust':None,'_risk_gate_doc':lambda:{'posture':'NORMAL'},'get_regime_context':lambda:{},'load_calibration_weights':lambda:weights if weights is not None else {},'build_ticker_index':lambda:(copy.deepcopy(idx),feeds),'fetch_json':lambda key,*a,**kw:copy.deepcopy(packets.get(key)),'collect_macro_signals':lambda:macro or [],'census_idx':lambda *a:{}}.items():stack.enter_context(patch.object(m,name,value))
            stack.enter_context(patch.dict(sys.modules,{'ticker_coverage_context':types.SimpleNamespace(load=lambda *a:{}),'wl_fusion':types.SimpleNamespace(block=lambda *a:{}),'system_events':types.SimpleNamespace(publish_many=events.append)}))
            try:result=m.lambda_handler({'suppress_events':True},None)
            except ValueError:
                self.assertEqual(writes,{})
                raise
        self.assertEqual(result['statusCode'],200);self.assertEqual(events,[])
        return writes[m.S3_KEY_OUT],writes[m.S3_KEY_OUT+'.prev'],reads
    def test_whole_handler_preserves_valid_scores_overlays_rationale_and_order(self):
        idx={'QAONE':{'a':{'score':10}},'QATWO':{'compound':{'score':50,'n_systems':3}},'QAZERO':{'a':{'score':0}}}
        packets={'feeds':{'capital_flow':{'complexes':[{'pump_probability':80,'complex':'invented','ref_stocks':['QAONE']}]}},'data/accumulation-radar.json':{'tops':{'stocks':[{'ticker':'QAONE','flag':'LIKELY_TOP','divergence':'bearish'}]}},'data/beneish.json':{'red_flags':[{'ticker':'QAONE'}]}}
        old,_,_=self.execute(idx,packets=packets,legacy=True);new,_,_=self.execute(idx,packets=packets)
        for before,after in zip(old['top_tickers'],new['top_tickers']):
            trace=after['score_calculation'];self.assertEqual(trace['adjustments'][-1]['after'],after['score']);self.assertEqual(len(trace['adjustments']),9)
            self.assertEqual(after['audit_trail']['score_calculation'],trace)
            clean=copy.deepcopy(after);clean.pop('score_calculation');clean['audit_trail'].pop('score_calculation');self.assertEqual(before,clean)
        self.assertEqual(new['numeric_quality']['rankable_tickers'],3)
    def test_invalid_rows_are_visible_but_cannot_sort_or_enter_events(self):
        for value in (None,True,'bad',float('nan'),float('inf')):
            out,state,_=self.execute({'QABAD':{'a':{'score':value}},'QAZERO':{'a':{'score':0}},'QAGOOD':{'a':{'score':20}}})
            self.assertEqual([r['ticker'] for r in out['top_tickers']],['QAGOOD','QAZERO']);self.assertEqual(out['numeric_quality']['status'],'partial')
            self.assertEqual(out['numeric_quality']['input_tickers'],3);self.assertEqual(out['numeric_quality']['unranked_tickers'],1)
            self.assertIsNone(out['unranked_tickers'][0]['score']);self.assertNotIn('QABAD',[r['ticker'] for r in state['top_tickers']]);json.dumps(out,allow_nan=False)
    def test_no_usable_calibration_is_not_positive_flat_ranking(self):
        out,_,_=self.execute({'QAONE':{'a':{'score':50}}},n.Calibration({},available=False))
        self.assertEqual(out['top_tickers'],[]);self.assertEqual(out['numeric_quality']['status'],'unavailable');self.assertFalse(out['calibration_quality']['available'])
    def test_row_local_overlay_failure_does_not_destroy_other_rows(self):
        out,_,_=self.execute({'QAONE':{'a':{'score':50}},'QATWO':{'a':{'score':20}}},packets={'feeds':{'capital_flow':{'complexes':[{'pump_probability':'bad','ref_stocks':['QAONE']}]}}})
        self.assertEqual([r['ticker'] for r in out['top_tickers']],['QATWO']);self.assertEqual(out['unranked_tickers'][0]['reasons'][-1]['reason'],'overlay_or_rationale_unavailable')
    def test_unrelated_nonfinite_payload_fails_before_any_write(self):
        with self.assertRaises(ValueError):self.execute({'QAONE':{'a':{'score':10}}},macro=[{'type':'fixture','score':float('nan')}])
    def test_actual_index_keeps_zero_and_two_explicit_context_records(self):
        m=load('justhodl-master-ranker')
        packets={'data/asymmetric-scorer.json':{'top_setups':[{'symbol':'QAONE','score':0,'asymmetry_score':88}]},
                 'data/nobrainers.json':{'top_setups':[{'ticker':'QAONE','score':0,'asymmetry_score':99}]},
                 'data/insider-clusters.json':{'clusters':[{'ticker':'QAONE','has_ceo':True},{'ticker':'QACONTEXT','has_ceo':True}]},
                 'data/fmp-ratios.json':{'tickers':{'QAONE':{'pe':0},'QACONTEXT':{'pe':0}}}}
        with patch.object(m,'fetch_json',side_effect=lambda key,*a,**kw:packets.get(key)):idx,_=m.build_ticker_index()
        self.assertEqual(idx['QAONE']['asymmetric']['score'],0);self.assertEqual(idx['QAONE']['nobrainers']['score'],0)
        out,_,_=self.execute(idx);self.assertEqual(len(out['top_tickers']),1);row=out['top_tickers'][0]
        self.assertEqual(row['score'],0);self.assertEqual(row['details']['fmp_ratios']['pe'],0)
        components=row['score_calculation']['base']['components'];self.assertEqual(sum(c['status']=='context_only' for c in components),2)
        self.assertEqual(out['unranked_tickers'][0]['ticker'],'QACONTEXT');self.assertIsNone(out['unranked_tickers'][0]['score'])
    def test_context_metadata_preserves_valid_predecessor_arithmetic_without_a_vote(self):
        idx={'QAONE':{'a':{'score':20},'insider':{'has_ceo':True},'fmp_ratios':{'pe':0}}}
        old,_,_=self.execute(idx,legacy=True);out,_,_=self.execute(idx)
        self.assertEqual(old['top_tickers'][0]['score'],out['top_tickers'][0]['score'])
        self.assertEqual(len(out['top_tickers'][0]['contributions']),1)
    def test_context_exception_cannot_hide_declared_missing_scores_or_invalid_details(self):
        for name,details in [('insider',{'score':None}),('fmp_ratios',{'score':None}),('insider',{'detail':float('nan')}),('unknown_context',{'detail':1})]:
            out=n.evidence({'a':{'score':10},name:details},{})
            self.assertIsNone(out['score'])

if __name__=='__main__':unittest.main(verbosity=2)
