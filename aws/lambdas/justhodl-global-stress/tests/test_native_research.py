"""Original vendor fixtures, adversarial data, replay and actual consumer boundary tests."""
import ast,copy,hashlib,io,json,sys,unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-global-stress/source'),str(ROOT/'aws/shared')]
import global_research_model as m
import global_research_store as s
import gsi_authority as a
FIXTURES=Path(__file__).parent/'fixtures'

def fixture():
    # Source bytes are real preflight captures; this is a synthetic run envelope.
    inputs={'contract':'global-stress-inputs.v1','evaluation_date':'2026-09-20','generated_at':'2026-09-20T08:10:00+00:00',
        'history_start':'2024-07-12','price_end':'2026-09-19','sources':{},'context':{}}
    bodies={}
    for name,meta in json.loads((FIXTURES/'sources.json').read_bytes()).items():
        raw=(FIXTURES/(name.replace(':','-')+'.json')).read_bytes();sha=hashlib.sha256(raw).hexdigest();req=hashlib.sha256(meta['request_url'].encode()).hexdigest()
        inputs['sources'][name]={'status':'captured','request_url':meta['request_url'],'request_sha256':req,
            'acquired_at':'2026-09-20T08:09:29+00:00','bytes':len(raw),'sha256':sha,'key':m.PREFIX+'sources/'+req+'/'+sha+'.json'}
        bodies[name]=raw
    return inputs,bodies
def mutate(inputs,bodies,name,change):
    doc=json.loads(bodies[name]);change(doc);raw=m.encoded(doc);bodies[name]=raw;ref=inputs['sources'][name]
    ref['sha256']=hashlib.sha256(raw).hexdigest();ref['bytes']=len(raw);ref['key']=m.PREFIX+'sources/'+ref['request_sha256']+'/'+ref['sha256']+'.json'
class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self):self.objects={};self.writes=[]
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[kw['Key']];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest(),'LastModified':datetime(2026,9,20,tzinfo=timezone.utc)}
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)
def actual(fn,names,scope=None):
    p=ROOT/'aws/lambdas'/('justhodl-'+fn)/'source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names),(fn,names)
    result=scope or {};exec(compile(ast.Module(body=nodes,type_ignores=[]),str(p),'exec'),result);return result

class Native(unittest.TestCase):
    def setUp(self):self.i,self.b=fixture()
    def build(self):return m.build(self.i,self.b)
    def test_original_provider_values_separate_adjustments_and_dates(self):
        out=self.build();self.assertEqual(out['source_failures'],{})
        self.assertEqual(out['quality']['fresh_instruments'],14);self.assertEqual(out['quality']['fresh_native_series'],10)
        spy=out['instruments']['SPY'];self.assertEqual(spy['close'],761.69)
        self.assertEqual(spy['returns']['21']['price']['start_date'],'2026-08-19')
        self.assertAlmostEqual(spy['returns']['21']['price']['value'],-.9583127454294904)
        self.assertNotEqual(spy['returns']['21']['price']['value'],spy['returns']['21']['dividend_adjusted']['value'])
        self.assertEqual(len(spy['history']),549);self.assertEqual(spy['isin'],'US78462F1030')
        self.assertIsNone(out['global_stress_index']);self.assertFalse(out['sizing_eligible'])
    def test_wrong_currency_exchange_identity_and_boolean_etf_are_rejected(self):
        for k,v in [('currency','EUR'),('isin','US0000000000'),('exchange','FOREX'),('isEtf',False)]:
            i,b=fixture();mutate(i,b,'profile:SPY',lambda d:d[0].update({k:v}))
            self.assertIn('SPY',m.build(i,b)['source_failures'])
    def test_missing_latest_does_not_use_prior_row_or_neutral_value(self):
        mutate(self.i,self.b,'dividend-adjusted:SPY',lambda d:d[0].update(adjClose=None))
        spy=self.build()['instruments']['SPY'];self.assertIsNone(spy['adjusted_close']);self.assertIsNone(spy['drawdown']['value'])
        self.assertIsNone(spy['returns']['21']['dividend_adjusted']['value']);self.assertEqual(spy['quality']['status'],'unavailable')
    def test_one_ledger_missing_date_remains_visible_and_breaks_derived_windows(self):
        mutate(self.i,self.b,'dividend-adjusted:SPY',lambda d:d.pop(5))
        spy=self.build()['instruments']['SPY'];self.assertEqual(spy['coverage']['missing_in_one_ledger'],1)
        self.assertEqual(len(spy['history']),549);self.assertIsNone(spy['volatility']['value'])
        self.assertIsNone(spy['returns']['21']['dividend_adjusted']['value'])
    def test_tampered_request_and_source_bytes_are_rejected(self):
        self.b['full:SPY']+=b' ';self.assertIn('SPY',self.build()['source_failures'])
        self.i,self.b=fixture();ref=self.i['sources']['full:SPY'];ref['request_url']=ref['request_url'].split('&from=')[0]
        ref['request_sha256']=hashlib.sha256(ref['request_url'].encode()).hexdigest()
        self.assertIn('SPY',self.build()['source_failures'])
    def test_duplicate_future_invalid_and_mismatched_symbol_rows_rejected(self):
        for change in ({'date':'2026-09-17'},{'date':'2026-09-21'},{'close':'NaN'},{'close':0},{'symbol':'SPX'}):
            i,b=fixture();mutate(i,b,'full:SPY',lambda d:d[0].update(change));self.assertIn('SPY',m.build(i,b)['source_failures'])
    def test_native_frequency_units_and_incomplete_fred_response_rejected(self):
        mutate(self.i,self.b,'definition:DGS10',lambda d:d['seriess'][0].update(units='Basis Points'))
        self.assertIn('DGS10',self.build()['source_failures'])
        self.i,self.b=fixture();mutate(self.i,self.b,'observations:DGS10',lambda d:d.update(count=d['count']+1))
        self.assertIn('DGS10',self.build()['source_failures'])
    def test_credit_spread_requires_same_date_not_latest_of_each(self):
        out=self.build();day=out['credit_dispersion']['observation_date'];self.assertIsNotNone(out['credit_dispersion']['value'])
        mutate(self.i,self.b,'observations:BAMLH0A1HYBB',lambda d:[r.update(value='.') for r in d['observations'] if r['date']==day])
        self.assertIsNone(self.build()['credit_dispersion']['value'])
    def test_date_matched_correlation_never_aligns_by_row_number(self):
        mutate(self.i,self.b,'full:FEZ',lambda d:d.pop(10));mutate(self.i,self.b,'dividend-adjusted:FEZ',lambda d:d.pop(10))
        out=self.build();r=next(x for x in out['correlations']['pairs'] if x['left']=='SPY' and x['right']=='FEZ')
        self.assertEqual(r['n_matched_intervals'],58)
        # The two-date interval in FEZ is excluded rather than matched to a SPY daily row.
        spy=m.interval_returns(out['instruments']['SPY']['history'],'adjusted_close');fez=m.interval_returns(out['instruments']['FEZ']['history'],'adjusted_close')
        self.assertTrue(all(tuple(d) in spy and tuple(d) in fez for d in r['matched_intervals']))
    def test_fewer_than_40_matched_intervals_cannot_earn_correlation(self):
        out=m.correlations({'SPY':{'history':[{'date':'2026-09-01','adjusted_close':'1'},{'date':'2026-09-02','adjusted_close':'2'}]}})
        self.assertTrue(all(r['value'] is None for r in out['pairs']))
    def test_stale_age_uses_observation_not_generated_at(self):
        self.assertEqual(m.quality('2026-09-02','2026-09-20')['status'],'stale')
        self.i['sources']['full:SPY']['acquired_at']='2026-09-19T08:10:00Z';self.assertIn('SPY',self.build()['source_failures'])
    def test_no_sources_is_abstention_not_zero_stress(self):
        self.i['sources']={};out=self.build();self.assertEqual(len(out['source_failures']),24);self.assertEqual(out['decision']['verb'],'WAIT')
        self.assertIsNone(out['global_stress_index']);self.assertFalse(out['calls_eligible'])
    def test_complete_output_matches_portable_reference_digest(self):
        expected=(FIXTURES/'expected-output.sha256').read_text().strip()
        self.assertEqual(m.digest(self.build()),expected)
    def test_ambient_decimal_precision_and_rounding_cannot_change_replay(self):
        from decimal import localcontext,ROUND_UP
        expected=self.build()
        with localcontext() as ctx:
            ctx.prec=8;ctx.rounding=ROUND_UP
            self.assertEqual(self.build(),expected)
    def test_immutable_replay_and_tamper_rejection(self):
        store=Storage()
        for name,ref in self.i['sources'].items():s.immutable(store,'b',ref['key'],self.b[name])
        out=self.build();ref=s.retain(store,'b',self.i,out);self.assertEqual(s.replay(ref,s.reader(store,'b')),out)
        store.objects[self.i['sources']['full:SPY']['key']]+=b' '
        with self.assertRaises(ValueError):s.replay(ref,s.reader(store,'b'))
    def test_publication_preserves_full_prior_packet_and_prevents_older_write(self):
        store=Storage();old=b'{"generated_at":"2026-09-19T00:00:00Z","legacy_score":99}'
        store.objects[m.CURRENT]=old;out=self.build();self.assertTrue(s.publish(store,'b',out))
        self.assertEqual(store.objects[s.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin'],old)
        self.assertFalse(s.publish(store,'b',json.loads(old)))
        other=copy.deepcopy(out);other['global_stress_index']=99
        with self.assertRaises(ValueError):s.publish(store,'b',other)
    def test_collector_preserves_full_query_without_credentials_and_bounds_requests(self):
        store=Storage();requests=[]
        def transport(req,**kw):requests.append(req);return io.BytesIO(b'[{"ok":true}]')
        c=s.Collector(store,'b','fred-secret','fmp-secret',transport)
        url='https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=SPY&from=2024-07-12&to=2026-09-19'
        self.assertIsNotNone(c.fetch('full:SPY',url,'fmp'));self.assertEqual(c.sources['full:SPY']['request_url'],url)
        self.assertNotIn('fmp-secret',json.dumps(c.sources));self.assertNotIn('fmp-secret',requests[0].full_url)
        self.assertEqual(requests[0].get_header('Apikey'),'fmp-secret')
        c.calls=62;self.assertIsNone(c.fetch('x',url,'fmp'));self.assertEqual(len(requests),1)
    def test_reflected_secret_not_retained(self):
        store=Storage();c=s.Collector(store,'b','fred-secret','fmp-secret',lambda *a,**k:io.BytesIO(b'{"x":"fmp-secret"}'))
        self.assertIsNone(c.fetch('profile:SPY','https://financialmodelingprep.com/stable/profile?symbol=SPY','fmp'));self.assertEqual(store.writes,[])

class Boundaries(unittest.TestCase):
    SPOOF={'global_stress_index':99,'global_stress_level':'ACUTE','score':99,'gsi':99,'composite':99,
        'equities':[{'stress':99}],'credit':{'spreads':[{'stress_score':99}]},'calls_eligible':True,'sizing_eligible':True,'decision_qualification':{'status':'qualified'}}
    def test_actual_crisis_signal_and_canary_consumers_deny_spoofed_scores(self):
        for fn,name in [('crisis-composite','comp_global_stress'),('signal-board','n_global_stress'),('canary-warroom','norm_global_stress')]:
            f=actual(fn,[name])[name];out=f(self.SPOOF)
            if fn=='crisis-composite':self.assertIsNone(out)
            elif fn=='signal-board':self.assertIsNone(out[0])
            else:self.assertEqual(out[1],[]);self.assertIsNone(out[0]['score']);self.assertEqual(out[0]['n_total'],0)
    def test_actual_router_does_not_convert_missing_gsi_to_zero_or_hundred(self):
        scope=actual('regime-conditional-router',['detect_dollar_shortage','detect_dollar_smile_left','detect_dollar_smile_right'],{'safe_get':lambda d,k:d.get(k) if isinstance(d,dict) else None})
        score,e=scope['detect_dollar_smile_left']({'score':70},self.SPOOF,{'score':0});self.assertEqual(score,0);self.assertIsNone(e['global_stress'])
        score,e=scope['detect_dollar_smile_left']({'score':70},self.SPOOF,{'score':60});self.assertEqual(score,65)
        score,e=scope['detect_dollar_smile_right']({'score':70},self.SPOOF,{'score':0},{});self.assertEqual(score,0);self.assertIsNone(e['global_stress'])
    def test_actual_fanout_cannot_broadcast_unqualified_gsi(self):
        scope=actual('streaming-fanout',['_extract_summary','_is_meaningful_delta'])
        engine={'name':'global_stress','summary_fields':list(self.SPOOF)}
        result=scope['_extract_summary'](engine,self.SPOOF);self.assertNotIn('global_stress_index',result)
        self.assertEqual(scope['_is_meaningful_delta'](engine,None,result),(False,'unqualified_global_stress'))
    def test_actual_website_distillation_removes_score_before_narrative(self):
        store=Storage();store.objects[m.CURRENT]=m.encoded(self.SPOOF)
        scope=actual('ai-website-synthesis',['fetch_engine'],{'s3':store,'S3_BUCKET':'b','json':json,'datetime':datetime,'timezone':timezone})
        _,d=scope['fetch_engine']('global_stress',{'key':m.CURRENT,'fields':['global_stress_index','global_stress_level']})
        self.assertIsNone(d['global_stress_index']);self.assertIsNone(d['global_stress_level'])
    def test_historical_gsi_cannot_reenter_redundancy_weights(self):
        rows=[{'date':'2026-09-01','scores':{'global_stress':99,'gsi_total':99,'gsi_credit':88,'other':25}}]
        clean=a.filter_history(rows);self.assertEqual(clean[0]['scores'],{'other':25});self.assertEqual(rows[0]['scores']['global_stress'],99)
    def test_actual_status_handlers_preserve_full_reports_without_weights_or_notifications(self):
        for fn,key in [('gsi-calibrator','data/gsi-calibration.json'),('gsi-horizons','data/gsi-horizons.json')]:
            store=Storage();old=b'{"weights":{"credit":0.5},"unqualified_history":[1,2,3]}'
            store.objects[key]=old;store.objects[m.CURRENT]=m.encoded({'contract':m.CONTRACT,'replay':{'manifest_key':'example'}})
            scope=actual(fn,['lambda_handler'],{'s3':store,'S3_BUCKET':'b','REPORT_KEY':key,'json':json,'datetime':datetime,'timezone':timezone})
            result=scope['lambda_handler']({'test_telegram':True,'backfill':True});self.assertEqual(result['statusCode'],200)
            self.assertEqual(store.objects[s.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin'],old)
            out=json.loads(store.objects[key]);self.assertEqual(out['weights'],{});self.assertEqual(out['term_structure'],[]);self.assertEqual(out['ssm_weight_writes'],0)
    def test_actual_producer_event_cannot_select_legacy_side_effects(self):
        calls=[]
        scope=actual('global-stress',['lambda_handler'],{'s3':'client','S3_BUCKET':'b','FRED_KEY':'fred','FMP':'fmp','json':json})
        with patch.object(s,'run',lambda *args,**kw:calls.append(args) or {'published':True}):
            result=scope['lambda_handler']({'backfill':True,'test_telegram':True,'force':True})
        self.assertEqual(result['statusCode'],200);self.assertEqual(calls,[('client','b','fred','fmp')])
    def test_self_asserted_qualification_never_confers_authority(self):
        for p in (self.SPOOF,{},None):self.assertIsNone(a.qualified_score(p));self.assertFalse(a.decision_view(p)['sizing_eligible'])
    def test_actual_allocator_primary_and_defensive_override_paths(self):
        p=ROOT/'aws/lambdas/justhodl-master-allocator/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        gather=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='gather_signals')
        start=next(i for i,n in enumerate(gather.body) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='gs')
        scope={'read_json':lambda k:self.SPOOF,'out':{}}
        exec(compile(ast.Module(body=gather.body[start:start+3],type_ignores=[]),'actual-allocator-GSI','exec'),scope)
        self.assertEqual(scope['out'],{})
        # The independent best-asset defensive override previously accepted aliases.
        owner=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and any(isinstance(x,ast.Assign) and isinstance(x.targets[0],ast.Name) and x.targets[0].id=='gsi' for x in n.body) and n.name!='gather_signals')
        start=next(i for i,n in enumerate(owner.body) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='gsi')
        scope={'_ba_s3json':lambda k:self.SPOOF,'reasons':[]}
        exec(compile(ast.Module(body=owner.body[start:start+2],type_ignores=[]),'actual-allocator-override','exec'),scope)
        self.assertEqual(scope['reasons'],[])
    def test_actual_cycle_and_live_pulse_strip_narrative_and_drift(self):
        p=ROOT/'aws/lambdas/justhodl-cycle-clock/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        assignments=[n for n in ast.walk(tree) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id in ('gstress','gstress_idx')]
        scope={'load':lambda *a:{**self.SPOOF,'stress_momentum':{'percentile':99}}}
        exec(compile(ast.Module(body=sorted(assignments,key=lambda n:n.lineno),type_ignores=[]),'actual-cycle-GSI','exec'),scope)
        self.assertIsNone(scope['gstress_idx']);self.assertNotIn('stress_momentum',scope['gstress'])
        p=ROOT/'aws/lambdas/justhodl-live-pulse/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        owner=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        start=next(i for i,n in enumerate(owner.body) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='gsi_doc')
        scope={'read_json':lambda k:self.SPOOF,'GSI_KEY':m.CURRENT,'pulse':80}
        exec(compile(ast.Module(body=owner.body[start:start+4],type_ignores=[]),'actual-pulse-GSI','exec'),scope)
        self.assertIsNone(scope['morning_gsi']);self.assertIsNone(scope['drift'])
    def test_actual_morning_narrative_cannot_restore_global_score_aliases(self):
        p=ROOT/'aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        node=next(n for n in ast.walk(tree) if isinstance(n,ast.Lambda) and [a.arg for a in n.args.args]==['gs'])
        f=eval(compile(ast.Expression(body=node),'actual-morning-GSI','eval'),{'data':{'global_stress':self.SPOOF}})
        result=f();self.assertIsNone(result['global_stress_index']);self.assertIsNone(result['global_stress_level']);self.assertEqual(result['global_flashing_red'],[])
    def test_actual_calibration_fleet_excludes_current_and_all_legacy_history_paths(self):
        p=ROOT/'aws/lambdas/justhodl-calibration-fleet/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        loops=[n for n in handler.body if isinstance(n,ast.For) and isinstance(n.iter,ast.Name) and n.iter.id=='REGISTRY']
        scope={'REGISTRY':[{'name':'global_stress','label':'Global Stress','direction':'stress','source_key':m.CURRENT,'score_path':['global_stress_index']}],
            'read_json':lambda k:self.SPOOF,'current_scores':{},'engine_status':[],'engines_out':[],'weight_props':{}}
        # Current-score and calibration loops run; any historical read after the guard would fail.
        exec(compile(ast.Module(body=loops,type_ignores=[]),'actual-calibration-fleet-GSI','exec'),scope)
        self.assertEqual(scope['current_scores'],{});self.assertEqual(scope['weight_props'],{})
        self.assertEqual(scope['engines_out'][0]['quality_rating'],'UNQUALIFIED')

if __name__=='__main__':unittest.main()
