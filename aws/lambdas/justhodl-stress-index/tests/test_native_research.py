import ast,copy,hashlib,io,json
from pathlib import Path
import sys,unittest
from urllib.parse import urlencode
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/lambdas/justhodl-stress-index/source'),str(ROOT/'aws/shared')]
import stress_model as m
import stress_store as s
import jsi_authority as a
FIXTURES=Path(__file__).parent/'fixtures'


def fixture():
    inputs={'contract':'stress-inputs.v1','evaluation_date':'2026-09-20','generated_at':'2026-09-20T08:00:00+00:00','sources':{},'context':{}}
    bodies={}
    for sid in m.SERIES:
        q=dict(series_id=sid,file_type='json',realtime_start='2026-09-20',realtime_end='2026-09-20')
        for kind in ('definition','observations'):
            if kind=='observations':q.update(observation_start='1990-01-01',observation_end='2026-09-20',units='lin',sort_order='asc',limit=20000,offset=0,output_type=1)
            name=kind+':'+sid;url='https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?'+urlencode(q)
            raw=(FIXTURES/(kind+'-'+sid+'.json')).read_bytes();sha=hashlib.sha256(raw).hexdigest();req=hashlib.sha256(url.encode()).hexdigest()
            inputs['sources'][name]={'status':'captured','provider':'fred','request_url':url,'request_sha256':req,'key':m.PREFIX+'sources/'+req+'/'+sha+'.json',
                'sha256':sha,'bytes':len(raw),'acquired_at':'2026-09-20T07:42:39+00:00'}
            bodies[name]=raw
    return inputs,bodies


def mutate(inputs,bodies,name,edit):
    doc=json.loads(bodies[name]);edit(doc);raw=m.encoded(doc);bodies[name]=raw;ref=inputs['sources'][name]
    ref['sha256']=hashlib.sha256(raw).hexdigest();ref['bytes']=len(raw);ref['key']=m.PREFIX+'sources/'+ref['request_sha256']+'/'+ref['sha256']+'.json'


class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.writes=[]
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[kw['Key']];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}


class Native(unittest.TestCase):
    def setUp(self):self.inputs,self.bodies=fixture()
    def build(self):return m.build(self.inputs,self.bodies)
    def test_original_provider_values_and_frequency(self):
        out=self.build();self.assertEqual(out['source_failures'],{})
        rows=out['measurements'];self.assertEqual(rows['NFCI']['value'],-.56)
        self.assertEqual(rows['KCFSI']['observation_period'],'2026-08');self.assertEqual(rows['KCFSI']['frequency_short'],'M')
        self.assertEqual(rows['WRESBAL']['value'],3013794);self.assertEqual(rows['WRESBAL']['unit'],'Millions of U.S. Dollars')
        self.assertEqual(len(rows['WALCL']['history']),1240);self.assertEqual(len(rows['VIXCLS']['history']),9578)
        self.assertIsNone(out['jsi']);self.assertEqual(out['decision']['verb'],'WAIT');self.assertFalse(out['sizing_eligible'])
    def test_credit_history_cannot_earn_1990_or_five_year_rank(self):
        row=self.build()['measurements']['BAMLH0A0HYM2'];self.assertEqual(row['history_span']['first_finite'],'2023-09-19')
        self.assertIsNone(row['percentiles']['5y']['value']);self.assertIsNotNone(row['percentiles']['2y']['value'])
    def test_weekly_changes_use_calendar_dates_not_63_rows(self):
        row=self.build()['measurements']['WALCL'];c=row['change']
        self.assertEqual(c['target_date'],'2026-06-17');self.assertEqual(c['current_row_index']-c['baseline_row_index'],13)
        self.assertAlmostEqual(c['value'],row['value']-row['history'][c['baseline_row_index']]['value'])
    def test_monthly_comparison_is_three_months_not_three_days(self):
        c=self.build()['measurements']['KCFSI']['change'];self.assertEqual(c['target_date'],'2026-05-01')
        self.assertEqual(c['current_row_index']-c['baseline_row_index'],3)
    def test_missing_latest_never_becomes_last_good(self):
        mutate(self.inputs,self.bodies,'observations:NFCI',lambda d:d['observations'][-1].update(value='.'))
        r=self.build()['measurements']['NFCI'];self.assertIsNone(r['value']);self.assertIsNone(r['percentiles']['2y']['value'])
    def test_missing_exact_baseline_never_uses_nearby_date(self):
        mutate(self.inputs,self.bodies,'observations:WALCL',lambda d:d['observations'][-14].update(value='.'))
        self.assertIsNone(self.build()['measurements']['WALCL']['change']['value'])
    def test_same_date_funding_spread_uses_sofr_date_not_future_iorb(self):
        r=self.build()['spreads']['sofr_iorb'];self.assertEqual(r['observation_date'],'2026-09-17')
        self.assertEqual(r['right_latest_date'],'2026-09-20');self.assertEqual(r['value'],-5)
        mutate(self.inputs,self.bodies,'observations:IORB',lambda d:[v.update(value='.') for v in d['observations'] if v['date']=='2026-09-17'])
        self.assertIsNone(self.build()['spreads']['sofr_iorb']['value'])
    def test_units_and_frequency_drift_rejected(self):
        for field,value in [('units','Billions of US Dollars'),('frequency_short','D')]:
            i,b=fixture();mutate(i,b,'definition:WRESBAL',lambda d:d['seriess'][0].update({field:value}))
            self.assertIn('WRESBAL',m.build(i,b)['source_failures'])
    def test_tamper_and_incomplete_query_rejected(self):
        self.bodies['observations:VIXCLS']+=b' ';self.assertIn('VIXCLS',self.build()['source_failures'])
        self.inputs,self.bodies=fixture();mutate(self.inputs,self.bodies,'observations:VIXCLS',lambda d:d.update(count=d['count']+1))
        self.assertIn('VIXCLS',self.build()['source_failures'])
    def test_duplicates_wrong_vintage_and_future_rows_rejected(self):
        for change in ({'date':'2026-09-16'},{'date':'2026-09-21'},{'realtime_start':'2020-01-01'},{'value':'NaN'}):
            i,b=fixture();mutate(i,b,'observations:VIXCLS',lambda d:d['observations'][-1].update(change))
            self.assertIn('VIXCLS',m.build(i,b)['source_failures'])
    def test_source_request_identity_and_run_clock(self):
        self.inputs['sources']['observations:VIXCLS']['request_url']+='&units=pc1'
        self.assertIn('VIXCLS',self.build()['source_failures'])
        self.inputs['generated_at']='2026-09-21T08:00:00+00:00'
        with self.assertRaises(ValueError):self.build()
    def test_native_frequency_age_not_wrapper_freshness(self):
        i,b=fixture();i['generated_at']='2026-10-15T08:00:00+00:00';i['evaluation_date']='2026-10-15'
        # Direct series age logic uses evaluation_date after strictly bound source query checks.
        for ref in i['sources'].values():ref['request_url']=ref['request_url'].replace('2026-09-20','2026-10-15');ref['request_sha256']=hashlib.sha256(ref['request_url'].encode()).hexdigest();ref['acquired_at']=i['generated_at']
        for name in b:
            def update(d):
                d['realtime_start']=d['realtime_end']='2026-10-15'
                for row in d.get('observations',[]):row['realtime_start']=row['realtime_end']='2026-10-15'
            mutate(i,b,name,update)
        out=m.build(i,b);self.assertEqual(out['measurements']['NFCI']['quality']['status'],'stale')
        self.assertEqual(out['measurements']['KCFSI']['quality']['age_days'],75)
    def test_midrank_with_ties_is_not_minmax_rescaling(self):
        row=self.build()['measurements']['NFCI'];p=row['percentiles']['2y'];values=[r['value'] for r in row['history'] if r['date']>=p['window_start'] and r['value'] is not None]
        expected=100*(sum(v<row['value'] for v in values)+.5*sum(v==row['value'] for v in values))/len(values)
        self.assertAlmostEqual(p['value'],expected)
    def test_no_sources_is_research_abstention(self):
        self.inputs['sources']={};out=self.build();self.assertEqual(len(out['source_failures']),13)
        self.assertFalse(out['calls_eligible']);self.assertIsNone(out['jsi'])
    def test_immutable_replay_and_source_tamper(self):
        store=Storage()
        for name,ref in self.inputs['sources'].items():s.immutable(store,'b',ref['key'],self.bodies[name])
        out=self.build();ref=s.retain(store,'b',self.inputs,out)
        self.assertEqual(s.replay(ref,s.reader(store,'b')),out)
        key=self.inputs['sources']['observations:NFCI']['key'];store.objects[key]+=b' '
        with self.assertRaises(ValueError):s.replay(ref,s.reader(store,'b'))
    def test_publication_preserves_complete_predecessor_and_refuses_older_run(self):
        store=Storage();old=b'{"generated_at":"2026-09-19T00:00:00Z","legacy":99}'
        store.objects[m.CURRENT]=old;packet=self.build();self.assertTrue(s.publish(store,'b',packet))
        self.assertEqual(store.objects[s.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin'],old)
        self.assertFalse(s.publish(store,'b',json.loads(old)))
        bad=copy.deepcopy(packet);bad['jsi']=99
        with self.assertRaises(ValueError):s.publish(store,'b',bad)
    def test_collector_does_not_publish_reflected_key(self):
        store=Storage();collector=s.Collector(store,'b','secret123',None,lambda *a,**k:io.BytesIO(b'{"echo":"secret123"}'))
        self.assertIsNone(collector.fetch('definition:NFCI','https://api.stlouisfed.org/fred/series?series_id=NFCI','fred'))
        self.assertEqual(store.writes,[]);self.assertNotIn('secret123',json.dumps(collector.sources))
    def test_collector_budget_and_no_redirect(self):
        store=Storage();collector=s.Collector(store,'b','secret',None,lambda *a,**k:(_ for _ in ()).throw(AssertionError('no request allowed')))
        collector.calls=26;self.assertIsNone(collector.fetch('x','https://api.stlouisfed.org/fred/series?series_id=NFCI','fred'))
        self.assertEqual(collector.sources['x']['reason'],'TimeoutError')


class Boundaries(unittest.TestCase):
    def test_previous_jsi_signal_records_cannot_reenter_via_scorecard_label(self):
        p=ROOT/'aws/lambdas/justhodl-proven-portfolio/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='qualifying_types')
        scope={};exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-signal-type-boundary','exec'),scope)
        rows=[{'signal_type':name,'status':'proven','n':999,'hit_rate':.99} for name in a.UNQUALIFIED_SIGNAL_TYPES]
        self.assertEqual(scope['qualifying_types']({'scorecard':rows}),{})
    def test_actual_calibrator_preserves_old_public_report_and_writes_status_only(self):
        from datetime import datetime,timezone
        p=ROOT/'aws/lambdas/justhodl-jsi-calibrator/source/lambda_function.py';tree=ast.parse(p.read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        store=Storage();old=b'{"legacy_weights":{"VIXCLS":0.5}}';store.objects['data/jsi-calibration.json']=old
        store.objects['data/jsi.json']=m.encoded({'contract':m.CONTRACT,'measurements':{}})
        scope={'s3':store,'S3_BUCKET':'b','REPORT_KEY':'data/jsi-calibration.json','json':json,'datetime':datetime,'timezone':timezone}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-calibration-boundary','exec'),scope)
        scope['lambda_handler']()
        self.assertEqual(store.objects[s.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin'],old)
        result=json.loads(store.objects['data/jsi-calibration.json']);self.assertEqual(result['ssm_weight_writes'],0)
        self.assertEqual(result['spine']['weights'],{});self.assertFalse(result['sizing_eligible'])
    def test_spoofed_qualification_and_legacy_percentiles_are_denied(self):
        for packet in ({'percentile_since_1990':99},{'sizing_eligible':True,'decision_qualification':{'status':'qualified'}},None):
            self.assertIsNone(a.qualified_percentile(packet));self.assertIsNone(a.alert_view(packet)['regime'])
    def test_actual_portfolio_excludes_jsi_without_reading_history_or_default_50(self):
        path=ROOT/'aws/lambdas/justhodl-proven-portfolio/source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf-8'))
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        # Execute only its public regime-input section, before any positions, marks or ledger I/O.
        start=next(i for i,n in enumerate(handler.body) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='jsi')
        end=next(i for i,n in enumerate(handler.body) if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='orth')
        reads=[]
        def rj(key):
            reads.append(key)
            return {'percentile_since_1990':99,'latest':{'pctile':99,'gssi':0},'v2':{'percentile':99}}
        scope={'rj':rj};exec(compile(ast.Module(body=handler.body[start:end],type_ignores=[]),'actual-portfolio-boundary','exec'),scope)
        self.assertIsNone(scope['_pv']);self.assertEqual(scope['gross_scale'],1);self.assertNotIn('data/jsi-history.json',reads)
    def test_active_stress_handler_cannot_reach_legacy_signal_writer(self):
        source=(ROOT/'aws/lambdas/justhodl-stress-index/source/lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(source)
        handler=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler'];self.assertEqual(len(handler),1)
        text=ast.get_source_segment(source,handler[0]);self.assertNotIn('legacy',text);self.assertNotIn('log_signal',text)
        self.assertNotIn('dynamodb',(ROOT/'aws/lambdas/justhodl-stress-index/source/stress_store.py').read_text(encoding='utf-8'))
    def test_active_calibrator_writes_no_weights_or_forecasts(self):
        p=ROOT/'aws/lambdas/justhodl-jsi-calibrator/source/lambda_function.py';text=p.read_text(encoding='utf-8');tree=ast.parse(text)
        handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        active=ast.get_source_segment(text,handler);self.assertNotIn('_put_ssm(',active);self.assertNotIn('calibrate_spine(',active)
        status=a.calibration_status({'measurements':{'A':{}},'generated_at':'2026-09-20'},'2026-09-20')
        self.assertFalse(status['sizing_eligible']);self.assertEqual(status['validated_forecast_observations'],0)


if __name__=='__main__':unittest.main()
