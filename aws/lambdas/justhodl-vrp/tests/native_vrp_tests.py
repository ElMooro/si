"""Synthetic canonical originals and actual model/store/consumer boundaries; no network."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,date,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_UP
from unittest.mock import patch
import ast,gzip,hashlib,io,json,sys,textwrap,unittest
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import vrp_research_model as model
import vrp_research_store as store
import vrp_research as adapter
import report_observations as compiler
STAMP='2026-09-20T18:00:00+00:00'

class Failure(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.reads=[];self.writes=[]
    def get_object(self,**kw):
        k=kw['Key'];self.reads.append(k)
        if k not in self.objects:raise Failure('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[k]),'ETag':model.sha(self.objects[k])}
    def put_object(self,**kw):
        k=kw['Key'];old=self.objects.get(k)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
        self.objects[k]=kw['Body'];self.writes.append(kw)

def fixture():
    objects={};entries={};measurements={};originals={}
    start=date(2025,1,1);days=[start+timedelta(days=i) for i in range(626) if (start+timedelta(days=i)).weekday()<5]
    for sid in model.SERIES:
        definition={'seriess':[{'id':sid,'title':'Synthetic '+sid,'units':'Index','frequency_short':'D','frequency':'Daily, Close','seasonal_adjustment':'Not Seasonally Adjusted','seasonal_adjustment_short':'NSA'}]}
        rows=[{'date':str(d),'value':str(Decimal(5000)+i*3+(i%7)*10) if sid=='SP500' else str(Decimal(15 if sid=='VIXCLS' else 18)+Decimal(i%13)/10)} for i,d in enumerate(days)]
        rows[200]['value']='.'
        observations={'observations':rows[::-1],'units':'lin','output_type':1,'count':len(rows),'limit':4000,'offset':0}
        evidence={}
        for kind,doc in (('definition',definition),('observations',observations)):
            raw=model.encoded(doc);digest=model.sha(raw)
            url='https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?series_id='+sid
            if kind=='observations':url+='&units=lin&limit=4000&sort_order=desc'
            key='data/evidence/fred/'+model.sha(url.encode())+'/'+digest+'.bin.gz'
            evidence[kind]={'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':STAMP}
            objects[key]=gzip.compress(raw,mtime=0)
        entries[sid]={'evidence':evidence,'acquired_at':STAMP}
        originals[sid]={**entries[sid],'definition':definition,'observations':observations}
        measurements[sid]=compiler.measurement(sid,definition,observations,evidence,STAMP,STAMP)
    packet={'contract':compiler.CONTRACT,'generated_at':STAMP,'measurements':measurements}
    code=Path(compiler.__file__).read_bytes();digest=model.sha(code);ck='data/report-research/compilers/'+digest+'.py';objects[ck]=code
    md={'contract':'report-research-replay.v1','generated_at':STAMP,'inputs':entries,'compiler':{'key':ck,'sha256':digest},'output_sha256':model.sha(model.encoded(packet))}
    raw=model.encoded(md);key='data/report-research/runs/'+model.sha(raw)+'.json';objects[key]=raw
    packet['replay']={'manifest_key':key,'output_sha256':md['output_sha256']};objects[store.SOURCES[0]]=model.encoded(packet)
    for key in store.SOURCES[1:]:objects[key]=b'{"regime":"RICH","score":99}'
    client=Storage(objects)
    inputs={'contract':'vrp-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),
        'legacy':{k:store.snapshot(client,'b',k) for k in store.SOURCES[1:]}}
    return client,inputs,packet,originals

class Native(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.client,cls.inputs,cls.packet,cls.originals=fixture();cls.output=store.compile_output(cls.inputs,store.reader(cls.client,'b'))
    def test_original_same_underlying_measurements_and_authority(self):
        o=self.output;self.assertEqual(o['quality']['within_age_ceiling'],3)
        self.assertEqual(o['measurements']['SP500']['unit'],'Index')
        self.assertTrue(all(x is False for x in (o[k] for k in model.PERMISSIONS)))
        self.assertTrue(all(v is None for v in o['vrp'].values()));self.assertIsNone(o['regime']);self.assertIsNone(o['interpretation']['for_vol_sellers'])
        self.assertEqual(set(o['legacy_context']),set(store.SOURCES[1:]));self.assertTrue(all(not x['qualified'] for x in o['legacy_context'].values()))
    def test_sample_standard_deviation_matches_independent_float_math(self):
        import math,statistics
        vals=[Decimal(r['value']) for r in self.originals['SP500']['observations']['observations'] if r['value']!='.'][::-1][-22:]
        logs=[math.log(float(b/a)) for a,b in zip(vals,vals[1:])]
        self.assertAlmostEqual(self.output['trailing_realized']['21']['value'],statistics.stdev(logs)*math.sqrt(252)*100,places=7)
        self.assertEqual(self.output['trailing_realized']['21']['observed_intervals'],21)
        self.assertIn('ddof=1',self.output['trailing_realized']['21']['method'])
    def test_same_date_gap_refuses_mismatched_latest(self):
        rows,h=model.observed(self.packet,self.originals,STAMP);rv=model.trailing(rows,h,model.intervals(h['SP500']))
        self.assertTrue(model.gap(rows,rv,'VIXCLS',21)['available'])
        rows['VIXCLS']['observation_date']='2026-09-17'
        self.assertFalse(model.gap(rows,rv,'VIXCLS',21)['available']);self.assertIsNone(model.gap(rows,rv,'VIXCLS',21)['volatility_points'])
    def test_calendar_30_outcome_matches_independent_quadratic_variation(self):
        import math
        ex=self.output['expost_research'];r=ex['rows'][-1]
        self.assertEqual((date.fromisoformat(r['maturity_date'])-date.fromisoformat(r['origin_date'])).days,30)
        rows=self.originals['SP500']['observations']['observations'];values=[float(rows[i]['value']) for i in r['price_original_row_indices']]
        variance=365/30*sum(math.log(b/a)**2 for a,b in zip(values,values[1:]))*10000
        self.assertAlmostEqual(r['variance_difference_percent_squared'],r['implied_volatility_percent']**2-variance,places=7)
        self.assertAlmostEqual(r['realized_close_sampled_volatility_percent'],math.sqrt(variance),places=7)
        self.assertIn('exact_calendar_endpoint_unavailable',ex['exclusions']);self.assertIn('not_matured',ex['exclusions'])
        self.assertFalse(ex['independence_established']);self.assertLess(len(ex['nonoverlapping_window_origins']),ex['observations'])
    def test_large_gap_or_insufficient_history_is_not_filled(self):
        pair={'from_date':'2026-09-01','to_date':'2026-09-02','calendar_days':1,'log_return':Decimal(0),'original_row_indices':[0,1],'missing_markers_between':0}
        xs=[deepcopy(pair) for _ in range(10)];self.assertEqual(model.window(xs,10,'2026-09-02')['value'],0)
        xs[4]['calendar_days']=5;self.assertFalse(model.window(xs,10,'2026-09-02')['available'])
        self.assertIsNone(model.window([],10,'2026-09-02')['value'])
    def test_duplicates_nonpositive_and_future_rows(self):
        orig={'observations':{'observations':[{'date':'2026-09-18','value':'1'},{'date':'2026-09-21','value':'2'}]}}
        self.assertEqual(len(model.records(orig,'2026-09-20')),1)
        orig['observations']['observations'][0]['value']='0'
        with self.assertRaises(ValueError):model.records(orig,'2026-09-20')
        orig['observations']['observations'][0]['value']=False
        self.assertIsNone(model.records(orig,'2026-09-20')[0]['value'])
        orig['observations']['observations'].append({'date':'2026-09-18','value':'1'})
        with self.assertRaisesRegex(ValueError,'Duplicate'):model.records(orig,'2026-09-20')
    def test_stale_sources_withhold_current_keep_labeled_retrospective(self):
        o=model.build(self.packet,self.originals,{},'2026-09-23T18:00:00Z')
        self.assertEqual(o['quality']['within_age_ceiling'],0)
        self.assertTrue(all(not x['available'] for x in o['descriptive_gaps'].values()));self.assertTrue(o['expost_research']['rows'])
    def test_definition_change_and_future_source_refused(self):
        p=deepcopy(self.packet);p['measurements']['SP500']['unit']='Percent'
        with self.assertRaisesRegex(ValueError,'definition'):model.build(p,self.originals,{},STAMP)
        with self.assertRaisesRegex(ValueError,'Future'):model.build(self.packet,self.originals,{},'2026-09-19T18:00:00Z')
    def test_missing_price_series_cannot_become_zero_volatility(self):
        p=deepcopy(self.packet);del p['measurements']['SP500'];originals=deepcopy(self.originals);del originals['SP500']
        o=model.build(p,originals,{},STAMP)
        self.assertEqual(o['quality']['within_age_ceiling'],2)
        self.assertTrue(all(w['value'] is None for w in o['trailing_realized'].values()))
        self.assertTrue(all(g['volatility_points'] is None for g in o['descriptive_gaps'].values()))
        self.assertEqual(o['expost_research']['rows'],[]);self.assertIsNone(o['regime'])
    def test_history_statistics_describe_actual_coverage_and_rounding(self):
        h=self.output['gap_history'];self.assertEqual(h['statistics']['observations'],252)
        self.assertEqual(h['statistics']['last_date'],h['rows'][-1]['date']);self.assertIsNone(h['independent_samples'])
        self.assertNotIn('hit_rate',self.output['expost_research'])
        with localcontext() as ctx:
            ctx.prec=7;ctx.rounding=ROUND_UP
            self.assertEqual(model.build(self.packet,self.originals,{k:{'regime':'RICH','score':99} for k in store.SOURCES[1:]},STAMP),self.output)

class StoreCases(unittest.TestCase):
    def setUp(self):self.client,self.inputs,self.packet,self.originals=fixture()
    def test_full_replay_and_source_tamper_failure(self):
        read=store.reader(self.client,'b');out=store.compile_output(self.inputs,read);ref=store.retain(self.client,'b',self.inputs,out)
        self.assertEqual(store.replay(ref,read),out)
        key=self.packet['measurements']['SP500']['evidence']['observations']['key'];self.client.objects[key]=gzip.compress(b'{}')
        with self.assertRaises(ValueError):store.replay(ref,read)
    def test_compiler_change_refused_and_accounts_inaccessible(self):
        read=store.reader(self.client,'b');out=store.compile_output(self.inputs,read);ref=store.retain(self.client,'b',self.inputs,out)
        m=json.loads(read(ref['manifest_key']));self.client.objects[m['compilers']['vrp_research_model']['key']]+=b'\n'
        with self.assertRaisesRegex(ValueError,'compiler'):store.replay(ref,read)
        for key in ('portfolio/state.json','data/brain.json','data/vrp-history.json'):
            with self.assertRaises(ValueError):read(key)
    def test_publication_preserves_old_history_and_refuses_regression(self):
        old=b'{"generated_at":"2026-09-19T18:00:00Z","regime":"RICH"}';history=b'{"snapshots":[{"score":99}]}'
        self.client.objects.update({store.CURRENT:old,'data/vrp-history.json':history})
        out=store.compile_output(self.inputs,store.reader(self.client,'b'));self.assertTrue(store.publish(self.client,'b',out))
        self.assertEqual(self.client.objects[store.PRIVATE+model.sha(old)+'.bin'],old);self.assertEqual(self.client.objects['data/vrp-history.json'],history)
        bad=deepcopy(out);bad['generated_at']='2026-09-20T18:01:00Z';bad['source_generated_at']='2026-09-19T18:00:00Z'
        self.assertFalse(store.publish(self.client,'b',bad));bad=deepcopy(out);bad['headline']='different'
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(self.client,'b',bad)
    def test_idempotency_missing_context_and_no_provider_calls(self):
        del self.client.objects[store.SOURCES[1]]
        with patch.object(store,'now',return_value=STAMP):
            result=store.run(self.client,'b','test-request','test-execution');n=len(self.client.writes)
            again=store.run(self.client,'b','test-request','another-execution')
        self.assertEqual(result,again);self.assertEqual(len(self.client.writes),n);self.assertEqual(result['status'],'complete')
        self.assertTrue(all(result[k]==0 for k in ('private_account_reads','provider_requests','paid_ai_calls','notifications_sent','portfolio_writes')))
        live=json.loads(self.client.objects[store.CURRENT]);self.assertFalse(live['legacy_context'][store.SOURCES[1]]['retained'])
    def test_failed_write_marks_failure_and_keeps_current(self):
        old=b'{"generated_at":"2026-09-19T18:00:00Z"}';self.client.objects[store.CURRENT]=old;actual=self.client.put_object
        def put(**kw):
            if kw['Key']==store.CURRENT:raise Failure('AccessDenied')
            return actual(**kw)
        self.client.put_object=put
        with patch.object(store,'now',return_value=STAMP):
            with self.assertRaises(RuntimeError):store.run(self.client,'b','write-failure','execution')
        self.assertEqual(self.client.objects[store.CURRENT],old)
        self.assertEqual(json.loads(self.client.objects[store.request_key('write-failure')])['status'],'failed')
    def test_validation_and_http_cannot_publish(self):
        path=HERE.parent/'source/lambda_function.py';nodes=ast.parse(path.read_text(encoding='utf-8')).body
        fn=next(n for n in nodes if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        def forbidden(*a,**kw):raise AssertionError('unexpected publication or client')
        ns={'json':json,'CONTRACT':model.CONTRACT,'boto3':type('B',(),{'client':forbidden})()}
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'actual-handler','exec'),ns)
        self.assertEqual(ns['lambda_handler']({'validate_only':True})['statusCode'],200)
        ns.update(boto3=type('B',(),{'client':lambda *a,**kw:self.client})(),Config=lambda **kw:None,CURRENT=store.CURRENT,reader=lambda *a:lambda k:model.encoded({'contract':model.CONTRACT}),run=forbidden)
        self.assertEqual(ns['lambda_handler']({'httpMethod':'GET'})['statusCode'],200)

class Consumers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client,i,p,o=fixture();read=store.reader(cls.client,'b');out=store.compile_output(i,read)
        cls.packet={**out,'replay':store.retain(cls.client,'b',i,out)};cls.at=datetime.fromisoformat(STAMP)
    def test_native_typed_context_zero_authority_and_tampering(self):
        self.assertEqual(len(adapter.context(self.packet,self.at)['measurements']),3)
        self.assertFalse(adapter.context({'regime':'RICH','calls_eligible':True},self.at)['available'])
        p=deepcopy(self.packet);p['measurements']['VIXCLS']['value']=999
        self.assertFalse(adapter.context(p,self.at)['available'])
        p=deepcopy(self.packet);p['calls_eligible']=True;p['replay']['output_sha256']=model.sha(model.encoded({k:v for k,v in p.items() if k!='replay'}))
        self.assertFalse(adapter.context(p,self.at)['available'])
        self.assertFalse(adapter.context(self.packet,self.at+timedelta(days=2))['available']);self.assertIsNone(adapter.qualified_score(self.packet))
    def test_actual_vol_radar_read_never_scores_legacy_or_native(self):
        s=(ROOT/'aws/lambdas/justhodl-vol-radar/source/lambda_function.py').read_text(encoding='utf-8')
        line=next(l for l in s.splitlines() if 'vrp = ' in l and 'data/vrp.json' in l)
        for p in ({'regime':'RICH','vrp':{'vrp_30d':50,'vrp_30d_percentile_1y':99},'realized':{'rv_21d':1}},self.packet):
            ns={'read_json':lambda k:p};exec(textwrap.dedent(line),ns)
            self.assertIsNone(ns['vrp']['vrp']['vrp_30d']);self.assertIsNone(ns['vrp']['realized']['rv_21d'])
    def test_calibration_old_history_never_requalifies_vrp(self):
        s=(ROOT/'aws/lambdas/justhodl-calibration-fleet/source/lambda_function.py').read_text(encoding='utf-8')
        start=s.index('    # ---- 5. per-engine');end=s.index('    # normalise weight',start)
        def forbidden(*a,**kw):raise AssertionError('Unqualified history used')
        ns={'REGISTRY':[{'name':'vrp','direction':'stress','label':'VRP','source_key':'data/vrp.json','score_path':['score']}],
            'snaps':[{'date':'2026-09-01','scores':{'vrp':99}}],'forward_dd':forbidden,'ddb_history_snapshots':forbidden}
        exec(textwrap.dedent(s[start:end]),ns);self.assertEqual(ns['engines_out'][0]['quality_rating'],'UNQUALIFIED');self.assertEqual(ns['weight_props'],{})
    def test_market_extremes_receives_roots_not_extra_timing_score(self):
        import extremes_native_model as extremes
        rows,summary,reason=extremes.project('vrp',self.packet,self.at)
        self.assertEqual(len(rows),3);self.assertEqual({r['series_id'] for r in rows},{'FRED:'+s for s in model.SERIES})
        self.assertTrue(all(isinstance(r['label'],str) for r in rows));self.assertTrue(all(r['calls_eligible'] is False for r in rows))
        self.assertEqual(next(r for r in rows if r['series_id']=='FRED:SP500')['provider_family'],'S&P_Dow_Jones_Indices')

if __name__=='__main__':unittest.main()
