from copy import deepcopy
from datetime import datetime,timedelta
from pathlib import Path
from decimal import localcontext
import hashlib,json,math,sys,unittest
ROOT=Path(__file__).resolve().parents[3];sys.path.insert(0,str(ROOT/'aws/shared'))
import sector_market_replay as replay
import sector_research_model as model
import daily_market_model,daily_macro_model
STAMP='2026-09-21T01:00:00+00:00'

def fixtures(missing=()):
    objects={};sources={};stocks={};start=datetime(2025,8,15,tzinfo=daily_market_model.ET)
    for k,symbol in enumerate(model.SYMBOLS):
        if symbol in missing:sources[symbol]=None;continue
        rows=[]
        for i in range(400):
            day=start+timedelta(days=i)
            if day.weekday()>4:continue
            price=100+k+0.03*i+math.sin(i/7)*(1+k/30)
            rows.append({'t':int(day.timestamp()*1000),'o':price,'h':price+1,'l':price-1,'c':price,'v':1000+i})
        request={'symbol':symbol,'start':'2025-08-15','end':'2026-09-19','multiplier':1,'timespan':'day','adjusted':True,'sort':'desc','limit':50000}
        response={'ticker':symbol,'status':'OK','adjusted':True,'resultsCount':len(rows),'results':rows[::-1]}
        raw=model.encoded(response);digest=model.sha(raw);url='https://api.polygon.io/v2/aggs/ticker/'+symbol+'/range/1/day/2025-08-15/2026-09-19?adjusted=true&sort=desc&limit=50000';url=replay.public_source_url(url);key='data/evidence/polygon/'+model.sha(url.encode())+'/'+digest+'.bin.gz';objects[key]=raw
        sources[symbol]={'request':request,'response':response,'name':symbol,'acquired_at':STAMP,
            'evidence':{'contract':'source-evidence.v1','provider':'polygon','captured':True,'source_url':url,'sha256':digest,'key':key,'bytes':len(raw),'first_received_at':STAMP}}
        row=daily_market_model.equity(sources[symbol],STAMP)
        stocks[symbol]={**row,**{field:None for field in daily_macro_model.STOCK_AUTHORITY},'source_collected_at':STAMP}
    inputs={'auxiliary':{'collected_at':STAMP,'market_sources':{'contract':daily_market_model.CONTRACT,'universe':list(model.SYMBOLS),'equities':{s:v for s,v in sources.items() if v},'errors':{}}}}
    raw=model.encoded(inputs);digest=model.sha(raw);key=replay.PREFIX+'inputs/'+digest+'.json';objects[key]=raw
    manifest={'contract':'daily-research-replay.v1','generated_at':STAMP,'input':{'key':key,'sha256':digest,'bytes':len(raw)},'output_sha256':'a'*64,'compilers':{}}
    for module in (daily_market_model,daily_macro_model):
        raw=Path(module.__file__).read_bytes();digest=model.sha(raw);key=replay.PREFIX+'compilers/'+digest+'.py';objects[key]=raw;manifest['compilers'][module.__name__]={'key':key,'sha256':digest}
    raw=model.encoded(manifest);key=replay.PREFIX+'runs/'+model.sha(raw)+'.json';objects[key]=raw
    packet={'contract':daily_macro_model.CONTRACT,'generated_at':STAMP,'stocks':stocks,'replay':{'manifest_key':key,'output_sha256':manifest['output_sha256'],'compilers':deepcopy(manifest['compilers'])}}
    return packet,sources,objects


class SectorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.packet,cls.sources,cls.objects=fixtures()
    def test_selected_original_replay_and_missing_universe_are_explicit(self):
        before=deepcopy(self.packet);actual=replay.restore(self.packet,model.SYMBOLS,self.objects.__getitem__)
        self.assertEqual(actual,self.sources);self.assertEqual(before,self.packet)
        p,s,o=fixtures(('SMH',));self.assertIsNone(replay.restore(p,model.SYMBOLS,o.__getitem__)['SMH'])
    def test_provider_compiler_current_and_input_tampering_fail(self):
        p=deepcopy(self.packet);p['stocks']['XLK']['price']+=1
        with self.assertRaisesRegex(ValueError,'Published market'):replay.restore(p,model.SYMBOLS,self.objects.__getitem__)
        for key in [self.sources['XLK']['evidence']['key'],next(k for k in self.objects if '/compilers/' in k),next(k for k in self.objects if '/inputs/' in k)]:
            o=dict(self.objects);o[key]+=b' '
            with self.assertRaises(ValueError):replay.restore(self.packet,model.SYMBOLS,o.__getitem__)
        with self.assertRaises(ValueError):replay.restore(self.packet,('XLK','XLK'),self.objects.__getitem__)
    def test_same_dated_endpoints_and_distinct_excess_ratio_units(self):
        dates=['2026-09-01','2026-09-02'];point=lambda d,v:{'date':d,'close':v,'original_row_index':0}
        left={dates[0]:point(dates[0],100),dates[1]:point(dates[1],110)};right={dates[0]:point(dates[0],100),dates[1]:point(dates[1],105)}
        out=model.comparison(left,right,dates,1,1)
        self.assertEqual(out['value'],10);self.assertEqual(out['excess_percentage_points'],5);self.assertAlmostEqual(out['ratio_return_percent'],100*(1.1/1.05-1),9)
        left['2026-08-31']=point('2026-08-31',100);del left[dates[0]]
        self.assertIsNone(model.comparison(left,right,dates,1,1)['value'])
        self.assertEqual(model.comparison(left,right,dates,1,1)['missing_endpoints'],[{'leg':'numerator','date':'2026-09-01'}])
    def test_native_sample_is_dated_complete_and_has_no_allocation_authority(self):
        out=model.build(self.packet,self.sources,STAMP)
        self.assertEqual(out['quality']['replayed_sources'],28);self.assertEqual(out['quality']['core_reference_coverage'],11)
        self.assertEqual(len(out['sectors']),11);self.assertEqual(len(out['ratios']),14)
        self.assertEqual(out['risk_sample']['status'],'available');self.assertEqual(len(out['risk_sample']['return_rows']),63)
        self.assertEqual(out['risk_sample']['statistics']['SPY']['beta_to_SPY'],1);self.assertEqual(out['risk_sample']['statistics']['SPY']['annualized_tracking_error_percent'],0)
        self.assertEqual(out['portfolio_action'],'WAIT');self.assertIsNone(out['call']);self.assertTrue(all(out[k] is False for k in model.PERMISSIONS))
        self.assertTrue(all(row['rotation_score'] is None and row['implication']=='WAIT' for row in out['sectors']))
        self.assertIsNone(out['relative_performance_history'][0]['sectors']['XLK']['value']);self.assertFalse(out['quality']['calendar_completeness_verified'])
    def test_covariance_matches_independent_sample_and_is_symmetric(self):
        out=model.build(self.packet,self.sources,STAMP)['risk_sample'];rows=out['return_rows'];a=[r['returns_percent']['XLK'] for r in rows];b=[r['returns_percent']['SPY'] for r in rows]
        ma=sum(a)/len(a);mb=sum(b)/len(b);expected=sum((x-ma)*(y-mb) for x,y in zip(a,b))/(len(a)-1)
        self.assertAlmostEqual(out['covariance']['XLK']['SPY'],expected,9)
        for x in out['symbols']:
            for y in out['symbols']:self.assertEqual(out['covariance'][x][y],out['covariance'][y][x])
    def test_missing_price_disables_full_covariance_without_pairwise_deletion(self):
        s=deepcopy(self.sources);s['XLK']['response']['results'].pop(5);s['XLK']['response']['resultsCount']-=1
        out=model.build(self.packet,s,STAMP)['risk_sample'];self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['covariance']);self.assertEqual(len(out['missing_dates']['XLK']),1)
    def test_flat_prices_zero_variance_and_zero_volume_are_not_invented_risk(self):
        s=deepcopy(self.sources)
        for source in s.values():
            for row in source['response']['results']:row.update(o=100,h=100,l=100,c=100,v=0)
        out=model.build(self.packet,s,STAMP);self.assertIsNone(out['risk_sample']['statistics']['XLK']['beta_to_SPY']);self.assertIsNone(out['risk_sample']['correlation']['XLK']['SPY'])
        self.assertEqual(out['risk_sample']['statistics']['XLK']['sample_stddev_percent'],0)
        self.assertIsNone(out['sectors'][0]['technical']['close_location_volume']['value'])
    def test_missing_subsector_and_expired_sources_remain_research_gaps(self):
        s=deepcopy(self.sources);s['SMH']=None;out=model.build(self.packet,s,'2026-09-23T01:00:00+00:00')
        self.assertEqual(out['quality']['replayed_sources'],27);self.assertEqual(out['quality']['within_age_ceiling'],0)
        self.assertIsNone(out['observations']['XLK']['value']);self.assertIsNotNone(out['observations']['XLK']['last_observed_price'])
        self.assertEqual(out['sectors'][0]['subsector']['status'],'missing_source');self.assertIsNone(out['sectors'][0]['subsector']['comparison']['value'])
    def test_decimal_context_does_not_change_research(self):
        with localcontext() as ctx:ctx.prec=6;a=model.build(self.packet,self.sources,STAMP)
        with localcontext() as ctx:ctx.prec=45;b=model.build(self.packet,self.sources,STAMP)
        self.assertEqual(a,b)

class Failure(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Storage:
    def __init__(self,objects):self.objects=dict(objects);self.writes=[];self.reads=[];self.races=0
    def get_object(self,**kw):
        import io
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise Failure('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[key]),'ETag':model.sha(self.objects[key])}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(kw)


def storage():
    import gzip,sector_research_store as store
    p,s,o=fixtures();o={k:gzip.compress(v,mtime=0) if k.endswith('.gz') else v for k,v in o.items()};o[store.SOURCES[0]]=model.encoded(p)
    for key in store.SOURCES[1:]:o[key]=model.encoded({'legacy':'whole','generated_at':'2026-09-20T01:00:00+00:00','source':key})
    return Storage(o)


class SectorStoreTests(unittest.TestCase):
    def test_original_replay_idempotent_success_and_whole_legacy_preservation(self):
        import sector_research_store as store
        from unittest.mock import patch
        client=storage();before=dict(client.objects)
        with patch.object(store,'now',return_value=STAMP):out=store.run(client,'b','test-request','aws-execution')
        self.assertTrue(out['published']);packet=json.loads(client.objects[store.CURRENT]);ref=packet.pop('replay')
        self.assertEqual(store.replay(ref,store.reader(client,'b')),packet)
        self.assertEqual(client.objects[store.PRIVATE+model.sha(before[store.CURRENT])+'.bin'],before[store.CURRENT])
        writes=len(client.writes)
        with patch.object(store,'now',return_value=STAMP):again=store.run(client,'b','test-request','second-execution')
        self.assertEqual(again,out);self.assertEqual(len(client.writes),writes)
    def test_corrupt_original_preserves_prior_publication_and_records_failure(self):
        import sector_research_store as store
        from unittest.mock import patch
        client=storage();old=client.objects[store.CURRENT];key=next(k for k in client.objects if k.endswith('.gz'));client.objects[key]=b'bad'
        with patch.object(store,'now',return_value=STAMP),self.assertRaises(RuntimeError):store.run(client,'b','broken','aws-execution')
        self.assertEqual(client.objects[store.CURRENT],old);self.assertEqual(json.loads(client.objects[store.request_key('broken')])['status'],'failed')
    def test_path_allowlist_and_newer_publication_guard(self):
        import sector_research_store as store
        client=storage()
        for key in ('accounts/foo.json','secrets/token','data/daily-research/inputs/../x.json'):
            with self.assertRaises(ValueError):store.reader(client,'b')(key)
        old=model.encoded({'generated_at':'2099-01-01T00:00:00Z'});client.objects[store.CURRENT]=old
        self.assertFalse(store.publish(client,'b',{'generated_at':STAMP,'source_generated_at':STAMP}))
        self.assertEqual(client.objects[store.CURRENT],old)


class SectorTiltTests(unittest.TestCase):
    def test_tilt_reconstructs_rotation_originals_and_preserves_no_prescription(self):
        import sector_research_store as rotation,sector_tilt_store as tilt
        from unittest.mock import patch
        client=storage()
        with patch.object(rotation,'now',return_value=STAMP):rotation.run(client,'b','rotation-for-tilt','aws-one')
        with patch.object(tilt,'now',return_value='2026-09-21T01:01:00+00:00'):result=tilt.run(client,'b','tilt-native','aws-two')
        self.assertTrue(result['published']);p=json.loads(client.objects[tilt.CURRENT]);ref=p.pop('replay');self.assertEqual(tilt.replay(ref,tilt.reader(client,'b')),p)
        self.assertEqual(len(p['tilts']),11);self.assertEqual(p['summary']['n_unavailable'],11);self.assertEqual(p['summary']['n_neutral'],0)
        self.assertTrue(all(x['regime_tilt_score'] is None and x['implication']=='WAIT' for x in p['tilts']))
        self.assertEqual(p['scenario_data']['covariance_unit'],'percent_squared');self.assertEqual(p['scenario_data']['status'],'available')
        self.assertEqual(len(p['scenario_data']['return_rows']),63);self.assertIsNone(p['regime'])
        self.assertEqual(p['retained_contexts'][0]['status'],'retained_unqualified_context')
    def test_tilt_rejects_altered_native_source_before_publication(self):
        import sector_research_store as rotation,sector_tilt_store as tilt
        from unittest.mock import patch
        client=storage();before=client.objects[tilt.CURRENT]
        with patch.object(rotation,'now',return_value=STAMP):rotation.run(client,'b','rotation-corrupt','aws-one')
        p=json.loads(client.objects[rotation.CURRENT]);p['sectors'][0]['name']='changed';client.objects[rotation.CURRENT]=model.encoded(p)
        with patch.object(tilt,'now',return_value=STAMP),self.assertRaises(RuntimeError):tilt.run(client,'b','bad-tilt','aws-two')
        self.assertEqual(client.objects[tilt.CURRENT],before)
    def test_both_actual_handlers_validation_and_http_are_read_only(self):
        import ast,types
        for fn in ('justhodl-sector-rotation','justhodl-sector-tilt'):
            path=ROOT/'aws/lambdas'/fn/'source/lambda_function.py';node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
            def forbidden(*args,**kwargs):raise AssertionError('Production route invoked')
            ns={'json':json,'CONTRACT':'native','boto3':types.SimpleNamespace(client=forbidden),'run':forbidden}
            exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-sector-handler','exec'),ns)
            self.assertEqual(ns['lambda_handler']({'validate_only':True},None)['statusCode'],200)
            ns.update(boto3=types.SimpleNamespace(client=lambda *a,**kw:None),Config=lambda **kw:None,CURRENT='data/public.json',reader=lambda *a:lambda k:model.encoded({'contract':'native'}))
            self.assertEqual(ns['lambda_handler']({'httpMethod':'GET'},None)['statusCode'],200)
            self.assertEqual(ns['lambda_handler']({'action':'current_state'},None)['statusCode'],200)
            with self.assertRaisesRegex(ValueError,'AWS execution'):ns['lambda_handler']({},None)

if __name__=='__main__':unittest.main()
