from copy import deepcopy
from datetime import datetime,timedelta
from decimal import Decimal,localcontext
from pathlib import Path
from unittest.mock import patch
import io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[3];sys.path.insert(0,str(ROOT/'aws/shared'))
import money_volume_model as model
import money_volume_source as source
import money_volume_store as store
import money_volume_sessions as native
from money_volume_pins import BREADTH_COMPILERS
STAMP='2026-09-21T04:00:00+00:00';ACQUIRED='2026-09-20T13:20:00+00:00';COLLECTED='2026-09-20T13:21:00+00:00'


def fixture(modify=None):
    objects={};days=native.sessions(COLLECTED,6);items={}
    prices={'AAA':[100,110,110,90,95,120],'BBB':[50,50,50,50,50,50],'CCC':[20,21,22,23,24,25]}
    for i,day in enumerate(days):
        rows=[]
        for ticker,history in prices.items():
            t=datetime.fromisoformat(day).replace(hour=16,tzinfo=native.EASTERN)
            row={'T':ticker,'c':history[i],'vw':10,'v':100,'t':int(t.timestamp()*1000)}
            rows.append(row)
        document={'status':'OK','adjusted':True,'queryCount':len(rows),'resultsCount':len(rows),'results':rows}
        if modify:modify(i,document)
        document['queryCount']=document['resultsCount']=len(document['results'])
        raw=model.encoded(document);digest=model.sha(raw);key=source.PRIVATE+digest+'.bin';objects[key]=raw
        items[day]={'acquired_at':ACQUIRED,'evidence':{'key':key,'sha256':digest,'bytes':len(raw),'provider':'massive',
            'source_url':'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'+day+'?adjusted=true&include_otc=false'}}
    inputs={'contract':'breadth-native-inputs.v1','started_at':ACQUIRED,'generated_at':COLLECTED,'expected_days':days,'sources':items}
    output={'contract':'breadth-native-research.v1','generated_at':COLLECTED,'test_scope':'Reviewed six-session source adapter fixture; no invented breadth statistics'}
    manifest={'contract':'breadth-native-replay.v1','generated_at':COLLECTED,'compilers':{}}
    for kind,doc in [('input',inputs),('output',output)]:
        raw=model.encoded(doc);digest=model.sha(raw);key=source.PREFIX+kind+'s/'+digest+'.json';objects[key]=raw
        manifest[kind]={'key':key,'sha256':digest,'bytes':len(raw)}
    manifest['output_sha256']=manifest['output']['sha256']
    for name,digest in BREADTH_COMPILERS.items():
        raw=(ROOT/'aws/lambdas/justhodl-market-internals/source'/f'{name}.py').read_bytes()
        assert model.sha(raw)==digest,'Upstream compiler revision requires review'
        key=source.PREFIX+'compilers/'+digest+'.py';objects[key]=raw;manifest['compilers'][name]={'key':key,'sha256':digest}
    raw=model.encoded(manifest);key=source.PREFIX+'runs/'+model.sha(raw)+'.json';objects[key]=raw
    packet={**output,'replay':{'manifest_key':key,'output_sha256':manifest['output_sha256']}}
    universe={'generated_at':COLLECTED,'stocks':[{'symbol':'AAA','name':'Alpha','sector':'Technology','industry':'Software'},
        {'symbol':'BBB','name':'Beta','sector':'Technology','industry':'Software'},
        {'symbol':'CCC','name':'Gamma','sector':'Energy','industry':'Oil'}]}
    refs={'data/universe.json':{'sha256':model.sha(model.encoded(universe))}}
    return packet,universe,objects,refs


def build(modify=None,at=STAMP):
    packet,universe,objects,refs=fixture(modify)
    return model.build(packet,universe,at,objects.__getitem__,{},refs)


class Arithmetic(unittest.TestCase):
    def test_period_arithmetic_unchanged_bucket_and_distinct_proxy_labels(self):
        out=build();a=out['stocks'][0];b=out['stocks'][1]
        self.assertEqual(out['period']['price_intervals'],5);self.assertEqual(out['period']['turnover_sessions'],6)
        self.assertEqual(a['price_return_pct'],20);self.assertEqual(a['mean_session_turnover_usd_proxy'],1000)
        self.assertEqual(a['price_volume_pressure_usd_proxy'],200)
        self.assertEqual([a[k] for k in ('up_close_turnover_usd_proxy','down_close_turnover_usd_proxy','unchanged_close_turnover_usd_proxy')],[3000,1000,1000])
        self.assertEqual(b['unchanged_close_turnover_usd_proxy'],5000);self.assertEqual(b['price_volume_pressure_usd_proxy'],0)
        self.assertEqual(out['covered_universe']['price_volume_pressure_usd_proxy'],450)
        self.assertTrue(all(out[k] is False for k in model.PERMISSIONS));self.assertNotIn('stocks_in',out)
    def test_missing_middle_session_does_not_silently_shorten_the_window(self):
        def change(i,d):
            if i==2:d['results']=[r for r in d['results'] if r['T']!='AAA']
        out=build(change);a=out['stocks'][0]
        self.assertIsNone(a['price_return_pct']);self.assertIsNone(a['price_volume_pressure_usd_proxy'])
        tech=next(r for r in out['sector_measurements'] if r['label']=='Technology')
        self.assertEqual(tech['included_tickers'],['BBB']);self.assertEqual(tech['excluded_tickers'],['AAA']);self.assertEqual(tech['coverage_pct'],50)
        self.assertEqual(tech['price_volume_pressure_usd_proxy'],0);self.assertFalse(tech['complete_universe'])
    def test_missing_vwap_never_substitutes_close_and_zero_volume_is_explicit(self):
        def change(i,d):
            if i==0:d['results'][0].pop('vw');d['results'][1].pop('vw');d['results'][1]['v']=0
        out=build(change);a,b=out['stocks'][:2]
        self.assertEqual(a['price_return_pct'],20);self.assertIsNone(a['mean_session_turnover_usd_proxy'])
        self.assertEqual(len(a['missing_turnover_sessions']),1)
        self.assertTrue(b['calculation_complete']);self.assertAlmostEqual(b['mean_session_turnover_usd_proxy'],5000/6,6)
    def test_all_missing_group_is_null_not_zero(self):
        def change(i,d):d['results']=[r for r in d['results'] if r['T']!='CCC']
        out=build(change);energy=next(r for r in out['sector_measurements'] if r['label']=='Energy')
        self.assertEqual(energy['included_count'],0);self.assertIsNone(energy['price_volume_pressure_usd_proxy']);self.assertIsNone(energy['pressure_intensity_bps'])
    def test_zero_turnover_has_no_invented_intensity(self):
        def change(i,d):
            for r in d['results']:r['v']=0;r.pop('vw')
        out=build(change);self.assertEqual(out['covered_universe']['price_volume_pressure_usd_proxy'],0)
        self.assertIsNone(out['covered_universe']['pressure_intensity_bps']);self.assertIsNone(out['covered_universe']['top_five_turnover_share_pct'])
    def test_unknown_classification_stays_unclassified_and_industry_keys_include_sector(self):
        p,u,o,r=fixture();u['stocks'][2].update(sector='Unreviewed classification',industry='Software')
        out=model.build(p,u,STAMP,o.__getitem__,{},r)
        self.assertEqual(len(out['industry_measurements']),2);self.assertIsNone(out['stocks'][2]['sector'])
        self.assertEqual(sum(x['included_count'] for x in out['sector_measurements']),3)
    def test_aged_source_keeps_dated_measurement_with_explicit_expiry(self):
        out=build(at='2026-09-23T04:00:00+00:00')
        self.assertEqual(out['quality']['status'],'source_check_due');self.assertTrue(all(not r['within_age_ceiling'] for r in out['stocks']))
        self.assertEqual(out['stocks'][0]['price_return_pct'],20);self.assertEqual(out['source_generated_at'],COLLECTED)
    def test_decimal_context_and_mutation_independence(self):
        p,u,o,r=fixture();before=deepcopy((p,u,o,r))
        with localcontext() as ctx:ctx.prec=6;a=model.build(p,u,STAMP,o.__getitem__,{},r)
        with localcontext() as ctx:ctx.prec=50;b=model.build(p,u,STAMP,o.__getitem__,{},r)
        self.assertEqual(a,b);self.assertEqual((p,u,o,r),before)
    def test_duplicate_universe_and_ambiguous_provider_identity_fail(self):
        p,u,o,r=fixture();u['stocks'].append(u['stocks'][0])
        with self.assertRaises(ValueError):model.build(p,u,STAMP,o.__getitem__,{},r)
        def change(i,d):d['results'].append(d['results'][0])
        with self.assertRaises(ValueError):build(change)
    def test_originals_inputs_compilers_and_current_packet_tampering_fail(self):
        p,u,o,r=fixture()
        for key in [next(k for k in o if '/compilers/' in k),next(k for k in o if '/inputs/' in k),next(k for k in o if k.startswith(source.PRIVATE))]:
            changed=dict(o);changed[key]+=b' '
            with self.assertRaises(ValueError):model.build(p,u,STAMP,changed.__getitem__,{},r)
        p['test_scope']='altered'
        with self.assertRaises(ValueError):model.build(p,u,STAMP,o.__getitem__,{},r)
    def test_source_boolean_numeric_future_timestamp_and_wrong_session_rejected(self):
        for key,value in [('c',True),('v',-1),('t',1)]:
            def change(i,d):d['results'][0][key]=value
            with self.assertRaises(ValueError):build(change)
        with self.assertRaises(ValueError):build(at='2026-09-19T00:00:00+00:00')


class Failure(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self,objects):self.objects=dict(objects);self.writes=[];self.contention=0
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise Failure('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[key]),'ETag':model.sha(self.objects[key])}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if key==store.CURRENT and self.contention:self.contention-=1;raise Failure('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(kw)


def storage():
    p,u,o,r=fixture();o[store.SOURCES[0]]=model.encoded(p);o[store.SOURCES[1]]=model.encoded(u)
    for key in store.SOURCES[2:]:o[key]=model.encoded({'generated_at':COLLECTED,'legacy_whole':{'fields':['all','preserved']},'key':key})
    return Storage(o)


class Publication(unittest.TestCase):
    def test_actual_handler_validation_and_http_do_not_publish(self):
        import ast,types
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-money-flow-state/source/lambda_function.py').read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        def forbidden(*args,**kwargs):self.fail('Unexpected production invocation')
        ns={'json':json,'CONTRACT':model.CONTRACT,'boto3':types.SimpleNamespace(client=forbidden),'run':forbidden}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-money-volume-handler','exec'),ns)
        self.assertEqual(ns['lambda_handler']({'validate_only':True},None)['statusCode'],200)
        ns.update(boto3=types.SimpleNamespace(client=lambda *a,**kw:None),Config=lambda **kw:None,CURRENT=store.CURRENT,reader=lambda *a:lambda k:model.encoded({'contract':model.CONTRACT}))
        self.assertEqual(ns['lambda_handler']({'httpMethod':'GET'},None)['statusCode'],200)
        self.assertEqual(ns['lambda_handler']({'action':'current_state'},None)['statusCode'],200)
        with self.assertRaisesRegex(ValueError,'AWS execution'):ns['lambda_handler']({},None)
    def test_whole_context_preservation_replay_and_idempotent_request(self):
        client=storage();before=dict(client.objects)
        with patch.object(store,'now',return_value=STAMP):result=store.run(client,'b','example-request','execution-one')
        self.assertTrue(result['published']);packet=json.loads(client.objects[store.CURRENT])
        self.assertEqual(store.replay(packet['replay'],store.reader(client,'b')),{k:v for k,v in packet.items() if k!='replay'})
        for key in store.SOURCES:self.assertEqual(client.objects[store.PRIVATE+model.sha(before[key])+'.bin'],before[key])
        writes=len(client.writes)
        with patch.object(store,'now',return_value=STAMP):again=store.run(client,'b','example-request','execution-two')
        self.assertEqual(again,result);self.assertEqual(len(client.writes),writes)
    def test_corrupt_original_does_not_overwrite_previous_publication(self):
        client=storage();before=client.objects[store.CURRENT];key=next(k for k in client.objects if k.startswith(source.PRIVATE));client.objects[key]+=b' '
        with patch.object(store,'now',return_value=STAMP),self.assertRaises(RuntimeError):store.run(client,'b','bad-original','execution')
        self.assertEqual(client.objects[store.CURRENT],before)
        self.assertEqual(json.loads(client.objects[store.request_key('bad-original')])['status'],'failed')
    def test_contention_retry_and_source_vintage_rollback(self):
        client=storage();client.contention=1
        with patch.object(store,'now',return_value=STAMP):store.run(client,'b','race','execution')
        before=client.objects[store.CURRENT];p=json.loads(before)
        p['generated_at']='2026-09-21T04:01:00+00:00';p['source_generated_at']='2026-09-19T13:21:00+00:00'
        self.assertFalse(store.publish(client,'b',p));self.assertEqual(before,client.objects[store.CURRENT])
        p=json.loads(before);p['generated_at']='2026-09-21T04:01:00+00:00';p['universe']['generated_at']='2026-09-19T13:21:00+00:00'
        self.assertFalse(store.publish(client,'b',p))
    def test_replay_tampering_and_unreviewed_paths_fail(self):
        client=storage()
        with patch.object(store,'now',return_value=STAMP):store.run(client,'b','reference','execution')
        packet=json.loads(client.objects[store.CURRENT]);run=json.loads(client.objects[packet['replay']['manifest_key']])
        key=run['output']['key'];client.objects[key]+=b' '
        with self.assertRaises(ValueError):store.replay(packet['replay'],store.reader(client,'b'))
        with self.assertRaises(ValueError):store.reader(client,'b')('private/holdings.json')


if __name__=='__main__':unittest.main(verbosity=2)
