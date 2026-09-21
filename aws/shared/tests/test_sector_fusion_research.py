from copy import deepcopy
from datetime import datetime,timedelta
from decimal import Decimal,localcontext
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape
import ast,gzip,hashlib,importlib.util,io,json,sys,types,unittest,zipfile
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/shared'))
import sector_fusion_model as model
import sector_fusion_store as store
import sector_fusion_issuer as issuer
import sector_issuer_native as native

STAMP='2026-09-21T05:00:00+00:00';SOURCE='2026-09-20T13:00:00+00:00';DUE='2026-09-21T15:00:00+00:00'
DAYS=['2026-09-10','2026-09-11','2026-09-14','2026-09-15','2026-09-16','2026-09-17']


def fixtures():
    observations={s:{'history':[{'date':day,'close':100+i*(2 if s!='SPY' else 1),'original_row_index':5-i} for i,day in enumerate(DAYS)],
        'source_valid_until':DUE,'source':{'sha256':'a'*64},'acquired_at':SOURCE} for s in (*model.SECTORS,'SPY')}
    rotation={'contract':'sector-native-research.v1','generated_at':SOURCE,'source_generated_at':SOURCE,'source_valid_until':DUE,'observations':observations,
        'replay':{'manifest_key':'fixture-price-root','output_sha256':'1'*64},'call':None,**model.PERMISSIONS}
    volume={'contract':'money-volume-research.v1','generated_at':SOURCE,'source_generated_at':SOURCE,'source_valid_until':DUE,
        'period':{'start_date':'2026-09-11','end_date':'2026-09-18'},'sector_measurements':[{'label':s,'included_count':1,'configured_count':2,
            'pressure_intensity_bps':200,'price_volume_pressure_usd_proxy':50,'mean_session_turnover_usd_proxy':2500,
            'included_tickers':['AAA'],'excluded_tickers':['BBB']} for s in model.SECTORS.values()],
        'replay':{'manifest_key':'fixture-stock-root','output_sha256':'2'*64},'call':None,**model.PERMISSIONS}
    issues={}
    for t in model.SECTORS:
        history=[{'date':d,'net_assets_decimal':'100000','nav_decimal':str(100+i),'shares_decimal':str(1000+i*10),'source_row':10-i} for i,d in enumerate(DAYS)]
        windows={str(n)+'d':{'start_date':DAYS[0],'end_date':DAYS[-1],'value_decimal':'-5000','precision_sensitivity_decimal':'100',
            'status':'complete_descriptive_estimate','observations_required':n,'observations_available':n,'excluded_dates':[]} for n in (1,5,20)}
        issues[t]={'history':history,'identity':{'ticker':t,'issuer':'State Street'},'status':'original_replayed','windows':windows,
            'source_valid_until':DUE,'latest_observation_date':DAYS[-1],'comparison':{'status':'within_published_precision'}}
    issue={'sectors':issues,'generated_at':SOURCE,'source_generated_at':SOURCE,'replay':{'manifest_key':'fixture-issuer-root','output_sha256':'3'*64},'reference_source':{},'reference_calendar':{}}
    return rotation,volume,issue


def build():return model.build(*fixtures(),STAMP,{}, {})


class Arithmetic(unittest.TestCase):
    def test_exact_period_arithmetic_and_separate_units(self):
        p=build();r=p['sectors'][0];w=r['windows']['5d']
        self.assertEqual(w['price']['price_return_pct'],10);self.assertEqual(w['price']['benchmark_return_pct'],5)
        self.assertEqual(w['price']['excess_percentage_points'],5)
        self.assertAlmostEqual(w['price']['ratio_return_pct'],(1.1/1.05-1)*100)
        self.assertEqual(w['issuer']['issuance_as_pct_starting_assets'],-5)
        self.assertEqual(w['comparison']['status'],'different_sign_descriptive');self.assertFalse(w['comparison']['directional_inference_qualified'])
        self.assertFalse(r['stock_volume']['same_period_as_issuer_5d']);self.assertFalse(r['stock_volume']['universe_matches_ETF_holdings'])
        self.assertEqual(p['quality']['independent_investment_votes'],0);self.assertEqual(p['overweight'],[])
    def test_missing_exact_endpoint_never_uses_nearest_date(self):
        r,v,i=fixtures();r['observations']['XLK']['history'].pop(0)
        w=model.build(r,v,i,STAMP,{}, {})['sectors'][0]['windows']['5d']
        self.assertIsNone(w['price']['price_return_pct']);self.assertEqual(w['price']['missing_endpoints'],['fund_start'])
        self.assertEqual(w['comparison']['status'],'unavailable');self.assertEqual(w['issuer']['value_usd'],-5000)
    def test_precision_caution_includes_equality_and_never_becomes_confidence(self):
        for value in ('-100','0','100'):
            r,v,i=fixtures();i['sectors']['XLK']['windows']['5d']['value_decimal']=value
            w=model.build(r,v,i,STAMP,{}, {})['sectors'][0]['windows']['5d']
            self.assertEqual(w['comparison']['status'],'issuance_not_larger_than_display_sensitivity')
            self.assertIn('not a confidence interval',w['issuer']['precision_definition'])
    def test_catalogue_conflict_and_unavailable_issuer_never_form_a_conclusion(self):
        r,v,i=fixtures();i['sectors']['XLK']['comparison']['status']='conflict_or_missing'
        self.assertEqual(model.build(r,v,i,STAMP,{}, {})['sectors'][0]['windows']['5d']['comparison']['status'],'issuer_catalogue_conflict')
        i['sectors']['XLK'].update(status='reported_unavailable',history=[],windows={})
        p=model.build(r,v,i,STAMP,{}, {});self.assertEqual(p['quality']['issuer_price_five_window_available'],10)
        self.assertIsNone(p['sectors'][0]['windows']['5d']['issuer']['value_usd'])
    def test_zero_assets_missing_group_and_expiry_remain_explicit(self):
        r,v,i=fixtures();i['sectors']['XLK']['history'][0]['net_assets_decimal']='0';v['sector_measurements']=v['sector_measurements'][1:]
        p=model.build(r,v,i,'2026-09-22T05:00:00+00:00',{}, {})
        self.assertIsNone(p['sectors'][0]['windows']['5d']['issuer']['issuance_as_pct_starting_assets'])
        self.assertIsNone(p['sectors'][0]['stock_volume']['measurement']);self.assertEqual(p['quality']['status'],'source_check_due')
        self.assertEqual(p['source_valid_until'],DUE);self.assertEqual(p['sectors'][0]['windows']['5d']['price']['price_return_pct'],10)
    def test_future_source_and_nonpositive_price_rejected(self):
        r,v,i=fixtures()
        with self.assertRaises(ValueError):model.build(r,v,i,'2026-09-19T00:00:00+00:00',{}, {})
        r['observations']['XLK']['history'][0]['close']=0
        with self.assertRaises(ValueError):model.build(r,v,i,STAMP,{}, {})
    def test_decimal_environment_and_projection_do_not_mutate_inputs(self):
        r,v,i=fixtures();before=deepcopy((r,v,i));expected=model.build(r,v,i,STAMP,{}, {})
        with localcontext() as ctx:
            ctx.prec=8;self.assertEqual(model.build(r,v,i,STAMP,{}, {}),expected)
        self.assertEqual((r,v,i),before)
        canonical={**expected,'replay':{'manifest_key':'retained-run','output_sha256':'a'*64}}
        projected=model.capital_projection(canonical,STAMP)
        self.assertEqual(projected['sectors'],canonical['sectors']);self.assertEqual(projected['canonical_replay'],canonical['replay'])
        self.assertFalse(projected['calls_eligible']);self.assertEqual(projected['quality']['independent_investment_votes'],0)


def issuer_fixture():
    folder=ROOT/'aws/lambdas/justhodl-etf-true-flows'
    sys.path.insert(0,str(folder/'source'))
    spec=importlib.util.spec_from_file_location('sector_etf_test_fixture',folder/'tests/test_research.py');helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    import etf_research,etf_store
    objects,inputs,rows=helper.fixture();at=helper.AT
    def add(label,url,raw):
        raw=raw if isinstance(raw,bytes) else model.encoded(raw);public=native.public_source_url(url);digest=model.sha(raw)
        key='data/evidence/etf_original/'+model.sha(public.encode())+'/'+digest+'.bin.gz';objects[key]=raw
        inputs['originals'][label]={'url':url,'acquired_at':at,'evidence':{'key':key,'sha256':digest,'bytes':len(raw),'provider':'etf_original',
            'source_url':public,'contract':'source-evidence.v1','captured':True,'first_received_at':at}}
    catalogue=[];ns='http://schemas.openxmlformats.org/spreadsheetml/2006/main'
    for ticker in model.SECTORS:
        fund='Test '+ticker;last=rows[-1];nav=last['nav_decimal'];assets=str(Decimal(nav)*Decimal(last['shares_decimal']))
        path='/library-content/products/fund-data/etfs/us/navhist-us-en-'+ticker.lower()+'.xlsx'
        catalogue.append({'fundTicker':ticker,'domicile':'US','fundUri':'/us/en/intermediary/etfs/test-'+ticker.lower(),'fundName':fund,
            'documentPdf':[{'docType':'Navhist','docs':[{'path':path}]}],'nav':['$'+nav,nav],'aum':['$1 M','1'],'asOfDate':['Sep 18 2026','2026-09-18']})
        strings=[];xml=[]
        def row_xml(index,values):
            cells=[]
            for col,value in values.items():
                if col=='A' or index<=4:
                    strings.append(str(value));cells.append('<c r="'+col+str(index)+'" t="s"><v>'+str(len(strings)-1)+'</v></c>')
                else:cells.append('<c r="'+col+str(index)+'"><v>'+str(value)+'</v></c>')
            return '<row r="'+str(index)+'">'+''.join(cells)+'</row>'
        xml+=[row_xml(1,{'A':'Fund Name:','B':fund}),row_xml(2,{'A':'Ticker Symbol:','B':ticker}),row_xml(4,dict(zip('ABCD',['Date','NAV','Shares Outstanding','Total Net Assets'])))]
        for index,r in enumerate(reversed(rows),5):xml.append(row_xml(index,{'A':datetime.fromisoformat(r['date']).strftime('%d-%b-%Y'),'B':r['nav_decimal'],'C':r['shares_decimal'],'D':str(Decimal(r['nav_decimal'])*Decimal(r['shares_decimal']))}))
        body=io.BytesIO()
        with zipfile.ZipFile(body,'w',zipfile.ZIP_DEFLATED) as z:
            z.writestr('xl/sharedStrings.xml','<sst xmlns="'+ns+'">'+''.join('<si><t>'+escape(s)+'</t></si>' for s in strings)+'</sst>')
            z.writestr('xl/worksheets/sheet1.xml','<worksheet xmlns="'+ns+'"><sheetData>'+''.join(xml)+'</sheetData></worksheet>')
        add('ssga_'+ticker,'https://www.ssga.com'+path,body.getvalue())
    add('ssga_catalog',native.SSGA_URL,{'data':{'funds':{'etfs':{'datas':catalogue}}}})
    output,histories=etf_research.build(inputs,objects.__getitem__,at);objects.update(histories)
    run={'contract':'etf-original-replay.v1','generated_at':at,'compilers':{}}
    for name,doc in [('input',inputs),('output',output)]:
        raw=model.encoded(doc);digest=model.sha(raw);key=issuer.PREFIX+name+'s/'+digest+'.json';objects[key]=raw;run[name]={'key':key,'sha256':digest,'bytes':len(raw)}
    for module in etf_store.COMPILERS:
        raw=Path(module.__file__).read_bytes();digest=model.sha(raw);key=issuer.PREFIX+'compilers/'+digest+'.py';objects[key]=raw;run['compilers'][module.__name__]={'key':key,'sha256':digest}
    run['output_sha256']=run['output']['sha256'];raw=model.encoded(run);key=issuer.PREFIX+'runs/'+model.sha(raw)+'.json';objects[key]=raw
    return {**output,'replay':{'manifest_key':key,'output_sha256':run['output_sha256']}},objects


class Issuer(unittest.TestCase):
    def test_exact_parser_copy_and_actual_upstream_output_reconstructs(self):
        self.assertEqual(Path(native.__file__).read_bytes(),(ROOT/'aws/lambdas/justhodl-etf-true-flows/source/etf_native.py').read_bytes())
        p,o=issuer_fixture();r=issuer.restore(p,o.__getitem__)
        self.assertEqual(len(r['sectors']),11)
        self.assertEqual(r['sectors']['XLK']['windows'],p['by_etf']['XLK']['flow_windows'])
        self.assertEqual(r['sectors']['XLK']['comparison'],p['by_etf']['XLK']['source_comparison'])
    def test_original_compiler_history_and_current_body_tampering_fail(self):
        p,o=issuer_fixture()
        keys=[p['by_etf']['XLK']['source']['evidence']['key'],p['by_etf']['XLK']['history']['key'],next(k for k in o if '/compilers/' in k)]
        for key in keys:
            bad=dict(o);bad[key]+=b' '
            with self.assertRaises(ValueError):issuer.restore(p,bad.__getitem__)
        p['by_etf']['XLK']['flow_windows']['5d']['value_decimal']='999999'
        with self.assertRaises(ValueError):issuer.restore(p,o.__getitem__)


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
        if key in (model.CURRENT,model.CAPITAL_CURRENT) and self.contention:self.contention-=1;raise Failure('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(kw)


def storage():
    r,v,i=fixtures();objects={key:model.encoded({'generated_at':SOURCE,'whole_context':{'preserved':[1,2,3]},'key':key}) for key in store.SOURCES}
    objects[store.ROOTS[0]]=model.encoded(r);objects[store.ROOTS[1]]=model.encoded(v);objects[store.ROOTS[2]]=model.encoded({'contract':'fixture-issuer-root'})
    return Storage(objects),r,v,i


class Replay(unittest.TestCase):
    def patches(self,r,v,i):
        self.enterContext(patch.object(store.prices,'replay',return_value={k:x for k,x in r.items() if k!='replay'}))
        self.enterContext(patch.object(store.volume,'replay',return_value={k:x for k,x in v.items() if k!='replay'}))
        self.enterContext(patch.object(store.issuer,'restore',return_value=i));self.enterContext(patch.object(store,'now',return_value=STAMP))
    def test_whole_contexts_native_storage_replay_and_projection_share_roots(self):
        client,r,v,i=storage();before=dict(client.objects);self.patches(r,v,i)
        result=store.run(client,'b','flow','first','execution');self.assertTrue(result['published'])
        for key,raw in before.items():self.assertEqual(client.objects[model.PRIVATE+model.sha(raw)+'.bin'],raw)
        current=json.loads(client.objects[model.CURRENT]);self.assertEqual(store.replay(current['replay'],store.reader(client,'b')),{k:x for k,x in current.items() if k!='replay'})
        projection=store.run(client,'b','capital','view','execution-two');self.assertTrue(projection['published'])
        capital=json.loads(client.objects[model.CAPITAL_CURRENT]);self.assertEqual(capital['sectors'],current['sectors']);self.assertEqual(capital['canonical_replay'],current['replay'])
        writes=len(client.writes);self.assertEqual(store.run(client,'b','flow','first','execution-three'),result);self.assertEqual(len(client.writes),writes)
    def test_failed_root_or_original_replay_keeps_preceding_publication(self):
        client,r,v,i=storage();before=client.objects[model.CURRENT];self.patches(r,v,i)
        with patch.object(store.prices,'replay',side_effect=ValueError('bad root')),self.assertRaises(RuntimeError):store.run(client,'b','flow','bad','execution')
        self.assertEqual(client.objects[model.CURRENT],before);self.assertEqual(json.loads(client.objects[store.request_key('flow','bad')])['status'],'failed')
    def test_contention_source_rollback_and_tampered_artifacts(self):
        client,r,v,i=storage();self.patches(r,v,i);client.contention=1;store.run(client,'b','flow','race','execution')
        before=client.objects[model.CURRENT];p=json.loads(before);p['generated_at']='2026-09-21T05:01:00+00:00';p['source_clocks']['issuer']['source_generated_at']='2026-09-19T12:00:00+00:00'
        self.assertFalse(store.publish(client,'b','flow',p));self.assertEqual(client.objects[model.CURRENT],before)
        p=json.loads(before);run=json.loads(client.objects[p['replay']['manifest_key']]);client.objects[run['output']['key']]+=b' '
        with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(client,'b'))
        for key in ('private/accounts.json','data/sector-fusion-research/outputs/../../accounts.json'):
            with self.assertRaises(ValueError):store.reader(client,'b')(key)
    def test_actual_handlers_http_validation_and_missing_execution_do_not_publish(self):
        for fn,contract,current,kind in [('justhodl-sector-flow-state',model.CONTRACT,model.CURRENT,'flow'),('justhodl-sector-capital-fusion',model.CAPITAL_CONTRACT,model.CAPITAL_CURRENT,'capital')]:
            tree=ast.parse((ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
            def forbidden(*a,**kw):self.fail('Unexpected publication')
            ns={'json':json,'CONTRACT':contract,'boto3':types.SimpleNamespace(client=forbidden),'run':forbidden}
            exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-sector-handler','exec'),ns)
            self.assertEqual(ns['lambda_handler']({'validate_only':True},None)['statusCode'],200)
            ns.update(boto3=types.SimpleNamespace(client=lambda *a,**kw:None),Config=lambda **kw:None,PUBLISHED_KEY=current,KIND=kind,reader=lambda *a:lambda k:model.encoded({'contract':contract}))
            self.assertEqual(ns['lambda_handler']({'httpMethod':'GET'},None)['statusCode'],200)
            with self.assertRaisesRegex(ValueError,'AWS execution'):ns['lambda_handler']({},None)


if __name__=='__main__':unittest.main(verbosity=2)
