import ast,copy,csv,gzip,hashlib,io,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SOURCE)]
import carry_equity as e
import carry_original as n
import carry_research as m
import carry_store as s
import carry_catalog as c
import evidence_store
AT='2026-09-19T14:00:00+00:00'

class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self):self.objects={};self.meta={}
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest(),'Metadata':self.meta.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise StorageError('412')
        if 'IfMatch' in kw and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise StorageError('412')
        self.objects[key]=kw['Body'];self.meta[key]=kw.get('Metadata',{})

def retain(client,url,doc):
    raw=doc if isinstance(doc,bytes) else m.encoded(doc)
    receipt=evidence_store.capture(client,'b','carry',url,raw,n.clock(AT))
    return {'url':url,'acquired_at':AT,'evidence':receipt}

def documents():
    def div(day,value,adjusted,payment):return {'symbol':'HDV','date':day,'dividend':value,'adjDividend':adjusted,'paymentDate':payment}
    return {'profile':[{'symbol':'HDV','isEtf':True,'isActivelyTrading':True,'currency':'USD'}],
      'historical-price-eod/light':[{'symbol':'HDV','date':'2026-09-18','price':'28.74'},{'symbol':'HDV','date':'2026-04-28','price':'26.90'}],
      'historical-price-eod/non-split-adjusted':[{'symbol':'HDV','date':'2026-09-18','adjClose':'28.74'},{'symbol':'HDV','date':'2026-04-28','adjClose':'134.50'}],
      'dividends':[div('2025-09-01','1','0.2','2025-09-10'),div('2026-03-10','1','0.2','2026-03-20'),div('2026-09-16','0.3','0.3','2026-09-21')],
      'splits':[{'symbol':'HDV','date':'2026-04-29','numerator':5,'denominator':1}],
      'ratios-ttm':[{'symbol':'HDV','dividendYieldTTM':'0.02'}],'key-metrics-ttm':[{'symbol':'HDV'}]}

def equity_refs(client):return {k:retain(client,n.fmp_url('HDV',k),v) for k,v in documents().items()}

def fred_refs(client,sid='DFF',value='4'):
    meta={'id':sid,'title':sid,'units':'Percent','frequency_short':c.FRED[sid]['frequency'],'seasonal_adjustment':'Not Seasonally Adjusted',
      'observation_start':'2026-08-01','observation_end':'2026-09-18'}
    definition=retain(client,n.definition_url(sid),{'seriess':[meta]})
    url=n.observation_urls(sid,meta,AT)[0]
    doc={'units':'lin','count':2,'limit':4000,'offset':0,'observations':[{'date':'2026-09-18','value':value},{'date':'2026-08-01','value':'3.9'}]}
    return {'definition':definition,'observations':[retain(client,url,doc)]}

class Originals(unittest.TestCase):
    def test_split_adjusted_distribution_income_and_price_are_independent(self):
        p=e.compile_equity('HDV',documents(),'2026-09-18')
        self.assertEqual(p['trailing_distribution']['current_share_distribution_decimal'],'0.5')
        self.assertAlmostEqual(float(p['trailing_distribution']['yield_pct_decimal']),100*.5/28.74)
        self.assertEqual(p['prices'][0]['value_decimal'],'26.90')
        self.assertEqual(p['prices'][0]['split_factor_decimal'],'5')
        self.assertEqual(p['paid_distribution_window']['current_share_distribution_decimal'],'0.2')
        self.assertEqual(p['trailing_distribution']['unpaid_or_undated_selected_ex_dates'],['2026-09-16'])
        self.assertFalse(p['calls_eligible']);self.assertIsNone(p['carry_per_vol'])

    def test_ambiguous_duplicate_current_event_blocks_income_but_old_event_is_retained(self):
        for index,valid in ((0,True),(1,False)):
            docs=documents();docs['dividends'].append(copy.deepcopy(docs['dividends'][index]))
            p=e.compile_equity('HDV',docs,'2026-09-18')
            self.assertEqual(len(p['distributions']),4)
            self.assertEqual(p['trailing_distribution']['yield_pct_decimal'] is not None,valid)
            self.assertIn(docs['dividends'][index]['date'],p['ambiguous_distribution_dates'])

    def test_empty_distribution_history_does_not_inherit_undated_zero_ratio(self):
        docs=documents();docs['dividends']=[];docs['ratios-ttm'][0]['dividendYieldTTM']=0
        p=e.compile_equity('HDV',docs,'2026-09-18')
        self.assertIsNone(p['trailing_distribution']['yield_pct_decimal'])
        self.assertEqual(p['reported_ttm_ratio']['percent_decimal'],'0')

    def test_wrong_currency_symbol_nonfinite_and_duplicate_price_fail(self):
        for case in ('currency','symbol','nan','duplicate'):
            docs=documents()
            if case=='currency':docs['profile'][0]['currency']='EUR'
            elif case=='symbol':docs['dividends'][0]['symbol']='OTHER'
            elif case=='nan':docs['dividends'][1]['dividend']='NaN'
            else:docs['historical-price-eod/light'].append(docs['historical-price-eod/light'][0])
            with self.assertRaises(ValueError):e.compile_equity('HDV',docs,'2026-09-18')

    def test_current_price_or_distribution_reconciliation_failure_blocks_yield(self):
        for case in ('price','distribution'):
            docs=documents()
            if case=='price':docs['historical-price-eod/light'][0]['price']='30'
            else:docs['dividends'][1]['adjDividend']='1'
            p=e.compile_equity('HDV',docs,'2026-09-18')
            self.assertIsNone(p['trailing_distribution']['yield_pct_decimal'])

    def test_future_price_is_retained_and_not_used(self):
        docs=documents()
        for endpoint,field in [('historical-price-eod/light','price'),('historical-price-eod/non-split-adjusted','adjClose')]:
            docs[endpoint].insert(0,{'symbol':'HDV','date':'2026-09-19',field:'100'})
        p=e.compile_equity('HDV',docs,'2026-09-18')
        self.assertEqual(p['price_decimal'],'28.74');self.assertEqual(len(p['future_or_incomplete_session_prices']),1)

    def test_completed_session_cutoff_is_conservative_in_new_york(self):
        self.assertEqual(n.completed_cutoff('2026-09-19T01:00:00Z'),'2026-09-17')
        self.assertEqual(n.completed_cutoff(AT),'2026-09-18')

    def test_original_response_tampering_and_wrong_request_identity_fail(self):
        client=Storage();refs=equity_refs(client)
        self.assertEqual(n.equity('HDV',refs,s.raw_reader(client,'b'),AT)['as_of'],'2026-09-18')
        key=refs['dividends']['evidence']['key'];client.objects[key]=gzip.compress(b'[]')
        with self.assertRaises(ValueError):n.equity('HDV',refs,s.raw_reader(client,'b'),AT)
        client=Storage();refs=equity_refs(client);refs['profile']['url']=n.fmp_url('SPY','profile')
        with self.assertRaises(ValueError):n.equity('HDV',refs,s.raw_reader(client,'b'),AT)

    def test_fred_missing_latest_never_falls_back_or_becomes_zero(self):
        client=Storage();refs=fred_refs(client,value='.')
        item=n.fred('DFF',refs,s.raw_reader(client,'b'),AT)
        self.assertIsNone(item['value_decimal']);self.assertEqual(item['quality']['status'],'incomplete')
        self.assertEqual(len(item['rows']),2)

    def test_fred_rolling_license_window_not_extended_with_invented_history(self):
        meta={'observation_start':'2023-09-19','observation_end':'2026-09-18'}
        urls=n.observation_urls('BAMLH0A0HYM2EY',meta,AT)
        self.assertEqual(len(urls),1);self.assertIn('observation_start=2023-09-19',urls[0])
        meta['observation_start']='1954-07-01'
        self.assertEqual(len(n.observation_urls('DFF',meta,AT)),3)

    def test_truncated_fred_or_changed_unit_rejected(self):
        for case in ('count','unit'):
            client=Storage();refs=fred_refs(client)
            ref=refs['observations'][0] if case=='count' else refs['definition']
            doc=json.loads(s.raw_reader(client,'b')(ref['evidence']['key']))
            if case=='count':doc['count']=3;refs['observations'][0]=retain(client,ref['url'],doc)
            else:doc['seriess'][0]['units']='Index';refs['definition']=retain(client,ref['url'],doc)
            with self.assertRaises(ValueError):n.fred('DFF',refs,s.raw_reader(client,'b'),AT)

    def test_same_month_requires_every_calendar_day_and_no_old_month_fallback(self):
        row=lambda d,v:{'date':d,'value_decimal':v,'row_index':0,'segment':0,'status':'observed'}
        fund={'rows':[row(f'2026-07-{i:02}','4') for i in range(1,32)],'quality':{'status':'fresh'}}
        foreign={'id':'X','as_of':'2026-08-01','rows':[row('2026-07-01','1'),row('2026-08-01','1')],'quality':{'status':'fresh'}}
        pair=m.monthly_gap(foreign,fund,AT)
        self.assertEqual(pair['rows'][0]['value_decimal'],'-3');self.assertIsNone(pair['value_decimal'])
        self.assertEqual(pair['status'],'latest_month_incomplete')
        fund['rows'].pop();pair=m.monthly_gap(foreign,fund,AT)
        self.assertIsNone(pair['rows'][0]['value_decimal'])

    def test_real_yield_never_subtracted_from_nominal_funding_and_no_forward_fill(self):
        item={'id':'X','kind':'real_yield','as_of':'2026-09-18','rows':[]}
        self.assertEqual(m.daily_gap(item,None)['status'],'real_nominal_comparison_prohibited')
        item.update(kind='nominal_treasury_yield',quality={'status':'fresh'},rows=[{'date':'2026-09-18','value_decimal':'4','row_index':0}])
        fund={'quality':{'status':'fresh'},'rows':[{'date':'2026-09-17','value_decimal':'3','row_index':0}]}
        result=m.daily_gap(item,fund);self.assertIsNone(result['value_decimal']);self.assertEqual(result['status'],'latest_date_incomplete')

    def test_ecb_identity_units_duplicate_months_and_status(self):
        fields={'KEY':c.ECB_EURIBOR_ID,'FREQ':'M','REF_AREA':'U2','CURRENCY':'EUR','PROVIDER_FM':'RT','INSTRUMENT_FM':'MM',
          'PROVIDER_FM_ID':'EURIBOR3MD_','DATA_TYPE_FM':'HSTA','UNIT':'PCPA','UNIT_MULT':'0','COLLECTION':'A',
          'TIME_PERIOD':'2026-08','OBS_VALUE':'2.5','OBS_STATUS':'A','OBS_CONF':'F','TITLE_COMPL':'Euribor 3M'}
        def capture(rows):
            text=io.StringIO();w=csv.DictWriter(text,fieldnames=fields);w.writeheader();w.writerows(rows)
            return retain(client,c.ECB_EURIBOR_URL,text.getvalue().encode())
        client=Storage();r=n.ecb(capture([fields]),s.raw_reader(client,'b'),AT)
        self.assertEqual(r['value_decimal'],'2.5');self.assertEqual(r['quality']['status'],'fresh')
        with self.assertRaises(ValueError):n.ecb(capture([fields,fields]),s.raw_reader(client,'b'),AT)
        bad={**fields,'UNIT_MULT':'6'}
        with self.assertRaises(ValueError):n.ecb(capture([bad]),s.raw_reader(client,'b'),AT)

    def test_whole_universe_stays_visible_when_sources_are_missing(self):
        out,h=m.build({'contract':'carry-original-inputs.v1'},lambda k:b'',AT)
        self.assertEqual(len(out['by_class']['equity']),110);self.assertEqual(len(out['by_class']['fx']),18)
        self.assertEqual(len(out['by_class']['fixed_income']),18);self.assertEqual(len(out['by_class']['commodity']),14)
        self.assertFalse(out['calls_eligible']);self.assertEqual(out['quality']['status'],'unavailable')
        self.assertFalse(out['cross_asset_top']);self.assertIsNone(out['unwind_overlay']['cohort_fragility'])
        self.assertEqual(next(r for r in out['by_class']['fx'] if r['symbol']=='KRW')['legacy_alias'],'KOR')

    def test_complete_replay_history_and_private_preservation_before_publish(self):
        client=Storage();legacy=m.encoded({'generated_at':AT,'inputs':['all retained'],'opaque_old_diagnostics':'private'})
        client.objects[m.CURRENT]=legacy;eq={'HDV':equity_refs(client)};rates={'DFF':fred_refs(client)}
        with patch.object(s,'MIN_EQUITIES',1),patch.object(s,'MIN_RATES',1),patch.object(s,'collect',return_value=(rates,eq,None,{},{})),patch.object(s,'now',return_value=AT):result=s.run(client,'b','configured-fred','configured-fmp')
        self.assertTrue(result['published']);out=json.loads(client.objects[m.CURRENT]);manifest=json.loads(client.objects[out['replay']['manifest_key']])
        self.assertEqual(s.replay(manifest,s.raw_reader(client,'b')),{k:v for k,v in out.items() if k!='replay'})
        self.assertEqual(client.objects[s.PRIVATE+hashlib.sha256(legacy).hexdigest()+'.bin'],legacy)
        self.assertNotIn('opaque_old_diagnostics',json.dumps(out))
        old=copy.deepcopy(out);old['generated_at']='2026-09-18T14:00:00Z';self.assertFalse(s.publish(client,'b',old))
        key=out['equities']['HDV']['prices_history']['key'];client.objects[key]=b'{}'
        with self.assertRaises(ValueError):s.replay(manifest,s.raw_reader(client,'b'))

    def test_missing_history_put_or_failed_replay_does_not_publish(self):
        client=Storage();legacy=m.encoded({'generated_at':AT});client.objects[m.CURRENT]=legacy
        eq={'HDV':equity_refs(client)}
        with patch.object(s,'MIN_EQUITIES',1),patch.object(s,'MIN_RATES',0),patch.object(s,'collect',return_value=({},eq,None,{},{})),patch.object(s,'now',return_value=AT),patch.object(s,'replay',side_effect=ValueError('mismatch')):
            with self.assertRaises(ValueError):s.run(client,'b','configured-fred','configured-fmp')
        self.assertEqual(client.objects[m.CURRENT],legacy)

    def test_collection_coverage_floor_retains_last_publication(self):
        client=Storage();legacy=m.encoded({'generated_at':AT});client.objects[m.CURRENT]=legacy
        eq={'HDV':equity_refs(client)}
        with patch.object(s,'collect',return_value=({},eq,None,{},{})),patch.object(s,'now',return_value=AT):
            with self.assertRaisesRegex(ValueError,'publication floor'):s.run(client,'b','configured-fred','configured-fmp')
        self.assertEqual(client.objects[m.CURRENT],legacy)

    def test_active_http_handler_never_collects_or_emits_notifications(self):
        tree=ast.parse((SOURCE/'lambda_function.py').read_text());function=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='lambda_handler')
        client=Storage();client.objects[m.CURRENT]=m.encoded({'contract':m.CONTRACT,'generated_at':AT})
        env={'s3':client,'BUCKET':'b','FRED_KEY':'test','FMP_KEY':'test','json':json}
        exec(compile(ast.Module(body=[function],type_ignores=[]),'active-handler','exec'),env)
        with patch.object(s,'run',side_effect=AssertionError('HTTP must not collect')):
            result=env['lambda_handler']({'requestContext':{'http':{'method':'GET'}}})
        self.assertEqual(result['statusCode'],200);self.assertEqual(result['headers']['Cache-Control'],'no-store')
        self.assertEqual(len(client.objects),1)

if __name__=='__main__':unittest.main()
