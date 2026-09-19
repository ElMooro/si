import ast
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
import gzip,hashlib,io,json
from pathlib import Path
import sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(Path(__file__).resolve().parents[1]/'source')]
import funding_original as n
import funding_research as m
import funding_store as s
from evidence_store import capture
AT='2026-09-19T13:00:00+00:00'


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.metadata={}
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest(),'Metadata':self.metadata.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key']
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or kw['IfMatch']!=hashlib.sha256(self.objects[key]).hexdigest()):raise StorageError('PreconditionFailed')
        self.objects[key]=kw['Body'];self.metadata[key]=kw.get('Metadata',{})


def ofr(sid='REPO-DVP_TV_TOT-P',values=None):
    rate='_AR_' in sid;monthly=sid.endswith('-M')
    return m.encoded({sid:{'metadata':{'mnemonic':sid,'unit':{'name':'Percent' if rate else 'USD','magnitude':0},
        'description':{'name':'Original fixture','vintage':'Final' if sid.endswith('-F') else 'Preliminary'},
        'schedule':{'observation_frequency':'Monthly' if monthly else 'Daily'}},
        'timeseries':{'aggregation':values or [['2026-09-17','0'],['2026-09-18','1234567890000.01']]}}})


def measurement(sid,values):
    rows=[{'date':d,'value_decimal':v,'row_index':i,'status':'observed' if v is not None else 'missing'} for i,(d,v) in enumerate(values)]
    return n.entry(sid,sid,'Percent','D',rows,{}, {},AT,AT,5)


class FundingTests(unittest.TestCase):
    def test_usd_no_fake_millions_and_zero_retained(self):
        r=n.ofr(ofr(),'REPO-DVP_TV_TOT-P',{},AT,AT)['REPO-DVP_TV_TOT-P']
        self.assertEqual(r['unit'],'USD');self.assertEqual(r['value_decimal'],'1234567890000.01')
        self.assertEqual(r['rows'][0]['value_decimal'],'0')
        self.assertEqual(r['quality']['status'],'fresh')

    def test_final_is_historical_even_with_current_acquisition(self):
        sid='REPO-DVP_TV_TOT-F';r=n.ofr(ofr(sid,[['2026-03-31','3']]),sid,{},AT,AT)[sid]
        self.assertEqual(r['quality']['status'],'historical_final');self.assertEqual(r['as_of'],'2026-03-31')
        self.assertFalse(r['calls_eligible'])

    def test_ofr_metadata_and_disclosure_errors(self):
        sid='REPO-DVP_TV_TOT-P';doc=json.loads(ofr());doc[sid]['metadata']['unit']['magnitude']=6
        with self.assertRaises(ValueError):n.ofr(m.encoded(doc),sid,{},AT,AT)
        with self.assertRaises(ValueError):n.ofr(ofr(sid,[['2026-09-18','-1']]),sid,{},AT,AT)
        r=n.ofr(ofr(sid,[['2026-09-17','1'],['2026-09-18',None]]),sid,{},AT,AT)[sid]
        self.assertIsNone(r['value']);self.assertEqual(r['quality']['status'],'incomplete')

    def test_duplicate_future_and_nonfinite_rejected(self):
        for value in (True,'NaN','Infinity','1e20'):
            with self.assertRaises(ValueError):n.amount(value)
        with self.assertRaises(ValueError):n.strict_json(b'{"x":1,"x":2}')
        for values in ([['2026-09-18','1'],['2026-09-18','2']],[['2026-09-20','1']]):
            with self.assertRaises(ValueError):n.ofr(ofr(values=values),'REPO-DVP_TV_TOT-P',{},AT,AT)

    def test_exact_date_pairs_never_use_old_common_date(self):
        a=measurement('SOFR',[('2026-09-17','4'),('2026-09-18','4.1')])
        b=measurement('IORB',[('2026-09-17','4.05')]);r=m.pair({'SOFR':a,'IORB':b},*m.PAIRS[0])
        self.assertIsNone(r['value']);self.assertEqual(r['as_of'],'2026-09-18');self.assertEqual(r['status'],'same_date_leg_missing')
        b=measurement('IORB',[('2026-09-17','4.05'),('2026-09-18','4.05')])
        r=m.pair({'SOFR':a,'IORB':b},*m.PAIRS[0]);self.assertEqual(r['value'],5)
        b['quality']['status']='stale_source';self.assertIsNone(m.pair({'SOFR':a,'IORB':b},*m.PAIRS[0])['value'])
        self.assertIn('not a CP-OIS',m.PAIRS[4][-1])

    def test_prior_statistics_exclude_current_and_break_at_missing(self):
        rows=[{'date':(date(2025,9,13)+timedelta(days=7*i)).isoformat(),'value_decimal':str(i),'row_index':i} for i in range(53)]
        rows[-1]['value_decimal']='1000';r=m.statistics(rows,'W')
        self.assertEqual(r['prior_n'],52);self.assertEqual(r['mean_decimal'],'25.5')
        self.assertEqual(r['percentile'],100)
        rows[-2]['value_decimal']=None;self.assertEqual(m.statistics(rows,'W')['prior_n'],0)
        self.assertIsNone(m.statistics([],'D')['z'])

    def test_calendar_changes_not_number_of_observations(self):
        rows=[{'date':'2026-06-19','value_decimal':'20','row_index':0},
              {'date':'2026-09-18','value_decimal':'12','row_index':1}]
        r=m.changes(rows);self.assertEqual(r['13w']['difference'],-8);self.assertIsNone(r['1w']['difference'])

    def test_daily_missing_record_is_excluded_without_filling_or_resetting_year(self):
        rows=[{'date':(date(2025,12,1)+timedelta(days=i)).isoformat(),'value_decimal':str(i),'row_index':i} for i in range(270)]
        rows[-3]['value_decimal']=None
        result=m.statistics(rows,'D')
        self.assertEqual(result['prior_n'],252);self.assertEqual(result['excluded_missing_observations'],1)
        self.assertEqual(result['status'],'available');self.assertIsNone(rows[-3]['value_decimal'])
        for row in rows[-10:-1]:row['value_decimal']=None
        self.assertEqual(m.statistics(rows,'D')['prior_n'],0)

    def test_ecb_definition_all_foreign_currencies_and_period_date(self):
        import csv
        title='Euro area (changing composition), Eurosystem reporting sector - Claims on euro area residents denominated in foreign currency, All currencies except EUR - Euro area (changing composition) counterpart'
        row={'KEY':n.ECB_KEY,'UNIT':'EUR','UNIT_MULT':'6','FREQ':'W','TITLE_COMPL':title,'TIME_PERIOD':'2026-W37','OBS_VALUE':'20184','OBS_STATUS':'A','OBS_CONF':'F','TITLE':'Claims','COLLECTION':'E'}
        def body():
            out=io.StringIO();writer=csv.DictWriter(out,fieldnames=list(row));writer.writeheader();writer.writerow(row);return out.getvalue().encode()
        result=n.ecb(body(),{},AT,AT)[n.ECB_KEY]
        self.assertEqual(result['as_of'],'2026-09-11');self.assertEqual(result['value'],20184);self.assertEqual(result['unit'],'EUR_millions')
        self.assertIn('not isolated USD',result['limitation'])
        row['OBS_STATUS']='Q';self.assertIsNone(n.ecb(body(),{},AT,AT)[n.ECB_KEY]['value'])
        row['UNIT']='USD'
        with self.assertRaises(ValueError):n.ecb(body(),{},AT,AT)

    def test_tma_columns_preserve_date_and_tenor(self):
        html='<h1>CNH Hong Kong Interbank Offered Rate</h1><table><tr><td>Date</td><td>18/09/2026</td><td>17/09/2026</td></tr>'
        html+=''.join('<tr><td>'+t+'</td><td>0</td><td>-1</td></tr>' for t in n.TENORS)+'</table>'
        r=n.tma(html.encode(),{},AT,AT)['CNH_HIBOR:ON']
        self.assertEqual(r['value'],0);self.assertEqual(r['rows'][0]['value_decimal'],'-1');self.assertEqual(r['rows'][0]['column_index'],2)
        with self.assertRaises(ValueError):n.tma(html.replace('18/09/2026','17/09/2026').encode(),{},AT,AT)

    def test_fx_gap_requires_close_quote_times_and_no_forward_claim(self):
        def quote(sid,seconds):
            ts=int(datetime(2026,9,18,20,tzinfo=timezone.utc).timestamp())+seconds
            return n.fx(m.encoded([{'symbol':sid,'price':'7.1' if sid=='USDCNH' else '7','timestamp':ts}]),sid,{},AT,AT)[sid]
        rows={'USDCNH':quote('USDCNH',0),'USDCNY':quote('USDCNY',61)}
        self.assertEqual(m.spot_gap(rows)['status'],'quote_times_not_aligned')
        rows['USDCNY']=quote('USDCNY',60);r=m.spot_gap(rows);self.assertEqual(r['value_decimal'],'0.1')
        self.assertEqual(r['unit'],'yuan_per_USD');self.assertFalse(r['sizing_eligible'])

    def test_exact_original_request_hash_and_full_shards(self):
        c=Storage();sid='REPO-DVP_TV_TOT-P';body=ofr()
        ref=capture(c,'fixture','funding',n.URLS[sid],body,datetime.fromisoformat(AT))
        descriptor={'url':n.URLS[sid],'evidence':ref,'acquired_at':AT};read=s.raw_reader(c,'fixture')
        measured=n.load(sid,descriptor,read,AT);out={'measurements':measured,'comparisons':{}}
        artifacts=m.compact_histories(out);r=out['measurements'][sid];self.assertNotIn('rows',r)
        self.assertEqual(r['history']['observations'],2);self.assertEqual(hashlib.sha256(artifacts[r['history']['key']]).hexdigest(),r['history']['sha256'])
        descriptor['url']+='&not_reviewed=1'
        with self.assertRaises(ValueError):n.load(sid,descriptor,read,AT)
        descriptor['url']=n.URLS[sid];c.objects[ref['key']]=gzip.compress(body+b' ')
        with self.assertRaises(ValueError):n.load(sid,descriptor,read,AT)

    def test_legacy_projection_retains_typed_claims_not_private_prose(self):
        c=Storage();old={'layers':{'us_core':{'metrics':[{'id':'old','value':0,'unit':'bp','asof':'2026-09-18'}]}},'ai':'PRIVATE TEXT'}
        c.objects[m.CURRENT]=m.encoded(old);marker,inventory=s.preserve(c,'fixture',s.raw_reader(c,'fixture'))
        self.assertEqual(c.objects[s.PRIVATE+marker['sha256']+'.bin'],m.encoded(old));self.assertEqual(inventory[0]['reported_decimal'],'0')
        self.assertNotIn('PRIVATE TEXT',json.dumps(marker)+json.dumps(inventory))
        self.assertEqual(s.preserve(c,'fixture',s.raw_reader(c,'fixture')),(marker,inventory))

    def test_no_older_publication_or_source_clock_regression(self):
        c=Storage();p={'contract':m.CONTRACT,'generated_at':AT,'source_clocks':{'canonical_macro':AT}}
        self.assertTrue(s.publish(c,'fixture',p));old={**p,'generated_at':'2026-09-18T13:00:00+00:00'}
        self.assertFalse(s.publish(c,'fixture',old))
        late={**p,'generated_at':'2026-09-20T13:00:00+00:00','source_clocks':{'canonical_macro':'2026-09-18T13:00:00+00:00'}}
        self.assertFalse(s.publish(c,'fixture',late))
        with self.assertRaises(ValueError):s.publish(c,'fixture',{**p,'different':True})

    def test_active_handler_http_is_read_only_and_errors_safe(self):
        path=Path(__file__).parents[1]/'source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf-8'))
        handler=next(v for v in tree.body if isinstance(v,ast.FunctionDef) and v.name=='lambda_handler')
        source=ast.unparse(handler)
        for forbidden in ('ai_scan','complete(','wl_series','wl_fusion','build_layers','composite('):self.assertNotIn(forbidden,source)
        c=Storage();c.objects[m.CURRENT]=m.encoded({'contract':m.CONTRACT,'generated_at':AT})
        scope={'S3':c,'BUCKET':'fixture','FMP_KEY':'TEST-SECRET','json':json}
        exec(compile(ast.Module(body=[handler],type_ignores=[]),'actual-handler','exec'),scope)
        with patch.object(s,'run',side_effect=AssertionError('must not collect')):
            r=scope['lambda_handler']({'requestContext':{'http':{'method':'GET'}}},None)
        self.assertEqual(r['statusCode'],200);self.assertEqual(json.loads(r['body'])['generated_at'],AT)
        with patch.object(s,'run',side_effect=ValueError('TEST-SECRET')):
            r=scope['lambda_handler']({},None)
        self.assertEqual(r['statusCode'],503);self.assertNotIn('TEST-SECRET',r['body'])

    def test_catalog_extends_without_replacing_existing_series(self):
        from funding_research_catalog import extend_catalog,SERIES
        original={'SOFR':{'custom':True}};extended=extend_catalog(original)
        self.assertEqual(extended['SOFR'],{'custom':True});self.assertTrue(set(SERIES)<=set(extended))


if __name__=='__main__':unittest.main()
