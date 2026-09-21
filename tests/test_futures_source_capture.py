from pathlib import Path
from io import BytesIO
from unittest.mock import Mock
import json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import futures_source_capture as c
START='2026-06-23';END='2026-09-21'
def body(**kw):return json.dumps({'status':'OK','results':[],**kw}).encode()
def contract(**kw):return {'ticker':'ESZ6','product_code':'ES','trading_venue':'XCME','type':'single','date':END,
    'active':True,'first_trade_date':'2025-01-01','last_trade_date':'2026-12-18','settlement_date':'2026-12-18',**kw}
class Response(BytesIO):status=200
class Opener:
    def __init__(self,items):self.items=iter(items);self.calls=[]
    def open(self,req,**kwargs):
        self.calls.append(req);item=next(self.items)
        if isinstance(item,Exception):raise item
        return Response(item)
class Tests(unittest.TestCase):
    def collect(self,items,kind='contracts'):
        scope=c.spec(kind,'ES',START,END,'ESZ6' if kind=='bars' else None)
        originals=[];states=[];opener=Opener(items);budget=c.Budget(requests=168)
        def retain(raw):originals.append(raw);return {'sha256':c.sha(raw),'bytes':len(raw)}
        out=c.collect(scope,'synthetic-key',c.time.monotonic()+60,retain,budget,states.append,opener)
        return out,originals,states,opener,budget
    def test_exact_original_numbers_nanoseconds_null_zero_negative(self):
        raw=b'{"status":"OK","results":[{"close":-37.63000000000000000001,"settlement_price":0,"window_start":1790028000000000001},{"close":null},{}]}'
        out,originals,states,opener,budget=self.collect([raw],'bars')
        self.assertEqual(originals,[raw]);self.assertTrue(out['pagination_complete'])
        rows=c.envelope(raw)['results'];self.assertEqual(str(rows[0]['close']),'-37.63000000000000000001')
        s=c.summarize(rows,'bars');self.assertEqual(s['numeric_states']['close'],{'negative':1,'null':1,'missing':1})
        self.assertEqual(s['numeric_states']['settlement_price'],{'zero':1,'missing':2})
        self.assertEqual(s['first_window_start_ns'],'1790028000000000001');self.assertEqual(states[-1],out)
        self.assertNotIn('synthetic-key',json.dumps(out));self.assertFalse(out['bar_finality_independently_verified'])
    def test_no_equity_endpoint_or_constructed_undated_identity(self):
        for t in ('ES','CL','F:ES','ESZ6/extra','ESZ6?x=y','ESZ6%2f','NQZ6'):
            with self.assertRaises(ValueError):c.spec('bars','ES',START,END,t)
        with self.assertRaises(ValueError):c.spec('bars','VX',START,END,'VXZ6')
    def test_foreign_destination_and_scope_changes_rejected(self):
        scope=c.spec('contracts','ES',START,END);url=c.initial(scope)
        for bad in (url.replace('https:','http:'),url.replace('api.massive.com','127.0.0.1'),url.replace('api.massive.com','api.massive.com:443'),
                    url.replace('/contracts','/products'),url+'#x',url.replace('ES','NQ'),url.replace(END,'2026-09-20'),url+'&limit=3',url+'&field=x'):
            with self.assertRaises(ValueError):c.canonical(bad,scope)
    def test_bounded_pagination_removes_embedded_keys(self):
        url='https://api.massive.com/futures/v1/contracts?cursor=two&apiKey=old'
        out,originals,states,opener,budget=self.collect([body(next_url=url),body()])
        self.assertTrue(out['pagination_complete']);self.assertEqual(len(originals),2)
        self.assertNotIn('apiKey=old',json.dumps(out));self.assertEqual(budget.requests,2)
    def test_pagination_cycle_invalid_host_and_page_cap(self):
        for url,stop in ((c.initial(c.spec('contracts','ES',START,END)),'pagination_cycle'),('https://evil.example/x','invalid_pagination_address')):
            out,originals,*_=self.collect([body(next_url=url)]);self.assertEqual(out['stop'],stop);self.assertEqual(len(originals),1)
        out,originals,*_=self.collect([body(next_url='https://api.massive.com/futures/v1/contracts?cursor='+str(i)) for i in range(c.MAX_PAGES)])
        self.assertEqual(out['stop'],'page_limit');self.assertFalse(out['pagination_complete']);self.assertEqual(len(originals),4)
    def test_bad_envelope_retained_but_ineligible(self):
        for raw in (body(status='NOT_AUTHORIZED'),body(results={}),body(results=[{}]*1001),b'{"status":"OK","status":"OK"}',b'{"c":NaN}'):
            out,originals,*_=self.collect([raw]);self.assertEqual(originals,[raw]);self.assertEqual(out['stop'],'invalid_provider_envelope')
    def test_provider_denial_is_retained_without_retry(self):
        error=urllib.error.HTTPError('https://api.massive.com',403,'denied',{},BytesIO(b'{"status":"NOT_AUTHORIZED"}'))
        out,originals,states,opener,budget=self.collect([error]);self.assertEqual(out['stop'],'provider_http_failure')
        self.assertEqual(out['pages'][0]['http_status'],403);self.assertEqual(budget.requests,1)
    def test_credential_reflection_and_expired_deadline(self):
        with self.assertRaises(ValueError):self.collect([b'{"error":"synthetic-key"}'])
        scope=c.spec('products','ES',START,END);retain=Mock();opener=Mock();budget=c.Budget()
        out,_=c.request(c.initial(scope),scope,'synthetic-key',0,retain,budget,opener)
        self.assertEqual(out['status'],'time_budget');opener.open.assert_not_called();retain.assert_not_called()
    def test_complete_catalog_sorts_by_expiry_not_month_letter_or_price(self):
        rows=[contract(),contract(ticker='ESH7',last_trade_date='2027-03-19',settlement_date='2027-03-19')]
        result=c.select_contracts(rows[::-1],'ES',END,True)
        self.assertEqual([r['ticker'] for r in result['selected']],['ESZ6','ESH7'])
        self.assertEqual(result['selected'][0]['source_row_index'],1)
    def test_partial_catalog_cannot_claim_nearest_contracts(self):
        r=c.select_contracts([contract()],'ES',END,False);self.assertFalse(r['selection_qualified']);self.assertEqual(r['selected'],[])
    def test_missing_forged_and_expired_metadata_not_inferred(self):
        for kw in ({'product_code':None},{'trading_venue':'XNYM'},{'type':'combo'},{'date':'2025-01-01'},{'active':1},
                   {'ticker':'ES'},{'first_trade_date':None},{'last_trade_date':'2026-09-18'},{'settlement_date':'2026-12-17'}):
            r=c.select_contracts([contract(**kw)],'ES',END,True)
            self.assertFalse(r['selection_qualified']);self.assertEqual(r['selected'],[])
    def test_duplicate_and_ambiguous_catalog_fail_closed(self):
        r=c.select_contracts([contract(),contract()],'ES',END,True)
        self.assertEqual(r['duplicate_tickers'],['ESZ6']);self.assertEqual(r['selected'],[])
        self.assertFalse(c.select_contracts([contract(),None],'ES',END,True)['selection_qualified'])
    def test_empty_complete_is_empty_not_signal(self):
        r=c.select_contracts([],'ES',END,True);self.assertEqual(r['selected'],[]);self.assertEqual(r['qualified_contracts'],0)
        s=c.summarize([],'bars');self.assertEqual(s['numeric_states']['close'],{});self.assertTrue(s['no_returns_curve_signals_or_portfolio_decisions_computed'])
    def test_source_query_is_explicit_and_scoped(self):
        for kind in ('products','contracts','schedules','bars'):
            scope=c.spec(kind,'CL',START,END,'CLX6' if kind=='bars' else None)
            self.assertTrue(c.initial(scope).startswith('https://api.massive.com/futures/v1/'))
        with self.assertRaises(ValueError):c.spec('contracts','ES','2025-01-01',END)
        with self.assertRaises(ValueError):c.spec('trades','ES',START,END)
    def test_budget_and_body_limit(self):
        b=c.Budget(requests=1,byte_limit=1);b.request();b.received(1)
        with self.assertRaises(ValueError):b.request()
        with self.assertRaises(ValueError):b.received(1)
        with self.assertRaises(ValueError):c.bounded(BytesIO(b'123'),2)
    def test_product_specs_keep_precision_without_inventing_units(self):
        rows=c.decode(b'{"results":[{"product_code":"CL","unit_of_measure_qty":1000.000000000000000001}]}')['results']
        r=c.summarize(rows,'products')['returned_specifications'][0]
        self.assertEqual(r['unit_of_measure_qty'],'1000.000000000000000001')
        self.assertIsNone(r['price_quotation']);self.assertIsNone(r['trade_currency_code'])
if __name__=='__main__':unittest.main(verbosity=2)
