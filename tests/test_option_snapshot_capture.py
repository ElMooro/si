from pathlib import Path
from decimal import Decimal
import io,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import option_snapshot_capture as source

class Response(io.BytesIO):status=200
class Transport:
    def __init__(self,docs):self.docs=iter(docs);self.urls=[]
    def open(self,req,**kwargs):
        self.urls.append(req.full_url)
        doc=next(self.docs)
        return Response(doc if isinstance(doc,bytes) else json.dumps(doc).encode())

class Tests(unittest.TestCase):
    def run_source(self,docs):
        saved={};checkpoints=[];transport=Transport(docs)
        def retain(raw):
            digest=source.sha(raw);saved[digest]=raw
            return {'sha256':digest,'bytes':len(raw),'key':digest}
        budget={'requests':0,'bytes':0,'max_requests':10,'max_bytes':1000000}
        result=source.collect('SPY','secret-value',time.monotonic()+30,retain,budget,lambda p:checkpoints.append(len(p['pages'])),transport)
        return result,saved,checkpoints,budget,transport
    def test_two_complete_pages_and_private_key_not_in_descriptor(self):
        next_url='https://api.polygon.io/v3/snapshot/options/SPY?cursor=two&apiKey=secret-value'
        # Provider bodies must not echo credentials, even in their own next URL.
        with self.assertRaisesRegex(ValueError,'reflection'):self.run_source([{'status':'OK','results':[], 'next_url':next_url}])
        next_url=next_url.split('&apiKey')[0]
        result,saved,checkpoints,budget,transport=self.run_source([
            {'status':'OK','results':[{'open_interest':0}],'next_url':next_url},
            {'status':'DELAYED','results':[{'open_interest':None}]}])
        self.assertTrue(result['pagination_complete']);self.assertEqual(len(saved),2)
        self.assertEqual(checkpoints,[1,2,2]);self.assertEqual(budget['requests'],2)
        self.assertNotIn('secret-value',json.dumps(result))
        self.assertFalse(result['exchange_chain_completeness_verified'])
    def test_cross_underlying_and_unreviewed_host_rejected(self):
        for url in ('https://api.polygon.io/v3/snapshot/options/AAPL?cursor=x','https://example.com/v3/snapshot/options/SPY?cursor=x',
                'http://api.polygon.io/v3/snapshot/options/SPY?cursor=x','https://api.polygon.io/v3/snapshot/options/SPY?cursor=x&cursor=y',
                'https://api.polygon.io/v3/snapshot/options/SPY?limit=1000','https://api.polygon.io/v3/snapshot/options/SPY?expiration_date=2026-09-25'):
            with self.subTest(url=url),self.assertRaises(ValueError):source.next_url(url,'SPY')
    def test_cycle_is_partial(self):
        u='https://api.polygon.io/v3/snapshot/options/SPY?cursor=x'
        result,*_=self.run_source([{'status':'OK','results':[],'next_url':u},{'status':'OK','results':[],'next_url':u}])
        self.assertEqual(result['stop'],'pagination_cycle');self.assertFalse(result['pagination_complete'])
    def test_invalid_envelope_retained_without_claiming_complete(self):
        result,saved,*_=self.run_source([b'{"status":"OK","results":[],"status":"DELAYED"}'])
        self.assertEqual(result['stop'],'invalid_provider_envelope');self.assertEqual(len(saved),1)
    def test_missing_null_zero_and_bad_type_preserved(self):
        result=source.summary([{}, {'open_interest':None},{'open_interest':0},{'open_interest':False},
            {'open_interest':1,'day':{'volume':0,'last_updated':1789900000000000001}}])
        self.assertEqual(result['numeric_states']['open_interest'],{'missing':1,'null':1,'zero':1,'invalid_type':1,'positive':1})
        self.assertEqual(result['reported_clocks']['daily_bar']['min_raw_ns'],'1789900000000000001')
        self.assertEqual(result['reported_clocks']['underlying']['present_non_null'],0)
    def test_deadline_does_not_send(self):
        budget={'requests':0,'bytes':0,'max_requests':10,'max_bytes':1000}
        meta,raw=source.request(source.next_url(source.initial_url('SPY'),'SPY'),'secret',1000,time.monotonic()-1,lambda _:self.fail(),budget,Transport([]))
        self.assertEqual(meta['status'],'time_budget');self.assertEqual(budget['requests'],0);self.assertIsNone(raw)
    def test_redirect_is_never_followed(self):
        with self.assertRaises(ValueError):source.NoRedirect().redirect_request(None,None,302,'',None,'https://example.com')
    def test_invalid_ticker_cannot_change_path(self):
        with self.assertRaises(ValueError):source.initial_url('../accounts')
    def test_request_boundary_rejects_unreviewed_credential_destination(self):
        with self.assertRaises(ValueError):source.request('https://example.com/v3/snapshot/options/SPY?cursor=x','secret',1000,time.monotonic()+1,lambda _:self.fail(),{},Transport([]))
    def test_exact_decimals_and_nonfinite_rejection(self):
        self.assertEqual(source.decode(b'{"v":0.1234567890123456789}')['v'],Decimal('0.1234567890123456789'))
        with self.assertRaises(ValueError):source.decode(b'{"v":NaN}')

if __name__=='__main__':unittest.main()
