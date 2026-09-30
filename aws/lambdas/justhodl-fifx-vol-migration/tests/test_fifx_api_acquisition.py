from pathlib import Path
from email.message import Message
import io,json,os,sys,types,unittest,urllib.error
from unittest.mock import patch
import run_tests as native
import fifx_acquire as acquisition
KEY='testcredential'+('a'*18)
URL=acquisition.fred.source_url('DGS10','2026-09-30')
class Response(io.BytesIO):
    code=200
    def __init__(self,raw,url):
        super().__init__(raw);self.url=url;self.headers=Message();self.headers['Content-Length']=str(len(raw));self.headers['Content-Type']='application/json';self.headers['Set-Cookie']='do not retain'
    def geturl(self):return self.url

class Tests(unittest.TestCase):
    def setUp(self):acquisition._credential_value=None;acquisition._credential_expires=0
    def test_plan_has_six_public_full_api_requests_and_same_eleven_quote_requests(self):
        plan=acquisition.plan('2026-09-30T21:20:00Z');self.assertEqual(len(plan),17);self.assertNotIn('^MOVE',plan)
        for sid in acquisition.catalog.FRED:acquisition.fred.request_identity(plan[sid],sid,'2026-09-30');self.assertNotIn('api_key',plan[sid])
        self.assertEqual(sum('query1.finance.yahoo.com' in url for url in plan.values()),11)
    def test_existing_key_stays_only_in_wire_request_not_receipt_headers_or_original(self):
        raw=b'{"invented_whole_provider_reply":true}';seen=[]
        class Opener:
            def open(self,request,timeout):seen.append((request.full_url,timeout));return Response(raw,request.full_url)
        with patch.dict(os.environ,{'FRED_API_KEY':KEY},clear=True),patch.object(acquisition.urllib.request,'build_opener',return_value=Opener()):
            body,receipt=acquisition.acquire(URL,timeout=3)
        self.assertEqual(body,raw);self.assertEqual(len(seen),1);self.assertIn('api_key='+KEY,seen[0][0]);self.assertEqual(seen[0][1],3)
        self.assertEqual(receipt['source_url'],URL);self.assertNotIn(KEY,json.dumps(receipt));self.assertNotIn('Set-Cookie',receipt['headers'])
    def test_echoed_credential_cannot_be_retained_even_in_complete_error_body(self):
        class Opener:
            def open(self,request,timeout):return Response(('complete invented error echo '+KEY).encode(),request.full_url)
        with patch.dict(os.environ,{'FRED_API_KEY':KEY},clear=True),patch.object(acquisition.urllib.request,'build_opener',return_value=Opener()):
            with self.assertRaisesRegex(ValueError,'Sensitive provider response'):acquisition.acquire(URL)
    def test_unreviewed_public_parameters_fail_before_credential_resolution(self):
        with patch.object(acquisition,'fred_credential') as secret:
            with self.assertRaises(ValueError):acquisition.acquire(URL+'&api_key=unexpected')
            with self.assertRaises(ValueError):acquisition.acquire(URL.replace('DGS10','NOTREVIEWED'))
            secret.assert_not_called()
    def test_wrong_managed_key_never_reaches_provider(self):
        with patch.dict(os.environ,{'FRED_API_KEY':'bad'},clear=True),patch.object(acquisition.urllib.request,'build_opener') as opener:
            with self.assertRaises(acquisition.CredentialUnavailable):acquisition.acquire(URL)
            opener.assert_not_called()
    def test_existing_ssm_path_has_one_bounded_attempt_and_expires_cached_credentials(self):
        calls=[]
        class Config:
            def __init__(self,**kwargs):self.values=kwargs
        class Client:
            def get_parameter(self,**kwargs):
                calls.append(('parameter',kwargs));return {'Parameter':{'Value':KEY}}
        def client(service,**kwargs):calls.append(('client',service,kwargs));return Client()
        modules={'boto3':types.SimpleNamespace(client=client),'botocore':types.ModuleType('botocore'),'botocore.config':types.SimpleNamespace(Config=Config)}
        with patch.dict(os.environ,{'FRED_API_KEY':''},clear=True),patch.dict(sys.modules,modules),patch.object(acquisition.time,'monotonic',return_value=100):
            self.assertEqual(acquisition.fred_credential(),KEY);self.assertEqual(acquisition.fred_credential(),KEY)
            self.assertEqual(len(calls),2);self.assertEqual(calls[0][1],'ssm');self.assertEqual(calls[0][2]['region_name'],'us-east-1')
            self.assertEqual(calls[0][2]['config'].values,{'connect_timeout':3,'read_timeout':5,'retries':{'total_max_attempts':1}})
            self.assertEqual(calls[1],('parameter',{'Name':'/justhodl/fred/api-key','WithDecryption':True}))
            acquisition._credential_expires=0;self.assertEqual(acquisition.fred_credential(),KEY);self.assertEqual(len(calls),4)
    def test_invalid_parameter_is_not_cached_and_next_ordinary_attempt_can_recover(self):
        values=iter(['bad',KEY]);calls=[]
        class Client:
            def get_parameter(self,**kwargs):
                calls.append(kwargs);return {'Parameter':{'Value':next(values)}}
        modules={'boto3':types.SimpleNamespace(client=lambda *args,**kwargs:Client()),'botocore':types.ModuleType('botocore'),'botocore.config':types.SimpleNamespace(Config=lambda **kwargs:kwargs)}
        with patch.dict(os.environ,{},clear=True),patch.dict(sys.modules,modules),patch.object(acquisition.time,'monotonic',return_value=100):
            with self.assertRaises(acquisition.CredentialUnavailable):acquisition.fred_credential()
            self.assertIsNone(acquisition._credential_value);self.assertEqual(acquisition._credential_expires,0)
            self.assertEqual(len(calls),1)
            self.assertEqual(acquisition.fred_credential(),KEY);self.assertEqual(acquisition.fred_credential(),KEY)
            self.assertEqual(len(calls),2)
    def test_ssm_failure_cannot_expose_exception_text_or_open_provider_transport(self):
        def client(*args,**kwargs):raise RuntimeError('invented sensitive exception '+KEY)
        modules={'boto3':types.SimpleNamespace(client=client),'botocore':types.ModuleType('botocore'),'botocore.config':types.SimpleNamespace(Config=lambda **kwargs:kwargs)}
        with patch.dict(os.environ,{},clear=True),patch.dict(sys.modules,modules),patch.object(acquisition.urllib.request,'build_opener') as opener:
            with self.assertRaises(acquisition.CredentialUnavailable) as raised:acquisition.acquire(URL)
            self.assertNotIn(KEY,str(raised.exception));opener.assert_not_called()
    def test_original_csv_and_quote_replay_routes_do_not_read_a_credential(self):
        class Opener:
            def open(self,request,timeout):return Response(b'whole invented original',request.full_url)
        with patch.object(acquisition,'fred_credential') as secret,patch.object(acquisition.urllib.request,'build_opener',return_value=Opener()):
            for url in ('https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10','https://query1.finance.yahoo.com/v8/finance/chart/%5EKS11'):
                body,receipt=acquisition.acquire(url);self.assertEqual(receipt['source_url'],url)
            secret.assert_not_called()

if __name__=='__main__':unittest.main(verbosity=2)
