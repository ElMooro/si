"""Isolated actual writer and transport nodes; no live source or native I/O."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from types import SimpleNamespace
from threading import Event
from concurrent.futures import ThreadPoolExecutor,as_completed
from unittest.mock import patch
from copy import deepcopy
import ast,json,sys,time,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-pump-positioning/source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SRC)]
import context_evidence_store as store
import managed_secret
import positioning_observations as m
from test_positioning_observations import captures,AT,ASOF


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        inputs,self.attempts,self.sources=captures()
        self.data={a['source_key']:self.sources[a['original_ref']['key']] for a in inputs.values()}
        self.previous=b'{"synthetic_previous":"whole original"}';self.data[m.HEAD]=self.previous
        self.reads=[];self.writes=[];self.corrupt=False;self.fail_retention=False;self.lost_ack=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(m.PRIVATE):raw+=b'bad'
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(kw)
        if self.fail_retention and key.startswith(m.PRIVATE):raise Error('AccessDenied')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=store.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']
        if self.lost_ack and key==m.HEAD:raise Error('RequestTimeout')


class FixedDateTime(datetime):
    @classmethod
    def now(cls,tz=None):return datetime.fromisoformat(AT.replace('Z','+00:00'))


def native(memory=None):
    mem=memory or Memory();requests=[]
    ns={'Path':Path,'__file__':str(SRC/'lambda_function.py'),'Event':Event,'time':time,
        'datetime':FixedDateTime,'timezone':timezone,'ThreadPoolExecutor':ThreadPoolExecutor,'as_completed':as_completed,
        'boto3':SimpleNamespace(client=lambda *a,**k:mem),'Config':lambda **kw:kw,'S3_BUCKET':'synthetic',
        'observations':m,'context_evidence_store':store,'managed_secret_module':managed_secret,
        'ContextStore':store.ContextStore,'encode':store.encode,'code':store.code,'now':lambda:AT,
        'json':json,'urllib':urllib,'FMP_KEY':'synthetic-only'}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in
           ('lambda_handler','_positioning_request','_PositioningNoRedirect')]
    assert len(nodes)==3
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated positioning writer>','exec'),ns)
    real_request=ns['_positioning_request']
    def synthetic_request(ticker,kind,as_of,started,stop):
        requests.append((ticker,kind,as_of));a=deepcopy(next(a for a in mem.attempts if a['ticker']==ticker and a['kind']==kind))
        return a,mem.sources[a.pop('original_ref')['key']]
    ns['_positioning_request']=synthetic_request
    return ns,mem,requests,real_request


class Response:
    def __init__(self,raw=b'[]',status=200,length=None):
        self.stream=BytesIO(raw);self.status=status;self.headers={'Content-Length':str(len(raw) if length is None else length)};self.closed=False
    def read1(self,n):return self.stream.read(n)
    def close(self):self.closed=True


class Tests(unittest.TestCase):
    def test_complete_writer_replay_original_retention_and_fixed_scope(self):
        ns,mem,requests,_=native();r=ns['lambda_handler']({'head':'private/accounts.json','tickers':['OTHER']},None)
        self.assertEqual(r['statusCode'],200);p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']])
        replay=m.build(manifest['input_attempts'],manifest['provider_attempts'],mem.data,manifest['generated_at'])
        for key,value in replay.items():self.assertEqual(p[key],value)
        self.assertEqual(len(manifest['source_files']),4);self.assertEqual(len(manifest['input_attempts']),6)
        self.assertEqual(sorted(requests),sorted(('SYNA',kind,ASOF) for kind in ('history','quote','profile')))
        self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        for name,ref in manifest['source_files'].items():
            path=SRC/name if name in ('lambda_function.py','positioning_observations.py') else ROOT/'aws/shared'/name
            self.assertEqual(mem.data[ref['key']],path.read_bytes())
        self.assertEqual(mem.data[store.identity(mem.data[m.HEAD],m.PRIVATE,'outputs')['key']],mem.data[m.HEAD])
        self.assertTrue(all(k in (*m.INPUTS.values(),m.HEAD) or k.startswith(m.PRIVATE) for k in mem.reads))
        self.assertEqual(p['call'],'WAIT');self.assertEqual(p['coverage']['eligible_votes'],0)
    def test_prepublication_retention_failures_never_replace_previous(self):
        for setting in ('corrupt','fail_retention'):
            ns,mem,_,_=native();setattr(mem,setting,True);r=ns['lambda_handler']({},None)
            self.assertEqual(r['statusCode'],503);self.assertEqual(mem.data[m.HEAD],mem.previous)
            self.assertIs(json.loads(r['body'])['previous_publication_preserved'],True)
            self.assertFalse(any(w['Key']==m.HEAD for w in mem.writes))
    def test_commit_then_timeout_never_claims_preservation_or_retries(self):
        ns,mem,_,_=native();mem.lost_ack=True;r=ns['lambda_handler']({},None)
        self.assertEqual(r['statusCode'],503);self.assertNotEqual(mem.data[m.HEAD],mem.previous)
        self.assertIsNone(json.loads(r['body'])['previous_publication_preserved'])
        self.assertEqual(json.loads(r['body'])['publication_status'],'acknowledgement_unknown')
        self.assertEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
    def test_missing_primary_and_invalid_population_never_fabricate_empty_selection(self):
        for value in (None,{}, {'pump_candidates':[{'ticker':'../BAD'}]}):
            ns,mem,requests,_=native()
            if value is None:mem.data.pop(m.INPUTS['radar'])
            else:mem.data[m.INPUTS['radar']]=store.encode(value)
            self.assertEqual(ns['lambda_handler']({},None)['statusCode'],503)
            self.assertEqual(mem.data[m.HEAD],mem.previous);self.assertEqual(requests,[])
    def test_explicit_empty_selection_is_research_only_without_provider_requests(self):
        ns,mem,requests,_=native();mem.data[m.INPUTS['radar']]=b'{"pump_candidates":[]}'
        self.assertEqual(ns['lambda_handler']({},None)['statusCode'],200);p=json.loads(mem.data[m.HEAD])
        self.assertEqual(p['selection']['status'],'reported_empty_selection');self.assertEqual(requests,[])
        self.assertIsNone(p['aggressive_basket']['total_exposure'])
    def test_transport_retains_complete_body_and_key_stays_out_of_identity(self):
        _,_,_,request=native();response=Response(b'{"complete":true}');seen=[]
        def open_request(req,timeout):seen.append(req);self.assertEqual(timeout,12);return response
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=open_request)):
            a,raw=request('SYNA','history',ASOF,time.monotonic(),Event())
        self.assertEqual(raw,b'{"complete":true}');self.assertEqual(a['status'],'received');self.assertTrue(response.closed)
        self.assertNotIn('synthetic-only',a['endpoint']);self.assertNotIn('apikey',seen[0].full_url)
        self.assertEqual(seen[0].get_header('Apikey'),'synthetic-only')
    def test_declared_length_truncation_oversize_and_elapsed_transfer_are_unavailable(self):
        for response in (Response(b'[]',length=20),Response(b'[]',length=m.MAX_SOURCE_BYTES+1)):
            _,_,_,request=native()
            with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:response)):
                a,raw=request('SYNA','history',ASOF,time.monotonic(),Event())
            self.assertIsNone(raw);self.assertNotEqual(a['status'],'received');self.assertTrue(response.closed)
        _,_,_,request=native();response=Response()
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:response)),patch.object(time,'monotonic',side_effect=[0,0,21]):
            a,raw=request('SYNA','history',ASOF,0,Event())
        self.assertEqual(a['status'],'transport_unavailable');self.assertIsNone(raw)
    def test_rate_limit_stops_new_requests_without_retry_and_retains_error_body(self):
        _,_,_,request=native();response=Response(b'{"error":"synthetic quota"}',429);stop=Event()
        with patch.object(urllib.request,'build_opener',return_value=SimpleNamespace(open=lambda *a,**k:response)) as opener:
            a,raw=request('SYNA','history',ASOF,time.monotonic(),stop)
            b,empty=request('SYNA','quote',ASOF,time.monotonic(),stop)
        self.assertTrue(stop.is_set());self.assertEqual(a['status'],'http_error');self.assertEqual(raw,b'{"error":"synthetic quota"}')
        self.assertEqual(b['status'],'not_attempted_stop');self.assertFalse(b['network_attempted']);self.assertIsNone(empty);self.assertEqual(opener.call_count,1)
    def test_redirects_refused_before_credentials_can_follow(self):
        ns,_,_,_=native();handler=ns['_PositioningNoRedirect']();req=urllib.request.Request(m.endpoint('SYNA','quote',ASOF))
        with self.assertRaises(urllib.error.HTTPError):handler.redirect_request(req,BytesIO(),302,'redirect',{},'https://example.com/steal')
    def test_original_runtime_and_schedule_are_preserved(self):
        cfg=json.loads((SRC.parent/'config.json').read_bytes());original=json.loads((ROOT/'tests/fixtures/pre-positioning-observations-config.json.txt').read_bytes())
        for key in ('runtime','memory','timeout','handler','role_arn','environment','eventbridge_scheduler'):self.assertEqual(cfg[key],original[key])


if __name__=='__main__':unittest.main(verbosity=2)
