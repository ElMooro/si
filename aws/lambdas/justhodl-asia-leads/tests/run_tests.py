"""Complete native entry, exact export calendars, original retention and replay."""
from pathlib import Path
from copy import deepcopy
from io import BytesIO
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
import ast,importlib.util,json,sys,unittest,urllib.parse,urllib.error,ssl
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE));import asia_store as store;import asia_measurements as model
AT='2026-09-28T10:20:00+00:00'


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.data={store.HEAD:store.encode({'generated_at':'2026-09-27T10:20:38Z','version':'legacy','unknown':[0,False,None],
                  'korea_exports':{'old_nested':42},'methodology':{'old_method':'kept'}}),
                  store.KEYS[1]:b'{"prior_tape":true}',store.KEYS[2]:store.encode({'releases':{str(i):{'old':i} for i in range(55)}}),
                  store.KEYS[3]:b'{"levels":{"2020-01":0,"2025-08":65},"unknown":false}'}
        self.reads=[];self.writes=[];self.denied=set();self.truncated=set();self.race=None;self.corrupt=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key in self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(store.PRIVATE):raw+=b'!'
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(key in self.truncated),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        if self.race:self.race(key)
        if key in self.denied:raise Error('AccessDenied')
        old=self.data.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or store.sha(old)!=kw['IfMatch']):raise Error('PreconditionFailed')
        self.data[key]=raw;self.writes.append(key)


class Response(BytesIO):
    def __init__(self,raw,url,status=200):
        super().__init__(raw);self.url=url;self.status=status;self.headers={'Content-Length':str(len(raw))}
    def getcode(self):return self.status
    def geturl(self):return self.url


def packets(sid):
    unit=model.PROFILES[sid][1]
    meta={'seriess':[{'id':sid,'units':unit,'frequency_short':'M','seasonal_adjustment_short':'NSA'}]}
    rows=[{'date':model.shift('1985-01',i)+'-01','value':str(10000+i),'realtime_start':AT[:10],'realtime_end':AT[:10]} for i in range(500)]
    return meta,{'count':len(rows),'offset':0,'units':'lin','output_type':1,'observations':rows}


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fake=ModuleType('boto3');fake.client=lambda *a,**kw:None
        secrets=ModuleType('managed_secret');secrets.managed_secret=lambda *a,**kw:'fixture-only-secret'
        spec=importlib.util.spec_from_file_location('asia_native_test',SOURCE/'lambda_function.py')
        cls.module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=cls.module
        with patch.dict(sys.modules,{'boto3':fake,'managed_secret':secrets}):spec.loader.exec_module(cls.module)
    def source(self,req,timeout=None):
        url=req if isinstance(req,str) else req.full_url;self.calls.append(url)
        p=urllib.parse.urlsplit(url);q=dict(urllib.parse.parse_qsl(p.query))
        if p.netloc=='api.stlouisfed.org':
            meta,obs=packets(q['series_id']);raw=store.encode(meta if p.path.endswith('/series') else obs)
        elif p.netloc=='news.google.com':
            raw=('<rss><channel>'+''.join('<item><title>South Korea exports first 20 days rose 3 percent August 1-20</title><link>https://example.invalid/report</link><pubDate>Fri, 21 Aug 2026 09:00:00 GMT</pubDate></item>' for _ in range(60))+'</channel></rss>').encode()
        elif p.netloc=='newsapi.org':raw=b'{"status":"ok","articles":[]}'
        elif p.netloc=='financialmodelingprep.com':raw=b'[]'
        else:
            target=urllib.parse.urlsplit(q['u']) if 'u' in q else p
            if target.netloc=='www.customs.go.kr':
                raw=('<a href="/kcs/report.do">2026 September 수출입 현황</a>' if 'selectNttList' in target.path else '수출 100 억 달러 (3 %) 반도체 4 %').encode()+b' '*401
            elif target.netloc=='eng.stat.gov.tw':
                raw=(b'<a href="/orders.aspx">Export Orders</a>' if target.path=='/Point.aspx' else b'<p>Export Orders US$ 73 billion increase by 4 % August 2026</p>')+b' '*410000
            else:raise AssertionError('Undeclared fixture host')
        return Response(raw,url)
    def execute(self,m=None,opener=None,at=AT):
        m=m or Memory();self.calls=[];self.module.s3=m;self.module.FRED='fixture-only-secret';self.module.NEWSAPI_KEY='fixture-news';self.module.FMP_KEY='fixture-fmp'
        result=store.run(self.module,at=at,opener=opener or self.source)
        return m,store.strict(m.data[store.HEAD]),result
    def test_original_functions_preserved_except_reviewed_clock_transport_bindings(self):
        raw=(ROOT/'tests/fixtures/pre-asia-research.py.txt').read_bytes()
        self.assertEqual(store.sha(raw),'1a2b4500ee50c13c3889bfe7f23487de6231015f3372830833a9420169897dfb')
        before={n.name:n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef)}
        after={n.name:n for n in ast.parse((SOURCE/'lambda_function.py').read_bytes()).body if isinstance(n,ast.FunctionDef)}
        for name,node in before.items():
            if name in ('_edge','korea_flash','korea_flash_tape'):continue
            other=deepcopy(after['_legacy_lambda_handler' if name=='lambda_handler' else name]);other.name=name
            self.assertEqual(ast.dump(node),ast.dump(other),name)
        self.assertNotIn('CERT_NONE',(SOURCE/'lambda_function.py').read_text(encoding='utf-8'))
        with patch.object(store,'run',return_value={'entry':True}) as run:
            self.assertEqual(self.module.lambda_handler({'check':1},None),{'entry':True});self.assertIs(run.call_args.args[0],self.module)
    def test_whole_native_entry_caches_all_responses_1000_observations_and_replay(self):
        m,p,out=self.execute();self.assertTrue(out['published']);self.assertFalse(p['calls_eligible'])
        self.assertEqual(p['unknown'],[0,False,None]);self.assertEqual(p['korea_exports']['old_nested'],42);self.assertEqual(p['methodology']['old_method'],'kept')
        self.assertEqual(len(store.strict(m.data[store.KEYS[2]])['releases']),56)
        self.assertEqual(store.strict(m.data[store.KEYS[3]])['levels']['2020-01'],0)
        self.assertEqual(p['korea_exports']['unit'],'US dollars, exchange rate converted');self.assertEqual(p['taiwan_exports']['unit'],'Millions of Dollars')
        self.assertEqual(sum(r['returned_rows'] for r in p['measurement_review']['series'].values()),1000)
        plan=store.strict(store.retained(m,self.module.BUCKET,p['publication_context']['manifest']))
        self.assertEqual(len(plan['native_outputs']),4);self.assertEqual(plan['credential_presence'],[True]*3)
        self.assertTrue(any(r.get('original',{}).get('bytes',0)>410000 for r in plan['http_attempts']))
        for row in plan['http_attempts']:self.assertNotIn('fixture-',str(row))
        before=len(self.calls);replay=store.replay(self.module,m,self.module.BUCKET,p)
        self.assertEqual(len(self.calls),before);self.assertEqual(replay['export_observations'],1000);self.assertEqual(replay['provider_requests'],0)
        self.assertEqual(m.writes[-1],store.HEAD);self.assertFalse(p['publication_context']['multiple_head_atomic'])
    def test_exact_calendar_holes_missing_zero_and_numeric_grammar(self):
        sid='XTEXVA01KRM667N';meta,obs=packets(sid);r=model.fred(sid,meta,obs,AT)
        self.assertEqual(r['latest_month'],'2026-08');self.assertEqual(r['yoy']['previous_month'],'2025-08')
        self.assertAlmostEqual(r['yoy']['percent'],(10499/10487-1)*100)
        self.assertIsNone(r['annualized_three_month_percent'])
        for case in ('hole','null','zero','latest'):
            v=deepcopy(obs)
            if case=='hole':del v['observations'][-13];v['count']-=1
            elif case=='null':v['observations'][-13]['value']='.'
            elif case=='zero':v['observations'][-13]['value']='0'
            else:v['observations'][-1]['value']='.'
            q=model.fred(sid,meta,v,AT);self.assertIsNone(q['yoy']['percent'])
        for value in ('1_000','NaN','1e999','-1','1e-9999',True):
            with self.assertRaises(model.MeasurementError):model.number(value)
    def test_definitions_counts_duplicates_future_and_transforms_are_not_measurements(self):
        sid='VALEXPTWM052N';meta,obs=packets(sid)
        for case in ('unit','frequency','count','offset','units','duplicate','future'):
            mm=deepcopy(meta);oo=deepcopy(obs)
            if case=='unit':mm['seriess'][0]['units']='Dollars'
            elif case=='frequency':mm['seriess'][0]['frequency_short']='Q'
            elif case in ('count','offset'):oo[case]+=1
            elif case=='units':oo['units']='pch'
            elif case=='duplicate':oo['observations'][-1]=oo['observations'][-2]
            else:oo['observations'][-1]['date']='2099-01-01'
            self.assertNotEqual(model.fred(sid,mm,oo,AT)['status'],'measured')
    def test_denied_truncated_corrupt_or_newer_predecessor_prevents_provider_and_writes(self):
        for case in ('denied','truncated','corrupt','newer'):
            m=Memory();before=deepcopy(m.data)
            if case=='denied':m.denied.add(store.HEAD)
            elif case=='truncated':m.truncated.add(store.KEYS[3])
            elif case=='corrupt':m.corrupt=True
            else:m.data[store.HEAD]=store.encode({'generated_at':'2099-01-01T00:00:00Z'});before=deepcopy(m.data)
            with self.assertRaises(Exception):self.execute(m)
            self.assertEqual(self.calls,[]);self.assertEqual({k:m.data[k] for k in before},before)
    def test_partial_response_and_redirect_preserve_all_prior_heads(self):
        for case in ('length','redirect','retention'):
            m=Memory();before=deepcopy(m.data)
            def broken(req,timeout=None):
                response=self.source(req,timeout)
                if case=='length':response.headers['Content-Length']=str(int(response.headers['Content-Length'])+1)
                elif case=='redirect':response.url='https://foreign.invalid'
                else:m.corrupt=True
                return response
            with self.assertRaises(store.EvidenceError):self.execute(m,broken)
            self.assertEqual({k:m.data[k] for k in before},before)
    def test_government_429_is_not_retried_through_direct_route(self):
        attempts=[]
        def limited(req,timeout=None):
            url=req if isinstance(req,str) else req.full_url
            if 'eng.stat.gov.tw' in urllib.parse.unquote(url):attempts.append(url);return Response(b'limited',url,429)
            return self.source(req,timeout)
        m,p,_=self.execute(opener=limited);self.assertEqual(len(attempts),1)
        self.assertEqual(p['taiwan_orders']['research_status'],'retained_legacy_extraction_unqualified')
        self.assertEqual(store.replay(self.module,m,self.module.BUCKET,p)['provider_requests'],0)
    def test_transport_failure_replays_and_does_not_fake_monthly_values(self):
        def timeout(req,timeout=None):
            url=req if isinstance(req,str) else req.full_url
            if 'api.stlouisfed.org' in url:raise TimeoutError('fixture')
            return self.source(req,timeout)
        m,p,_=self.execute(opener=timeout);self.assertEqual(p['quality']['status'],'partial_measurements')
        self.assertIsNone(p['korea_exports']['yoy_pct']);self.assertIsNone(p['taiwan_exports']['last_value'])
        self.assertEqual(store.replay(self.module,m,self.module.BUCKET,p)['provider_requests'],0)
    def test_unknown_source_or_native_read_fails_closed(self):
        for url in ('http://eng.stat.gov.tw/a','https://169.254.169.254/latest','https://eng.stat.gov.tw.evil.invalid/a',
                    'https://justhodl-data-proxy.raafouis.workers.dev/gov?u=https%3A%2F%2Fprivate.invalid',
                    'https://api.stlouisfed.org/fred/series?series_id=OTHER&file_type=json'):
            with self.assertRaises(store.EvidenceError):store.identity(url)
        s=store.Session(Memory(),'bucket',AT,{})
        with self.assertRaises(store.EvidenceError):s.get_object(Bucket='bucket',Key='private/account')
    def test_tampered_packet_original_or_compiler_cannot_replay(self):
        m,p,_=self.execute()
        bad=deepcopy(p);bad['korea_exports']['yoy_pct']=99
        with self.assertRaises(store.EvidenceError):store.replay(self.module,m,self.module.BUCKET,bad)
        m.corrupt=True
        with self.assertRaises(store.EvidenceError):store.replay(self.module,m,self.module.BUCKET,p)
    def test_foreign_writer_is_preserved_without_rollback(self):
        m=Memory();foreign=b'{"generated_at":"2026-09-28T10:21:00Z","foreign":true}'
        def race(key):
            if key==store.HEAD:m.data[key]=foreign
        m.race=race
        with self.assertRaises(Error):self.execute(m)
        self.assertEqual(m.data[store.HEAD],foreign)
    def test_repeated_publication_keeps_old_nested_fields_and_all_months(self):
        m,p,_=self.execute();first=m.data[store.HEAD]
        m,q,_=self.execute(m,at='2026-09-29T10:20:00+00:00')
        plan=store.strict(store.retained(m,self.module.BUCKET,q['publication_context']['manifest']))
        self.assertEqual(store.retained(m,self.module.BUCKET,plan['predecessors'][store.HEAD]['original']),first)
        self.assertEqual(q['korea_exports']['old_nested'],42);self.assertEqual(q['korea_exports']['n_obs'],500)
        self.assertEqual(store.replay(self.module,m,self.module.BUCKET,q)['provider_requests'],0)
    def test_http_boundary_makes_no_aws_or_provider_request(self):
        m=Memory();self.module.s3=m
        self.assertEqual(self.module.lambda_handler({'httpMethod':'GET'},None)['statusCode'],409)
        self.assertEqual(m.reads,[]);self.assertEqual(m.writes,[])
    def test_capture_budget_and_malformed_cache_cannot_create_false_complete_data(self):
        m=Memory();self.module.s3=m;self.calls=[]
        with self.assertRaises(store.EvidenceError):
            store.run(self.module,context=SimpleNamespace(get_remaining_time_in_millis=lambda:20000),at=AT,opener=self.source)
        self.assertEqual(self.calls,[]);self.assertEqual(m.writes,[])
        session=store.Session(m,self.module.BUCKET,AT,{},self.source);session.started-=211
        with self.assertRaises(store.EvidenceError):session.urlopen('https://news.google.com/rss/search?q=x&hl=en-US&gl=US&ceid=US:en',timeout=10)
        self.assertEqual(self.calls,[]);self.assertEqual(session.http[0]['status'],'budget_not_attempted')
        m=Memory();m.data[store.KEYS[2]]=b'{malformed';before=deepcopy(m.data)
        with self.assertRaises(Exception):self.execute(m)
        self.assertEqual({k:m.data[k] for k in before},before)
    def test_sparse_calendar_keeps_raw_missing_rows_and_foreign_month_is_not_shifted(self):
        sid='VALEXPTWM052N';meta,obs=packets(sid);obs['observations'][-4]['value']='.'
        out=model.fred(sid,meta,obs,AT)
        self.assertEqual(out['returned_rows'],500);self.assertEqual(len(out['observations']),500)
        self.assertEqual(out['observations'][-4]['original']['value'],'.');self.assertIsNone(out['three_month']['percent'])
        self.assertEqual(out['three_month']['previous_month'],'2026-05')


if __name__=='__main__':unittest.main()
