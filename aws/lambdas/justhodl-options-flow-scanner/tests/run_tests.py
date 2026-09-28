"""Synthetic-only whole-source, flow interpretation and publication regressions."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import quote_plus
from copy import deepcopy
import ast,hashlib,json,sys,time,threading,unittest,urllib.request,urllib.error
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-options-flow-scanner/source';sys.path[:0]=[str(SRC),str(ROOT/'aws/shared')]
import offexchange_measurements as offexchange
from flow_observations import *
TODAY='2026-09-27'


def member(ticker='TEST'):return {'source_index':0,'ticker':ticker,'raw':{'symbol':ticker}}
def captured(value,url,sources=None,kind='json',status=200):
    sources={} if sources is None else sources;raw=value if isinstance(value,bytes) else json.dumps(value).encode()
    a=envelope(raw,url,TODAY+'T01:00:00Z',kind);a['http_status']=status;sources[a['original_ref']['key']]=raw;return a,sources

def contract(kind='call',ticker='TEST'):
    return {'ticker':'O:'+ticker+'261120'+('C' if kind=='call' else 'P')+'00010000','underlying_ticker':ticker,'contract_type':kind,'strike_price':10,'expiration_date':'2026-11-20','shares_per_contract':100,'exercise_style':'american'}
def cnms(stamp):return ('Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n'+stamp.replace('-','')+'|TEST|1.125|0.125|2.25|Q,N\n1\n').encode()
def bars(kind='call',volume=10):
    t=int(datetime(2026,9,25,4,tzinfo=timezone.utc).timestamp()*1000)
    return {'status':'OK','ticker':contract(kind)['ticker'],'results':[{'t':t,'v':volume,'c':1.25,'unknown':'retain'}]}

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"schema_version":1,"all_qualifying":[{"symbol":"OLD"}]}'
        self.data={'data/options-flow-scanner.json':self.previous,'data/universe.json':json.dumps({'stocks':[member()['raw'],member()['raw'],None,{'symbol':'NEXT','cap_bucket':'small'}]}).encode()}
        self.reads=[];self.writes=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'corrupt' if self.corrupt and ('/history/' in key or '/sources/' in key) else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/options-flow-scanner.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    ns={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':'data/options-flow-scanner.json','POLY_KEY':'synthetic-fixture-secret','DAYS_BACK':20,'managed_secret':lambda *a:'synthetic-fixture-secret',
        'N_WORKERS':4,'MAX_TICKERS':3,'TIMEOUT_BUDGET_S':260,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,
        'ThreadPoolExecutor':ThreadPoolExecutor,'urllib':urllib,'quote_plus':quote_plus,'hashlib':hashlib,'json':json,'time':time,'threading':threading,
        'Path':Path,'__file__':str(SRC/'lambda_function.py'),'offexchange':offexchange,**extra}
    for name in ('CONTRACT','strict','clock','number','symbol','envelope','content','original','source_ref','validate_ref','universe','finra_files','dossier','spot','initial_url','cursor_url','provider_rows','contracts','bars_url'):ns[name]=globals()[name]
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_option_') or n.name in ('_OptionSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated active microcap functions>','exec'),ns)
    return ns

def fake_fetch(url,sources,remaining,kind='json'):
    if kind=='txt':raw=cnms(url[-12:-4][:4]+'-'+url[-12:-4][4:6]+'-'+url[-12:-4][6:8])
    elif '/stable/quote?' in url:raw=json.dumps([{'symbol':'TEST' if 'TEST' in url else 'NEXT','price':10}]).encode()
    elif '/reference/options/contracts?' in url:raw=json.dumps({'status':'OK','results':[contract(),contract('put')]}).encode()
    else:raw=json.dumps(bars('put' if '261120P' in url else 'call')).encode()
    a=sources.capture(raw,url,kind);a['http_status']=200;return a

def publication(memory=None,**extra):
    m=memory or Memory();ns=native(m,**extra);ns['_option_fetch']=fake_fetch;ns['lambda_handler']();return m,strict(m.data['data/options-flow-scanner.json'])


class Tests(unittest.TestCase):
    def test_complete_predecessor_retained(self):
        raw=(ROOT/'tests/fixtures/pre-options-flow-observations.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'e5f75781e53c644863a8be172a89980f1056cb89683b824bb06b8fce1b2d1d33')
        self.assertTrue((SRC/'lambda_function.py').read_bytes().startswith(raw.replace(b'def lambda_handler(',b'def _legacy_lambda_handler(',1)))
    def test_correct_expiry_query_and_safe_cursor(self):
        url=initial_url('TEST',10,TODAY);self.assertIn('expiration_date.gte=2026-10-11',url);self.assertIn('expiration_date.lte=2026-12-26',url)
        for bad in ['https://evil.example/v3/reference/options/contracts?cursor=x','https://api.polygon.io@evil.example/v3/reference/options/contracts?cursor=x','https://api.polygon.io/v3/reference/options/contracts?cursor=x&cursor=y','https://api.polygon.io/v3/reference/options/contracts?cursor=x&underlying_ticker=OTHER']:
            with self.assertRaises(ValueError):cursor_url(bad)
        self.assertNotIn('secret',cursor_url('https://api.polygon.io/v3/reference/options/contracts?cursor=x&apiKey=secret'))
    def test_pagination_and_full_contract_populations(self):
        url=initial_url('TEST',10,TODAY);next_url='https://api.polygon.io/v3/reference/options/contracts?cursor=two'
        a,s=captured({'status':'OK','results':[contract()],'next_url':next_url},url)
        b,s=captured({'status':'OK','results':[contract('put')]},next_url,s)
        p=contracts('TEST',10,[a,b],s,TODAY);self.assertTrue(p['pagination_complete']);self.assertEqual(len(p['records']),2)
        self.assertFalse(contracts('TEST',10,[a],s,TODAY)['pagination_complete'])
        with self.assertRaises(ValueError):contracts('TEST',10,[b,a],s,TODAY)
    def test_duplicates_mismatch_nonstandard_and_out_of_window_preserved_not_selected(self):
        rows=[contract(),contract(),contract(ticker='OTHER'),{**contract(),'expiration_date':'2030-01-01'},{**contract(),'shares_per_contract':True},{**contract(),'strike_price':True}]
        a,s=captured({'status':'OK','results':rows},initial_url('TEST',10,TODAY));p=contracts('TEST',10,[a],s,TODAY)
        self.assertEqual(len(p['records']),6);self.assertEqual(p['selected_record_indices'],[])
    def test_future_missing_boolean_and_duplicate_bar_volume_remains_unqualified(self):
        row=contract();row['contract_id']=row['ticker'];url=bars_url(row['ticker'],TODAY,20)
        for mutate in [lambda p:p.update(ticker='OTHER'),lambda p:p['results'][0].update(v=True),lambda p:p['results'][0].update(t=1893456000000),lambda p:p['results'][0].pop('v'),lambda p:p['results'].append(dict(p['results'][0])),lambda p:p.update(next_url='https://api.polygon.io/unknown')]:
            value=bars();mutate(value);a,s=captured(value,url);p=bar_records(row,a,s,TODAY,20)
            self.assertTrue(all(r['issues'] for r in p['records']));self.assertEqual(original(a,s),value)
    def test_denied_and_mismatched_quotes_are_not_spot_gates(self):
        for value,status in [([{'symbol':'OTHER','price':10}],200),([{'symbol':'TEST','price':True}],200),([{'symbol':'TEST','price':10}],403)]:
            a,s=captured(value,'quote',status=status);self.assertIsNone(spot(a,s,'TEST'))
    def test_denied_contract_body_is_retained_as_failure(self):
        a,s=captured({'status':'NOT_AUTHORIZED','results':[contract()]},initial_url('TEST',10,TODAY),status=403)
        p=contracts('TEST',10,[a],s,TODAY);self.assertEqual(p['records'],[]);self.assertFalse(p['pagination_complete']);self.assertIsNotNone(content(a,s))
    def test_exact_fractional_finra_and_bad_file_or_http_rejection(self):
        url='https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260925.txt';a,s=captured(cnms('2026-09-25'),url,kind='txt');a['observation_date']='2026-09-25'
        p=finra_files([a],s,[member()],TODAY);row=p['by_literal_symbol']['TEST'][0];self.assertEqual(row['short_volume_pct'],'50.000000000000');self.assertEqual(row['short_exempt_volume_shares'],'0.125')
        a['http_status']=403;self.assertEqual(finra_files([a],s,[member()],TODAY)['by_literal_symbol']['TEST'],[])
    def test_full_publication_preserves_population_and_abstains(self):
        m,p=publication();self.assertEqual(len(p['request_records']),3);self.assertEqual(p['all_qualifying'],[])
        self.assertEqual(p['summary'],{'top_25_overall':[],'tier_a':[]});self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible'])
        self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        self.assertTrue(all(k.startswith('data/options-flow-scanner') or k=='data/universe.json' for k in m.reads+m.writes))
        self.assertGreater(len(p['request_records'][0]['finra_observations']),0)
    def test_ratios_require_all_selected_same_date_rows_and_zero_puts_not_invented(self):
        # Frozen check date keeps the synthetic bar in the explicit window.
        s={};a,s=captured([{'symbol':'TEST','price':10}],'quote',s);cp,s=captured({'status':'OK','results':[contract(),contract('put')]},initial_url('TEST',10,TODAY),s)
        group={'quote':a,'contract_pages':[cp],'bar_requests':[]};flow={'by_literal_symbol':{}}
        for kind in ('call','put'):
            b,s=captured(bars(kind,0 if kind=='call' else 10),bars_url(contract(kind)['ticker'],TODAY,20),s);group['bar_requests'].append(b)
        p=dossier(member(),group,s,flow,TODAY,20);self.assertEqual(p['daily_observations'][0]['call_put_volume_ratio'],'0.000000000000')
        missing=deepcopy(group);missing['bar_requests'][1]={'endpoint':bars_url(contract('put')['ticker'],TODAY,20),'status':'not_attempted'}
        self.assertIsNone(dossier(member(),missing,s,flow,TODAY,20)['daily_observations'][0]['call_put_volume_ratio'])
        b,s=captured(bars('put',0),bars_url(contract('put')['ticker'],TODAY,20),s);group['bar_requests'][1]=b
        self.assertIsNone(dossier(member(),group,s,flow,TODAY,20)['daily_observations'][0]['call_put_volume_ratio'])
    def test_denied_corrupt_racing_or_unavailable_preserves_current(self):
        for mode in ('denied','corrupt','race','failure'):
            m=Memory();setattr(m,mode,True);ns=native(m);ns['_option_fetch']=fake_fetch
            if mode=='failure':ns['_option_fetch']=lambda url,*a,**k:{'endpoint':url,'status':'unavailable'}
            with self.assertRaises(Exception):ns['lambda_handler']()
            self.assertEqual(m.data['data/options-flow-scanner.json'],m.previous)
    def test_readback_corruption_and_modified_source_ref_reject(self):
        a,s=captured({'value':1},'quote');s[a['original_ref']['key']]=b'{}'
        with self.assertRaises(ValueError):original(a,s)
        a['original_ref']['key']='private/account.json'
        with self.assertRaises(ValueError):original(a,s)
    def test_fetch_retains_error_body_and_stops_after_429_without_redirect_or_retry(self):
        ns=native();sources=ns['_OptionSources']();raw=b'{"status":"NOT_AUTHORIZED"}'
        response=SimpleNamespace()
        class Response(BytesIO):
            status=429
        with patch('urllib.request.build_opener',return_value=SimpleNamespace(open=lambda *a,**k:Response(raw))):
            a=ns['_option_fetch'](initial_url('TEST',10,TODAY),sources,lambda:200);self.assertEqual(a['http_status'],429);self.assertEqual(content(a,sources.raw),raw)
            self.assertEqual(ns['_option_fetch'](initial_url('TEST',10,TODAY),sources,lambda:200)['status'],'not_attempted_runtime_rate_or_size_limit')
    def test_secret_echo_is_never_retained(self):
        ns=native();sources=ns['_OptionSources']()
        class Response(BytesIO):status=200
        with patch('urllib.request.build_opener',return_value=SimpleNamespace(open=lambda *a,**k:Response(b'synthetic-fixture-secret'))):
            a=ns['_option_fetch'](initial_url('TEST',10,TODAY),sources,lambda:200);self.assertEqual(a['status'],'credential_echo_withheld');self.assertEqual(sources.raw,{})
        with patch('urllib.request.build_opener',return_value=SimpleNamespace(open=lambda *a,**k:Response(b'{"url":"\\u0073ynthetic-fixture-secret"}'))):
            a=ns['_option_fetch'](initial_url('TEST',10,TODAY),sources,lambda:200);self.assertEqual(a['status'],'credential_echo_withheld');self.assertEqual(sources.raw,{})
    def test_original_decimal_precision_cannot_round_fractional_volume_or_identity_into_validity(self):
        row=contract();row['contract_id']=row['ticker'];url=bars_url(row['ticker'],TODAY,20)
        for token,valid in [('10.0',True),('10.0000000000000001',False),('9007199254740991.1',False)]:
            raw=json.dumps(bars()).encode().replace(b'"v": 10',b'"v": '+token.encode());a,s=captured(raw,url);p=bar_records(row,a,s,TODAY,20)
            self.assertEqual(p['records'][0]['reported_volume_contracts'],'10' if valid else None)
        for field in ('strike_price','shares_per_contract'):
            raw=json.dumps({'status':'OK','results':[contract()]}).encode();needle=('"'+field+'": '+('10' if field=='strike_price' else '100')).encode();raw=raw.replace(needle,needle+b'.0000000000000001')
            a,s=captured(raw,initial_url('TEST',10,TODAY));p=contracts('TEST',10,[a],s,TODAY);self.assertEqual(p['selected_record_indices'],[])
    def test_canonical_ownership_under_obsolete_environment(self):
        with patch.dict(__import__('os').environ,{'S3_KEY':'data/options-flow.json'}):m,p=publication()
        self.assertIn('data/options-flow-scanner.json',m.writes);self.assertNotIn('data/options-flow.json',m.writes)
        tree=ast.parse((SRC/'lambda_function.py').read_bytes());assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='S3_KEY' for t in n.targets))
        self.assertEqual(ast.literal_eval(assignment.value),'data/options-flow-scanner.json')

if __name__=='__main__':
    sys.path.insert(0,str(ROOT/'tests'))
    from test_engine_output_ownership import OutputOwnershipTests
    suite=unittest.TestLoader().loadTestsFromTestCase(Tests)
    suite.addTest(OutputOwnershipTests('test_source_keys_and_dedicated_pages_match_each_schema'))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
