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
ROOT=Path(__file__).resolve().parents[4];SRC=ROOT/'aws/lambdas/justhodl-microcap-float-squeeze/source';sys.path[:0]=[str(SRC),str(ROOT/'aws/shared')]
import offexchange_measurements as offexchange
from float_observations import *
TODAY='2026-09-27'


def member(ticker='TEST'):return {'source_index':0,'ticker':ticker,'raw':{'symbol':ticker,'cap_bucket':'small','market_cap':500_000_000,'price':10}}
def prices(count=95):return [{'symbol':'TEST','date':(datetime(2026,9,25)-timedelta(days=i)).date().isoformat(),'close':10,'volume':0 if i<30 else 100,'unknown':'retain'} for i in range(count)]
def cnms(stamp='2026-09-25',rows=None):
    rows=rows if rows is not None else [('TEST','1.125','0.125','2.25','Q,N'),('NEXT','0','0','0','B')]
    return ('Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n'+'\n'.join(stamp.replace('-','')+'|'+'|'.join(row) for row in rows)+'\n'+str(len(rows))+'\n').encode()
def captured(value,endpoint='quote',sources=None,kind='json',stamp=TODAY+'T01:00:00Z'):
    sources={} if sources is None else sources;raw=value if isinstance(value,bytes) else json.dumps(value).encode();a=envelope(raw,endpoint,stamp,kind);sources[a['original_ref']['key']]=raw;return a,sources
def finra(stamp='2026-09-25',raw=None,sources=None):
    a,sources=captured(cnms(stamp) if raw is None else raw,'https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+stamp.replace('-','')+'.txt',sources,'txt');a['observation_date']=stamp;return a,sources

class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.previous=b'{"schema_version":1,"all_qualifying":[{"symbol":"OLD"}]}'
        self.data={'data/microcap-float-squeeze.json':self.previous,'data/universe.json':json.dumps({'stocks':[member()['raw'],member()['raw'],None,{'symbol':'NEXT','cap_bucket':'small'}]}).encode()}
        self.reads=[];self.writes=[];self.denied=False;self.corrupt=False;self.race=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=b'corrupt' if self.corrupt and ('/history/' in key or '/sources/' in key) else self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key=='data/microcap-float-squeeze.json' and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=hashlib.sha256(self.data.get(key,b'')).hexdigest():raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None,**extra):
    ns={'S3':memory or Memory(),'BUCKET':'fixture','S3_KEY':'data/microcap-float-squeeze.json','FMP_KEY':'synthetic-fixture-secret',
        'N_WORKERS':4,'MAX_TICKERS':3,'TIMEOUT_BUDGET_S':260,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,
        'ThreadPoolExecutor':ThreadPoolExecutor,'urllib':urllib,'quote_plus':quote_plus,'hashlib':hashlib,'json':json,'time':time,'threading':threading,
        'Path':Path,'__file__':str(SRC/'lambda_function.py'),'offexchange':offexchange,**extra}
    for name in ('CONTRACT','strict','clock','number','symbol','envelope','content','original','source_ref','validate_ref','universe','finra_files','price_evidence','dossier'):ns[name]=globals()[name]
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and (n.name.startswith('_float_') or n.name in ('_FloatSources','lambda_handler'))]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated active microcap functions>','exec'),ns)
    return ns
def company(member,sources):
    rows=prices()
    for row in rows:row['symbol']=member['ticker']
    return [sources.capture(json.dumps(rows).encode(),'historical-price-eod/full')]
def daily(stamp,sources):
    a=sources.capture(cnms(stamp.isoformat()),'https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+stamp.strftime('%Y%m%d')+'.txt','txt');a['observation_date']=stamp.isoformat();return a
def publication(memory=None,**extra):
    m=memory or Memory();ns=native(m,**extra);ns['_float_company']=company;ns['_float_finra_day']=daily;ns['lambda_handler']();return m,strict(m.data['data/microcap-float-squeeze.json'])


class Tests(unittest.TestCase):
    def test_complete_predecessor_retained_without_native_import(self):
        raw=(ROOT/'tests/fixtures/pre-microcap-float-observations.py.txt').read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),'bcbf618e2879b842e9e8d44ad500cb4e4eb6b82595a6f2ce92c31e549b3afb17')
        self.assertTrue((SRC/'lambda_function.py').read_bytes().startswith(raw.replace(b'def lambda_handler(',b'def _legacy_lambda_handler(',1)))
    def test_existing_finra_parser_preserves_fractional_shares_and_inclusive_exempt_scope(self):
        a,sources=finra();flow=finra_files([a],sources,[member(),member('NEXT')],TODAY);r=flow['by_literal_symbol']['TEST'][0]
        self.assertEqual(r['short_volume_shares'],'1.125');self.assertEqual(r['short_exempt_volume_shares'],'0.125');self.assertEqual(r['short_volume_pct'],'50.000000000000')
        self.assertTrue(r['short_includes_exempt']);self.assertEqual(content(a,sources),cnms());self.assertFalse(flow['security_identity_continuity_verified'])
    def test_whole_invalid_files_retained_and_rejected_without_partial_rows(self):
        raw=cnms()
        for bad in [raw.replace(b'20260925',b'20260924'),raw.replace(b'2\n',b'3\n'),cnms(rows=[('TEST','200','0','100','Q')]),cnms(rows=[('TEST','1','2','3','Q')]),cnms(rows=[('TEST','1','0','2','X')]),cnms(rows=[('TEST','1','0','2','Q')]*2)]:
            a,sources=finra(raw=bad);out=finra_files([a],sources,[member()],TODAY)
            self.assertEqual(out['files'][0]['status'],'retained_file_rejected');self.assertEqual(out['by_literal_symbol']['TEST'],[]);self.assertEqual(content(a,sources),bad)
    def test_missing_latest_symbol_and_zero_denominator_never_fall_back_or_become_zero(self):
        a,sources=finra('2026-09-24');b,sources=finra('2026-09-25',cnms(rows=[('NEXT','0','0','0','Q')]),sources)
        flow=finra_files([b,a],sources,[member(),member('NEXT')],TODAY)
        out=dossier(member(),[],sources,flow,TODAY);self.assertIsNone(out['latest_short_sale_volume_pct']);self.assertEqual(len(out['finra_observations']),1)
        out=dossier(member('NEXT'),[],sources,flow,TODAY);self.assertIsNone(out['latest_short_sale_volume_pct'])
    def test_source_path_hash_length_kind_and_future_finra_requests_reject(self):
        a,sources=finra()
        for patcher in [lambda x:x['original_ref'].update(key='private/account.json'),lambda x:x['original_ref'].update(bytes=1),lambda x:x.update(observation_date='2027-01-01'),lambda x:x.update(endpoint='https://other.example/file')]:
            bad=deepcopy(a);patcher(bad)
            with self.assertRaises(ValueError):finra_files([bad],sources,[member()],TODAY)
        with self.assertRaises(ValueError):finra_files([a,a],sources,[member()],TODAY)
    def test_all_price_rows_and_zero_volume_survive_with_explicit_observation_windows(self):
        rows=prices();a,sources=captured(rows,'historical-price-eod/full');out=price_evidence('TEST',a,sources,TODAY)
        self.assertEqual(original(a,sources),rows);self.assertEqual(out['records'],95);self.assertEqual(out['averages']['30']['reported_volume_mean'],0)
        self.assertEqual(out['averages']['60']['reported_volume_mean'],50);self.assertFalse(out['averages']['30']['session_calendar_verified']);self.assertFalse(out['returns_qualified'])
    def test_missing_boolean_duplicate_future_and_foreign_prices_cannot_qualify_window(self):
        for update in [{'volume':None},{'volume':True},{'close':True},{'date':'2027-01-01'},{'symbol':'OTHER'},{'date':None}]:
            rows=prices();rows[0].update(update);a,sources=captured(rows,'historical-price-eod/full')
            self.assertIsNone(price_evidence('TEST',a,sources,TODAY)['averages']['30']['reported_volume_mean'])
        rows=prices();rows[1]['date']=rows[0]['date'];a,sources=captured(rows,'historical-price-eod/full');out=price_evidence('TEST',a,sources,TODAY)
        self.assertIsNone(out['averages']['30']['reported_volume_mean']);self.assertEqual(len(out['duplicate_dates']),1)
    def test_complete_selected_occurrences_do_not_gain_float_short_interest_or_tier(self):
        m,p=publication();self.assertEqual(len(p['request_records']),3);self.assertEqual(p['n_price_records'],285);self.assertEqual(p['n_finra_observations'],60)
        self.assertEqual(p['all_qualifying'],[]);self.assertEqual(p['summary'],{'top_25_overall':[],'tier_s':[]})
        for row in p['request_records']:
            for key in ('float_shares','short_interest_shares','days_to_cover','borrow_rate','score','call'):self.assertIsNone(row[key])
            self.assertFalse(row['sizing_eligible'])
        self.assertEqual(p['source_files']['offexchange_measurements.py']['sha256'],hashlib.sha256(Path(offexchange.__file__).read_bytes()).hexdigest())
    def test_content_archives_keep_exact_sources_and_current_prior_history(self):
        m,p=publication();self.assertEqual(m.data[p['previous_publication']['key']],m.previous)
        acq=p['universe_acquisition'];ref=acq['original_ref'];self.assertEqual(source_ref(m.data[ref['key']],'json'),ref)
        self.assertNotIn('original_base64',json.dumps(p));self.assertGreater(len([k for k in m.data if '/sources/' in k]),20)
        self.assertTrue(all(k.startswith('data/microcap-float-squeeze') or k=='data/universe.json' for k in m.reads+m.writes))
    def test_denied_corrupt_racing_and_all_unavailable_sources_preserve_current(self):
        for mode in ('denied','corrupt','race','failure'):
            m=Memory();setattr(m,mode,True);ns=native(m);ns['_float_company']=company;ns['_float_finra_day']=daily
            if mode=='failure':
                ns['_float_company']=lambda *a:[{'endpoint':'quote','status':'unavailable'}]
                ns['_float_finra_day']=lambda stamp,sources:{'endpoint':'https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+stamp.strftime('%Y%m%d')+'.txt','observation_date':stamp.isoformat(),'status':'unavailable'}
            with self.assertRaises(Exception):ns['lambda_handler']()
            self.assertEqual(m.data['data/microcap-float-squeeze.json'],m.previous)
    def test_original_nominal_acquisition_gate_does_not_claim_currency_or_float(self):
        ns=native();sources=ns['_FloatSources']();seen=[]
        def fetch(ticker,endpoint,source):seen.append(endpoint);return source.capture(json.dumps([{'symbol':'OTHER','currency':'JPY','price':10,'marketCap':500_000_000}]).encode(),endpoint)
        ns['_float_fetch']=fetch;ns['_float_company'](member(),sources);self.assertEqual(seen,['historical-price-eod/full'])
        m=member();m['raw'].pop('price');seen.clear();ns['_float_company'](m,sources);self.assertEqual(seen,['quote','historical-price-eod/full'])
        m['raw'].update(price=0.5);seen.clear();out=ns['_float_company'](m,sources);self.assertEqual(seen,[]);self.assertEqual(out[-1]['status'],'not_requested_original_nominal_gate')
    def test_rate_limit_keeps_unattempted_requests_without_retry(self):
        m=Memory();ns=native(m,N_WORKERS=1);ns['_float_finra_day']=daily;seen=[]
        def limited(member,sources):
            seen.append(member['ticker']);return company(member,sources) if len(seen)==1 else [{'endpoint':'historical-price-eod/full','status':'rate_limited'}]
        ns['_float_company']=limited;ns['lambda_handler']();p=strict(m.data['data/microcap-float-squeeze.json'])
        self.assertEqual(len(seen),2);self.assertEqual(p['request_records'][2]['acquisitions'][0]['status'],'not_attempted_runtime_rate_or_size_limit')
    def test_future_previous_clock_or_wrong_target_is_not_overwritten(self):
        for extra in ({'S3_KEY':'private/account.json'},{}):
            m=Memory();m.data['data/microcap-float-squeeze.json']=json.dumps({'measurement_contract':CONTRACT,'generated_at':'2099-01-01T00:00:00Z'}).encode()
            with self.assertRaises(ValueError):native(m,**extra)['lambda_handler']()
            self.assertEqual(m.writes,[])
    def test_credentials_never_enter_url_or_retained_response_and_redirects_disabled(self):
        ns=native();sources=ns['_FloatSources']();seen=[]
        class Response:
            def __init__(self,raw):self.raw=raw
            def __enter__(self):return self
            def __exit__(self,*a):pass
            def read(self,n):return self.raw[:n]
        def opener(*handlers):
            self.assertIsNone(handlers[0].redirect_request(None,None,302,'',{},'https://other.example'))
            def opened(req,timeout):seen.append(req);return Response(b'[]')
            return SimpleNamespace(open=opened)
        with patch.object(urllib.request,'build_opener',opener):ns['_float_fetch']('TEST','quote',sources)
        self.assertNotIn(ns['FMP_KEY'],seen[0].full_url)
        with patch.object(urllib.request,'build_opener',lambda *a:SimpleNamespace(open=lambda *a,**k:Response(ns['FMP_KEY'].encode()))):self.assertEqual(ns['_float_fetch']('TEST','quote',sources)['status'],'credential_echo_withheld')
        self.assertFalse(any(ns['FMP_KEY'].encode() in raw for raw in sources.raw.values()))

    def test_source_window_reserve_retains_all_launched_responses_and_refuses_overflow(self):
        ns=native();sources=ns['_FloatSources']();sources.bytes=79*1024*1024
        for i in range(8):sources.capture(b' '*(8*1024*1024-1)+str(i).encode(),'quote')
        self.assertEqual(sources.bytes,143*1024*1024);self.assertEqual(len(sources.raw),8)
        sources.bytes=160*1024*1024
        with self.assertRaises(ValueError):sources.capture(b'{"next":1}','quote')
        self.assertEqual(len(sources.raw),8)
    def test_invalid_json_and_truncated_originals_never_become_valid_prices(self):
        for raw in (b'{"x":1,"x":2}',b'[NaN]',b'[{"symbol":"TEST"'):
            a,sources=captured(raw,'historical-price-eod/full');self.assertEqual(a['status'],'invalid_original')
            self.assertIsNone(original(a,sources));self.assertEqual(content(a,sources),raw)
            self.assertEqual(price_evidence('TEST',a,sources,TODAY)['status'],'price_array_unavailable')


if __name__=='__main__':unittest.main(verbosity=2)
