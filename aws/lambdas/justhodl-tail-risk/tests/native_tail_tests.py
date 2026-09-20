"""Exercise source identities, clocks, pagination, replay and publication boundaries."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,io,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
import tail_research_model as model
import tail_research_store as store
STAMP='2026-09-20T21:20:00+00:00'
NS=1789761600000000000 # Friday September 18 20:00 UTC
class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.writes=[];self.reads=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)

def row(symbol='SPY',kind='put',delta=-.25,strike=100,expiry='2026-10-16',iv=.20,quotes=True):
    d={'details':{'ticker':'O:'+symbol+expiry[2:].replace('-','')+('P' if kind=='put' else 'C')+f'{round(strike*1000):08d}',
        'contract_type':kind,'strike_price':strike,'expiration_date':expiry,'exercise_style':'american','shares_per_contract':100},
        'underlying_asset':{'ticker':symbol},'greeks':{'delta':delta},'implied_volatility':iv,'open_interest':0}
    if quotes:
        d['last_quote']={'bid':1,'ask':1.2,'bid_size':2,'ask_size':3,'last_updated':NS,'timeframe':'DELAYED'}
        d['underlying_asset'].update(price=105,last_updated=NS,timeframe='DELAYED')
    return d
def source_rows(symbol):
    return [row(symbol,kind,delta,90+i*5,expiry,.30 if kind=='put' else .20) for expiry in ('2026-10-16','2026-11-20')
        for i,(kind,delta,_) in enumerate(model.SLOTS.values())]
def fixture():
    s=Storage();chains={}
    for symbol in model.SYMBOLS:
        raw=model.encoded({'status':'OK','results':source_rows(symbol)})
        chains[symbol]={'pages':[{'page':1,'request_identity_sha256':model.sha(model.initial_url(symbol,STAMP).encode()),'acquired_at':STAMP,
            'status':'received','http_status':200,'original':store.original(s,'b',raw)}],'stop':'complete'}
    contexts={k:{'status':'retained_unqualified_context','measurement_eligible':False,'original':store.original(s,'b',model.encoded({'old_context':'RETAIN_ONLY','all_rows':[1,2]}))} for k in store.CONTEXT_KEYS}
    i={'contract':'tail-native-inputs.v1','started_at':STAMP,'generated_at':STAMP,'collection':{'chains':chains,'context_evidence':contexts,
        'source_bytes':sum(c['pages'][0]['original']['bytes'] for c in chains.values())}}
    return s,i
def changed(s,i,mutate,symbol='SPY'):
    page=i['collection']['chains'][symbol]['pages'][0];doc=model.decode(s.objects[page['original']['key']]);mutate(doc)
    page['original']=store.original(s,'b',model.encoded(doc));i['collection']['source_bytes']=sum(p['original']['bytes'] for c in i['collection']['chains'].values() for p in c['pages'] if p.get('original'))
def packet():
    s,i=fixture();o=store.compile_output(i,store.reader(s,'b'));return s,i,{**o,'replay':store.retain(s,'b',i,o)}

class Native(unittest.TestCase):
    def test_actual_expiries_and_vendor_model_comparisons_without_probability(self):
        _,_,p=packet();r=p['indices'][0]
        self.assertEqual([t['calendar_days_to_expiry'] for t in r['terms']],[26,61])
        self.assertAlmostEqual(r['terms'][0]['comparisons']['call25_minus_put25']['value'],-10)
        self.assertIsNone(r['terms'][0]['comparisons']['call25_minus_put25']['observed_at'])
        self.assertEqual(p['quality']['eligible_identity_rows'],30);self.assertEqual(p['quality']['complete_paginations'],3)
        for k in model.PERMISSIONS:self.assertIs(p[k],False)
        for k in ('system_tail_gauge','tail_valuation','tail_regime','call'):self.assertIsNone(p[k])
        for k in ('p_drop_10','p_drop_20','rn_skew','tail_stress','spot'):self.assertIsNone(r[k])
    def test_missing_quotes_underlying_and_iv_clocks_not_invented_from_receipt(self):
        s,i=fixture();changed(s,i,lambda d:[(r.pop('last_quote'),r.update(underlying_asset={'ticker':'SPY'})) for r in d['results']])
        r=store.compile_output(i,store.reader(s,'b'))['indices'][0]['terms'][0]['selections']['put25']['contract']
        self.assertIsNone(r['quote']['midpoint']);self.assertIsNone(r['underlying_mark']['observed_at']);self.assertIsNone(r['provider_iv_observed_at'])
        self.assertIsNone(r['strike_to_underlying_ratio']);self.assertEqual(r['received_at'],STAMP)
    def test_contract_symbol_expiry_strike_deliverable_and_underlying_consistency(self):
        base=row();page={'acquired_at':STAMP,'page':1,'original':{}}
        for path,value in [('ticker','O:QQQ261016P00100000'),('strike_price',99),('expiration_date','2026-10-17'),('shares_per_contract',True),('shares_per_contract',10),('exercise_style','european'),('additional_underlyings',[{'cash':1}]),('ticker',4)]:
            r=deepcopy(base);r['details'][path]=value;self.assertIsNone(model.contract(r,'SPY',STAMP,page,0)[0])
        r=deepcopy(base);r['underlying_asset']['ticker']='QQQ';self.assertIsNone(model.contract(r,'SPY',STAMP,page,0)[0])
        r=deepcopy(base);r['details']=[];self.assertIsNone(model.contract(r,'SPY',STAMP,page,0)[0])
    def test_quote_crossing_sizes_and_clocks_are_separate_from_iv(self):
        for update,reason in [({'bid':2},'crossed_quote'),({'ask_size':0},'missing_or_zero_quote_size'),({'bid':True},'missing_or_invalid_bid_ask'),
            ({'last_updated':4102444800000000000},'missing_or_invalid_quote_clock'),({'last_updated':NS/1e9},'missing_or_invalid_quote_clock'),
            ({'last_updated':NS-10*86400*1000000000},'quote_older_than_96_hours')]:
            r=row();r['last_quote'].update(update);q=model.quote(r,STAMP);self.assertIn(reason,q['quality']['reasons']);self.assertIsNone(q['midpoint'])
        q=model.quote(row(),STAMP);self.assertEqual(q['midpoint'],1.1);self.assertEqual(q['timestamp_ns'],str(NS));self.assertFalse(q['quality']['executable_quote'])
    def test_mismatched_clocks_prevent_spot_ratio(self):
        r=row();r['underlying_asset']['last_updated']=NS+61000000000
        out,_=model.contract(r,'SPY',STAMP,{'acquired_at':STAMP,'page':1,'original':{}},0)
        self.assertFalse(out['quote_and_underlying_within_60s']);self.assertIsNone(out['strike_to_underlying_ratio'])
    def test_nearest_delta_has_explicit_tolerance_and_no_invented_wing(self):
        s,i=fixture();changed(s,i,lambda d:[r['greeks'].update(delta=-.40) for r in d['results'] if r['details']['contract_type']=='put'])
        t=store.compile_output(i,store.reader(s,'b'))['indices'][0]['terms'][0]
        for label in ('put10','put25','put50'):self.assertIsNone(t['selections'][label]['contract'])
        self.assertIsNone(t['comparisons']['call25_minus_put25']['value'])
    def test_same_expiry_never_appears_as_term_structure(self):
        s,i=fixture();changed(s,i,lambda d:d.update(results=d['results'][:5]));r=store.compile_output(i,store.reader(s,'b'))['indices'][0]
        self.assertTrue(r['same_selected_expiry']);self.assertIsNone(r['descriptive_term_difference']['value'])
    def test_duplicate_contract_is_removed_not_last_page_wins(self):
        s,i=fixture();changed(s,i,lambda d:d['results'].append(deepcopy(d['results'][0])))
        r=store.compile_output(i,store.reader(s,'b'))['indices'][0];self.assertEqual(r['sample']['eligible_identity_rows'],9)
        self.assertEqual(r['sample']['excluded_reasons']['duplicate_contracts_removed'],1)
    def test_request_chain_and_completeness_are_replayed(self):
        s,i=fixture();changed(s,i,lambda d:d.update(next_url='https://api.polygon.io/v3/snapshot/options/SPY?cursor=next'))
        with self.assertRaisesRegex(ValueError,'truncated'):store.compile_output(i,store.reader(s,'b'))
        i['collection']['chains']['SPY']['pages'][0]['request_identity_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'identity'):store.compile_output(i,store.reader(s,'b'))
    def test_pagination_refuses_cross_origin_userinfo_and_unknown_parameters(self):
        for u in ['http://api.polygon.io/v3/snapshot/options/SPY?cursor=one','https://evil.invalid/v3/snapshot/options/SPY?cursor=one',
            'https://api.polygon.io@evil.invalid/v3/snapshot/options/SPY?cursor=one','https://api.polygon.io/v3/snapshot/options/QQQ?cursor=one',
            'https://api.polygon.io/v3/snapshot/options/SPY?cursor=one&cursor=two','https://api.polygon.io/v3/snapshot/options/SPY?redirect=evil']:
            with self.assertRaises(ValueError):model.next_url(u,'SPY')
        self.assertEqual(model.next_url('https://api.polygon.io/v3/snapshot/options/SPY?cursor=next&apiKey=SECRET','SPY'),'https://api.polygon.io/v3/snapshot/options/SPY?cursor=next')
    def test_collection_pages_are_followed_without_credentials_in_public_inputs(self):
        class Transport:
            def __init__(self):self.calls=[]
            def open(self,request,timeout):
                self.calls.append(request.full_url);symbol=request.full_url.split('/options/')[1].split('?')[0]
                doc={'status':'OK','results':source_rows(symbol)[:5] if 'cursor=' not in request.full_url else source_rows(symbol)[5:]}
                if 'cursor=' not in request.full_url:doc['next_url']='https://api.polygon.io/v3/snapshot/options/'+symbol+'?cursor=next&apiKey=SECRET'
                r=io.BytesIO(model.encoded(doc));r.status=200;return r
        t=Transport();s=Storage()
        with patch.object(store,'now',return_value=STAMP):c=store.collect(s,'b',STAMP,time.monotonic()+10,opener=t,secret='SECRET')
        self.assertEqual(len(t.calls),6);self.assertNotIn('SECRET',model.encoded(c).decode());self.assertTrue(all(v['stop']=='complete' for v in c['chains'].values()))
        p=store.compile_output({'contract':'tail-native-inputs.v1','started_at':STAMP,'generated_at':STAMP,'collection':c},store.reader(s,'b'))
        self.assertEqual(p['quality']['eligible_identity_rows'],30)
    def test_collection_limits_and_missing_configuration_are_explicit(self):
        s=Storage()
        with patch.object(store,'now',return_value=STAMP):c=store.collect(s,'b',STAMP,time.monotonic()+10,secret='')
        self.assertEqual(c['source_bytes'],0);self.assertTrue(all(x['stop']=='provider_not_configured' for x in c['chains'].values()))
        p=store.compile_output({'contract':'tail-native-inputs.v1','started_at':STAMP,'generated_at':STAMP,'collection':c},store.reader(s,'b'))
        self.assertEqual(p['collection']['provider_requests'],0);self.assertEqual(p['quality']['eligible_identity_rows'],0)
    def test_cyclic_cursor_and_page_ceiling_replay_as_incomplete(self):
        for cyclic in (True,False):
            class Transport:
                def open(self,request,timeout):
                    from urllib.parse import parse_qs,urlsplit
                    symbol=request.full_url.split('/options/')[1].split('?')[0];q=parse_qs(urlsplit(request.full_url).query);page=int(q.get('cursor',['0'])[0])
                    doc={'status':'OK','results':[],'next_url':'https://api.polygon.io/v3/snapshot/options/'+symbol+'?cursor='+str(1 if cyclic else page+1)}
                    r=io.BytesIO(model.encoded(doc));r.status=200;return r
            s=Storage()
            with patch.object(store,'now',return_value=STAMP):c=store.collect(s,'b',STAMP,time.monotonic()+10,opener=Transport(),secret='SECRET')
            expected='pagination_cycle' if cyclic else 'page_limit'
            self.assertTrue(all(x['stop']==expected for x in c['chains'].values()))
            p=store.compile_output({'contract':'tail-native-inputs.v1','started_at':STAMP,'generated_at':STAMP,'collection':c},store.reader(s,'b'))
            self.assertEqual(p['quality']['complete_paginations'],0)
    def test_redirects_and_overlong_responses_never_become_evidence(self):
        with self.assertRaisesRegex(ValueError,'redirect'):store.NoRedirect().redirect_request(None,None,302,'',{},'https://evil.invalid/')
        stream=io.BytesIO(b'12345')
        with self.assertRaisesRegex(ValueError,'byte bound'):store.bounded(stream,4)
        self.assertTrue(stream.closed)
    def test_future_receipt_and_forged_terminal_reason_fail_replay(self):
        s,i=fixture();i['collection']['chains']['SPY']['pages'][0]['acquired_at']='2026-09-21T00:00:00Z'
        with self.assertRaisesRegex(ValueError,'clock'):store.compile_output(i,store.reader(s,'b'))
        s,i=fixture();i['collection']['chains']['SPY']['stop']='page_limit'
        with self.assertRaisesRegex(ValueError,'completeness'):store.compile_output(i,store.reader(s,'b'))
    def test_bad_types_nonfinite_and_duplicate_json_keys_fail_safely(self):
        for v in (True,None,'0.2',float('nan'),float('inf'),10**1000):self.assertIsNone(model.number(v))
        for raw in (b'{"x":NaN}',b'{"x":1,"x":2}'):
            with self.assertRaises(ValueError):model.decode(raw)
        s,i=fixture();changed(s,i,lambda d:d['results'].extend([None,{'details':'broken'}]))
        p=store.compile_output(i,store.reader(s,'b'));self.assertEqual(p['quality']['eligible_identity_rows'],30)
    def test_exact_original_replay_and_matching_compiler_required(self):
        s,i,p=packet();self.assertEqual(store.replay(p['replay'],store.reader(s,'b')),{k:v for k,v in p.items() if k!='replay'})
        source=i['collection']['chains']['SPY']['pages'][0]['original']['key'];s.objects[source]=b'{}'
        with self.assertRaisesRegex(ValueError,'Original bytes'):store.replay(p['replay'],store.reader(s,'b'))
        s,i,p=packet();run=json.loads(s.objects[p['replay']['manifest_key']]);s.objects[run['compilers']['tail_research_model']['key']]=b'changed'
        with self.assertRaisesRegex(ValueError,'compiler'):store.replay(p['replay'],store.reader(s,'b'))
    def test_arbitrary_private_reads_and_tampered_reference_refused(self):
        s,i=fixture()
        with self.assertRaises(ValueError):store.reader(s,'b')('portfolio/state.json')
        i['collection']['chains']['SPY']['pages'][0]['original']['key']='portfolio/state.json'
        with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))
        self.assertNotIn('portfolio/state.json',s.reads)
    def test_protected_predecessor_history_retained_without_history_mutation(self):
        s,i,p=packet();old=model.encoded({'generated_at':'2026-09-19T00:00:00Z','tail_valuation':'CHEAP'});hist=b'{"SPY":[{"d":"2026-09-19","p10":0.01}]}'
        s.objects[store.CURRENT]=old;s.objects[store.CONTEXT_KEYS[1]]=hist
        self.assertTrue(store.publish(s,'b',p));self.assertEqual(s.objects[store.CONTEXT_KEYS[1]],hist)
        self.assertEqual(s.objects[store.PRIVATE+model.sha(old)+'.bin'],old)
        self.assertNotIn('RETAIN_ONLY',model.encoded(p).decode())
    def test_current_publication_races_and_idempotent_request(self):
        s,i,p=packet();new={**p,'generated_at':'2027-01-01T00:00:00Z'};s.objects[store.CURRENT]=model.encoded(new)
        self.assertFalse(store.publish(s,'b',p));s.objects[store.CURRENT]=model.encoded({**p,'different':True})
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(s,'b',p)
        existing={'status':'complete','request_id':'known'};s.objects[store.request_key('known')]=model.encoded(existing)
        with patch.object(store,'collect',side_effect=AssertionError('must not collect')):self.assertEqual(store.run(s,'b','known','aws-test'),existing)
    def test_failure_preserves_current_and_redacts_provider_exception(self):
        s=Storage({store.CURRENT:b'{"old":true}'})
        with patch.object(store,'collect',side_effect=RuntimeError('SECRET_URL')):
            with self.assertRaisesRegex(RuntimeError,'publication failed'):store.run(s,'b','fail','aws-test')
        self.assertEqual(s.objects[store.CURRENT],b'{"old":true}');self.assertNotIn(b'SECRET_URL',s.objects[store.request_key('fail')])
    def test_http_and_validation_do_not_acquire_or_publish(self):
        import lambda_function as handler
        with patch('boto3.client',side_effect=AssertionError('no AWS')):self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
        s,_,p=packet();s.objects[store.CURRENT]=model.encoded(p);before=len(s.writes)
        with patch('boto3.client',return_value=s),patch.object(store,'collect',side_effect=AssertionError('no provider')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],200)
        self.assertEqual(len(s.writes),before)
    def test_standalone_replay_help_starts_without_aws_or_preloaded_modules(self):
        import subprocess,os
        env=os.environ.copy();env.pop('PYTHONPATH',None);env['AWS_EC2_METADATA_DISABLED']='true'
        result=subprocess.run([sys.executable,str(ROOT/'scripts/replay_tail_research.py'),'--help'],env=env,capture_output=True,text=True,timeout=20)
        self.assertEqual(result.returncode,0,result.stderr);self.assertIn('usage:',result.stdout)

if __name__=='__main__':unittest.main()
