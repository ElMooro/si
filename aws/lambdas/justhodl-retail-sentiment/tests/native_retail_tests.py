"""Source semantics, retained-byte replay, clocks and side-effect boundaries."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime
from decimal import localcontext,ROUND_UP
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,io,json,sys,time,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE));sys.path.insert(0,str(ROOT/'aws/shared'))
import retail_research_model as model
import retail_research_store as store
import retail_research as adapter
STAMP='2026-09-20T19:40:00+00:00'
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
def row(ticker='TEST',**changes):
    return {'ticker':ticker,'rank':1,'mentions':20,'mentions_24h_ago':10,'rank_24h_ago':2,'upvotes':50,**changes}
def fixture(size=4):
    s=Storage();pages=[]
    def add(kind,identity,page,doc):
        raw=model.encoded(doc);pages.append({'kind':kind,'identity':identity,'page':page,'request_url':model.source_url(kind,identity,page),
            'acquired_at':STAMP,'status':'received','http_status':200,'original':store.original(s,'b',raw)})
    first=None
    for category,n in model.CATEGORIES.items():
        for page in range(1,n+1):
            rows=[row('T'+str((page-1)*100+i),rank=(page-1)*100+i+1,mentions=200-i,mentions_24h_ago=None if i%4==0 else 0 if i%4==1 else 100) for i in range(size)]
            doc={'count':663,'pages':7,'current_page':page,'results':rows};add('apewisdom',category,page,doc)
            if category=='all-stocks' and page==1:first=doc
    add('stocktwits','trending',1,{'symbols':[{'symbol':'TEST','watchlist_count':0,'trending_score':999}]})
    for ticker in store.selected_symbols(first):
        add('stocktwits',ticker,1,{'symbol':{'symbol':ticker},'messages':[
            {'id':1,'created_at':'2026-09-20T19:30:00Z','body':'PRIVATE_TEXT','user':{'id':11,'username':'PRIVATE_NAME'},'entities':{'sentiment':{'basic':'Bullish'}}},
            {'id':2,'created_at':'2026-09-20T18:30:00Z','user':{'id':11},'entities':{'sentiment':None}},
            {'id':3,'created_at':'2026-09-20T18:00:00Z','user':{'id':12},'entities':{'sentiment':{'basic':'Bullish'}}}]})
    contexts={k:{'status':'retained_unqualified_context','measurement_eligible':False,'original':store.original(s,'b',model.encoded({'legacy':'PRIVATE_ACCOUNT_FLAG','whole':[1,2,3]}))} for k in model.CONTEXT_KEYS}
    inputs={'contract':'retail-native-inputs.v1','started_at':STAMP,'generated_at':STAMP,'collection':{'pages':pages,'context_evidence':contexts,'source_bytes':sum(p['original']['bytes'] for p in pages)}}
    return s,inputs
def packet(size=4):
    s,i=fixture(size);o=store.compile_output(i,store.reader(s,'b'));return s,i,{**o,'replay':store.retain(s,'b',i,o)}
def changed_source(s,i,index,mutate):
    item=i['collection']['pages'][index];doc=json.loads(s.objects[item['original']['key']]);mutate(doc)
    item['original']=store.original(s,'b',model.encoded(doc));i['collection']['source_bytes']=sum(p['original']['bytes'] for p in i['collection']['pages'] if p['original'])

class Native(unittest.TestCase):
    def test_real_count_changes_zero_and_missing_baselines_differ(self):
        s,i,p=packet();c=p['communities']['all-stocks'];rows=c['rows']
        self.assertEqual(c['eligible_symbols'],8);self.assertEqual(c['reported_symbol_count'],663)
        self.assertIsNone(rows[0]['mention_change_pct']);self.assertEqual(rows[0]['baseline_status'],'missing_or_invalid')
        self.assertIsNone(rows[1]['mention_change_pct']);self.assertEqual(rows[1]['mention_change'],199)
        self.assertEqual(rows[2]['mention_change_pct'],98);self.assertFalse(c['population_complete'])
        self.assertEqual(c['paired_sample']['symbols'],6);self.assertEqual(c['paired_sample']['prior_mentions'],400)
    def test_integer_strings_supported_bools_negatives_fractional_not_imputed(self):
        self.assertEqual(model.count('20'),20)
        for value in (True,False,-1,1.2,'NaN','20.0','-2',None,9007199254740992):self.assertIsNone(model.count(value))
        row,why=model.attention_row({'ticker':'TEST','mentions':None,'rank':1},1,0);self.assertIsNone(row)
        self.assertEqual(model.attention_row(globals()['row'](mentions='0',mentions_24h_ago='10'),1,0)[0]['mention_change_pct'],-100)
    def test_communities_never_summed_or_promoted_to_unique_population(self):
        _,_,p=packet();self.assertIsNone(p['market_regime']);self.assertIsNone(p['as_of']);self.assertIsNone(p['n_all_stocks'])
        self.assertEqual(p['lineage']['independent_investment_votes'],0)
        self.assertEqual(p['communities']['all-stocks']['eligible_symbols'],8)
        self.assertEqual(p['communities']['wallstreetbets']['eligible_symbols'],4)
    def test_pagination_identity_drift_and_conflicting_symbol_not_silent(self):
        s,i=fixture();changed_source(s,i,1,lambda d:d['results'][0].update(ticker='T0',mentions=999))
        o=store.compile_output(i,store.reader(s,'b'));self.assertEqual(o['communities']['all-stocks']['conflicting_symbols_removed'],1)
        self.assertNotIn('T0',[r['symbol'] for r in o['communities']['all-stocks']['rows']])
        changed_source(s,i,1,lambda d:d.update(count=700));o=store.compile_output(i,store.reader(s,'b'))
        self.assertTrue(o['communities']['all-stocks']['pagination_changed_between_requests']);self.assertIsNone(o['communities']['all-stocks']['reported_symbol_count'])
    def test_stream_is_tagged_bounded_sample_without_invented_ratio(self):
        _,_,p=packet();r=p['stocktwits']['streams'][0]
        self.assertEqual((r['bullish'],r['bearish'],r['unclassified']),(2,0,1));self.assertIsNone(r['bull_bear_ratio'])
        self.assertEqual(r['bullish_share_of_classified_pct'],100);self.assertEqual(r['unique_sample_user_count'],2)
        self.assertEqual(r['sample_span_seconds'],5400);self.assertNotIn('sentiment_score',r)
    def test_no_tags_and_unavailable_stream_are_not_neutral_or_zero(self):
        s,i=fixture();changed_source(s,i,6,lambda d:[r.update(entities={}) for r in d['messages']])
        o=store.compile_output(i,store.reader(s,'b'));r=o['stocktwits']['streams'][0]
        self.assertIsNone(r['bullish_share_of_classified_pct']);self.assertEqual(r['unclassified'],3)
        item=i['collection']['pages'][6];item.update(status='time_budget',original=None,http_status=None);i['collection']['source_bytes']=sum(p['original']['bytes'] for p in i['collection']['pages'] if p['original'])
        o=store.compile_output(i,store.reader(s,'b'));self.assertIsNone(o['stocktwits']['streams'][0]['eligible_messages'])
    def test_duplicate_and_future_message_exclusion_with_real_times(self):
        s,i=fixture();changed_source(s,i,6,lambda d:d['messages'].extend([d['messages'][0],{'id':7,'created_at':'2027-01-01T00:00:00Z'}]))
        r=store.compile_output(i,store.reader(s,'b'))['stocktwits']['streams'][0]
        self.assertEqual(r['messages_received'],5);self.assertEqual(r['eligible_messages'],3);self.assertEqual(r['excluded_reasons'],{'duplicate_message':1,'future_message_clock':1})
    def test_protected_bodies_and_held_flags_not_republished(self):
        s,i,p=packet();public=model.encoded(p)
        for token in (b'PRIVATE_TEXT',b'PRIVATE_NAME',b'PRIVATE_ACCOUNT_FLAG'):self.assertNotIn(token,public);self.assertTrue(any(token in raw for raw in s.objects.values()))
        self.assertEqual(set(p['context_evidence']),set(model.CONTEXT_KEYS))
    def test_compiler_input_and_original_bytes_independently_replay(self):
        s,i,p=packet();self.assertEqual(store.replay(p['replay'],store.reader(s,'b')),{k:v for k,v in p.items() if k!='replay'})
        key=i['collection']['pages'][0]['original']['key'];s.objects[key]+=b' '
        with self.assertRaisesRegex(ValueError,'bytes differ'):store.replay(p['replay'],store.reader(s,'b'))
    def test_bad_source_inventory_future_clock_and_external_path_refused(self):
        for mutate in (lambda i:i['collection']['pages'].pop(),lambda i:i['collection']['pages'][1].update(acquired_at='2027-01-01T00:00:00Z'),lambda i:i['collection']['pages'][0].update(request_url='https://other.invalid')):
            s,i=fixture();mutate(i)
            with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))
        s,i=fixture()
        with self.assertRaises(ValueError):store.reader(s,'b')('data/portfolio.json')
    def test_daily_pipeline_and_short_sample_deadlines_not_conflated(self):
        _,_,p=packet();self.assertEqual(p['freshness']['pipeline_check_due_at'],'2026-09-21T20:10:00+00:00')
        self.assertEqual(p['freshness']['sample_valid_until'],'2026-09-20T21:40:00+00:00')
        self.assertTrue(adapter.context(p,datetime.fromisoformat(STAMP))['available'])
        for at in ('2026-09-20T19:39:59+00:00','2026-09-20T21:40:00+00:00'):self.assertFalse(adapter.context(p,datetime.fromisoformat(at))['available'])
    def test_authority_and_body_tampering_rejected(self):
        _,_,p=packet()
        for key in model.PERMISSIONS:
            x=deepcopy(p);x[key]=True;self.assertFalse(adapter.context(x,datetime.fromisoformat(STAMP))['available'])
        x=deepcopy(p);x['communities']['all-stocks']['sample_mentions']=999;self.assertFalse(adapter.context(x,datetime.fromisoformat(STAMP))['available'])
        self.assertEqual(adapter.decision_view({'market_regime':'MANIA','biggest_velocity_surges':[{'ticker':'TEST','velocity_pct':9999}]} )['biggest_velocity_surges'],[])
    def test_decimal_context_does_not_change_calculations(self):
        s,i=fixture();normal=store.compile_output(i,store.reader(s,'b'))
        with localcontext() as ctx:ctx.prec=3;ctx.rounding=ROUND_UP;self.assertEqual(store.compile_output(i,store.reader(s,'b')),normal)
    def test_publish_preserves_whole_predecessor_and_refuses_older_or_conflicts(self):
        s,i,p=packet();old=model.encoded({'generated_at':'2026-09-19T00:00:00Z','whole_legacy':['preserve',1]});s.objects[model.CURRENT]=old
        self.assertTrue(store.publish(s,'b',p));self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old)
        x=deepcopy(p);x['generated_at']='2026-09-19T00:00:00Z';self.assertFalse(store.publish(s,'b',x))
        x=deepcopy(p);x['version']='conflict'
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(s,'b',x)
    def test_collection_failure_preserves_prior_and_redacts_error(self):
        s=Storage({model.CURRENT:b'OLD'})
        with patch.object(store,'collect',side_effect=RuntimeError('PRIVATE_DETAIL')):
            with self.assertRaisesRegex(RuntimeError,'Native Retail'):store.run(s,'b','test','execution')
        self.assertEqual(s.objects[model.CURRENT],b'OLD');self.assertNotIn(b'PRIVATE_DETAIL',s.objects[store.request_key('test')])
    def test_request_idempotency_and_readonly_HTTP_do_not_collect(self):
        s,i,p=packet();status={'status':'complete'};s.objects[store.request_key('done')]=model.encoded(status)
        with patch.object(store,'collect',side_effect=AssertionError('No second collection')):self.assertEqual(store.run(s,'b','done','execution'),status)
        import lambda_function as handler
        with patch.object(handler.boto3,'client',side_effect=AssertionError('No validation AWS')):self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
        s.objects[model.CURRENT]=model.encoded(p)
        with patch.object(handler.boto3,'client',return_value=s),patch.object(handler,'run',side_effect=AssertionError('HTTP cannot publish')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],200)
    def test_network_403_retained_without_access_bypass_and_contexts_preserved(self):
        class Response(io.BytesIO):status=200
        class Opener:
            def __init__(self):self.urls=[]
            def open(self,req,timeout):
                self.urls.append(req.full_url)
                if 'stocktwits' in req.full_url:raise urllib.error.HTTPError(req.full_url,403,'denied',{},io.BytesIO(b'DENIED_BODY'))
                page=int(req.full_url[-1]);return Response(model.encoded({'count':663,'pages':7,'current_page':page,'results':[row()]}))
        s=Storage({k:model.encoded({'retained':'whole'}) for k in model.CONTEXT_KEYS});op=Opener()
        got=store.collect(s,'b',time.monotonic()+60,op)
        self.assertEqual(sum('stocktwits' in u for u in op.urls),1);self.assertEqual(got['pages'][-1]['status'],'provider_access_or_rate_limit')
        self.assertTrue(all(x['status']=='retained_unqualified_context' for x in got['context_evidence'].values()));self.assertFalse(any('portfolio' in k for k in s.reads))
    def test_redirect_unknown_provider_and_oversized_body_refused(self):
        with self.assertRaises(ValueError):store.NoRedirect().redirect_request(None,None,None,None,None,'https://other.invalid')
        with self.assertRaises(ValueError):model.source_url('other','TEST')
        with self.assertRaises(ValueError):store.bounded(io.BytesIO(b'12345'),4)
    def test_all_community_pages_failed_cannot_publish_empty_wrapped_freshness(self):
        s,i=fixture()
        for item in i['collection']['pages'][:5]:item.update(http_status=503)
        i['collection']['pages']=i['collection']['pages'][:6]
        i['collection']['source_bytes']=sum(p['original']['bytes'] for p in i['collection']['pages'])
        with self.assertRaisesRegex(ValueError,'No usable community'):store.compile_output(i,store.reader(s,'b'))
    def test_malformed_message_entities_or_header_do_not_invent_sentiment(self):
        s,i=fixture();changed_source(s,i,6,lambda d:d['messages'][0].update(entities=['invalid']))
        o=store.compile_output(i,store.reader(s,'b'));self.assertEqual(o['stocktwits']['streams'][0]['unclassified'],2)
        changed_source(s,i,6,lambda d:d.update(symbol='invalid'))
        o=store.compile_output(i,store.reader(s,'b'));self.assertFalse(o['stocktwits']['streams'][0]['available']);self.assertIsNone(o['stocktwits']['streams'][0]['bullish'])

if __name__=='__main__':unittest.main(verbosity=2)
