from pathlib import Path
from datetime import datetime,timedelta,timezone
from io import BytesIO
from threading import Lock
import ast,copy,gzip,json,sys,unittest,urllib.error,zlib
from xml.sax.saxutils import escape
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
import geo_news_model as model
import geo_news_store as store
NOW=datetime(2026,9,27,3,0,tzinfo=timezone.utc)


def rss(title='China missile update',date='2026-09-27T02:00:00Z',extra=''):
    return ('<rss><channel><item><title>'+escape(title)+'</title>'+('<pubDate>'+date+'</pubDate>' if date else '')+extra+'</item></channel></rss>').encode()


def fixture(rows=None):
    feeds=model.corpus({'geopolitics':[{'name':'Source '+str(i),'url':'https://source.test/'+str(i)} for i in range(len(rows or [1]))]})
    captures=[{'feed_id':f['feed_id'],'acquired_at':NOW.isoformat(),'status':'http_response','http_status':200,'raw':raw,
               'body_sha256':model.sha(raw),'body_bytes':len(raw)} for f,raw in zip(feeds,rows or [rss()])]
    return feeds,captures


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.lock=Lock();self.data={};self.writes=[];self.reads=[];self.deny=None;self.conflict=None
        for key,obj in ((store.HEAD,{'generated_at':'2026-09-26T11:30:00Z','version':'1.1.0'}),
                        (store.HISTORY,{'days':{(NOW-timedelta(days=i)).date().isoformat():{'China':{'v':i,'c':0}} for i in range(120)}}),
                        (store.SOVEREIGN,{'generated_at':'2026-09-26T18:16:52Z','countries':[{'stress_0_100':99}]})):
            self.data[key]=model.encode(obj)
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key==self.deny:raise StorageError('AccessDenied')
        with self.lock:
            if key not in self.data:raise StorageError('NoSuchKey')
            raw=self.data[key]
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':'"'+model.sha(raw)+'"','LastModified':NOW}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        with self.lock:
            if key==self.conflict:
                self.data[key]=b'{"foreign_writer":true}';self.conflict=None
            old=self.data.get(key)
            if kw.get('IfNoneMatch')=='*' and old is not None:raise StorageError('PreconditionFailed')
            if 'IfMatch' in kw and (old is None or kw['IfMatch']!='"'+model.sha(old)+'"'):raise StorageError('PreconditionFailed')
            self.data[key]=raw;self.writes.append(key)
        return {'ETag':'"'+model.sha(raw)+'"'}


class HTTP(BytesIO):
    def __init__(self,raw,status=200,headers=None):super().__init__(raw);self.status=status;self.headers=headers or {'Content-Length':str(len(raw))}
    def getcode(self):return self.status


class ModelTests(unittest.TestCase):
    def test_date_declarations_never_borrow_ingestion_updated_or_future_clocks(self):
        feeds,captures=fixture([rss(date=None,extra='<updated>2026-09-27T02:00:00Z</updated>'),rss(date='2026-09-27T03:00:01Z'),rss(date='2026-09-27T02:00:00'),rss(date='2026-09-26T00:00:00Z')])
        out=model.build(feeds,captures,{'China':['china']},NOW.isoformat(),{})
        self.assertEqual(out['rankings'][0]['mentions_48h'],1);self.assertEqual(out['rankings'][0]['mentions_24h'],0)
        self.assertEqual(out['sources']['entry_window_counts'],{'undated_publication':2,'future_publication':1,'within_48h':1})
        self.assertEqual(len(out['entries']),4)
        self.assertIsNone(out['entries'][0]['published_at'])

    def test_title_groups_keep_all_members_and_country_counts_do_not_claim_events(self):
        feeds,captures=fixture([rss('CHINA missile update'),rss('China   missile update')])
        out=model.build(feeds,captures,{'China':['china'],'No mentions':['nowhere']},NOW.isoformat(),{})
        row=out['rankings'][0];self.assertEqual((row['mentions_48h'],row['raw_mentions_48h'],row['crisis_hits']),(1,2,1))
        self.assertEqual(len(row['entry_ids']),2);self.assertIsNone(out['rankings'][1]['crisis_share'])
        self.assertTrue(all(out[f] is False for f in model.FLAGS));self.assertEqual(out['portfolio_action'],'WAIT')
        self.assertIsNone(row['stress_score']);self.assertEqual(out['gssi_cross']['rows'],[])

    def test_parse_atom_rfc_dates_conflicts_empty_and_xml_entity_rejection(self):
        f=model.corpus({'geopolitics':[{'name':'Feed','url':'https://source.test/rss'}]})[0]
        raw=b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>A title</title><published>2026-09-27T03:00:00+01:00</published><updated>2026-09-27T03:00:00Z</updated><link href="/story"/></entry></feed>'
        p=model.parse(raw,f)[0];self.assertEqual(p['published_at'],'2026-09-27T02:00:00+00:00');self.assertEqual(p['link'],'https://source.test/story')
        self.assertEqual(model.instant('Sun, 27 Sep 2026 02:00:00 GMT').hour,2)
        p=model.parse(rss(extra='<published>2026-09-26T00:00:00Z</published>'),f)[0];self.assertIsNone(p['published_at'])
        self.assertEqual(model.parse(b'<rss><channel/></rss>',f),[])
        for bad in (b'',b'<html>not RSS</html>',b'<!DOCTYPE rss [<!ENTITY x "boom">]><rss/>'):
            with self.assertRaises(ValueError):model.parse(bad,f)

    def test_full_configured_population_and_failed_feed_visibility(self):
        feeds,captures=fixture([rss(),b'<html>an error</html>'])
        out=model.build(feeds,captures,{'China':['china']},NOW.isoformat(),{})
        self.assertEqual(out['sources']['feeds_attempted'],2);self.assertEqual(out['sources']['failed_or_unparseable_feeds'],1)
        self.assertEqual(out['quality']['status'],'partial')
        with self.assertRaises(ValueError):model.build(feeds,captures[:1],{},NOW.isoformat(),{})
        with self.assertRaises(ValueError):model.build(feeds,[captures[1],captures[1]],{},NOW.isoformat(),{})
        feeds,captures=fixture([b'<html>failed</html>'])
        with self.assertRaises(ValueError):model.build(feeds,captures,{},NOW.isoformat(),{})

    def test_declared_compression_keeps_original_bytes_and_rejects_bombs_and_dtds(self):
        feed=fixture()[0][0];raw=rss()
        for encoding,body in (('gzip',gzip.compress(raw)),('deflate',zlib.compress(raw))):
            parsed=model.parse(body,feed,encoding)[0];plain=model.parse(raw,feed)[0]
            self.assertEqual({k:v for k,v in parsed.items() if k!='entry_id'},{k:v for k,v in plain.items() if k!='entry_id'})
            self.assertEqual(parsed['entry_id'],model.sha(model.encode([feed['feed_id'],model.sha(body),0])))
        for body,encoding in ((gzip.compress(b'x'*(8*1024*1024+1)),'gzip'),(zlib.compress(raw)[:-2],'deflate'),(b'not gzip','gzip')):
            with self.assertRaises((ValueError,OSError,EOFError)):model.parse(body,feed,encoding)
        with self.assertRaises(ValueError):model.parse('<!DOCTYPE rss [<!ENTITY x "unsafe">]><rss/>'.encode('utf-16'),feed)

    def test_aware_boundary_zero_and_punctuation_aliases(self):
        feeds,captures=fixture([rss('U.S. update',date=(NOW-timedelta(hours=48)).isoformat())])
        out=model.build(feeds,captures,{'US':['u.s.']},NOW.isoformat(),{})
        self.assertEqual(out['rankings'][0]['mentions_48h'],1);self.assertEqual(out['rankings'][0]['mentions_24h'],0)
        self.assertEqual(out['rankings'][0]['crisis_hits'],0);self.assertEqual(out['rankings'][0]['crisis_share'],0)
        captures[0]['body_bytes']+=1
        with self.assertRaises(ValueError):model.build(feeds,captures,{},NOW.isoformat(),{})


class StoreTests(unittest.TestCase):
    def configured_run(self,mem=None,opener=None):
        mem=mem or Memory();corpus,countries=store.configuration();requests=[]
        body=rss(' · '.join(aliases[0] for aliases in countries.values())+' missile update')
        def response(request,timeout):requests.append(request.full_url);return HTTP(body)
        result=store.run(mem,'fixture',corpus,countries,NOW.isoformat(),opener or response)
        return mem,result,requests

    def test_all_164_native_feeds_22_countries_and_120_legacy_dates_survive_full_replay(self):
        mem=Memory();previous=copy.deepcopy(store.strict(mem.data[store.HISTORY]))
        mem,result,requests=self.configured_run(mem)
        self.assertTrue(result['published']);self.assertEqual(len(requests),164);self.assertEqual(len(set(requests)),164)
        packet=store.strict(mem.data[store.HEAD]);self.assertEqual(len(packet['rankings']),22)
        self.assertTrue(all(r['mentions_48h']==1 and r['raw_mentions_48h']==164 for r in packet['rankings']))
        self.assertEqual(store.strict(mem.data[store.HISTORY])['days'],previous['days'])
        self.assertEqual(len(store.strict(mem.data[store.HISTORY])['research_runs']),1)
        self.assertEqual([k for k in mem.writes if k in (store.HEAD,store.HISTORY)],[store.HISTORY,store.HEAD])
        before=list(mem.writes);proof=store.replay(mem,'fixture',packet)
        self.assertEqual(proof['feed_acquisitions'],164);self.assertEqual(proof['entries'],164);self.assertEqual(mem.writes,before)
        ref=packet['publication_context']['calculation'];mem.data[ref['key']]+=b' '
        with self.assertRaises(ValueError):store.replay(mem,'fixture',packet)

    def test_history_denial_corruption_and_truncation_cannot_reset_or_publish(self):
        for kind in ('denied','invalid','wrong_shape'):
            mem=Memory();before=dict(mem.data);calls=[]
            if kind=='denied':mem.deny=store.HISTORY
            else:mem.data[store.HISTORY]=b'{' if kind=='invalid' else b'{"days":[]}'
            head=mem.data[store.HEAD]
            with self.assertRaises(Exception):self.configured_run(mem,lambda *a,**k:calls.append(1))
            self.assertEqual(calls,[]);self.assertEqual(mem.data[store.HEAD],head)
            self.assertFalse(any(k in (store.HEAD,store.HISTORY) for k in mem.writes))
        stream=HTTP(b'too short',headers={'Content-Length':'999'})
        with self.assertRaises(ValueError):store.whole(stream,length=999)
        self.assertTrue(stream.closed)

    def test_failed_or_concurrent_publication_never_overwrites_a_foreign_head(self):
        mem=Memory();mem.conflict=store.HEAD
        mem,result,_=self.configured_run(mem)
        self.assertFalse(result['published']);self.assertEqual(result['completed_paths'],[store.HISTORY])
        self.assertEqual(mem.data[store.HEAD],b'{"foreign_writer":true}')
        self.assertEqual(len(store.strict(mem.data[store.HISTORY])['days']),120)

    def test_acquisition_keeps_complete_redirect_errors_and_blocks_unknown_hosts(self):
        mem=Memory();feed=model.corpus({'geopolitics':[{'name':'A','url':'https://source.test/rss'}]})[0]
        calls=[]
        def open_url(req,timeout):
            calls.append(req.full_url)
            return HTTP(b'redirect body',302,{'Content-Length':'13','Location':'https://unknown.test/private'})
        result=store.acquire(mem,'fixture',feed,{'source.test'},open_url)
        self.assertEqual(len(calls),1);self.assertTrue(result['attempts'][0]['redirect_not_followed'])
        self.assertEqual(store.retained(mem,'fixture',result['original']),b'redirect body')
        def fail(req,timeout):raise urllib.error.URLError('offline')
        result=store.acquire(mem,'fixture',feed,{'source.test'},fail)
        self.assertEqual(result['status'],'transport_error');self.assertNotIn('original',result)
        with self.assertRaises(ValueError):store.read(mem,'fixture','private/account.json')

    def test_complete_retired_source_functions_are_preserved(self):
        original=ast.parse((ROOT/'tests/fixtures/pre-research-geopolitical-risk.py.txt').read_text(encoding='utf-8'))
        current=ast.parse((Path(store.__file__).parent/'lambda_function.py').read_text(encoding='utf-8'))
        funcs={n.name:n for n in current.body if isinstance(n,ast.FunctionDef)}
        for old in original.body:
            if isinstance(old,ast.FunctionDef):
                new=copy.deepcopy(funcs['_legacy_unqualified_handler' if old.name=='lambda_handler' else old.name]);new.name=old.name
                self.assertEqual(ast.dump(old),ast.dump(new),old.name)


if __name__=='__main__':unittest.main()
