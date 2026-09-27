"""Full native synthetic-fixture replay and original acquisition failures."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from io import BytesIO,StringIO
from contextlib import redirect_stdout
from copy import deepcopy
from unittest.mock import patch
import gzip,json,math,sys,time,unittest,urllib.request,urllib.error
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'source'))
sys.path.insert(0,str(HERE.parents[3]/'aws/shared'))
import business_cycle_acquisition as acq
import business_cycle_store as store
from run_tests import Memory,fixture,packet,module,NOW,Frozen


class Client(Memory):
    def get_object(self,**kwargs):
        return {**super().get_object(**kwargs),'LastModified':NOW-timedelta(minutes=15)}
    def get_paginator(self,name):
        assert name=='list_objects_v2';client=self
        class Pages:
            def paginate(self,**kw):
                keys=sorted(k for k in client.rows if k.startswith(kw['Prefix']))
                for i in range(0,max(1,len(keys)),7):
                    yield {'Contents':[{'Key':k,'Size':len(client.rows[k]),'ETag':str(client.versions[k]),'LastModified':NOW} for k in keys[i:i+7]]}
        return Pages()


def session(memory=None,opener=None,cache=None):
    memory=memory or fixture();publication=store.PublicationClient(memory,'b',NOW.isoformat())
    return acq.Acquisition(publication,memory,'b',NOW.isoformat(),opener=opener,sleep=lambda t:None,cache={} if cache is None else cache)


def fred():return urllib.request.Request('https://api.stlouisfed.org/fred/series/observations?series_id=TEST&api_key=credential-canary&file_type=json&sort_order=desc&limit=10')


def yahoo():return urllib.request.Request('https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=5y&interval=1d')


def response(raw,status=200):return acq.Response(raw,status,{'Content-Length':str(len(raw)),'Content-Type':'application/json'})


def full_fixture():
    """Explicit synthetic test values, all configured countries, all three outputs."""
    memory=Client();engine=module(memory)
    for key in store.KEYS:memory.seed(key,store.encode({'generated_at':(NOW-timedelta(days=1)).isoformat(),'original_history':[0,None,1]}))
    months=[f'{y}-{m:02d}' for y in range(2014,2027) for m in range(1,13) if f'{y}-{m:02d}'<='2026-09']
    countries={}
    for iso,*rest in engine.COUNTRY_MAP:
        countries[iso]={'features':{name:{'values':[100+i*.03+math.sin(i/5) for i in range(len(months))],
            'pillar':pillar,'sign':1,'freq':'M','max_lag_months':4,'latest_period':months[-1]}
            for name,pillar in (('fixture_output','activity'),('fixture_survey','survey'),('fixture_credit','credit'))}}
    features={'generated_at':NOW.isoformat(),'grid':{'months':months},'countries':countries,'version':'synthetic-test-only'}
    memory.seed('data/cycle/features.json.gz',gzip.compress(store.encode(features),mtime=0))
    memory.seed('data/portwatch.json',store.encode({'generated_at':NOW.isoformat(),'ports':[]}))
    dates=[NOW-timedelta(days=399-i) for i in range(400)]
    raw_yahoo=store.encode({'chart':{'result':[{'timestamp':[int(d.timestamp()) for d in dates],
        'indicators':{'quote':[{'close':[100+i*.08+math.sin(i/7) for i in range(400)]}]}}]}})
    raw_fred=store.encode({'observations':[{'date':m+'-01','value':str(100+i*.1+math.sin(i/3))} for i,m in reversed(list(enumerate(months)))]})
    def opener(request,timeout):return response(raw_fred if 'stlouisfed' in request.full_url else raw_yahoo)
    return memory,engine,opener


class AcquisitionTests(unittest.TestCase):
    def test_native_complete_34_country_and_three_output_replay(self):
        memory,engine,opener=full_fixture();original=deepcopy(memory.rows);create=acq.Acquisition
        def constructor(*args,**kwargs):return create(*args,**kwargs,opener=opener,sleep=lambda seconds:None,cache={})
        with redirect_stdout(StringIO()),patch.object(acq,'Acquisition',side_effect=constructor):
            engine.lambda_handler()
        packet=store.strict(memory.rows[store.HEAD]);ctx=packet['publication_context'];ref=ctx['native_acquisition']['manifest']
        manifest=store.strict(memory.rows[ref['key']])
        self.assertEqual(len(packet['by_country']),34)
        self.assertEqual(set(manifest['complete_native_calculations']),set(store.KEYS))
        self.assertEqual({r['key'] for r in manifest['operations'] if r['kind']=='s3'},acq.FIXED)
        for key,raw in original.items():self.assertIn(raw,memory.rows.values())
        with redirect_stdout(StringIO()):result=acq.replay_native(engine,manifest,lambda key:memory.rows[key])
        self.assertEqual(result['outputs'],3);self.assertEqual(result['status'],'complete_native_calculations_replayed')
        self.assertNotIn('credential-canary',json.dumps(manifest))
        self.assertTrue(all(packet[k] is False for k in store.PERMISSIONS))
        for changed in ('body','compiler','clock','operation','unused_predecessor_identity'):
            damaged=deepcopy(manifest)
            if changed=='unused_predecessor_identity':next(r for r in damaged['operations'] if r.get('key')==store.HEAD)['original']['sha256']='0'*64
            if changed=='compiler':damaged['compiler_sha256']['lambda_function.py']='0'*64
            if changed=='clock':damaged['processing_clocks']=[]
            if changed=='operation':damaged['operations']=[r for r in damaged['operations'] if r['kind']!='http_result']
            fetch=(lambda key:b'corrupt') if changed=='body' else lambda key:memory.rows[key]
            with redirect_stdout(StringIO()),self.assertRaises(Exception):acq.replay_native(engine,damaged,fetch)

    def test_all_1254_warehouse_objects_and_every_listing_page_preserved(self):
        memory=Client();start=NOW-timedelta(days=1500)
        for i in range(1254):
            day=(start+timedelta(days=i)).strftime('%Y-%m-%d');key=acq.WAREHOUSE+day[:4]+'/'+day+'.json.gz'
            memory.seed(key,gzip.compress(store.encode({'results':[{'T':'SPY','c':100+i}],'whole_unused':[0,None,i]}),mtime=0))
        s=session(memory);keys=[]
        for year in range(2021,2027):
            for page in s.get_paginator('list_objects_v2').paginate(Bucket='b',Prefix=acq.WAREHOUSE+str(year)+'/'):
                keys.extend(row['Key'] for row in page.get('Contents',[]))
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=12) as pool:
            raw=list(pool.map(lambda key:s.get_object(Bucket='b',Key=key)['Body'].read(),keys))
        self.assertEqual(len(keys),1254);self.assertEqual(len(raw),1254)
        self.assertEqual(len([r for r in s.operations if r['kind']=='s3']),1254)
        for row in s.operations:
            if row['kind']=='s3':self.assertEqual(memory.rows[row['original']['key']],memory.rows[row['key']])
        self.assertGreater(sum(len(r['pages']) for r in s.operations if r['kind']=='listing'),6)

    def test_fred_cache_preserves_acquisition_clock_and_failed_attempt_bodies(self):
        cache={};requests=[];raw=b'{"observations":[]}'
        s=session(opener=lambda *a,**k:requests.append('get') or response(raw),cache=cache)
        s.urlopen(fred(),timeout=12).read();origin=s.http_attempts[0]['acquired_at']
        s.urlopen(fred(),timeout=12).read()
        self.assertEqual(requests,['get']);self.assertEqual(s.operations[-1]['mode'],'warm_cache')
        self.assertEqual(s.operations[-1]['source_acquired_at'],origin)
        for value in cache.values():value['cached_at']-=1900
        s.opener=lambda *a,**k:response(b'full rate-limit response',429)
        self.assertEqual(s.urlopen(fred(),timeout=12).read(),raw)
        self.assertEqual(len(s.http_attempts),5);self.assertEqual(s.operations[-1]['mode'],'stale_cache_fallback')
        self.assertEqual(s.operations[-1]['source_acquired_at'],origin)
        for row in s.http_attempts:self.assertEqual(s.client.rows[row['original']['key']],raw if row['http_status']==200 else b'full rate-limit response')
        self.assertNotIn('credential-canary',json.dumps(s.operations+s.http_attempts))
        s.opener=lambda *a,**k:response(b'bad credentials',400)
        with self.assertRaises(RuntimeError):s.urlopen(fred(),timeout=12)
        self.assertEqual(s.operations[-1]['status'],'unavailable')

    def test_redirects_private_requests_and_unknown_reads_never_reach_sources(self):
        calls=[]
        for request in ('https://evil.test/a','https://api.stlouisfed.org.evil.test/fred/series/observations',
                        'https://query1.finance.yahoo.com/v8/finance/chart/X?range=5y&interval=1d&account=private'):
            s=session(opener=lambda *a,**k:calls.append('request'))
            with self.assertRaises(acq.AcquisitionError):s.urlopen(request,timeout=12)
        self.assertEqual(calls,[]);self.assertIsNone(acq.NoRedirect().redirect_request(None,None,302,None,None,'https://evil.test'))
        for key in ('data/account.json','learning/morning_run_log.json',acq.WAREHOUSE+'../secret.json.gz'):
            with self.assertRaises(acq.AcquisitionError):session().get_object(Bucket='b',Key=key)

    def test_partial_retention_and_denied_inputs_latch_failure_before_publication(self):
        for mode in ('length','retention','denied'):
            memory=fixture();s=session(memory)
            if mode=='retention':memory.fail='retention'
            if mode=='denied':memory.fail=store.HEAD
            if mode=='length':
                get=memory.get_object;memory.get_object=lambda **kw:{**get(**kw),'ContentLength':0}
            with self.assertRaises(acq.AcquisitionError):s.get_object(Bucket='b',Key=store.HEAD)
            with self.assertRaises(acq.AcquisitionError):s.put_object(Bucket='b',Key=store.HEAD,Body=store.encode(packet()))
            self.assertFalse(any(key in store.KEYS for key in memory.writes))

    def test_empty_and_error_http_bodies_retained_but_partial_stream_aborts(self):
        s=session(opener=lambda *a,**k:response(b'',404))
        with self.assertRaises(RuntimeError):s.urlopen(yahoo(),timeout=15)
        ref=s.http_attempts[0]['original'];self.assertEqual(ref['bytes'],0);self.assertEqual(s.client.rows[ref['key']],b'')
        def broken(*args,**kwargs):
            result=response(b'partial');result.headers['Content-Length']='100';return result
        s=session(opener=broken)
        with self.assertRaises(acq.AcquisitionError):s.urlopen(yahoo(),timeout=15)
        with self.assertRaises(acq.AcquisitionError):s.finish({})

    def test_missing_optional_input_is_recorded_separately_from_denial(self):
        s=session(Client())
        with self.assertRaises(Exception):s.get_object(Bucket='b',Key='data/portwatch.json')
        self.assertIsNone(s.failure);self.assertEqual(s.operations[-1]['status'],'missing')
        s.client.fail='data/portwatch.json'
        with self.assertRaises(acq.AcquisitionError):s.get_object(Bucket='b',Key='data/portwatch.json')
        self.assertIsNotNone(s.failure)

    def test_unfinished_or_failed_listing_cannot_publish_a_partial_selection(self):
        s=session(Client());iterator=s.get_paginator('list_objects_v2').paginate(Bucket='b',Prefix=acq.WAREHOUSE+'2026/')
        next(iterator)
        with self.assertRaises(acq.AcquisitionError):s.finish({})
        iterator.close();self.assertIsNotNone(s.failure)
        s=session(Client());iterator=s.get_paginator('list_objects_v2').paginate(Bucket='b',Prefix=acq.WAREHOUSE+'2026/')
        next(iterator);iterator.close()
        with self.assertRaises(acq.AcquisitionError):s.put_object(Bucket='b',Key=store.HEAD,Body=store.encode(packet()))


if __name__=='__main__':unittest.main(verbosity=2)
