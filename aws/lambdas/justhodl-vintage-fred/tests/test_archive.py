import ast
from copy import deepcopy
from datetime import datetime,timezone
import hashlib,io,json,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
import fred_vintage_model as m
import vintage_source_store as store
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'shared/tests'))
from vintage_archive_test_support import inputs,record,packet,seal,liquidity_docs,STAMP


class MemoryS3:
    def __init__(self):self.objects={};self.on_write=None
    def error(self,code):
        exc=RuntimeError(code);exc.response={'Error':{'Code':code}};return exc
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise self.error('NoSuchKey')
        raw=self.objects[kw['Key']];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        if self.on_write:self.on_write(kw)
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise self.error('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise self.error('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body']


class ArchiveTests(unittest.TestCase):
    def test_provider_calendar_does_not_advance_at_utc_midnight(self):
        self.assertEqual(store.archive_cutoff(m.clock('2026-09-19T04:00:00Z')),'2026-09-18')
        self.assertEqual(store.archive_cutoff(m.clock('2026-09-19T06:00:00Z')),'2026-09-19')
        self.assertEqual(store.archive_cutoff(m.clock('2026-01-19T05:30:00Z')),'2026-01-18')
        with self.assertRaises(ValueError):store.archive_cutoff(datetime(2026,9,19))

    def test_bounded_windows_cover_every_day_and_never_hit_json_vintage_limit(self):
        spans=m.windows('2000-02-29','2026-09-18')
        self.assertEqual(spans[0][0],'2000-02-29');self.assertEqual(spans[-1][1],'2026-09-18')
        for i,(a,b) in enumerate(spans):
            self.assertLessEqual((m.day(b)-m.day(a)).days+1,1461)
            if i:self.assertEqual((m.day(a)-m.day(spans[i-1][1])).days,1)
        self.assertEqual(m.request('WALCL')['params']['realtime_end'],'9999-12-31')
        self.assertNotIn('api_key',m.encoded(m.request('WALCL')).decode())

    def test_exact_decimal_zero_and_clipped_boundary_not_fake_release(self):
        d=packet(value='0.00000000000000001');row=d['vintages'][0]
        self.assertEqual(row['value_decimal'],'0.00000000000000001')
        self.assertTrue(row['start_left_censored']);self.assertIsNone(row['known_on'])
        self.assertFalse(d['point_in_time']);self.assertFalse(d['calls_eligible'])
        d=packet(value='0');self.assertEqual(m.select_asof(d,'2026-09-03T12:00:00Z')['selected']['value'],0)

    def test_dated_definitions_and_revision_validity_are_selected_together(self):
        definition,page=inputs();meta=json.loads(definition['raw']);body=json.loads(page['raw'])
        meta['seriess'][0]['realtime_end']='2026-09-03';meta['seriess'][0]['units']='Billions of Dollars'
        meta['seriess'].append({**meta['seriess'][0],'realtime_start':'2026-09-04','realtime_end':m.MAX_DATE,'units':'Millions of U.S. Dollars'})
        body['observations'][0].update(realtime_end='2026-09-03',value='10')
        body['observations'].append({**body['observations'][0],'realtime_start':'2026-09-04','realtime_end':'2026-09-08','value':'10000'})
        body['count']=2
        d=seal(m.compile_series('WALCL',record('WALCL','series',meta),[record('WALCL','series/observations',body,'2026-09-01','2026-09-08')],STAMP,'one','2026-09-08','2026-09-08T19:00:00Z'))
        before=m.select_asof(d,'2026-09-05T11:59:59Z')['selected'];after=m.select_asof(d,'2026-09-05T12:00:00Z')['selected']
        self.assertEqual((before['value_decimal'],before['units']),('10','Billions of Dollars'))
        self.assertEqual((after['value_decimal'],after['units']),('10000','Millions of U.S. Dollars'))
        self.assertNotEqual(before['definition_index'],after['definition_index'])

    def test_missing_latest_observation_never_falls_back_to_an_older_value(self):
        definition,page=inputs();body=json.loads(page['raw'])
        body['observations'].append({'date':'2026-09-02','realtime_start':'2026-09-04','realtime_end':'2026-09-08','value':'.'});body['count']=2
        d=seal(m.compile_series('WALCL',definition,[record('WALCL','series/observations',body,'2026-09-01','2026-09-08')],STAMP,'one','2026-09-08','2026-09-08T19:00:00Z'))
        out=m.select_asof(d,'2026-09-05T12:00:00Z');self.assertEqual(out['status'],'missing')
        self.assertIsNone(out['selected']['value_decimal']);self.assertEqual(d['coverage']['missing_periods'],1)

    def test_original_binding_wrong_type_truncation_and_overlaps_fail_closed(self):
        definition,page=inputs()
        for mode in ('raw','count','type','overlap','date','missing_clock','infinity'):
            with self.subTest(mode=mode):
                bad=deepcopy(page);body=json.loads(bad['raw'])
                if mode=='raw':bad['raw']+=b' '
                else:
                    if mode=='count':body['count']=2
                    if mode=='type':body['output_type']=4
                    if mode=='overlap':body['count']=2;body['observations']*=2
                    if mode=='date':body['observations'][0]['realtime_end']='2026-09-20'
                    if mode=='missing_clock':del body['observations'][0]['realtime_start']
                    if mode=='infinity':body['observations'][0]['value']='1e9999'
                    bad=record('WALCL','series/observations',body,'2026-09-01','2026-09-08')
                with self.assertRaises((ValueError,KeyError)):
                    m.compile_series('WALCL',definition,[bad],STAMP,'one','2026-09-08','2026-09-08T19:00:00Z')
        with self.assertRaises(ValueError):m.compile_series('WALCL',definition,[],STAMP,'one','2026-09-08','2026-09-08T19:00:00Z')

    def test_tampered_unbound_future_or_outside_archive_selection(self):
        d=packet();d['vintages'][0]['value_decimal']='9'
        with self.assertRaises(ValueError):m.select_asof(d,'2026-09-05T12:00:00Z')
        with self.assertRaises(ValueError):m.select_asof({'vintages':[]},STAMP)
        self.assertEqual(m.select_asof(packet(),'2020-01-01T12:00:00Z')['status'],'outside_retained_archive')
        self.assertEqual(m.select_asof(packet(),'2030-01-01T12:00:00Z')['status'],'outside_retained_archive')

    def test_net_liquidity_uses_historical_units_and_retains_no_study_permission(self):
        docs=liquidity_docs();out=m.net_liquidity(docs,m.clock(STAMP))
        self.assertEqual(out['status'],'ARCHIVE_RESEARCH');self.assertEqual(out['series']['2026-09-02'],7000)
        self.assertEqual(out['components']['2026-09-02']['WTREGEN']['value_usd_mn_decimal'],'1000')
        self.assertFalse(out['point_in_time']);self.assertFalse(out['publication_eligible'])
        self.assertNotIn('2026-09-01',out['series'])
        docs['WTREGEN']['collection_id']='other';docs['WTREGEN']=seal(docs['WTREGEN'])
        self.assertEqual(m.net_liquidity(docs,m.clock(STAMP))['series'],{})
        self.assertIn('different collections',m.net_liquidity(docs,m.clock(STAMP))['reason'])

    def test_cas_preserves_newer_collection_even_when_older_finishes_later(self):
        client=MemoryS3();old=packet();key='data/vintage/WALCL.json'
        old['collection_started_at']='2026-09-09T10:00:00Z';old['generated_at']='2026-09-09T11:00:00Z'
        client.objects[key]=m.encoded(old)
        candidate=deepcopy(old);candidate['collection_started_at']='2026-09-09T09:00:00Z';candidate['generated_at']='2026-09-09T12:00:00Z'
        self.assertFalse(store.publish_current(client,'test',key,candidate));self.assertEqual(json.loads(client.objects[key]),old)

    def test_conditional_race_rechecks_winner_and_retains_legacy_bytes(self):
        client=MemoryS3();key='data/vintage/WALCL.json';legacy=m.encoded({'updated':'2020-01-01T00:00:00Z','vintages':[{'value':2}]})
        client.objects[key]=legacy
        self.assertTrue(store.publish_current(client,'test',key,packet()))
        self.assertEqual(client.objects[m.PREFIX+'legacy-unvalidated/'+hashlib.sha256(legacy).hexdigest()+'.json'],legacy)
        newer=packet();newer['generated_at']='2030-01-01T00:00:00Z'
        def race(kw):
            if kw['Key']==key:client.objects[key]=m.encoded(newer);client.on_write=None
        client.on_write=race
        self.assertFalse(store.publish_current(client,'test',key,packet()))
        self.assertEqual(json.loads(client.objects[key]),newer)

    def test_actual_series_store_replays_retained_originals_before_returning_reference(self):
        import gzip
        client=MemoryS3();definition,page=inputs()
        def acquire(client,bucket,req,*args):
            result=definition if req['endpoint']=='series' else page
            self.assertEqual(req,result['request'])
            store.immutable(client,bucket,result['evidence']['key'],gzip.compress(result['raw'],mtime=0),'application/gzip')
            return result
        with patch.object(store,'acquire',acquire),patch.object(store,'now',return_value=STAMP):
            entry=store.collect_series(client,'bucket','WALCL','managed',99999,'one-collection','2026-09-08',{'sha256':'a'*64,'key':'reviewed'},'2026-09-08T19:00:00Z')
        doc=json.loads(client.objects[entry['key']]);m.validate_packet(doc)
        self.assertEqual(entry['sha256'],m.digest(doc));self.assertEqual(doc['vintages'][0]['value'],10000)
        self.assertNotIn('data/vintage/WALCL.json',client.objects,'collection pointer is published separately')
        manifest=json.loads(client.objects[entry['replay']['manifest_key']])
        self.assertEqual(manifest['pages'][0]['evidence'],page['evidence'])

    def test_active_handler_never_enters_legacy_collector(self):
        source=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
        tree=ast.parse(source.read_text(encoding='utf-8'));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        seen=[];fake=types.SimpleNamespace(run=lambda *args:seen.append(args) or {'ok':True})
        scope={'s3':'client','BUCKET':'bucket','FRED_KEY':'managed'}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual_handler','exec'),scope)
        with patch.dict(sys.modules,{'vintage_source_store':fake}):self.assertTrue(scope['lambda_handler']()['ok'])
        self.assertEqual(seen,[('client','bucket','managed')])


if __name__=='__main__':unittest.main()
