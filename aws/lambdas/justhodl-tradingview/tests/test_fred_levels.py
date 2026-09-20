import copy
from datetime import datetime, timezone
import gzip
import io
import json
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1]/'source'
sys.path[:0] = [str(SOURCE), str(ROOT/'aws/shared'), str(ROOT/'scripts')]
import fred_level_model as model
from fred_level_io import Collector, publish, refresh_public
from replay_fred_levels import verify_vault

NOW = datetime(2026, 9, 20, tzinfo=timezone.utc)


def fixture(sid='CFNAI', values=('-0.08', '0.06')):
    obs = {'units': 'lin', 'output_type': 1, 'offset': 0, 'sort_order': 'desc', 'count': len(values), 'limit': 32,
           'realtime_start': '2026-09-20', 'realtime_end': '2026-09-20', 'observations': [
               {'date': day, 'value': value, 'realtime_start': '2026-09-20', 'realtime_end': '2026-09-20'}
               for day, value in zip(('2026-07-01', '2026-06-01', '2026-05-01'), values)]}
    definition = {'seriess': [{'id': sid, 'units': 'Index', 'frequency_short': 'M',
                              'realtime_start': '2026-09-20', 'realtime_end': '2026-09-20',
                              'seasonal_adjustment': 'Seasonally Adjusted', 'title': 'Fixture series'}]}
    return obs, definition


def compile_fixture(obs=None, definition=None):
    a,b = fixture()
    return model.compile_level('CFNAI', model.encoded(obs or a), model.encoded(definition or b), NOW.isoformat())


class Conflict(Exception):
    response = {'Error': {'Code': 'PreconditionFailed'}}


class Store:
    def __init__(self): self.objects = {}; self.puts = []; self.gets = []
    def put_object(self, **kw):
        key = kw['Key']; old = self.objects.get(key)
        if kw.get('IfNoneMatch') == '*' and old: raise Conflict()
        if kw.get('IfMatch') and (not old or old['ETag'] != kw['IfMatch']): raise Conflict()
        self.objects[key] = {**kw, 'ETag': model.digest(kw['Body'])}; self.puts.append(key)
    def get_object(self, **kw):
        self.gets.append(kw['Key']); item = self.objects[kw['Key']]
        return {**item, 'Body': io.BytesIO(item['Body'])}
    def read(self, key):
        raw = self.objects[key]['Body']
        return gzip.decompress(raw) if key.endswith('.gz') else raw


def collector(store, fail=False):
    requests = []
    def open_source(req, timeout):
        requests.append(req.full_url)
        if fail: raise TimeoutError('secret URL must not be reported')
        sid = parse_qs(urlparse(req.full_url).query)['series_id'][0]
        obs, definition = fixture(sid)
        return io.BytesIO(model.encoded(obs if '/observations?' in req.full_url else definition))
    return Collector(store, 'test', 'fixture-secret', NOW, open_source, lambda seconds: None), requests


class NativeLevels(unittest.TestCase):
    def test_signed_level_and_dated_baseline(self):
        out = compile_fixture()
        self.assertEqual(out['change'], -0.14); self.assertEqual(out['exact']['change'], '-0.14')
        self.assertIsNone(out['chg_pct']); self.assertEqual(out['previous_observation_date'], '2026-06-01')
        self.assertFalse(out['quality']['historical_availability_verified'])
        self.assertIsNone(out['published_at'])

    def test_zero_flat_and_rate_points(self):
        obs, definition = fixture(values=('0', '0'))
        definition['seriess'][0]['units'] = 'Percent'
        out = compile_fixture(obs, definition)
        self.assertEqual(out['change'], 0); self.assertEqual(out['change_unit'], 'percentage_points')
        self.assertIsNone(out['chg_pct'])

    def test_exact_delta_survives_large_levels_and_underflow_is_not_zero(self):
        obs, definition=fixture(values=('123456789012345678901234567890.1','123456789012345678901234567890'))
        out=compile_fixture(obs,definition);self.assertEqual(out['exact']['change'],'0.1')
        obs['observations'][0]['value']='1e-9999'
        with self.assertRaises(ValueError):compile_fixture(obs,definition)

    def test_missing_latest_or_previous_does_not_skip_to_another_period(self):
        obs, definition = fixture(values=('.', '0.06', '-0.1'))
        out = compile_fixture(obs, definition); self.assertIsNone(out['value'])
        self.assertEqual(out['observation_date'], '2026-07-01'); self.assertIsNone(out['change'])
        obs, definition = fixture(values=('0.1', '.', '0.05'))
        out = compile_fixture(obs, definition); self.assertIsNone(out['prev']); self.assertIsNone(out['change'])

    def test_bad_identity_units_vintage_shape_and_values_fail(self):
        for field, value in (('units', 'pc1'), ('offset', 1), ('count', 33), ('realtime_end', '2026-09-19')):
            obs, definition = fixture(); obs[field] = value
            with self.assertRaises(ValueError): compile_fixture(obs, definition)
        for value in (True, 'NaN', 'Infinity', 'abc'):
            obs, definition = fixture(); obs['observations'][0]['value'] = value
            with self.assertRaises(ValueError): compile_fixture(obs, definition)
        for day in ('2026-09-21', '2026-06-01'):
            obs, definition = fixture(); obs['observations'][0]['date'] = day
            with self.assertRaises(ValueError): compile_fixture(obs, definition)
        obs, definition = fixture(); definition['seriess'][0]['id'] = 'WRONG'
        with self.assertRaises(ValueError): compile_fixture(obs, definition)

    def test_observation_age_is_separate_from_collection_clock(self):
        obs, definition = fixture(); definition['seriess'][0]['frequency_short'] = 'D'
        out = compile_fixture(obs, definition)
        self.assertEqual(out['status'], 'STALE'); self.assertEqual(out['fetched_at'], NOW.isoformat())

    def test_aliases_share_one_source_and_full_original_replay(self):
        s3=Store(); coll,requests=collector(s3)
        a=coll.get('CFNAI'); b=coll.get('CFNAI'); b['value']=999
        self.assertEqual(len(requests),2); self.assertEqual(coll.get('CFNAI')['value'],-0.08)
        self.assertNotIn('fixture-secret',model.encoded(a).decode())
        self.assertEqual(verify_vault({'symbols':[a,coll.get('CFNAI')]},s3.read),
                         {'series_vintages_replayed':1,'alias_rows_checked':2})
        a['prev']=42
        with self.assertRaises(ValueError): verify_vault({'symbols':[a]},s3.read)

    def test_source_tampering_and_failed_capture_cannot_pass(self):
        s3=Store(); coll,_=collector(s3); a=coll.get('CFNAI')
        s3.objects[a['evidence']['observations']['key']]['Body']=gzip.compress(b'{}')
        with self.assertRaises(ValueError): verify_vault({'symbols':[a]},s3.read)
        class Denied(Store):
            def put_object(self,**kw): raise PermissionError('denied')
        coll,_=collector(Denied()); self.assertIsNone(coll.get('CFNAI'))

    def test_failed_and_budgeted_fetches_are_memoized(self):
        coll,req=collector(Store(),fail=True)
        self.assertIsNone(coll.get('CFNAI')); self.assertIsNone(coll.get('CFNAI'))
        self.assertEqual(len(req),1); self.assertEqual(coll.failures,{'CFNAI':'TimeoutError'})
        self.assertIsNone(coll.get('DGS10',allowed=False)); self.assertEqual(len(req),1)

    def test_yoy_and_explicit_other_provider_win_over_old_level(self):
        row={'symbol':'USIRYY','resolved_via':'fred:CPIAUCSL'}
        self.assertIsNone(model.series_for(row,{'USIRYY':'yoy:CPIAUCSL'}))
        self.assertIsNone(model.series_for(row,{'USIRYY':'yahoo:OTHER'}))
        self.assertEqual(model.series_for({'symbol':'CFNAI','resolved_via':'fred:CFNAI'},{}),'CFNAI')

    def test_public_refresh_preserves_catalog_clock_other_rows_and_predecessor(self):
        s3=Store(); packet={'generated_at':'2026-09-19T11:37:32Z','symbols':[
            {'symbol':'CFNAI','status':'LIVE','value':1,'resolved_via':'fred:CFNAI'},
            {'symbol':'USCFNAI','status':'LIVE','value':2,'resolved_via':'fred:CFNAI'},
            {'symbol':'AAPL','status':'LIVE','value':200}]}
        raw=model.encoded(packet); s3.put_object(Key='data/tradingview.json',Body=raw)
        coll,req=collector(s3); result=refresh_public(s3,'test','data/tradingview.json',coll,{'USCFNAI':'fred:CFNAI'})
        out=json.loads(s3.read('data/tradingview.json'))
        self.assertEqual(result['updated_rows'],2); self.assertEqual(len(req),2)
        self.assertEqual(out['generated_at'],packet['generated_at']); self.assertEqual(out['symbols'][2],packet['symbols'][2])
        self.assertEqual(out['symbols'][0]['replay'],out['symbols'][1]['replay'])
        self.assertEqual(s3.read('audit-private/20260909-originals/fred-vault/'+model.digest(raw)+'.bin'),raw)
        self.assertTrue(all('brain' not in key for key in s3.gets))

    def test_concurrent_pointer_change_cannot_be_overwritten(self):
        s3=Store(); s3.put_object(Key='data/tradingview.json',Body=b'{"newer":true}')
        with self.assertRaises(Conflict): publish(s3,'test','data/tradingview.json',{'stale':True},b'{}','old-etag')
        self.assertEqual(s3.read('data/tradingview.json'),b'{"newer":true}')

    def test_actual_handler_public_mode_never_reads_private_registry(self):
        s3=Store(); coll,_=collector(s3)
        s3.put_object(Key='data/tradingview.json',Body=model.encoded({'symbols':[
            {'symbol':'USCFNAI','status':'LIVE','value':2,'resolved_via':'fred:CFNAI'}]}))
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:s3),
                                     'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'fixture')}):
            env=runpy.run_path(str(SOURCE/'lambda_function.py'))
            env['lambda_handler'].__globals__['Collector']=lambda *a,**k:coll
            env['lambda_handler'].__globals__['get_brain']=lambda:(_ for _ in ()).throw(AssertionError('private read'))
            out=env['lambda_handler']({'public_fred_refresh':True,'series':['CFNAI']},None)
        self.assertTrue(out['ok']); self.assertEqual(out['updated_rows'],1)

    def test_scheduled_handler_bypasses_symbol_caches_and_shares_canonical_series(self):
        s3=Store(); coll,requests=collector(s3)
        old={'generated_at':'2026-09-19T11:37:32Z','symbols':[
            {'symbol':sym,'status':'LIVE','value':value,'resolved_via':'fred:CFNAI','fetched_at':'2026-09-19T11:37:32Z'}
            for sym,value in [('CFNAI',1),('USCFNAI',2)]]}
        s3.put_object(Key='data/tradingview.json',Body=model.encoded(old))
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:s3),
                                     'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'fixture')}):
            env=runpy.run_path(str(SOURCE/'lambda_function.py')); scope=env['lambda_handler'].__globals__
            scope.update(Collector=lambda *a,**k:coll, ALIASES={'USCFNAI':'fred:CFNAI'},
                get_brain=lambda:{}, build_registry=lambda brain:{'CFNAI':{'symbol':'CFNAI','exchanges':[], 'n_notes':1,'note_ids':[]}},
                gen_aliases=lambda:{}, fmp_quotes=lambda symbols:{}, _families=lambda:None, _fleet_prices=lambda:None)
            scope['lambda_handler']({},None)
        rows=json.loads(s3.read('data/tradingview.json'))['symbols']
        self.assertEqual(len(rows),2);self.assertEqual(len(requests),2)
        self.assertEqual([r['value'] for r in rows],[-0.08,-0.08])
        self.assertEqual(rows[0]['replay'],rows[1]['replay'])

    def test_failed_identity_change_preserves_the_old_measurement_identity(self):
        row={'symbol':'TEST','value':1,'source':'yahoo:TEST','resolved_via':'yahoo:TEST','asof':'2026-09-18'}
        model.mark_unavailable(row,'NEW')
        self.assertEqual(row['source'],'yahoo:TEST');self.assertEqual(row['asof'],'2026-09-18')
        self.assertEqual(row['expected_series_id'],'NEW');self.assertEqual(row['status'],'STALE')

    def test_retained_source_replay_overrules_altered_cache_without_refetch(self):
        s3=Store(); coll,_=collector(s3); old=coll.get('CFNAI');old['value']=999
        cached,requests=collector(s3);cached.seed([old]);result=cached.get('CFNAI',allowed=False)
        self.assertEqual(result['value'],-0.08);self.assertEqual(requests,[])
        self.assertEqual(cached.reused,{'CFNAI'});self.assertEqual(result['fetched_at'],old['fetched_at'])
        later,_=collector(s3);later.now=datetime(2026,9,22,tzinfo=timezone.utc);later.seed([old])
        self.assertIsNone(later.get('CFNAI',allowed=False))

    def test_bounded_rotation_advances_after_failures_without_renewing_collection(self):
        s3=Store();packet={'generated_at':'2026-09-19T11:00:00Z','symbols':[
            {'symbol':f'S{i:03}','resolved_via':f'fred:S{i:03}','status':'LIVE','value':i} for i in range(65)]}
        s3.put_object(Key='data/tradingview.json',Body=model.encoded(packet))
        coll,_=collector(s3,fail=True);a=refresh_public(s3,'test','data/tradingview.json',coll,{})
        self.assertEqual(len(a['failures']),64);self.assertNotIn('S064',a['failures'])
        coll,_=collector(s3,fail=True);b=refresh_public(s3,'test','data/tradingview.json',coll,{})
        self.assertIn('S064',b['failures']);self.assertEqual(len(b['failures']),64)
        self.assertEqual(json.loads(s3.read('data/tradingview.json'))['generated_at'],packet['generated_at'])


if __name__=='__main__': unittest.main()
