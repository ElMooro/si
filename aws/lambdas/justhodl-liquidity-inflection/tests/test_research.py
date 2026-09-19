import ast
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(ROOT/'aws/shared/tests'), str(Path(__file__).resolve().parents[1]/'source'), str(ROOT/'scripts')]
import inflection_research_model as model
import inflection_research_store as store
from inflection_research_catalog import SERIES, extend_catalog
from liquidity_calendar import calendar_features, calendar_trend
from report_observations import build as macro_build, encoded, digest
from test_report_observations import inputs, NOW
from vintage_archive_test_support import liquidity_docs
from evidence_store import capture


def fixture():
    original = {sid: inputs(sid, freq, [('2026-09-16', value)], unit) for sid, freq, value, unit in (
        ('WALCL', 'W', '6746548', 'Millions of U.S. Dollars'),
        ('WTREGEN', 'W', '877028', 'Millions of U.S. Dollars'),
        ('RRPONTSYD', 'D', '0.576', 'Billions of US Dollars'),
        ('SOFR', 'D', '4.0', 'Percent'), ('IORB', 'D', '4.1', 'Percent'))}
    source = macro_build(extend_catalog({}), original, NOW)
    source['replay'] = {'manifest_key': 'data/report-research/runs/'+'a'*64+'.json', 'output_sha256': digest(source)}
    return {'macro': source, 'archives': {}, 'auxiliary': {}}, original


class CalendarTests(unittest.TestCase):
    def weeks(self, n=200, fn=lambda i: 1000+2*i):
        start = date(2020, 1, 3)
        return {(start+timedelta(weeks=i)).isoformat(): str(fn(i)) for i in range(n)}

    def test_exact_calendar_window_and_no_double_scaling(self):
        rows = self.weeks(14); result = calendar_trend(rows, max(rows))
        self.assertEqual(result['calendar_days'], 91); self.assertEqual(result['present_weekly_samples'], 14)
        self.assertEqual(result['slope_usd_mn_per_week'], 2)
        inputs_, original = fixture(); out = model.build(inputs_, original, NOW)
        self.assertEqual(out['net_liquidity']['net'], 6746548-877028-576)
        self.assertEqual(out['series']['RRPONTSYD']['current_decimal'], '0.576')
        self.assertEqual(out['funding_spread']['spread_bps'], -10)

    def test_zero_is_data_missing_week_is_not_compacted(self):
        rows = self.weeks(14, lambda i: i); self.assertEqual(calendar_trend(rows, max(rows))['slope_usd_mn_per_week'], 1)
        del rows[sorted(rows)[3]]
        self.assertIsNone(calendar_trend(rows, max(rows))['slope_decimal'])
        self.assertEqual(calendar_trend(rows, max(rows))['present_weekly_samples'], 13)

    def test_acceleration_is_change_of_slope_per_calendar_week(self):
        result = calendar_features(self.weeks(200, lambda i: i*i))['latest']
        self.assertEqual(result['acceleration_usd_mn_per_week2'], 2)
        self.assertEqual(result['z_3y']['status'], 'descriptive')
        constant = calendar_features(self.weeks())['latest']
        self.assertEqual(constant['acceleration_usd_mn_per_week2'], 0)
        self.assertIsNone(constant['z_3y']['z'])

    def test_daily_duplicates_never_inflate_weekly_sample_and_invalid_levels_fail(self):
        weekly = self.weeks(14); daily = dict(weekly)
        for day, value in weekly.items(): daily[(date.fromisoformat(day)+timedelta(days=1)).isoformat()] = value
        self.assertEqual(calendar_features(weekly), calendar_features(daily))
        for value in (True, 1.0, 'NaN', 'Infinity', '1e-999'):
            broken = dict(weekly); broken[max(broken)] = value
            with self.assertRaises(ValueError): calendar_trend(broken, max(broken))


class ModelTests(unittest.TestCase):
    def test_native_observations_preserved_but_no_authority_or_paid_brief(self):
        inputs_, original = fixture(); before = deepcopy((inputs_, original))
        out = model.build(inputs_, original, NOW)
        self.assertEqual(len(out['series']), 20); self.assertEqual(out['quality']['fresh_series'], 5)
        self.assertIsNone(out['composite']['liquidity_score']); self.assertIsNone(out['usd']['impulse_z'])
        self.assertFalse(out['historical_validation']['point_in_time']); self.assertFalse(out['sizing_eligible'])
        self.assertEqual(out['decision']['meaning'], 'abstain'); self.assertEqual(out['signals_logged'], 0)
        self.assertEqual(before, (inputs_, original))

    def test_missing_stale_and_future_are_not_headlines(self):
        inputs_, original = fixture()
        out = model.build(inputs_, original, '2026-09-20T20:00:00Z')
        self.assertEqual(out['quality']['fresh_series'], 0); self.assertIsNone(out['net_liquidity']['net'])
        self.assertIsNone(out['series']['WALCL']['current']); self.assertEqual(out['series']['WALCL']['last_observed_value'], '6746548')
        with self.assertRaises(ValueError): model.build(inputs_, original, '2026-09-17T00:00:00Z')

    def test_mutated_packet_original_and_missing_original_fail(self):
        for kind in ('packet', 'original', 'missing'):
            inputs_, original = fixture()
            if kind == 'packet': inputs_['macro']['measurements']['WALCL']['current'] = 1
            elif kind == 'original': original['WALCL']['observations']['observations'][0]['value'] = '1'
            else: del original['WALCL']
            with self.assertRaises(ValueError): model.build(inputs_, original, NOW)

    def test_every_requested_identity_is_retained_without_substitution(self):
        previous = {'WALCL': {'display_name': 'custom'}, 'ANOTHER': {}}
        extended = extend_catalog(previous)
        self.assertTrue(set(SERIES) <= set(extended)); self.assertEqual(extended['WALCL'], previous['WALCL'])
        self.assertIn('ANOTHER', extended); self.assertEqual(len(previous), 2)

    def test_archive_scope_clocks_and_dated_units_remain_explicit(self):
        inputs_, original = fixture(); inputs_['archives'] = liquidity_docs()
        # These September 9 archives are expired at September 18; regeneration
        # cannot hide that fact or invent a current calendar slope.
        out = model.build(inputs_, original, NOW)
        self.assertEqual(out['calendar_research']['status'], 'BLOCKED')
        self.assertIsNone(out['calendar_research']['latest'])


class StorageError(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Storage:
    def __init__(self): self.objects = {}; self.race = False
    def get_object(self, Bucket, Key):
        if Key not in self.objects: raise StorageError('NoSuchKey')
        raw, meta = self.objects[Key]
        return {'Body': io.BytesIO(raw), 'Metadata': meta, 'ETag': hashlib.sha256(raw).hexdigest()}
    def put_object(self, Bucket, Key, Body, **kw):
        if Key == store.CURRENT and self.race:
            self.race = False; self.objects[Key] = (encoded({'generated_at': '2099-01-01T00:00:00Z'}), {})
            raise StorageError('PreconditionFailed')
        if kw.get('IfNoneMatch') == '*' and Key in self.objects: raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (Key not in self.objects or hashlib.sha256(self.objects[Key][0]).hexdigest() != kw['IfMatch']):
            raise StorageError('PreconditionFailed')
        self.objects[Key] = (Body, kw.get('Metadata', {}))


def prepared():
    client = Storage(); inputs_, original = fixture()
    for sid, item in original.items():
        for part in ('definition', 'observations'):
            item['evidence'][part] = capture(client, 'fixture', 'fred', item['evidence'][part]['source_url'], encoded(item[part]), received_at=datetime.fromisoformat(NOW))
    source = macro_build(extend_catalog({}), original, NOW)
    reviewed = Path(store.report_observations.__file__).read_bytes(); sha = hashlib.sha256(reviewed).hexdigest()
    compiler = {'key': 'data/report-research/compilers/'+sha+'.py', 'sha256': sha}
    client.put_object(Bucket='fixture', Key=compiler['key'], Body=reviewed)
    manifest = {'contract': 'report-research-replay.v1', 'catalog': source['catalog'], 'generated_at': NOW,
        'inputs': {sid: {'evidence': item['evidence'], 'acquired_at': item['acquired_at']} for sid, item in original.items()},
        'compiler': compiler, 'errors': {}, 'output_sha256': digest(source)}
    key = 'data/report-research/runs/'+digest(manifest)+'.json'
    client.put_object(Bucket='fixture', Key=key, Body=encoded(manifest))
    source['replay'] = {'manifest_key': key, 'output_sha256': manifest['output_sha256'], 'compiler_sha256': sha}
    client.put_object(Bucket='fixture', Key='data/report-measurements.json', Body=encoded(source))
    client.put_object(Bucket='fixture', Key='data/vintage/_index.json', Body=encoded({'contract': 'fred-vintage-index.v1', 'generated_at': NOW, 'detail': {}}))
    return client, source, original


class Frozen(datetime):
    @classmethod
    def now(cls, tz=None): return datetime.fromisoformat(NOW)


class StoreTests(unittest.TestCase):
    def test_malformed_optional_context_is_retained_without_disabling_verified_core(self):
        client, _, _ = prepared(); broken=b'{truncated'
        client.put_object(Bucket='fixture',Key='data/stablecoin-flow.json',Body=broken)
        with patch.object(store,'datetime',Frozen):result=store.run(client,'fixture')
        self.assertTrue(result['published'])
        packet=json.loads(client.objects[store.CURRENT][0]);context=packet['retained_context']['stablecoin-flow']
        self.assertEqual(context['status'],'unparsed_context');self.assertEqual(client.objects[context['key']][0],broken)
        self.assertIsNotNone(packet['net_liquidity']['net'])

    def test_archive_originals_are_replayed_before_a_packet_can_publish(self):
        import fred_vintage_model as vintage
        from vintage_archive_test_support import inputs as archive_inputs
        from inflection_sources import archive_originals
        client, _, _ = prepared()
        body = Path(vintage.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest()
        compiler = {'key': vintage.PREFIX+'compilers/'+sha+'.py', 'sha256': sha}
        client.put_object(Bucket='fixture', Key=compiler['key'], Body=body)
        index = {'contract': 'fred-vintage-index.v1', 'generated_at': '2026-09-09T00:00:00Z', 'collection_id': 'fixture', 'detail': {}}
        for sid, value, unit in (('WALCL','10000','Millions of U.S. Dollars'),('WTREGEN','1','Billions of U.S. Dollars'),('RRPONTSYD','2','Billions of US Dollars')):
            definition, page = archive_inputs(sid, value, unit)
            for item in (definition, page): client.put_object(Bucket='fixture', Key=item['evidence']['key'], Body=gzip.compress(item['raw']))
            doc = vintage.compile_series(sid, definition, [page], index['generated_at'], 'fixture', '2026-09-08', '2026-09-08T19:00:00Z')
            manifest = {'contract': 'fred-vintage-replay.v1', 'series': sid, 'generated_at': doc['generated_at'], 'collection_id': 'fixture',
                'archive_end': '2026-09-08', 'collection_started_at': '2026-09-08T19:00:00Z',
                'definition': {k:v for k,v in definition.items() if k!='raw'}, 'pages': [{k:v for k,v in page.items() if k!='raw'}],
                'compiler': compiler, 'output_sha256': digest(doc)}
            key = vintage.PREFIX+'runs/'+digest(manifest)+'.json'; client.put_object(Bucket='fixture', Key=key, Body=encoded(manifest))
            doc['replay'] = {'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler':compiler}
            key = vintage.PREFIX+'outputs/'+digest(doc)+'.json'; client.put_object(Bucket='fixture', Key=key, Body=encoded(doc))
            index['detail'][sid] = {'status':'source_replayed','key':key,'sha256':digest(doc)}
        client.put_object(Bucket='fixture', Key='data/vintage/_index.json', Body=encoded(index))
        read = store.raw_reader(client, 'fixture'); archive = archive_originals(index, read)
        self.assertEqual(len(archive), 3)
        self.assertEqual(vintage.net_liquidity(archive, datetime.fromisoformat(index['generated_at']))['series_decimal']['2026-09-08'],'7000')
        with patch.object(store, 'datetime', Frozen): result = store.run(client, 'fixture')
        from replay_inflection_research import replay
        manifest = json.loads(read(result['replay']['manifest_key']))
        self.assertEqual(replay(manifest, read)['calendar_research']['status'], 'BLOCKED')  # expired, still retained
        client.objects[page['evidence']['key']] = (gzip.compress(b'{}'), {})
        with self.assertRaises(ValueError): archive_originals(index, read)

    def test_full_replay_original_corruption_and_legacy_preservation(self):
        client, source, original = prepared(); legacy = encoded({'generated_at': '2020-01-01T00:00:00Z', 'forecast': [999]})
        client.put_object(Bucket='fixture', Key=store.CURRENT, Body=legacy)
        with patch.object(store, 'datetime', Frozen): result = store.run(client, 'fixture')
        self.assertTrue(result['published']); self.assertTrue(result['brief_published'])
        packet = json.loads(client.objects[store.CURRENT][0]); self.assertEqual(client.objects[packet['legacy_context']['key']][0], legacy)
        from replay_inflection_research import replay
        read = store.raw_reader(client, 'fixture'); manifest = json.loads(read(packet['replay']['manifest_key']))
        self.assertEqual(replay(manifest, read), {k: v for k, v in packet.items() if k != 'replay'})
        ref = original['RRPONTSYD']['evidence']['observations']; client.objects[ref['key']] = (gzip.compress(b'{}'), {})
        with self.assertRaises(ValueError): replay(manifest, read)

    def test_corruption_blocks_current_write_and_race_never_overwrites_newer(self):
        client, source, original = prepared(); ref = original['WALCL']['evidence']['definition']
        client.objects[ref['key']] = (gzip.compress(b'{}'), {})
        with patch.object(store, 'datetime', Frozen), self.assertRaises(ValueError): store.run(client, 'fixture')
        self.assertNotIn(store.CURRENT, client.objects)
        client, _, _ = prepared(); client.race = True
        with patch.object(store, 'datetime', Frozen): result = store.run(client, 'fixture')
        self.assertFalse(result['published']); self.assertNotIn(store.BRIEF, client.objects)

    def test_actual_consumer_normalizers_abstain_even_when_legacy_scalar_present(self):
        path = ROOT/'aws/lambdas/justhodl-signal-board/source/lambda_function.py'
        tree = ast.parse(path.read_text(encoding='utf-8')); names = {'n_liquidity_inflection', 'n_us_money'}
        ns = {}; exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names], type_ignores=[]), str(path), 'exec'), ns)
        packet = {'usd': {'impulse_z': 9}, 'us_money': {'z': 9}, 'calls_eligible': False}
        for name in names: self.assertIsNone(ns[name](packet)[0])

    def test_risk_regime_and_ranker_cannot_reauthorize_legacy_liquidity(self):
        packet = {'composite': {'liquidity_score': 100, 'regime': 'EXPANDING'}, 'trajectory': {'heading': 'EASING AHEAD'}}
        for fn, name, ns in (
            ('risk-regime','liquidity_block',{'_read':lambda key:packet}),
            ('master-ranker','get_regime_context',{'fetch_json':lambda key,**kw:packet if 'liquidity-inflection' in key else {},'REGIME_FORWARDS':{}})):
            path = ROOT/f'aws/lambdas/justhodl-{fn}/source/lambda_function.py'
            node = next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name==name)
            exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),ns)
            result = ns[name]()
            self.assertIsNone(result[0] if fn=='risk-regime' else result['liquidity_score'])

    def test_actual_handler_routes_only_to_public_research_and_sanitizes_errors(self):
        path = ROOT/'aws/lambdas/justhodl-liquidity-inflection/source/lambda_function.py'
        node = next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        ns = {'S3':object(),'BUCKET':'fixture','json':json}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),ns)
        with patch.object(store,'run',return_value={'published':True,'brief_published':True}) as run:
            self.assertEqual(ns['lambda_handler']({'action':'legacy'},None)['statusCode'],200);run.assert_called_once()
        with patch.object(store,'run',side_effect=ValueError('URL?api_key=private')):
            result=ns['lambda_handler']();self.assertEqual(result['statusCode'],503);self.assertNotIn('private',result['body'])


if __name__ == '__main__': unittest.main()
