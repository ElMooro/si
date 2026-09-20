from copy import deepcopy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

import capital_research as m
import capital_store as s
from test_capital_bridge import fixture as manager, position

AT = '2026-09-20T03:00:00+00:00'


def fixture():
    objects = {}
    def retain(prefix, kind, value):
        raw = m.encoded(value); key = prefix+kind+'/'+m.digest(value)+'.json'; objects[key] = raw
        return m.reference(key, raw)
    native = {'manifest_key': m.NATIVE+'runs/'+'a'*64+'.json', 'output_sha256': 'b'*64}
    detail = manager([position('111111111', 10, 100)], [position('111111111', 12, 144), position('222222222', 1, 3)])
    detail_ref = retain(m.NATIVE, 'funds', detail)
    holdings = {'contract': 'holdings-canonical.v1', 'research': native, 'generated_at': AT, 'source_generated_at': AT,
        'required_period_for_current_cohort': '2026-06-30', 'funds_total': 1, 'manager_comparison_count': 2,
        'by_fund': {'SYNTHETIC': {'cik': detail['cik'], 'official_name': detail['official_name'], 'positions_ref': detail_ref,
            'period_of_report': '2026-06-30', 'positions_count': 2, 'current_chain_status': m.bridge.COMPLETE}}}
    product = retain(m.CANONICAL, 'products', holdings)
    ref = retain(m.CANONICAL, 'runs', {'contract': 'holdings-canonical-replay.v1', 'research': native,
                'products': {'data/13f-positions.json': product}})
    objects['data/13f-positions.json'] = m.encoded({**holdings, 'canonical_replay': ref})
    flows = {'contract': 'global-flow-research.v1', 'generated_at': AT, 'source_generated_at': AT,
        'period': {'start_date': '2026-09-10', 'end_date': '2026-09-17', 'issuer_observations': 5},
        'funds': {'TEST': {'measurement': 'retained fixture', 'comparison': {'value_decimal': '-7.5'}}},
        'tic_context': {'source_status': 'unavailable'}, 'quality': {'status': 'partial'},
        'dependency_roots': ['ETF_ISSUER_OBSERVATIONS']}
    product = retain(m.FLOW, 'outputs', flows)
    ref = retain(m.FLOW, 'runs', {'contract': 'flow-desk-replay.v1', 'output': product, 'output_sha256': m.digest(flows)})
    objects['data/global-flow-desk.json'] = m.encoded({**flows, 'replay': {'manifest_key': ref['key'], 'output_sha256': m.digest(flows)}})
    objects[m.CURRENT] = b'{"engine":"capital-flow","version":"2.0","accumulating":[{"flow_score":999999}],"whole":"legacy"}'
    objects[s.HISTORY] = b'{"entries":[{"date":"2026-09-19","flagged_scores":{"SYNTHETIC":999999}}]}'
    return objects


class Error(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Client:
    def __init__(self, objects): self.objects = deepcopy(objects); self.writes = []; self.race = None
    def get_object(self, **kw):
        if kw['Key'] not in self.objects: raise Error('NoSuchKey')
        body = self.objects[kw['Key']]
        return {'Body': io.BytesIO(body), 'ETag': hashlib.sha256(body).hexdigest()}
    def put_object(self, **kw):
        key = kw['Key']
        if key == m.CURRENT and self.race: self.objects[key] = self.race
        before = self.objects.get(key)
        if kw.get('IfNoneMatch') == '*' and before is not None: raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (before is None or hashlib.sha256(before).hexdigest() != kw['IfMatch']): raise Error('PreconditionFailed')
        self.objects[key] = kw['Body']; self.writes.append(key)


class Tests(unittest.TestCase):
    def test_publish_complete_research_preserves_legacy_packet_and_history(self):
        objects = fixture(); c = Client(objects); receipt = s.run(c, 'fixture', AT)
        self.assertTrue(receipt['published'])
        packet = m.decode(c.objects[m.CURRENT]); self.assertEqual(packet['counts']['disclosure_comparisons'], 2)
        self.assertEqual(packet['counts']['configured_etfs'], 1)
        self.assertEqual(packet['fund_issuance']['funds']['TEST']['comparison']['value_decimal'], '-7.5')
        self.assertEqual(packet['foreign_transactions']['context']['source_status'], 'unavailable')
        self.assertEqual(packet['dependency_roots'], ['ETF_ISSUER_OBSERVATIONS', 'US_SEC:13F'])
        self.assertFalse(packet['calls_eligible']); self.assertIsNone(packet['portfolio_impact'])
        self.assertIsNone(packet['summary']['n_scored']); self.assertEqual(packet['accumulating'], [])
        for key in (m.CURRENT, s.HISTORY):
            self.assertEqual(c.objects[receipt['whole_preceding_products'][key]['private_key']], objects[key])
            self.assertEqual(c.objects[packet['legacy_contexts'][key]['artifact']['key']], objects[key])
        self.assertEqual(c.objects[s.HISTORY], objects[s.HISTORY]); self.assertNotIn(s.HISTORY, c.writes)
        manifest = m.verified(receipt['replay'], c.objects.__getitem__, m.PREFIX, 'runs')
        replayed = s.replay(manifest, c.objects.__getitem__)
        self.assertEqual(packet, {**replayed, 'replay': receipt['replay']})
        del c.objects['data/13f-positions.json']; del c.objects['data/global-flow-desk.json']
        self.assertEqual(s.replay(manifest, c.objects.__getitem__), replayed)
        key = replayed['managers']['SYNTHETIC']['bridge']['key']; c.objects[key] += b' '
        with self.assertRaisesRegex(ValueError, 'manager bridge replay'): s.replay(manifest, c.objects.__getitem__)

    def test_noop_does_not_refresh_clocks_but_expiry_publishes_stale(self):
        c = Client(fixture()); s.run(c, 'fixture', AT); before = c.objects[m.CURRENT]; writes = len(c.writes)
        out = s.run(c, 'fixture', '2026-09-21T03:00:00+00:00')
        self.assertFalse(out['published']); self.assertEqual(c.objects[m.CURRENT], before); self.assertEqual(len(c.writes), writes)
        out = s.run(c, 'fixture', '2026-09-23T03:00:00+00:00'); self.assertTrue(out['published'])
        packet = m.decode(c.objects[m.CURRENT]); self.assertEqual(packet['quality']['status'], 'stale')
        self.assertEqual(packet['source_clocks']['holdings']['source_generated_at'], AT)
        self.assertEqual(packet['source_clocks']['fund_flows']['fresh_until'], '2026-09-22T03:00:00+00:00')
        before = c.objects[m.CURRENT]; writes = len(c.writes)
        self.assertFalse(s.run(c, 'fixture', '2026-09-22T03:00:00+00:00')['published'])
        self.assertEqual(c.objects[m.CURRENT], before); self.assertEqual(len(c.writes), writes)

    def test_changed_current_or_native_source_bytes_and_bad_future_clocks_fail_before_writes(self):
        for key in ('data/13f-positions.json', 'data/global-flow-desk.json'):
            c = Client(fixture()); doc = m.decode(c.objects[key]); doc['generated_at'] = '2026-09-21T03:00:00+00:00'; c.objects[key] = m.encoded(doc)
            with self.assertRaisesRegex(ValueError, 'Current'): s.run(c, 'fixture', AT)
            self.assertEqual(c.writes, [])
        c = Client(fixture()); h, _, _ = m.source(c.objects.__getitem__)
        c.objects[h['by_fund']['SYNTHETIC']['positions_ref']['key']] += b' '
        with self.assertRaisesRegex(ValueError, 'bytes differ'): s.run(c, 'fixture', AT)
        self.assertEqual(c.writes, [])
        c = Client(fixture())
        with self.assertRaisesRegex(ValueError, 'Future'): s.run(c, 'fixture', '2026-09-19T03:00:00+00:00')
        self.assertEqual(c.writes, [])

    def test_invalid_legacy_history_build_and_concurrent_writer_never_replace_output(self):
        c = Client(fixture()); old = c.objects[m.CURRENT]; c.objects[s.HISTORY] = b'{"entries":null}'
        with self.assertRaisesRegex(ValueError, 'Whole preceding'): s.run(c, 'fixture', AT)
        self.assertEqual(c.writes, []); self.assertEqual(c.objects[m.CURRENT], old)
        c = Client(fixture())
        with patch.object(m, 'build', side_effect=ValueError('invalid source')):
            with self.assertRaises(ValueError): s.run(c, 'fixture', AT)
        self.assertEqual(c.writes, [])
        c.race = b'{"concurrent":"newer publication"}'
        with self.assertRaises(Error): s.run(c, 'fixture', AT)
        self.assertEqual(c.objects[m.CURRENT], c.race)

    def test_immutable_conflict_private_path_and_compiler_tamper_fail(self):
        c = Client(fixture()); receipt = s.run(c, 'fixture', AT)
        manifest = m.verified(receipt['replay'], c.objects.__getitem__, m.PREFIX, 'runs')
        compiler = manifest['compilers']['capital_bridge.py']['key']; c.objects[compiler] += b'\n'
        with self.assertRaisesRegex(ValueError, 'compiler bytes'): s.replay(manifest, c.objects.__getitem__)
        with self.assertRaisesRegex(ValueError, 'readback differs'): s.immutable(c, 'fixture', compiler, b'changed')
        with self.assertRaisesRegex(ValueError, 'Public research'): s.reader(c, 'fixture')('accounts/private.json')
        with self.assertRaisesRegex(ValueError, 'retention prefix'): s.immutable(c, 'fixture', 'data/elsewhere.json', b'{}')

    def test_actual_entry_never_calls_retired_collector_secret_or_write_on_http(self):
        path = Path(__file__).resolve().parents[1]/'source/lambda_function.py'
        with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: None),
                                     'managed_secret': types.SimpleNamespace(managed_secret=lambda *a, **k: self.fail('No secret lookup'))}):
            spec = importlib.util.spec_from_file_location('actual_capital_entry', path); entry = importlib.util.module_from_spec(spec); spec.loader.exec_module(entry)
        with patch.object(entry, 'legacy_lambda_handler', side_effect=AssertionError('Legacy retired')), patch.object(s, 'run', return_value={'published': True}) as run:
            self.assertEqual(entry.lambda_handler({})['statusCode'], 200); self.assertEqual(run.call_count, 1)
            with self.assertRaises(ValueError): entry.lambda_handler({'action': 'legacy'})
        c = Client(fixture())
        with patch.object(entry, 's3', c), patch.object(s, 'run', side_effect=AssertionError('No computation on HTTP read')):
            self.assertEqual(json.loads(entry.lambda_handler({'requestContext': {'http': {}}})['body'])['version'], '2.0')
            self.assertEqual(c.writes, [])


if __name__ == '__main__': unittest.main()
