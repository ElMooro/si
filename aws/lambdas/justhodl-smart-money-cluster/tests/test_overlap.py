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

import holdings_overlap as m
import overlap_store as s

AT = '2026-09-20T01:00:00+00:00'
PERIOD = '2026-06-30'


def position(cusip, quantity, kind='SH', option=None, title='COM'):
    ident = {'cusip': cusip, 'class': title, 'quantity_type': kind, 'put_call': option}
    return m.digest(ident), {'identity': ident, 'reported_quantity': str(quantity),
                           'native_rows': 1, 'issuer_names': ['SYNTHETIC '+cusip]}


def fixture():
    objects = {}
    def retain(prefix, kind, doc):
        raw = m.encoded(doc); key = prefix+kind+'/'+m.digest(doc)+'.json'; objects[key] = raw
        return m.reference(key, raw)
    rows = [('A', PERIOD, True, [position('111111111', 10), position('222222222', 0),
             position('111111111', 5, option='CALL'), position('333333333', 100, kind='PRN')]),
            ('B', PERIOD, True, [position('111111111', 20), position('444444444', 5), position('111111111', 9, option='PUT')]),
            ('C', '2026-03-31', True, [position('111111111', 1)]),
            ('D', PERIOD, True, []), ('E', PERIOD, True, []), ('F', None, False, [])]
    funds = {}
    for i, (name, period, complete, positions) in enumerate(rows):
        status = 'complete_selected_public_chain' if complete else 'not_acquired'
        detail = {'contract': 'holdings-native-fund.v1', 'fund': name, 'cik': str(i).zfill(10),
                  'official_name': 'SYNTHETIC '+name, 'current_holdings_period': period,
                  'periods': {period: {'status': status, 'positions': dict(positions), 'effective_accessions': []}} if period else {},
                  'filings': {}, 'comparison': {'eligible_for_disclosure_comparison': False}}
        ref = retain(m.NATIVE, 'funds', detail)
        funds[name] = {'positions_ref': ref, 'cik': detail['cik'], 'official_name': detail['official_name'],
                      'period_of_report': period, 'positions_count': len(positions), 'current_chain_status': status}
    native = {'manifest_key': m.NATIVE+'runs/'+('a'*64)+'.json', 'output_sha256': 'b'*64}
    product = {'contract': 'holdings-canonical.v1', 'by_fund': funds, 'funds_total': len(funds),
               'generated_at': AT, 'source_generated_at': AT, 'required_period_for_current_cohort': PERIOD,
               'research': native, 'calls_eligible': True}
    pref = retain(m.CANONICAL, 'products', product)
    manifest = {'contract': 'holdings-canonical-replay.v1', 'research': native,
                'products': {'data/13f-positions.json': pref}}
    ref = retain(m.CANONICAL, 'runs', manifest)
    objects['data/13f-positions.json'] = m.encoded({**product, 'canonical_replay': ref})
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


def result(objects=None, at=AT):
    objects = objects or fixture(); product, binding = m.source(objects.__getitem__)
    output, artifacts = m.build(product, binding, objects.__getitem__, at)
    return output, {key: m.decode(raw) for key, raw in artifacts.items()}


class Tests(unittest.TestCase):
    def test_exact_positive_membership_cohorts_instruments_and_missingness(self):
        out, artifacts = result(); cohort = out['cohorts'][PERIOD]
        self.assertEqual(cohort['funds'], ['A', 'B', 'D', 'E'])
        self.assertEqual(out['quality']['unavailable_managers'], ['F'])
        self.assertEqual(set(cohort['scopes']), {'SH|NONE', 'SH|CALL', 'SH|PUT', 'PRN|NONE'})
        scope = artifacts[cohort['scopes']['SH|NONE']['key']]
        self.assertEqual(len(scope['pairs']), 6)
        pair = next(v for v in scope['pairs'] if (v['fund_a'], v['fund_b']) == ('A', 'B'))
        self.assertEqual((pair['shared_count'], pair['union_count']), (1, 2))
        self.assertEqual((pair['jaccard_pct'], pair['coverage_a_pct'], pair['coverage_b_pct']), ('50.000000', '100.000000', '50.000000'))
        empty = next(v for v in scope['pairs'] if (v['fund_a'], v['fund_b']) == ('D', 'E'))
        self.assertIsNone(empty['jaccard_pct']); self.assertIsNone(empty['coverage_a_pct'])
        zero = next(v for v in scope['securities'] if v['identity']['cusip'] == '222222222')
        self.assertEqual(zero['reported_manager_count'], 1); self.assertEqual(zero['positive_quantity_manager_count'], 0)
        self.assertFalse(out['funds']['C']['current_cohort'])
        self.assertFalse(out['calls_eligible']); self.assertIsNone(out['call']); self.assertEqual(out['clusters'], [])
        self.assertIsNone(out['funds']['F']['positive_quantity_identity_count'])
        self.assertEqual(out['funds']['D']['positive_quantity_identity_count'], 0)

    def test_current_overlap_does_not_require_a_prior_quarter_comparison(self):
        out, _ = result()
        self.assertEqual(out['funds']['A']['positive_quantity_identity_count'], 3)
        self.assertTrue(out['funds']['A']['eligible_for_overlap'])

    def test_missing_or_forged_source_bytes_fail_before_calculation(self):
        objects = fixture(); product, binding = m.source(objects.__getitem__)
        key = product['by_fund']['A']['positions_ref']['key']; objects[key] += b' '
        with self.assertRaisesRegex(ValueError, 'bytes differ'): m.build(product, binding, objects.__getitem__, AT)
        objects = fixture(); objects['data/13f-positions.json'] = objects['data/13f-positions.json'].replace(b'"funds_total":6', b'"funds_total":7')
        with self.assertRaisesRegex(ValueError, 'Mutable source'): m.source(objects.__getitem__)
        with self.assertRaisesRegex(ValueError, 'identity'): m.verified({'key': 'data/private.json', 'sha256': 'a'*64}, objects.__getitem__, m.NATIVE, 'funds')

    def test_source_age_does_not_become_fresh_from_republication(self):
        out, _ = result(at='2026-09-23T01:00:00+00:00')
        self.assertEqual(out['quality']['status'], 'stale'); self.assertEqual(out['source_generated_at'], AT)
        self.assertEqual(out['funds']['A']['observation_age_days'], 85)
        with self.assertRaisesRegex(ValueError, 'Future'): result(at='2026-09-19T01:00:00+00:00')

    def test_publish_preserves_whole_predecessor_and_replays_every_scope(self):
        c = Client(fixture()); old = b'{"legacy":"whole product","clusters":[{"score":999999}]}'
        c.objects[m.CURRENT] = old
        receipt = s.run(c, 'fixture', AT); self.assertTrue(receipt['published'])
        self.assertEqual(c.objects[receipt['whole_preceding_product']['private_key']], old)
        manifest = m.verified(receipt['replay'], c.objects.__getitem__, m.PREFIX, 'runs')
        replayed = s.replay(manifest, c.objects.__getitem__)
        self.assertEqual(m.decode(c.objects[m.CURRENT]), {**replayed, 'replay': receipt['replay']})
        before = c.objects[m.CURRENT]; writes = len(c.writes)
        second = s.run(c, 'fixture', '2026-09-21T01:00:00+00:00')
        self.assertFalse(second['published']); self.assertEqual(c.objects[m.CURRENT], before); self.assertEqual(len(c.writes), writes)
        del c.objects['data/13f-positions.json']
        self.assertEqual(s.replay(manifest, c.objects.__getitem__), replayed)
        key = next(iter(replayed['cohorts'][PERIOD]['scopes'].values()))['key']; c.objects[key] += b' '
        with self.assertRaisesRegex(ValueError, 'scope replay'): s.replay(manifest, c.objects.__getitem__)

    def test_unchanged_source_crossing_expiry_publishes_stale_without_freshening_source(self):
        c = Client(fixture()); s.run(c, 'fixture', AT)
        late = s.run(c, 'fixture', '2026-09-23T01:00:00+00:00')
        self.assertTrue(late['published'])
        packet = m.decode(c.objects[m.CURRENT])
        self.assertEqual(packet['quality']['status'], 'stale')
        self.assertEqual(packet['source_generated_at'], AT)
        self.assertEqual(packet['quality']['fresh_until'], '2026-09-22T01:00:00+00:00')

    def test_clock_regression_cannot_reuse_a_future_publication(self):
        c = Client(fixture()); s.run(c, 'fixture', AT)
        before = deepcopy(c.objects); writes = len(c.writes)
        with self.assertRaisesRegex(ValueError, 'Future'):
            s.run(c, 'fixture', '2026-09-19T01:00:00+00:00')
        self.assertEqual(c.objects, before); self.assertEqual(len(c.writes), writes)

    def test_validation_failure_and_concurrent_writer_do_not_replace_live_output(self):
        c = Client(fixture()); old = b'{"legacy":"complete"}'; c.objects[m.CURRENT] = old
        with patch.object(m, 'build', side_effect=ValueError('invalid source')):
            with self.assertRaises(ValueError): s.run(c, 'fixture', AT)
        self.assertEqual(c.writes, []); self.assertEqual(c.objects[m.CURRENT], old)
        c.race = b'{"concurrent":"newer output"}'
        with self.assertRaises(Error): s.run(c, 'fixture', AT)
        self.assertEqual(c.objects[m.CURRENT], c.race)

    def test_actual_entry_point_never_calls_retired_collector_or_secret(self):
        path = Path(__file__).resolve().parents[1]/'source/lambda_function.py'
        with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: None),
                                     'managed_secret': types.SimpleNamespace(managed_secret=lambda *a, **k: self.fail('No secret lookup'))}):
            spec = importlib.util.spec_from_file_location('actual_overlap_entry', path); entry = importlib.util.module_from_spec(spec); spec.loader.exec_module(entry)
        with patch.object(entry, 'legacy_lambda_handler', side_effect=AssertionError('Legacy retired')), patch.object(s, 'run', return_value={'published': True}) as run:
            self.assertEqual(entry.lambda_handler({})['statusCode'], 200); self.assertEqual(run.call_count, 1)
            with self.assertRaises(ValueError): entry.lambda_handler({'action': 'legacy'})
        c = Client(fixture()); c.objects[m.CURRENT] = b'{"fixture":"read only"}'
        with patch.object(entry, 'S3', c), patch.object(s, 'run', side_effect=AssertionError('No computation on HTTP read')):
            self.assertEqual(json.loads(entry.lambda_handler({'requestContext': {'http': {}}})['body']), {'fixture': 'read only'})


if __name__ == '__main__': unittest.main()
