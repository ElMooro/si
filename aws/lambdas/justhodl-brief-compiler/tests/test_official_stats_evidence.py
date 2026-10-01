"""Synthetic, offline evidence-only regressions; no provider/AWS access."""
import copy
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import brief_compiler as compiler
import brief_contract as contract

NOW = datetime(2026, 10, 1, 2, tzinfo=timezone.utc)
STAMP = NOW.isoformat()
OLD = (NOW - timedelta(days=19)).isoformat()


def synthetic_canary():
    rows = {'schema_version': '2.0', 'generated_at': STAMP}
    for sid, unit in [('GDPNOW', 'Percent change at annual rate'), ('T10Y3M', 'Percentage points')]:
        rows[sid] = {'contract_version': 'measurement-provenance.v2', 'value': 0,
                     'unit': unit, 'as_of': '2026-09-30', 'observation_period': '2026-07-01',
                     'received_at': STAMP, 'published_at': None, 'data_unavailable': False,
                     'source': {'kind': 'fred', 'series_id': sid, 'url': 'https://example.invalid/synthetic'},
                     'freshness': {'status': 'within_age_ceiling', 'maximum_age_days': 21,
                                   'observation_age_days': 1, 'policy': 'synthetic_test_policy'}}
    return rows


def compile_with(canary, legacy_stamp=OLD, error=None):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    legacy = {'generated_at': legacy_stamp, 'status': 'LIVE', 'series': {
        'atlantafed': {'last': {'GDPNOW': '4.4164', 'observation_date': '2026-07-01'}},
        'clevelandfed': {'last': {'T10Y3M': '0.89', 'observation_date': '2026-09-11'}}}}
    reads, writes = [], []
    def load(s3, key):
        reads.append(key)
        return (legacy, legacy_stamp, None) if key == 'data/fed-nowcast-join.json' else (canary, STAMP, error)
    with patch.object(contract, 'datetime', Clock), patch.object(compiler, '_now', lambda: NOW), \
         patch.object(compiler, '_load', load), patch.object(compiler, '_put', lambda s3,k,d: writes.append((k,d))), \
         patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
        result = compiler.compile_official_stats(None)
    return result, reads, writes


class OfficialStatsEvidenceTests(unittest.TestCase):
    def test_new_observations_do_not_promote_held_legacy(self):
        d, reads, writes = compile_with(synthetic_canary())
        self.assertEqual(d['status'], 'HELD')
        self.assertEqual(d['inputs'], {'data/fed-nowcast-join.json': {
            'required': True, 'last_modified': OLD, 'as_of': OLD, 'freshness': 'EXPIRED', 'error': None}})
        self.assertEqual(d['fields'], {'gdpnow': '4.4164', 'gdpnow_date': '2026-07-01',
            't10y3m': '0.89', 't10y3m_date': '2026-09-11', 'nowcast_status': 'LIVE'})
        self.assertEqual(d['why'], 'GDPNow 4.4164 on 2026-07-01; T10Y3M 0.89 on 2026-09-11')
        self.assertEqual(d['evidence']['schema'], 'official-stats-evidence.v1')
        self.assertFalse(d['evidence']['decision_fields_replaced'])
        self.assertFalse(d['evidence']['source_replay_verified'])
        self.assertEqual(reads, ['data/fed-nowcast-join.json', 'data/canary-macro.json'])
        self.assertEqual(writes, [('data/official-stats-brief.json', d)])
        self.assertEqual(contract.validate_brief(d), [])

    def test_canary_failure_does_not_change_existing_live_gate(self):
        for canary in [None, [], 'invalid', {}, {'GDPNOW': []}]:
            d, _, _ = compile_with(canary, STAMP, 'synthetic missing source')
            self.assertEqual(d['status'], 'LIVE')
            self.assertIn('warehouse_read_failed', d['evidence']['warehouse']['issues'])
            self.assertIsNone(d['evidence']['measurements']['GDPNOW']['reported_value'])

    def test_numeric_zero_null_and_malformed_values(self):
        for value in [0, 0.0, None, False, '', '0', {}, [], float('nan'), float('inf'), 10**1000]:
            raw = synthetic_canary(); raw['GDPNOW']['value'] = value
            e = compiler._official_stats_evidence(raw, STAMP, None, NOW)
            row = e['measurements']['GDPNOW']
            if type(value) in (int, float) and value == 0:
                self.assertEqual(row['reported_value'], value)
                self.assertNotIn('value_missing_or_invalid', row['issues'])
            else:
                self.assertIsNone(row['reported_value'])
                self.assertIn('value_missing_or_invalid', row['issues'])
            json.dumps(e, allow_nan=False)

    def test_economic_receipt_publication_clocks_separate(self):
        raw = synthetic_canary(); raw['GDPNOW']['as_of'] = '2026-07-01'
        raw['GDPNOW']['freshness'].update(status='stale', observation_age_days=92)
        e = compiler._official_stats_evidence(raw, STAMP, None, NOW)
        row = e['measurements']['GDPNOW']
        self.assertEqual(row['economic_as_of'], '2026-07-01')
        self.assertEqual(row['received_at'], STAMP)
        self.assertIsNone(row['source_published_at'])
        self.assertEqual(e['warehouse']['generated_at'], STAMP)
        self.assertEqual(row['source_reported_freshness']['maximum_age_days'], 21)
        self.assertEqual(row['source_reported_freshness']['status'], 'stale')
        self.assertIn('source_freshness_not_within_age_ceiling', row['issues'])
        self.assertEqual(row['qualification'], 'NOT_DECISION_QUALIFIED')

    def test_future_missing_invalid_and_exact_now_clocks(self):
        for value, reason in [(STAMP, None), ((NOW + timedelta(microseconds=1)).isoformat(), 'future'),
                              (None, 'missing_or_invalid'), ('bad', 'missing_or_invalid'), ({}, 'missing_or_invalid')]:
            raw = synthetic_canary(); raw['generated_at'] = value
            for field in ['as_of', 'received_at', 'published_at']: raw['GDPNOW'][field] = value
            e = compiler._official_stats_evidence(raw, value, None, NOW)
            if reason:
                self.assertIn('publication:'+reason, e['warehouse']['issues'])
                for field in ['economic_as_of', 'received_at', 'source_published_at']:
                    self.assertIn(field+':'+reason, e['measurements']['GDPNOW']['issues'])
            else:
                self.assertEqual(e['warehouse']['issues'], [])
                self.assertEqual(e['measurements']['GDPNOW']['issues'], [])

    def test_expired_publication_cannot_refresh_observations(self):
        raw = synthetic_canary();raw['generated_at'] = OLD
        e = compiler._official_stats_evidence(raw, STAMP, None, NOW)
        self.assertIn('publication_expired', e['warehouse']['issues'])
        self.assertEqual(e['measurements']['GDPNOW']['qualification'], 'NOT_DECISION_QUALIFIED')

    def test_source_identity_units_contract_and_model_not_substituted(self):
        raw = synthetic_canary();row = raw['T10Y3M']
        row.update(unit='Percent', contract_version='unknown', data_unavailable=True)
        row['source']['series_id'] = 'RECPROUSM156N'
        e = compiler._official_stats_evidence(raw, STAMP, None, NOW)
        self.assertTrue({'unit_unverified','measurement_contract_unrecognized','source_identity_unverified',
                         'source_unavailable_or_unknown'}.issubset(e['measurements']['T10Y3M']['issues']))
        self.assertIsNone(e['cleveland_model']['value'])
        self.assertIn('not that model', e['cleveland_model']['reason'])

    def test_same_period_revision_is_context_only(self):
        raw = synthetic_canary()
        first, _, _ = compile_with(raw)
        raw['GDPNOW']['value'] = 3.7372
        second, _, _ = compile_with(raw)
        self.assertEqual(first['fields'], second['fields'])
        self.assertEqual(first['status'], second['status'])
        self.assertNotEqual(first['evidence']['measurements']['GDPNOW']['reported_value'],
                            second['evidence']['measurements']['GDPNOW']['reported_value'])
        self.assertEqual(first['evidence']['measurements']['GDPNOW']['observation_period'],
                         second['evidence']['measurements']['GDPNOW']['observation_period'])

    def test_actual_loader_access_failure_preserves_base_publication(self):
        class Warehouse:
            def __init__(self): self.reads, self.writes = [], []
            def get_object(self, Bucket, Key):
                self.reads.append(Key)
                if Key == 'data/canary-macro.json': raise PermissionError('synthetic denied')
                return {'Body': io.BytesIO(json.dumps({'generated_at': OLD}).encode()), 'LastModified': NOW}
            def put_object(self, **kwargs): self.writes.append(kwargs)
        warehouse = Warehouse()
        with patch('socket.socket.connect', side_effect=AssertionError('network forbidden')):
            result = compiler.compile_official_stats(warehouse)
        self.assertEqual(len(warehouse.reads), 2)
        self.assertEqual(len(warehouse.writes), 1)
        self.assertIn('warehouse_read_failed', result['evidence']['warehouse']['issues'])
        self.assertEqual(result['evidence']['warehouse']['error'], 'synthetic denied')
        self.assertEqual(json.loads(warehouse.writes[0]['Body']), result)

    def test_deterministic_repeat_no_input_mutation_and_policy_preserved(self):
        raw = synthetic_canary(); before = copy.deepcopy(raw)
        a, _, _ = compile_with(raw);b, _, _ = compile_with(raw)
        self.assertEqual(json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True))
        self.assertEqual(raw, before)
        self.assertEqual(contract.TTL_HOURS['official_stats'], 168)

if __name__ == '__main__': unittest.main()
