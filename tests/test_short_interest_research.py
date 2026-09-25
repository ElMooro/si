from pathlib import Path
from datetime import date
from decimal import Decimal, localcontext, ROUND_DOWN
from fractions import Fraction
from unittest.mock import patch
import copy, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops/checks', 'tests')]
import short_interest_measurements as measures
import short_interest_research_model as model
import short_interest_research_store as store
import short_interest_evidence as evidence
from test_option_flow_store import S3

STAMP = '2026-09-25T06:00:00+00:00'
DATES = ('2026-09-15', '2026-08-31', '2026-08-14', '2026-07-31', '2026-07-15')


def row(stamp='2026-09-15', **changes):
    value = dict(accountingYearMonthNumber=int(stamp.replace('-', '')), symbolCode='ABC', issueName='Example Class A',
                 issuerServicesGroupExchangeCode='R', marketClassCode='NNM',
                 currentShortPositionQuantity=100, previousShortPositionQuantity=80, stockSplitFlag=None,
                 averageDailyVolumeQuantity=200, daysToCoverQuantity=1, revisionFlag=None,
                 changePercent=25, changePreviousNumber=20, settlementDate=stamp)
    value.update(changes)
    return value


def capture(value, blobs, url, body=None, headers=None):
    data = model.encoded(value)
    ref = {'key': model.PRIVATE + model.sha(data) + '.bin', 'sha256': model.sha(data), 'bytes': len(data)}
    blobs[ref['key']] = data
    return {'url': url, 'body': body, 'requested_at': STAMP, 'received_at': STAMP,
            'http_status': 200, 'status': 'response_retained',
            'headers': {'content-length': str(len(data)), **(headers or {})}, 'original': ref}


def fixture():
    blobs, captures = {}, {}
    metadata = {'datasetName': 'CONSOLIDATEDSHORTINTEREST', 'datasetGroup': 'OTCMARKET', 'partitionFields': ['settlementDate'],
                'fields': [{'name': name, 'type': 'Date' if name == 'settlementDate' else 'String' if name in (*measures.GRAIN, 'stockSplitFlag', 'revisionFlag') else 'Number'} for name in measures.FIELDS]}
    captures['metadata'] = capture(metadata, blobs, model.URLS['metadata'])
    partitions = {'datasetName': 'consolidatedShortInterest', 'datasetGroup': 'otcMarket', 'partitionFields': ['settlementDate'],
                  'availablePartitions': [{'partitions': [stamp]} for stamp in DATES]}
    captures['partitions'] = capture(partitions, blobs, model.URLS['partitions'])
    for stamp in DATES[:4]:
        for scan in (1, 2):
            for offset in (0, 1):
                record = row(stamp, symbolCode='ABC' if offset == 0 else 'XYZ',
                             currentShortPositionQuantity=100 if stamp == DATES[0] else 80,
                             previousShortPositionQuantity=80,
                             changePreviousNumber=20 if stamp == DATES[0] else 0,
                             changePercent=25 if stamp == DATES[0] else 0)
                label = f'settlement:{stamp}:pass:{scan}:offset:{offset}'
                captures[label] = capture([record], blobs, model.URLS['data'], measures.request(stamp, offset),
                    {'record-total': '2', 'record-offset': str(offset), 'record-limit': '5000'})
    return blobs, {'contract': 'short-interest-original-inputs.v1', 'generated_at': STAMP, 'selection_cutoff': '2026-09-25',
                   'captures': captures, 'settlement_plan': measures.partitions(model.encoded(partitions), date(2026, 9, 25))}


def record(compiled, symbol='ABC'):
    issue = next(v for v in compiled['packet']['symbols'] if v['symbol'] == symbol)['issues'][0]
    identity = issue['record_id']
    return compiled['shards'][identity[:2]]['records'][identity]


class MeasurementsTests(unittest.TestCase):
    def measure(self, **changes):
        original = measures.rows(model.encoded([row(**changes)]), '2026-09-15')[0]
        return measures.measurement(original, '2026-08-31', 2, 17)

    def test_reported_dtc_floor_is_not_an_exact_calculated_ratio(self):
        actual = self.measure()
        self.assertEqual(actual['reported_days_to_cover'], '1')
        self.assertEqual(actual['reconstructed_position_to_reported_adv_days'], '0.500000000000')
        self.assertEqual(actual['position_to_adv_exact_fraction'], '1/2')
        self.assertEqual(actual['days_to_cover_status'], 'provider_display_floor_one')
        self.assertEqual(actual['average_volume_window']['first_date'], '2026-09-01')
        self.assertFalse(actual['average_volume_window']['underlying_daily_volumes_reconstructed'])

    def test_zero_adv_provider_999_99_stays_unavailable(self):
        actual = self.measure(averageDailyVolumeQuantity=0, daysToCoverQuantity=999.99)
        self.assertEqual(actual['reported_days_to_cover'], '999.99')
        self.assertIsNone(actual['reconstructed_position_to_reported_adv_days'])
        self.assertIsNone(actual['position_to_adv_exact_fraction'])
        self.assertEqual(actual['days_to_cover_status'], 'unavailable_zero_reported_adv')

    def test_previous_zero_convention_is_not_100_percent_growth(self):
        actual = self.measure(previousShortPositionQuantity=0, changePreviousNumber=100, changePercent=100)
        self.assertEqual(actual['reported_change_pct'], '100')
        self.assertIsNone(actual['computed_change_pct'])
        self.assertEqual(actual['change_pct_reconciliation'], 'undefined_previous_zero')

    def test_splits_revisions_and_provider_discrepancy_are_not_hidden(self):
        actual = self.measure(stockSplitFlag='S', revisionFlag='R', changePreviousNumber=99, changePercent=12)
        self.assertFalse(actual['change_shares_reconciled'])
        self.assertEqual(actual['computed_change_shares'], '20')
        self.assertEqual(actual['change_pct_reconciliation'], 'provider_differs_from_rounded_formula')
        self.assertFalse(actual['split_adjusted_comparison_verified'])
        self.assertFalse(actual['covering_inferred'])

    def test_high_ratio_keeps_provider_convention_and_exact_reconstruction(self):
        actual = self.measure(currentShortPositionQuantity=123456789, averageDailyVolumeQuantity=7, daysToCoverQuantity=999.99)
        self.assertEqual(Fraction(actual['position_to_adv_exact_fraction']), Fraction(123456789, 7))
        self.assertLessEqual(abs(Fraction(actual['reconstructed_position_to_reported_adv_days']) - Fraction(123456789, 7)), Fraction(1, 2 * 10**12))
        self.assertEqual(actual['days_to_cover_status'], 'provider_999_99_convention_unconfirmed')

    def test_rounding_does_not_inherit_an_unrelated_decimal_context(self):
        expected = measures.quotient(Decimal(2), Decimal(3))
        with localcontext() as ctx:
            ctx.rounding = ROUND_DOWN
            ctx.prec = 4
            self.assertEqual(measures.quotient(Decimal(2), Decimal(3)), expected)
        self.assertEqual(expected, '0.666666666667')


class ModelTests(unittest.TestCase):
    def test_full_population_reconstruction_keeps_dates_pages_and_source_rows(self):
        blobs, inputs = fixture()
        out = model.compile_output(inputs, blobs.__getitem__)
        self.assertEqual(out['packet']['counts']['source_pages'], 16)
        self.assertEqual(out['packet']['counts']['source_rows_each_scan'], 8)
        self.assertEqual(out['packet']['counts']['latest_issues'], 2)
        self.assertTrue(all(v is False for v in (out['packet'][f] for f in model.FLAGS)))
        actual = record(out)
        self.assertEqual(len(actual['observations']), 4)
        latest = dict(zip(measures.POINT_FIELDS, actual['observations'][-1]))
        self.assertEqual(latest['matched_prior_record_shares'], '80')
        self.assertTrue(latest['matches_reported_previous_quantity'])
        self.assertEqual(latest['source_row'], 0)
        self.assertEqual(out['packet']['sources'][latest['source_index']]['settlement_date'], '2026-09-15')
        self.assertFalse(actual['security_identity_continuity_verified'])
        self.assertEqual(out['packet']['by_ticker'], {})

    def test_changed_recheck_population_blocks_candidate(self):
        blobs, inputs = fixture()
        label = 'settlement:2026-09-15:pass:2:offset:1'
        cap = inputs['captures'][label]
        cap = capture([row(symbolCode='CHANGED')], blobs, cap['url'], cap['body'], cap['headers'])
        cap['headers']['content-length'] = str(cap['original']['bytes'])
        inputs['captures'][label] = cap
        with self.assertRaisesRegex(ValueError, 'scans disagree'):
            model.compile_output(inputs, blobs.__getitem__)

    def test_missing_final_page_duplicates_and_total_drift_cannot_be_complete(self):
        blobs, inputs = fixture()
        label = 'settlement:2026-09-15:pass:1:offset:1'
        broken = copy.deepcopy(inputs); broken['captures'].pop(label)
        with self.assertRaises(KeyError):
            model.compile_output(broken, blobs.__getitem__)
        broken = copy.deepcopy(inputs); broken['captures'][label]['headers']['record-total'] = '3'
        with self.assertRaisesRegex(ValueError, 'total changed'):
            model.compile_output(broken, blobs.__getitem__)
        broken = copy.deepcopy(inputs)
        cap = broken['captures'][label]
        cap['original'] = broken['captures']['settlement:2026-09-15:pass:1:offset:0']['original']
        with self.assertRaises(ValueError):
            model.compile_output(broken, blobs.__getitem__)

    def test_source_bytes_request_metadata_and_clock_tamper_fail(self):
        blobs, inputs = fixture()
        for change in ('bytes', 'request', 'clock', 'schema', 'extra'):
            data, value = dict(blobs), copy.deepcopy(inputs)
            cap = value['captures']['settlement:2026-09-15:pass:1:offset:0']
            if change == 'bytes':
                data[cap['original']['key']] += b'\n'
            elif change == 'request':
                cap['body']['sortFields'] = ['symbolCode']
            elif change == 'clock':
                cap['received_at'] = '2026-09-26T06:00:00+00:00'
            elif change == 'schema':
                metadata = model.strict(data[value['captures']['metadata']['original']['key']]); metadata['fields'][0]['type'] = 'String'
                value['captures']['metadata'] = capture(metadata, data, model.URLS['metadata'])
            else:
                value['captures']['unreviewed'] = copy.deepcopy(cap)
            with self.subTest(change=change), self.assertRaises(ValueError):
                model.compile_output(value, data.__getitem__)

    def test_missing_settlement_does_not_fill_zero_or_join_different_issue(self):
        point = measures.compact_point(measures.rows(model.encoded([row()]), '2026-09-15')[0], '2026-08-31', 0, 0)
        value = measures.history(measures.record_key(measures.source_fields(point)), [point], list(reversed(DATES[:4])))
        self.assertEqual(len(value['missing_settlements']), 3)
        self.assertIsNone(value['observations'][0][-1])
        with self.assertRaises(ValueError):
            measures.history(('ABC', 'Different', 'R', 'NNM'), [point], list(reversed(DATES[:4])))


class StoreTests(unittest.TestCase):
    def candidate(self):
        blobs, inputs = fixture(); client = S3(blobs)
        compiled = model.compile_output(inputs, blobs.__getitem__)
        replay = store.retain(client, 'bucket', inputs, compiled)
        return client, compiled, replay

    def test_immutable_retention_and_replay_never_publish_current_or_read_accounts(self):
        client, compiled, replay = self.candidate()
        before = dict(client.data)
        self.assertEqual(store.replay(replay, store.reader(client, 'bucket')), compiled)
        self.assertEqual(client.data, before)
        self.assertNotIn(model.CURRENT, client.data)
        for key in (model.CURRENT, 'portfolio/account.json', model.PREFIX + '../accounts.json'):
            with self.assertRaises(ValueError):
                store.reader(client, 'bucket')(key)

    def test_altered_compiler_run_or_record_is_rejected(self):
        for kind in ('compiler', 'run', 'record', 'original'):
            client, compiled, replay = self.candidate()
            run = model.strict(client.data[replay['manifest_key']])
            key = next(iter(run['compilers'].values()))['key'] if kind == 'compiler' else replay['manifest_key'] if kind == 'run' else next(iter(compiled['packet']['record_shards'].values()))['key'] if kind == 'record' else compiled['packet']['sources'][0]['original']['key']
            client.data[key] += b'changed'
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                store.replay(replay, store.reader(client, 'bucket'))

    def test_wrong_shard_is_rejected_before_any_public_write(self):
        blobs, inputs = fixture(); client = S3(blobs)
        compiled = model.compile_output(inputs, blobs.__getitem__)
        next(iter(compiled['shards'].values()))['unexpected'] = True
        with self.assertRaises(ValueError):
            store.retain(client, 'bucket', inputs, compiled)
        self.assertEqual(client.writes, [])


class EvidenceTests(unittest.TestCase):
    def test_whole_fixture_arithmetic_and_original_rows_are_checked(self):
        blobs, inputs = fixture()
        out = model.compile_output(inputs, blobs.__getitem__)
        proof = evidence.qualify(inputs, out, lambda ref: model.original(ref, blobs.__getitem__))
        self.assertEqual(proof['unique_observations_checked'], 8)
        self.assertEqual(proof['original_rows_checked_both_scans'], 16)
        self.assertFalse(proof['forecast_qualified'])

    def test_independent_arithmetic_catches_self_consistent_output_tamper(self):
        blobs, inputs = fixture()
        out = model.compile_output(inputs, blobs.__getitem__)
        point = record(out)['observations'][-1]
        point[measures.POINT_FIELDS.index('reconstructed_position_to_reported_adv_days')] = '1.000000000000'
        with self.assertRaises(AssertionError):
            evidence.qualify(inputs, out, lambda ref: model.original(ref, blobs.__getitem__))


if __name__ == '__main__':
    unittest.main(verbosity=2)
