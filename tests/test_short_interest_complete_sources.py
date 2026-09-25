from pathlib import Path
from datetime import date
from io import BytesIO
from unittest.mock import patch
import json, sys, time, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops/checks', 'aws/ops/staged', 'tests')]
import short_interest_inventory as model
import ops_6054_short_interest_complete_sources as audit
from test_option_flow_store import S3, Error


def row(**changes):
    value = dict(accountingYearMonthNumber=20260915, symbolCode='ABC', issueName='Example Class A',
                 issuerServicesGroupExchangeCode='Q', marketClassCode='NMS',
                 currentShortPositionQuantity=100, previousShortPositionQuantity=80,
                 stockSplitFlag=None, averageDailyVolumeQuantity=200, daysToCoverQuantity=1,
                 revisionFlag=None, changePercent=25, changePreviousNumber=20, settlementDate='2026-09-15')
    value.update(changes)
    return value


def source(value):
    return json.dumps(value).encode()


def headers(total, offset, limit=5000):
    return {'record-total': str(total), 'record-offset': str(offset), 'record-limit': str(limit)}


class Tests(unittest.TestCase):
    def test_plan_uses_published_dates_and_requires_previous_boundary(self):
        doc = {'datasetName': 'CONSOLIDATEDSHORTINTEREST', 'datasetGroup': 'OTCMARKET',
               'partitionFields': ['settlementDate'], 'availablePartitions': [{'partitions': [v]} for v in ('2026-09-15', '2026-08-31', '2026-08-14', '2026-07-31', '2026-07-15')]}
        result = model.partitions(source(doc), date(2026, 9, 25))
        self.assertEqual(result[-1]['previous_settlement_date'], '2026-07-15')
        doc['availablePartitions'].pop()
        with self.assertRaises(ValueError):
            model.partitions(source(doc), date(2026, 9, 25))
        doc['availablePartitions'].append({'partitions': ['2026-09-15']})
        with self.assertRaises(ValueError):
            model.partitions(source(doc), date(2026, 9, 25))

    def test_exact_quantities_and_provider_dtc_floor_remain_separate(self):
        body = source([row()]).replace(b'"currentShortPositionQuantity": 100', b'"currentShortPositionQuantity": 100.123456789012345678901')
        records = model.rows(body, '2026-09-15')
        self.assertEqual(records[0]['currentShortPositionQuantity'], '100.123456789012345678901')
        out = model.describe(records)
        self.assertEqual(out['dtc_relationship'], {'display_floor_one': 1})
        self.assertEqual(out['change_relationship'], {'reported_difference_mismatch': 1})
        self.assertFalse(out['population_arithmetic_qualified'])

    def test_no_denominator_invention_and_unknown_cap_is_not_qualified(self):
        records = model.rows(source([row(averageDailyVolumeQuantity=0, daysToCoverQuantity=0), row(symbolCode='B', currentShortPositionQuantity=2000000, previousShortPositionQuantity=0, averageDailyVolumeQuantity=1, daysToCoverQuantity=999.99, changePreviousNumber=2000000, changePercent=100)]), '2026-09-15')
        result = model.describe(records)
        self.assertEqual(result['dtc_relationship']['zero_adv'], 1)
        self.assertEqual(result['dtc_relationship']['reported_999_99_unconfirmed_cap'], 1)
        self.assertEqual(result['change_relationship']['previous_zero_percent_undefined'], 1)
        self.assertFalse(result['forecast_qualified'])

    def test_literal_symbol_is_not_assumed_unique_security(self):
        records = model.rows(source([row(), row(issueName='Different Class')]), '2026-09-15')
        self.assertEqual(model.describe(records)['symbol_collisions'], 1)
        self.assertEqual(model.fingerprint(records), model.fingerprint(list(reversed(records))))
        with self.assertRaises(ValueError):
            model.fingerprint(records + records[:1])

    def test_bad_grain_dates_flags_numbers_and_schema_fail(self):
        for changes in ({'symbolCode': None}, {'marketClassCode': ''}, {'settlementDate': '2026-08-31'},
                        {'accountingYearMonthNumber': 20260831}, {'stockSplitFlag': 'UNKNOWN'},
                        {'revisionFlag': 'Y'}, {'currentShortPositionQuantity': -1},
                        {'currentShortPositionQuantity': True}, {'previousShortPositionQuantity': '80'},
                        {'newUnreviewedField': 1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                model.rows(source([row(**changes)]), '2026-09-15')

    def test_short_page_continues_until_declared_total_and_rejects_drift(self):
        client = S3({})
        progress = {'captures': {}, 'settlements': {}}
        def fetch(s3, label, url, body, deadline):
            offset = body['offset']
            content = source([row(symbolCode=str(offset))])
            return {'http_status': 200, 'status': 'response_retained', 'headers': headers(2, offset), 'original': audit.base.retain(client, content)}
        with patch.object(audit, 'fetch', side_effect=fetch):
            records, summary = audit.scan(client, '2026-09-15', 1, time.monotonic() + 10, progress['captures'], progress)
        self.assertEqual(len(records), 2)
        self.assertEqual(summary['pages'], 2)
        self.assertFalse(summary['snapshot_atomic'])
        def drift(*args):
            result = fetch(*args)
            if args[3]['offset']:
                result['headers']['record-total'] = '3'
            return result
        with patch.object(audit, 'fetch', side_effect=drift), self.assertRaises(ValueError):
            audit.scan(client, '2026-09-15', 1, time.monotonic() + 10, {}, progress)

    def test_exact_request_uses_settlement_equality_and_complete_sort_grain(self):
        request = model.request('2026-09-15', 5000)
        self.assertEqual(request['sortFields'], list(model.GRAIN))
        self.assertEqual(request['compareFilters'][0]['compareType'], 'EQUAL')
        for offset in (-1, True, 100001):
            with self.assertRaises(ValueError):
                model.request('2026-09-15', offset)

    def test_fetch_claim_precedes_one_keyless_attempt_and_retains_error_response(self):
        client, calls = S3({}), []
        def transport(request, timeout):
            self.assertIn(audit.key('sample'), client.data)
            calls.append(request)
            response = BytesIO(b'{"error":"bounded"}')
            response.status, response.headers = 429, {'Content-Type': 'application/json', 'Set-Cookie': 'secret'}
            return response
        result = audit.fetch(client, 'sample', audit.DATA, model.request('2026-09-15', 0), time.monotonic() + 10, transport)
        self.assertEqual(len(calls), 1)
        self.assertNotIn('Authorization', calls[0].headers)
        self.assertNotIn('set-cookie', result['headers'])
        self.assertEqual(result['http_status'], 429)
        with self.assertRaises(ValueError):
            audit.successful(client, result)
        with self.assertRaises(Error):
            audit.fetch(client, 'sample', audit.DATA, model.request('2026-09-15', 0), time.monotonic() + 10, transport)
        with self.assertRaises(ValueError):
            audit.fetch(client, 'bad', 'https://example.com/', None, time.monotonic() + 10, transport)
        self.assertEqual(len(calls), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
