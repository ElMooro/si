from pathlib import Path
from io import BytesIO
from unittest.mock import Mock, MagicMock, patch
import sys, unittest, zipfile
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops/checks', 'aws/ops/staged', 'tests')]
import sec_ftd_inventory as sec
import ops_6057_squeeze_settlement_source_baseline as audit
from test_option_flow_store import S3
URL = 'https://www.sec.gov/files/data/fails-deliver-data/cnsfails202608b.zip'
TEXT = ('|'.join(sec.FIELDS) + '\n20260817|001234567|ABC|123|Reported class|.\n'
        '20260818|001234567|ABC.A|150|Reported class|12.3456\n'
        '20260818|901234567|ABC.A|2|Another class|25.00\n'
        'Trailer record count 3\nTrailer total quantity of shares 275\n').encode()


def zipped(body=TEXT, name='cnsfails202608b.txt', extra=None):
    out = BytesIO()
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as archive:
        info = zipfile.ZipInfo(name)
        info.compress_type = zipfile.ZIP_DEFLATED
        archive.writestr(info, body)
        if extra:
            archive.writestr(*extra)
    return out.getvalue()


class Tests(unittest.TestCase):
    def test_advertised_selection_orders_dates_and_excludes_unreviewed_or_future_urls(self):
        links = [URL.replace('202608b', x) for x in ('202608a', '202607b', '202608b', '202609b')]
        body = ''.join('<a href="' + u + '">Archive</a>' for u in links + ['https://evil.example/cnsfails202609a.zip']).encode()
        self.assertEqual(sec.advertised_archives(body, '2026-09-01'), [URL, URL.replace('b.zip', 'a.zip')])
        for value in (URL + '?redirect=other', URL.replace('www.sec.gov', 'evil.example')):
            with self.assertRaises(ValueError):
                sec.archive_period(value)

    def test_archive_window_must_be_explicit_complete_and_contiguous(self):
        body = ('<a href="' + URL + '"></a><a href="' + URL.replace('202608b', '202607b') + '"></a>').encode()
        with self.assertRaisesRegex(ValueError, 'gaps'):
            sec.advertised_archives(body, '2026-09-25')
        with self.assertRaises(ValueError):
            sec.advertised_archives(body, '2026-09-25', 3)

    def test_published_other_directory_is_preserved_and_ambiguous_period_is_rejected(self):
        alternate = URL.replace('/data/', '/data/other/')
        first = alternate.replace('b.zip', 'a.zip')
        html = ''.join('<a href="' + url + '">archive</a>' for url in (first, alternate)).encode()
        self.assertEqual(sec.advertised_archives(html, '2026-09-25'), [alternate, first])
        with self.assertRaisesRegex(ValueError, 'gaps'):
            sec.advertised_archives(html + ('<a href="' + URL + '">ambiguous</a>').encode(), '2026-09-25')
        with self.assertRaises(ValueError):
            sec.archive_period(URL.replace('/data/', '/data/arbitrary/'))

    def test_whole_records_keep_leading_zero_cusip_missing_price_and_source_line(self):
        records = sec.rows(TEXT, URL, '2026-09-25')
        self.assertEqual(records[0]['cusip'], '001234567')
        self.assertEqual(records[0]['fail_balance_shares'], '123')
        self.assertIsNone(records[0]['previous_day_reported_price'])
        self.assertEqual(records[1]['previous_day_reported_price'], '12.3456')
        self.assertEqual(records[2]['source_line'], 4)
        self.assertEqual(records[0]['reported_price'], '.')

    def test_inventory_keeps_identity_ambiguities_without_forced_buying_claims(self):
        result = sec.inventory(zipped(), URL, '2026-09-25')
        self.assertEqual(result['rows'], 3)
        self.assertEqual(result['cusips'], 2)
        self.assertEqual(result['symbols_with_multiple_cusips'], 1)
        self.assertEqual(result['cusips_with_multiple_reported_labels'], 1)
        self.assertEqual(result['settlements'], {'2026-08-17': 1, '2026-08-18': 2})
        self.assertEqual(result['missing_previous_day_prices'], 1)
        for field in ('daily_new_fail_flow_measured', 'fail_age_measured', 'short_sale_origin_verified', 'forced_buy_in_forecast_qualified', 'security_continuity_verified'):
            self.assertFalse(result[field])

    def test_duplicate_date_cusip_never_silently_overwrites(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            sec.rows(TEXT.replace(b'Trailer record count 3', TEXT.splitlines()[1] + b'\nTrailer record count 4'), URL, '2026-09-25')

    def test_both_exact_control_trailers_reconcile_without_becoming_economic_flow(self):
        value = sec.inventory(zipped(), URL, '2026-09-25')['control_totals']
        self.assertEqual(value['reported_record_count'], 3)
        self.assertEqual(value['reported_quantity_sum'], '275')
        self.assertEqual(value['record_count_source_line'], 5)
        self.assertEqual(value['quantity_sum_source_line'], 6)
        self.assertTrue(value['record_count_matches'])
        self.assertTrue(value['quantity_checksum_matches'])
        self.assertTrue(value['quantity_sum_is_file_integrity_control_not_economic_flow'])
        for body in (TEXT.replace(b'count 3', b'count 2'), TEXT.replace(b'shares 275', b'shares 276'),
                     TEXT.split(b'Trailer record count')[0], TEXT + b'Unknown trailer\n',
                     TEXT.replace(b'QUANTITY (FAILS)', b'UNKNOWN')):
            with self.assertRaises(ValueError):
                sec.rows(body, URL, '2026-09-25')

    def test_future_settlement_cannot_become_observed_at_an_earlier_capture(self):
        with self.assertRaisesRegex(ValueError, 'Future settlement'):
            sec.inventory(zipped(), URL, '2026-08-17')

    def test_invalid_rows_and_unadvertised_settlements_fail_without_partial_inventory(self):
        for body in (TEXT.replace(b'QUANTITY (FAILS)', b'VOLUME'), TEXT.replace(b'20260817', b'20260731'),
                     TEXT.replace(b'|123|', b'|NaN|'), TEXT.replace(b'|123|', b'|-1|'), TEXT.replace(b'|12.3456', b'|inf'),
                     TEXT + b'truncated|record\n', TEXT.splitlines()[0] + b'\n'):
            with self.assertRaises(ValueError):
                sec.inventory(zipped(body), URL, '2026-09-25')

    def test_reported_day_fifteen_is_preserved_without_an_invented_half_month_cutoff(self):
        body = TEXT.replace(b'20260817', b'20260715').replace(b'20260818', b'20260716')
        value = sec.inventory(zipped(body), URL.replace('202608', '202607'), '2026-09-25')
        self.assertEqual(value['settlements'], {'2026-07-15': 1, '2026-07-16': 2})
        self.assertEqual(value['rows'], 3)
        self.assertFalse(value['archive_scope']['fixed_half_month_day_boundary_assumed'])
        self.assertTrue(value['control_totals']['quantity_checksum_matches'])

    def test_zip_integrity_member_count_and_path_are_checked_without_extraction(self):
        for data in (zipped(name='../escape.txt'), zipped(extra=('extra.txt', b'x')), zipped()[:-30]):
            with self.assertRaises((ValueError, zipfile.BadZipFile)):
                sec.text_member(data)

    def test_archive_and_predecessor_bytes_remain_whole_and_account_paths_are_refused(self):
        client = S3({})
        body = zipped()
        ref = audit.retain(client, body)
        self.assertEqual(audit.checked(client, ref), body)
        for key in ('portfolio/account.json', 'data/trade-tickets.json', audit.PRIVATE + '../other'):
            with self.assertRaises(ValueError):
                audit.read(client, key)
        client.data[ref['key']] = b'tampered'
        with self.assertRaises(AssertionError):
            audit.checked(client, ref)

    def test_capture_is_one_keyless_reviewed_request_and_retains_error_bytes(self):
        calls, client = [], S3({})
        def transport(request, timeout):
            calls.append(request)
            result = BytesIO(b'provider unavailable')
            result.status, result.headers = 403, {'Content-Type': 'text/html', 'Set-Cookie': 'not retained'}
            return result
        result = audit.capture(client, URL, transport)
        self.assertEqual(len(calls), 1)
        self.assertNotIn('Authorization', calls[0].headers)
        self.assertNotIn('set-cookie', result['headers'])
        self.assertEqual(result['status'], 'provider_error_retained')
        self.assertEqual(audit.checked(client, result['original']), b'provider unavailable')
        with self.assertRaises(ValueError):
            audit.capture(client, 'https://example.com/private', transport)
        self.assertEqual(len(calls), 1)

    def test_predecessor_inventory_does_not_promote_old_scores_or_fabricate_ftd_rows(self):
        value = audit.inventory({'version': '1', 'board': [{'ticker': 'ABC', 'state': 'LOADED', 'score': 99}], 'top_picks': [{'ticker': 'ABC'}]})
        self.assertEqual(value['rows_with_retained_sec_balance'], 0)
        self.assertFalse(value['source_originals_verified'])
        self.assertFalse(value['legacy_forecast_qualified'])
        self.assertFalse(value['sizing_qualified'])

    def test_failed_baseline_is_durable_and_never_invokes_or_retries(self):
        client = S3({audit.CURRENT: b'{"board":[],"top_picks":[]}'})
        lam = Mock();lam.get_function_configuration.return_value = {'FunctionArn': 'arn:native'}
        calls = []
        def transport(request, timeout):
            calls.append(request)
            response = BytesIO(b'Unavailable SEC response')
            response.status, response.headers = 403, {}
            return response
        capture = audit.capture
        with patch.object(audit.boto3, 'client', side_effect=lambda name, **kw: client if name == 's3' else lam), \
             patch.object(audit, 'runtime', return_value={'receipt': {'status': 'matched', 'commit': 'fixture'}}), \
             patch.object(audit, 'source_commit', return_value='fixture'), \
             patch.object(audit, 'bindings', return_value={'schedules': [], 'classic_default_bus_rules': []}), \
             patch.object(audit, 'report', return_value=MagicMock()), \
             patch.object(audit, 'capture', side_effect=lambda s3, url: capture(s3, url, transport)):
            with self.assertRaisesRegex(AssertionError, 'Provider error retained'):
                audit.main()
            self.assertEqual(audit.raw.strict(client.data[audit.STATUS])['status'], 'failed')
            with self.assertRaisesRegex(AssertionError, 'never repeat'):
                audit.main()
        self.assertEqual(len(calls), 1)
        self.assertEqual(client.data[audit.CURRENT], b'{"board":[],"top_picks":[]}')
        self.assertTrue(all(w['Key'].startswith(audit.PRIVATE) for w in client.writes))
        lam.invoke.assert_not_called()


if __name__ == '__main__':
    unittest.main(verbosity=2)
