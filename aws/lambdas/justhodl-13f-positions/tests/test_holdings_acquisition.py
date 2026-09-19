"""Offline source fixtures exercise discovery, archive coverage and failure paths."""
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error

import holdings_acquire as acquisition
import holdings_native as model
import holdings_store as store
from test_holdings_store import MemoryS3
from test_native_holdings import CURRENT, PRIOR, FIXTURES, SOURCES


def columnar(rows):
    names = {'accessionNumber': 'accession', 'form': 'form', 'reportDate': 'period_of_report',
             'filingDate': 'filed_at', 'primaryDocument': 'primary_doc', 'acceptanceDateTime': 'accepted_at'}
    return {key: [v[field] for v in rows] for key, field in names.items()}


class Response(io.BytesIO):
    status = 200
    headers = {'Content-Type': 'application/json'}
    def __init__(self, body, url):
        super().__init__(body); self.url = url
    def geturl(self):
        return self.url


def fixture(archived=False):
    """Synthetic HTTP catalogue around the complete real Berkshire XML fixtures."""
    s3, urls = MemoryS3(), {}
    cik = SOURCES[CURRENT]['cik']
    current, prior = SOURCES[CURRENT]['filing'], SOURCES[PRIOR]['filing']
    index = {'by_fund': {'BERKSHIRE': {'cik': cik, 'name': 'Synthetic roster label',
                        'latest_filing': prior, 'prior_filing': None}}}
    s3.objects[acquisition.INDEX] = model.encoded(index)
    submissions = {'cik': int(cik), 'name': 'BERKSHIRE HATHAWAY INC', 'filings': {
        'recent': columnar([current] if archived else [current, prior]), 'files': []}}
    if archived:
        name = 'CIK' + cik.zfill(10) + '-submissions-001.json'
        submissions['filings']['files'] = [{'name': name, 'filingCount': 1, 'filingFrom': prior['filed_at'], 'filingTo': prior['filed_at']}]
        urls['https://data.sec.gov/submissions/' + name] = model.encoded(columnar([prior]))
    urls['https://data.sec.gov/submissions/CIK' + cik.zfill(10) + '.json'] = model.encoded(submissions)
    for accession in (CURRENT, PRIOR):
        base = 'https://www.sec.gov/Archives/edgar/data/' + str(int(cik)) + '/' + accession.replace('-', '') + '/'
        names = []
        for source in SOURCES[accession]['sources']:
            filename = 'primary_doc.xml' if source['kind'] == 'cover_or_other_xml' else 'table.xml'
            urls[base + filename] = (FIXTURES / source['file']).read_bytes(); names.append({'name': filename})
        urls[base + 'index.json'] = model.encoded({'directory': {'item': names}})
    return s3, urls


class Acquisition(unittest.TestCase):
    def collect(self, s3, urls):
        called, delays = [], []
        def open_(request, timeout):
            called.append(request.full_url)
            if request.full_url not in urls:
                raise urllib.error.HTTPError(request.full_url, 503, 'Synthetic unavailable', {}, None)
            return Response(urls[request.full_url], request.full_url)
        ref = acquisition.acquire(s3, 'SYNTHETIC_BUCKET', 'Synthetic SEC integration test',
                                  opener=open_, sleep=delays.append, monotonic=lambda: 0)
        return ref, called, delays

    def test_current_periods_discovered_despite_stale_detector(self):
        s3, urls = fixture()
        ref, called, delays = self.collect(s3, urls)
        probe = store.verified(ref, store.reader(s3, 'SYNTHETIC_BUCKET'), 'probes')
        self.assertEqual(set(probe['roster']['BERKSHIRE']['filings_for_selected_periods']), {CURRENT, PRIOR})
        self.assertEqual(len(called), 7)
        self.assertTrue(all(v == 0.35 for v in delays))
        output, artifacts = model.build(probe, store.reader(s3, 'SYNTHETIC_BUCKET'), probe['generated_at'])
        self.assertEqual(output['funds']['BERKSHIRE']['current_holdings_period'], '2026-06-30')
        self.assertTrue(output['funds']['BERKSHIRE']['comparison_available'])
        self.assertEqual(len(artifacts), 3)
        self.assertFalse(any(key == model.CURRENT for key, _ in s3.writes))

    def test_archived_prior_discovered_and_original_archive_retained(self):
        s3, urls = fixture(archived=True)
        ref, called, _ = self.collect(s3, urls)
        self.assertEqual(len(called), 8)
        probe = store.verified(ref, store.reader(s3, 'SYNTHETIC_BUCKET'), 'probes')
        output, artifacts = model.build(probe, store.reader(s3, 'SYNTHETIC_BUCKET'), probe['generated_at'])
        detail = json.loads(artifacts[output['funds']['BERKSHIRE']['detail']['key']])
        self.assertEqual(len(detail['submissions_archives']), 1)
        self.assertEqual(detail['unacquired_relevant_archives'], [])
        self.assertTrue(detail['comparison']['eligible_for_disclosure_comparison'])

    def test_missing_archive_preserves_previous_publication_and_retains_failure(self):
        s3, urls = fixture(archived=True)
        del urls[next(key for key in urls if '-submissions-001' in key)]
        s3.objects[model.CURRENT] = b'SYNTHETIC_LAST_GOOD'
        with self.assertRaises(urllib.error.HTTPError):
            self.collect(s3, urls)
        self.assertEqual(s3.objects[model.CURRENT], b'SYNTHETIC_LAST_GOOD')
        failures = [key for key in s3.objects if key.startswith(model.PREFIX + 'acquisition-attempts/')]
        self.assertEqual(len(failures), 1)
        packet = json.loads(s3.objects[failures[0]])
        self.assertEqual(packet['failure_class'], 'HTTP_503')
        self.assertEqual(len(packet['retained_sources']), 1)
        self.assertFalse(packet['published'])

    def test_incomplete_new_acquisition_cannot_claim_current_coverage(self):
        s3, urls = fixture(archived=True)
        ref, _, _ = self.collect(s3, urls)
        probe = store.verified(ref, store.reader(s3, 'SYNTHETIC_BUCKET'), 'probes')
        label = next(key for key in probe['refs'] if ':submissions-archive:' in key)
        del probe['refs'][label]
        with self.assertRaisesRegex(ValueError, 'omits required'):
            model.build(probe, store.reader(s3, 'SYNTHETIC_BUCKET'), probe['generated_at'])

    def test_conflicting_catalogue_rows_cannot_select_arbitrary_revision(self):
        current = SOURCES[CURRENT]['filing']; changed = {**current, 'filed_at': '2026-08-15', 'accepted_at': '2026-08-15T20:00:00Z'}
        with self.assertRaisesRegex(ValueError, 'Conflicting'):
            model.merge_submissions([columnar([current]), columnar([changed])])
        self.assertEqual(len(model.merge_submissions([columnar([current]), columnar([current])])), 1)

    def test_archives_before_selected_report_dates_excluded_with_evidence(self):
        candidates = model.submission_rows(columnar([SOURCES[CURRENT]['filing'], SOURCES[PRIOR]['filing']]))
        submissions = {'filings': {'files': [{'name': 'CIK0001067983-submissions-001.json',
            'filingFrom': '2010-01-01', 'filingTo': '2020-01-01', 'filingCount': 10}]}}
        self.assertEqual(model.pending_archives(submissions, candidates, set()), [])
        submissions['filings']['files'][0]['filingTo'] = '2026-05-31'
        self.assertEqual(len(model.pending_archives(submissions, candidates, set())), 1)

    def test_malformed_archive_bounds_and_unsafe_network_destinations_rejected(self):
        for url in ('http://www.sec.gov/Archives/a.xml','https://www.sec.gov.evil.test/a.xml',
                    'https://data.sec.gov/submissions/../../private.json','https://data.sec.gov/submissions/CIK0001067983.json?token=secret',
                    'https://person@data.sec.gov/submissions/CIK0001067983.json'):
            with self.assertRaises(ValueError):
                acquisition.validate_url(url)
        acquisition.validate_url('https://data.sec.gov/submissions/CIK0001067983.json')

    def test_explicit_collect_route_does_not_require_user_credentials_or_old_probe(self):
        s3 = MemoryS3()
        with patch('holdings_acquire.acquire', return_value={'key': 'retained'}) as acquire, patch('holdings_store.run', return_value={'published': True}) as run:
            result = store.handle({'action': 'holdings_research_collect'}, s3, 'bucket', user_agent='EXISTING_CONFIGURED_UA')
        self.assertEqual(result['statusCode'], 200)
        acquire.assert_called_once_with(s3, 'bucket', 'EXISTING_CONFIGURED_UA', request_id=None)
        run.assert_called_once_with(s3, 'bucket', {'key': 'retained'})

    def test_schedule_routes_only_the_research_action(self):
        config = json.loads((Path(__file__).parents[1] / 'config.json').read_bytes())
        schedule = config['eventbridge_scheduler']
        self.assertEqual(schedule['input'], {'action': 'holdings_research_collect', 'notify': False})
        self.assertEqual(schedule['cron'], 'cron(40 0/2 * * ? *)')
        self.assertEqual(config['schedule']['expression'], 'rate(2 hours)')


if __name__ == '__main__':
    unittest.main()
