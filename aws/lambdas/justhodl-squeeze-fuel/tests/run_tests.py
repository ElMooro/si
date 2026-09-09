"""D27 actual-handler donor ablation and failure tests. No AWS/provider calls."""
import contextlib
import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2] / 'shared'))
NOW = datetime.now(timezone.utc)
TODAY = NOW.date().isoformat()
OLD = (NOW - timedelta(days=14)).date().isoformat()


class MemoryS3:
    def __init__(self, docs): self.docs, self.writes = docs, {}
    def get_object(self, Bucket, Key): return {'Body': io.BytesIO(json.dumps(self.docs[Key]).encode())}
    def put_object(self, Bucket, Key, Body, **kwargs):
        def invalid(value): raise AssertionError('Non-finite public JSON: ' + value)
        self.writes[Key] = json.loads(Body, parse_constant=invalid)


def load():
    fake = types.ModuleType('boto3'); fake.client = lambda *a, **kw: None
    secret = types.ModuleType('managed_secret'); secret.managed_secret = lambda *a, **kw: ''
    with patch.dict(sys.modules, {'boto3': fake, 'managed_secret': secret}):
        spec = importlib.util.spec_from_file_location('squeeze_under_test', HERE.parent / 'source/lambda_function.py')
        mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod


def donor():
    return {'version': '1.1', 'measurement_contract':'short-positioning.v2',
            'measurement_units':{'short_interest':'shares','days_to_cover':'days','si_change_pct':'percent'}, 'generated_at': NOW.isoformat(), 'by_ticker': {'GME': {
        'ticker': 'GME', 'settlement_date': TODAY,
        'short_interest_source':'FINRA consolidated short interest','short_interest_as_of':TODAY,
        'days_to_cover_source':'FINRA consolidated short interest','days_to_cover_as_of':TODAY, 'short_interest': 20_000_000,
        'days_to_cover': 8, 'si_change_pct': 30, 'latest_short_pct': 99,
        'short_float_pct': 99, 'float_shares': 1, 'short_src': 'finviz'}}}


class DonorHandlerTests(unittest.TestCase):
    def run_handler(self, doc=None, direct=True, enrichment=None):
        mod = load(); s3 = MemoryS3({'data/short-interest.json': doc} if doc is not None else {})
        base = {'GME': {'name': 'GameStop', 'settlementDate': OLD, 'short_interest': 10_000_000,
                        'days_to_cover': 2, 'si_change_pct': 0}} if direct else {}
        enrich = {'sym': 'GME', 'float_shares': 100_000_000, 'float_as_of': TODAY,
                  'price': 100, 'ma50': 90, 'year_high': 110}
        if enrichment is not None: enrich = enrichment
        with patch.object(mod, 'S3', s3), patch.object(mod, 'CORE_WATCHLIST', ['GME']), \
             patch.object(mod, 'fetch_finra_si', return_value=(base, OLD)), \
             patch.object(mod, 'fetch_sec_ftd', return_value=({}, None)), \
             patch.object(mod, 'fetch_daily_shortvol', return_value={'GME': {'svr': 0.42, 'svr_z': 0}}), \
             patch.object(mod, 'fmp_float_and_price', return_value=enrich), \
             patch.object(mod.urllib.request, 'urlopen', side_effect=AssertionError('network forbidden')), \
             contextlib.redirect_stdout(io.StringIO()):
            response = mod.lambda_handler({}, None)
        self.assertEqual(response['statusCode'], 200)
        return s3.writes[mod.OUT_KEY]

    def test_newer_dated_donor_changes_actual_score_and_preserves_units(self):
        baseline = self.run_handler(); rich = self.run_handler(donor())
        before, row = baseline['board'][0], rich['board'][0]
        self.assertGreater(row['score'], before['score'])
        self.assertEqual(row['short_interest'], 20_000_000)
        self.assertEqual(row['pct_of_float'], 20)
        self.assertEqual(row['settlement_date'], TODAY)
        self.assertTrue(row['short_positioning']['applied'])
        self.assertEqual(row['short_positioning']['health']['units']['short_interest'], 'shares')
        self.assertEqual(row['daily_short_volume']['record']['svr'], 0.42)
        self.assertFalse(row['short_positioning']['daily_short_volume_used'])
        self.assertFalse(row['execution_eligible']); self.assertIsNone(row['borrow_availability'])
        self.assertFalse(row['locate_verified'])

    def test_canonical_donor_can_restore_missing_direct_finra(self):
        self.assertFalse(self.run_handler(direct=False)['ok'])
        out = self.run_handler(donor(), direct=False)
        self.assertTrue(out['ok']); self.assertEqual(out['n_scored'], 1)

    def test_mixed_daily_volume_and_undated_float_fields_have_no_score_effect(self):
        doc = donor(); initial = self.run_handler(doc)['board'][0]
        doc['by_ticker']['GME'].update(latest_short_pct=0, short_float_pct=0, float_shares=9999999999)
        changed = self.run_handler(doc)['board'][0]
        self.assertEqual(initial['score'], changed['score'])
        self.assertEqual(initial['pct_of_float'], changed['pct_of_float'])

    def test_stale_future_missing_schema_and_bad_numbers_ablate_to_direct(self):
        score = self.run_handler()['board'][0]['score']
        mutations = [
            ('stale envelope', lambda d: d.update(generated_at=(NOW-timedelta(hours=73)).isoformat())),
            ('future envelope', lambda d: d.update(generated_at=(NOW+timedelta(hours=1)).isoformat())),
            ('old observation', lambda d: d['by_ticker']['GME'].update(settlement_date=(NOW-timedelta(days=36)).date().isoformat())),
            ('undated', lambda d: d['by_ticker']['GME'].pop('settlement_date')),
            ('schema', lambda d: d.update(version='99')),
            ('nan', lambda d: d['by_ticker']['GME'].update(days_to_cover=float('nan'))),
            ('undated ratio fallback',lambda d:d['by_ticker']['GME'].update(days_to_cover_source='Finviz short ratio',days_to_cover_as_of=None)),
            ('ratio wrong date',lambda d:d['by_ticker']['GME'].update(days_to_cover_as_of='2020-01-01')),
            ('unit',lambda d:d['measurement_units'].update(short_interest='USD')),
            ('malformed units',lambda d:d.update(measurement_units=['shares'])),
            ('legacy contract',lambda d:d.pop('measurement_contract')),
            ('negative', lambda d: d['by_ticker']['GME'].update(short_interest=-1)),
            ('boolean', lambda d: d['by_ticker']['GME'].update(short_interest=True)),
        ]
        for label, change in mutations:
            with self.subTest(label=label):
                doc = donor(); change(doc); out = self.run_handler(doc)
                self.assertEqual(out['board'][0]['score'], score)
                evidence = out['board'][0].get('short_positioning')
                if evidence: self.assertFalse(evidence['applied'])

    def test_older_donor_never_replaces_newer_direct_settlement(self):
        doc = donor(); old = (NOW-timedelta(days=20)).date().isoformat()
        doc['by_ticker']['GME'].update(settlement_date=old,short_interest_as_of=old,days_to_cover_as_of=old)
        row = self.run_handler(doc)['board'][0]
        self.assertEqual(row['short_interest'], 10_000_000)
        self.assertFalse(row['short_positioning']['applied'])

    def test_undated_float_uses_explicit_dtc_fallback(self):
        row = self.run_handler(donor(), enrichment={'sym':'GME','float_shares':1})['board'][0]
        self.assertIsNone(row['pct_of_float'])
        self.assertEqual(row['float_evidence']['status'], 'UNAVAILABLE_OR_STALE')
        self.assertEqual(row['days_to_cover'], 8)

    def test_finviz_only_undated_position_is_not_inventory(self):
        doc = donor(); doc['by_ticker']['GME'] = {'ticker':'GME','latest_short_pct':99,'days_to_cover':30}
        out = self.run_handler(doc, direct=False)
        self.assertFalse(out['ok']); self.assertFalse(out['short_positioning']['GME']['positioning_usable'])


if __name__ == '__main__': unittest.main()
