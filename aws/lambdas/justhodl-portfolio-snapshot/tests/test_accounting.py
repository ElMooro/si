"""Invented accounting regressions through reviewed current code, no native I/O."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import copy
import importlib.util
import json
import math
import unittest
from unittest.mock import patch
from test_watchlist_sync import load_current

ROOT = Path(__file__).resolve().parents[4]
NOW = datetime(2026, 9, 30, 6, tzinfo=timezone.utc)


def position(**kw):
    return {"symbol": "AAA", "qty": 10, "cost_basis_per_share": 100, **kw}


def enriched(**kw):
    return {"AAA": {"symbol": "AAA", "current_price": 110,
                    "price_asof_unix_ms": NOW.timestamp() * 1000, **kw}}


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.mod = load_current()

    def calc(self, rows=None, marks=None):
        return self.mod.build_holdings_accounting(rows if rows is not None else [position()], marks if marks is not None else enriched(), NOW)

    def test_invalid_quantities_never_become_zero_or_one(self):
        for qty in (None, True, False, "10", "NaN", float("nan"), float("inf"), [], 10**400):
            rows, summary, _, gross = self.calc([position(qty=qty)])
            self.assertIsNone(rows[0]['qty']); self.assertIsNone(rows[0]['market_value'])
            self.assertIsNone(summary['total_pnl_dollars']); self.assertIsNone(gross)
            self.assertEqual(summary['accounting_coverage_status'], 'PARTIAL')

    def test_cost_basis_unknown_does_not_invent_a_gain(self):
        for cost in (None, True, "100", -100, float("nan")):
            rows, summary, _, gross = self.calc([position(cost_basis_per_share=cost)])
            self.assertEqual(rows[0]['market_value'], 1100); self.assertEqual(gross, 1100)
            self.assertIsNone(rows[0]['pnl_dollars']); self.assertIsNone(summary['total_pnl_dollars'])
            self.assertIsNone(summary['total_cost_basis']); self.assertEqual(summary['pnl_eligible_positions_count'], 0)

    def test_zero_quantity_and_zero_cost_are_preserved(self):
        rows, summary, _, gross = self.calc([position(qty=0)])
        self.assertEqual(rows[0]['market_value'], 0); self.assertEqual(summary['total_pnl_dollars'], 0)
        self.assertEqual(gross, 0); self.assertIsNone(rows[0]['stop_hit'])
        rows, summary, _, _ = self.calc([position(cost_basis_per_share=0)])
        self.assertEqual(rows[0]['cost_basis_total'], 0); self.assertEqual(summary['total_pnl_dollars'], 1100)
        self.assertIsNone(summary['total_pnl_pct'])

    def test_marks_require_typed_price_and_timestamp(self):
        for value in (True, "110", None, 0, -1, float("inf")):
            rows, summary, _, _ = self.calc(marks=enriched(current_price=value))
            self.assertIsNone(rows[0]['market_value']); self.assertIsNone(summary['total_pnl_dollars'])
        for value in (True, str(NOW.timestamp()*1000), None, float("nan")):
            rows, _, _, _ = self.calc(marks=enriched(price_asof_unix_ms=value))
            self.assertEqual(rows[0]['valuation_status'], 'INVALID_MARK')

    def test_exact_stale_and_future_boundaries_do_not_use_rounded_display(self):
        for age, eligible in ((432000, True), (432000.001, False), (432060, False), (-300, True), (-300.001, False)):
            rows, _, _, _ = self.calc(marks=enriched(price_asof_unix_ms=(NOW.timestamp()-age)*1000))
            self.assertEqual(rows[0]['market_value'] is not None, eligible, age)
        rows, _, _, _ = self.calc(marks=enriched(price_asof_unix_ms=(NOW.timestamp()-432060)*1000))
        self.assertEqual(rows[0]['mark_age_h'], 120.0); self.assertEqual(rows[0]['valuation_status'], 'STALE_MARK')

    def test_unpriced_nonempty_book_is_unknown_but_empty_book_is_zero(self):
        rows, summary, _, gross = self.calc(marks={})
        self.assertIsNone(summary['total_market_value']); self.assertIsNone(summary['total_pnl_dollars']); self.assertIsNone(gross)
        self.assertEqual(summary['total_cost_basis'], 1000); self.assertEqual(summary['unpriced_cost_basis'], 1000)
        _, summary, _, gross = self.calc(rows=[])
        self.assertEqual(summary['total_pnl_dollars'], 0); self.assertEqual(gross, 0)
        self.assertEqual(summary['accounting_coverage_status'], 'EMPTY')

    def test_long_short_pnl_uses_paired_gross_basis_without_net_cancellation(self):
        marks = enriched(); marks['BBB'] = {**marks['AAA'], 'symbol':'BBB', 'current_price':90}
        rows, summary, _, gross = self.calc([position(), position(symbol='BBB', qty=-10)], marks)
        self.assertEqual([r['pnl_dollars'] for r in rows], [100,100])
        self.assertEqual(summary['total_market_value'], 200); self.assertEqual(summary['total_cost_basis'], 0)
        self.assertEqual(summary['total_pnl_dollars'], 200); self.assertEqual(summary['total_pnl_pct'], 10)
        self.assertEqual(summary['pnl_pct_denominator'], 2000); self.assertEqual(gross, 2000)

    def test_partial_mark_and_basis_coverage_are_separate(self):
        marks = enriched(); marks['BBB'] = {**marks['AAA'], 'symbol':'BBB'}
        rows, summary, _, gross = self.calc([position(), position(symbol='BBB', cost_basis_per_share=None), position(symbol='CCC')], marks)
        self.assertEqual(summary['total_market_value'], 2200); self.assertEqual(summary['total_pnl_dollars'], 100)
        self.assertEqual((summary['priced_positions_count'], summary['basis_positions_count'], summary['pnl_eligible_positions_count']), (2,2,1))
        self.assertEqual(summary['pnl_excluded_positions'], ['BBB','CCC']); self.assertEqual(summary['total_pnl_pct'], 10)
        self.assertTrue(all(row['current_weight_pct'] is None for row in rows)); self.assertEqual(gross, 2200)

    def test_aggregate_unrounded_values_against_decimal_oracle(self):
        rows = [position(symbol='X'+str(i), qty=1, cost_basis_per_share=1) for i in range(100)]
        marks = {p['symbol']:{**enriched()['AAA'], 'symbol':p['symbol'], 'current_price':1.004} for p in rows}
        actual, summary, _, gross = self.calc(rows, marks)
        expected = (Decimal('1.004')-Decimal('1'))*100
        self.assertEqual(summary['total_pnl_dollars'], float(expected)); self.assertEqual(summary['total_market_value'],100.4)
        self.assertEqual(sum(row['pnl_dollars'] for row in actual),0); self.assertEqual(gross,100.4)

    def test_overflow_withholds_affected_totals_and_never_emits_nonfinite(self):
        rows, summary, _, gross = self.calc([position(qty=1e308)])
        self.assertIsNone(rows[0]['market_value']); self.assertIsNone(rows[0]['cost_basis_total'])
        self.assertIn('MARKET_VALUE_OVERFLOW',rows[0]['accounting_reason_codes'])
        marks = enriched(current_price=1e308); marks['BBB']={**marks['AAA'],'symbol':'BBB'}
        rows, summary, _, gross = self.calc([position(qty=1,cost_basis_per_share=0),position(symbol='BBB',qty=1,cost_basis_per_share=0)],marks)
        self.assertIsNone(summary['total_market_value']); self.assertIsNone(summary['total_pnl_dollars']); self.assertIsNone(gross)
        self.assertIn('PNL_AGGREGATE_OVERFLOW',summary['accounting_reason_codes']);json.dumps(summary,allow_nan=False)

    def test_malformed_and_duplicate_rows_are_retained_and_withheld(self):
        supplied=[None, True, position(symbol=True), position(), position()]
        rows, summary, _, _=self.calc(supplied)
        self.assertEqual(len(rows),len(supplied));self.assertEqual([r['source_record_index'] for r in rows],list(range(5)))
        self.assertTrue(all(r['market_value'] is None for r in rows));self.assertEqual(summary['priced_positions_count'],0)

    def test_invalid_stop_and_target_do_not_crash_or_imply_execution(self):
        rows, _, _, _=self.calc([position(stop_loss=True,target_weight_pct='NaN')])
        self.assertIsNone(rows[0]['stop_hit']);self.assertIsNone(rows[0]['target_weight_pct'])
        self.assertEqual(rows[0]['stop_comparison_scope'],'PREVIOUS_CLOSE_NOT_EXECUTION')
        rows, _, _, _=self.calc([position(stop_loss=120,target_weight_pct=5)])
        self.assertTrue(rows[0]['stop_hit']);self.assertIsNone(rows[0]['weight_drift_pct'])

    def test_accounting_does_not_mutate_complete_inputs(self):
        positions=[position(notes='invented owner note',extra={'x':[1,2]})];marks=enriched()
        before=copy.deepcopy((positions,marks));self.calc(positions,marks)
        self.assertEqual((positions,marks),before)

    def test_whole_seven_predecessor_cases_through_current_handler(self):
        fixture=json.loads((ROOT/'tests/fixtures/pre-snapshot-accounting/complete-synthetic.json').read_bytes())
        self.assertEqual(len(fixture['cases']),7)
        spec=importlib.util.spec_from_file_location('accounting_shared_fixture',ROOT/'aws/lambdas/justhodl-portfolio-admin/tests/run_tests.py')
        shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
        frozen=datetime.fromisoformat(fixture['observed_at'])
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return frozen
        for case in fixture['cases']:
            mod=shared._load_snapshot(case['price_frame']);mod.datetime=Clock
            mod.load_s3_json=lambda key,default:default
            mod.sync_auto_watchlist=lambda _:dict(added_S=[],added_A=[],removed_S=[],removed_A=[])
            mod.query_pk=lambda key:case['positions'] if key=='POSITION' else []
            writes=[];published=[];mod.publish_snapshot=lambda body,identity,context=None:published.append(json.loads(body))
            mod.s3.put_object=lambda **kw:writes.append(kw['Body'])
            self.assertEqual(mod.lambda_handler({},None)['statusCode'],200)
            payload=json.loads(writes[0],parse_constant=lambda x:(_ for _ in ()).throw(AssertionError(x)))
            self.assertIsNone(payload['portfolio_summary']['total_pnl_dollars'],case['name'])
            self.assertEqual(payload['accounting']['source_positions'],case['positions'])
            self.assertEqual(payload['accounting']['source_prices'],case['price_frame'])
            self.assertEqual(payload,published[0])

    def test_publication_never_receives_nonfinite_json(self):
        mod=self.mod;mod.load_s3_json=lambda key,default:default
        mod.sync_auto_watchlist=lambda _:dict(added_S=[],added_A=[],removed_S=[],removed_A=[])
        mod.query_pk=lambda key:[position(qty=float('nan'))] if key=='POSITION' else []
        mod.batch_fetch_prices=lambda syms,**kwargs:{'AAA':{'price':110,'as_of_unix_ms':NOW.timestamp()*1000}}
        published=[];mod.publish_snapshot=lambda body,identity,context=None:published.append(json.loads(body));written=[];mod.s3.put_object=lambda **kw:written.append(kw['Body'])
        mod.lambda_handler({},None)
        payload=json.loads(written[0]);self.assertEqual(payload['accounting']['source_positions'][0]['qty'],{'rejected_number_type':'float','representation':'nan'})
        self.assertIsNone(payload['positions'][0]['qty']);json.dumps(published[0],allow_nan=False)
        # An unrelated invalid enrichment cannot reach either publication sink.
        published.clear();written.clear()
        mod.query_pk=lambda key:[position()] if key=='POSITION' else []
        mod.batch_fetch_prices=lambda syms,**kwargs:{'AAA':{'price':110,'as_of_unix_ms':NOW.timestamp()*1000,'volume':float('inf')}}
        with self.assertRaises(ValueError):mod.lambda_handler({},None)
        self.assertEqual(published,[]);self.assertEqual(written,[])


if __name__ == '__main__':unittest.main()
