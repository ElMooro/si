"""Original-response, vintage, coverage and group arithmetic regressions."""
from pathlib import Path
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch
import ast, copy, json, sys, unittest
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import provider_flow_native as native
import provider_flow_model as model
import provider_flow_catalog as catalog

STAMP = '2026-09-21T06:00:00+00:00'
ACQUIRED = '2026-09-21T05:00:00+00:00'


def fixtures(ticker='SPY', n=25):
    dates = []; d = date(2026, 9, 18)
    while len(dates) < n:
        if d.weekday() < 5: dates.append(d.isoformat())
        d -= timedelta(days=1)
    rows = [{'composite_ticker': ticker, 'effective_date': d, 'processed_date': d,
             'fund_flow': i, 'nav': 100 + i, 'shares_outstanding': 1000 + i} for i, d in enumerate(dates)]
    return rows


def retain(ticker, rows, extra=None):
    doc = {'status': 'OK', 'results': rows, **(extra or {})}; raw = native.encoded(doc)
    ref = {'key': native.PRIVATE + native.sha(raw) + '.bin', 'sha256': native.sha(raw), 'bytes': len(raw)}
    collection = {'ticker': ticker, 'query_date': '2026-09-21', 'completed_at': ACQUIRED, 'status': 'retained',
        'pages': [{'url': native.initial_url(ticker, '2026-09-21'), 'acquired_at': ACQUIRED, 'original': ref}]}
    return collection, {ref['key']: raw}


def restore(rows=None, ticker='SPY', at=STAMP, extra=None):
    collection, objects = retain(ticker, fixtures(ticker) if rows is None else rows, extra)
    return native.reconstruct(ticker, collection, objects.__getitem__, at)


class OriginalFlow(unittest.TestCase):
    def test_exact_decimal_zero_and_source_positions(self):
        fund = restore(); self.assertEqual(fund['quality']['status'], 'complete_acquired_history')
        self.assertEqual(fund['history'][-1]['flow_decimal'], '0')
        self.assertEqual(fund['history'][-1]['source_rows'][0]['row_index'], 0)
        dates = [r['date'] for r in fund['history']]
        result = native.window(fund, dates, dates[-1], 5)
        self.assertEqual(result['flow_usd_decimal'], '10')
        self.assertEqual(result['end_reported_assets_usd_decimal'], '100000')
        self.assertEqual(Decimal(result['flow_to_end_assets_pct_decimal']), Decimal('.01'))

    def test_missing_latest_or_interior_flow_is_not_skipped(self):
        for idx in (0, 2):
            rows = fixtures(); rows[idx]['fund_flow'] = None; fund = restore(rows)
            dates = [r['date'] for r in fund['history']]
            result = native.window(fund, dates, dates[-1], 5)
            self.assertEqual(result['status'], 'incomplete'); self.assertIsNone(result['flow_usd_decimal'])

    def test_short_history_never_reports_longer_total(self):
        fund = restore(fixtures(n=3)); dates = [r['date'] for r in fund['history']]
        for n in (5, 21): self.assertIsNone(native.window(fund, dates, dates[-1], n)['flow_usd_decimal'])

    def test_reference_gap_is_explicit(self):
        all_rows = fixtures(); dates = sorted(r['effective_date'] for r in all_rows)
        rows = copy.deepcopy(all_rows); missing = rows.pop(2)['effective_date']; fund = restore(rows)
        out = native.window(fund, dates, dates[-1], 5)
        self.assertEqual(out['missing_dates'], [missing]); self.assertIsNone(out['flow_usd_decimal'])

    def test_latest_processed_revision_and_conflict_never_fall_back(self):
        rows = fixtures(); latest = {**rows[0], 'processed_date': '2026-09-19', 'fund_flow': 123}
        rows += [latest]; out = restore(rows)
        self.assertEqual(out['history'][-1]['flow_decimal'], '123'); self.assertEqual(out['revision_versions_selected'], 1)
        rows += [{**latest, 'fund_flow': 456}]; out = restore(rows)
        self.assertEqual(out['quality']['status'], 'invalid'); self.assertIsNone(out['history'][-1]['flow_decimal'])

    def test_equivalent_numeric_formats_are_duplicate_evidence(self):
        rows = fixtures(); rows += [{**rows[0], 'fund_flow': 0.0, 'nav': 100.0}]
        out = restore(rows); self.assertEqual(out['quality']['status'], 'complete_acquired_history')
        self.assertEqual(len(out['history'][-1]['source_rows']), 2)

    def test_invalid_dates_and_bools_are_never_promoted(self):
        for update in ({'effective_date': None}, {'processed_date': '2026-09-22'}, {'effective_date': '2026-09-19'},
                       {'fund_flow': True}, {'nav': -1}, {'shares_outstanding': '1000'}):
            rows = fixtures(); rows[0].update(update); self.assertEqual(restore(rows)['quality']['status'], 'invalid')

    def test_fresh_compilation_cannot_refresh_acquisition(self):
        out = restore(at='2026-09-22T08:00:00+00:00'); self.assertEqual(out['quality']['status'], 'stale')
        self.assertEqual(out['source_valid_until'], '2026-09-22T07:00:00+00:00')

    def test_fund_identity_count_hash_and_duplicate_fields_rejected(self):
        rows = fixtures(); rows[0]['composite_ticker'] = 'XLF'
        with self.assertRaises(ValueError): restore(rows)
        with self.assertRaises(ValueError): restore(extra={'count': 1})
        c, objects = retain('SPY', fixtures()); objects[c['pages'][0]['original']['key']] += b' '
        with self.assertRaises(ValueError): native.reconstruct('SPY', c, objects.__getitem__, STAMP)
        raw = b'{"status":"OK","results":[],"status":"OK"}'
        ref = {'key': native.PRIVATE + native.sha(raw) + '.bin', 'sha256': native.sha(raw), 'bytes': len(raw)}
        with self.assertRaises(ValueError): native.original(ref, lambda _: raw)

    def test_complete_linked_pagination_and_cycle_rejection(self):
        url = native.ENDPOINT + '?cursor=reviewed-page-two'
        rows = fixtures(); c, objects = retain('SPY', rows[:10], {'next_url': url})
        with self.assertRaises(ValueError): native.reconstruct('SPY', c, objects.__getitem__, STAMP)
        second, more = retain('SPY', rows[10:]); objects.update(more)
        c['pages'].append({**second['pages'][0], 'url': url})
        self.assertEqual(len(native.reconstruct('SPY', c, objects.__getitem__, STAMP)['history']), 25)
        c['pages'][1]['url'] = c['pages'][0]['url']
        with self.assertRaises(ValueError): native.reconstruct('SPY', c, objects.__getitem__, STAMP)
        for bad in ('http://api.polygon.io' + native.PATH + '?cursor=x',
                    'https://other.invalid' + native.PATH + '?cursor=x',
                    native.ENDPOINT + '?apiKey=hidden', native.ENDPOINT + '?cursor=x&cursor=y',
                    'https://api.polygon.io/another-route?cursor=x'):
            with self.assertRaises(ValueError): native.next_url(bad)

    def test_failed_partial_acquisition_retains_rows_without_arithmetic(self):
        c, objects = retain('SPY', fixtures()); c['status'] = 'provider_http_error'
        out = native.reconstruct('SPY', c, objects.__getitem__, STAMP)
        self.assertEqual(len(out['history']), 25); self.assertEqual(out['quality']['status'], 'unavailable')
        dates = [r['date'] for r in out['history']]
        self.assertIsNone(native.window(out, dates, dates[-1], 5)['flow_usd_decimal'])

    def test_reconciliation_never_substitutes_current_nav_for_reported_flow(self):
        rows = fixtures(n=2)
        rows[1].update(nav=100, shares_outstanding=1000, fund_flow=0)
        rows[0].update(nav=110, shares_outstanding=1010, fund_flow=1000)
        fund = restore(rows); result = native.reconcile(fund, sorted(r['effective_date'] for r in rows))[0]
        self.assertEqual(result['prior_nav_valued_change_usd_decimal'], '1000')
        self.assertEqual(result['current_nav_valued_change_usd_decimal'], '1100')
        self.assertEqual(result['reported_minus_current_nav_change_usd_decimal'], '-100')
        self.assertFalse(result['corporate_actions_verified'])


class GroupFlow(unittest.TestCase):
    def funds(self):
        spy = restore(); dates = [r['date'] for r in spy['history']]
        xlf = restore(ticker='XLF')
        return {t: model.fund_measurements(f, dates, dates[-1]) for t, f in [('SPY', spy), ('XLF', xlf)]}, dates

    def test_unique_group_total_and_partial_subtotal_are_separate(self):
        funds, dates = self.funds(); out = model.aggregate(['SPY', 'SPY', 'XLF'], funds, 5, dates[-1])
        self.assertEqual(out['required_count'], 2); self.assertEqual(out['flow_usd_decimal'], '20')
        out = model.aggregate(['SPY', 'XLF', 'QQQ'], funds, 5, dates[-1])
        self.assertIsNone(out['flow_usd_decimal']); self.assertEqual(out['observed_subset_flow_usd_decimal'], '20')
        self.assertEqual(out['excluded'], {'QQQ': ['fund_not_collected']})

    def test_flow_totals_differ_from_directional_comparison(self):
        funds, dates = self.funds()
        out = model.comparison('fixture', 'test', {'bull': ['SPY'], 'bear': ['XLF']}, funds, dates[-1])['windows']['5']
        self.assertEqual(out['total_reported_fund_flow_usd_decimal'], '20')
        self.assertEqual(out['bull_minus_bear_flow_usd_decimal'], '0')
        with self.assertRaises(ValueError): model.comparison('fixture', 'test', {'bull': ['SPY'], 'bear': ['SPY']}, funds, dates[-1])
        one_sided = model.comparison('fixture', 'test', {'bull': ['SPY']}, funds, dates[-1])['windows']['5']
        self.assertIsNone(one_sided['total_reported_fund_flow_usd_decimal'])

    def test_group_rejects_different_date_grids(self):
        funds, dates = self.funds(); funds['XLF']['aligned_windows']['5']['dates'][0] = '2026-09-10'
        with self.assertRaises(ValueError): model.aggregate(['SPY', 'XLF'], funds, 5, dates[-1])

    def test_all_catalogs_preserve_the_complete_predecessor(self):
        for fn, names in [('justhodl-etf-fund-flows', ('ETF_UNIVERSE',)),
                          ('justhodl-capital-flow-radar', ('COMPLEXES', 'SINGLE_STOCK_LEV'))]:
            source = ROOT / 'aws/lambdas' / fn / 'source'
            archived = source / ('legacy_etf_fund_flows.py' if fn.endswith('etf-fund-flows') else 'legacy_capital_flow_radar.py')
            tree = ast.parse((archived if archived.exists() else source / 'lambda_function.py').read_text(encoding='utf-8'))
            found = {}
            for node in tree.body:
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if getattr(target, 'id', None) in names: found[target.id] = ast.literal_eval(node.value)
            for name in names: self.assertEqual(found[name], getattr(catalog, name))
        self.assertEqual([len(catalog.ETF_UNIVERSE), len(catalog.COMPLEXES), len(catalog.SINGLE_STOCK_LEV)], [300, 46, 20])

    def test_complete_model_is_reproducible_and_has_no_investment_authority(self):
        collections = {}; objects = {}
        for ticker in catalog.ETF_UNIVERSE:
            collection, raw = retain(ticker, fixtures(ticker)); collections[ticker] = collection; objects.update(raw)
        inputs = {'contract': 'provider-flow-inputs.v1', 'generated_at': STAMP, 'collections': collections,
                  'contexts': {}, 'provider_requests': 300}
        packet, histories = model.build(inputs, objects.__getitem__)
        again, again_histories = model.build(copy.deepcopy(inputs), objects.__getitem__)
        self.assertEqual(packet, again); self.assertEqual(histories, again_histories)
        self.assertEqual(len(packet['funds']), 300); self.assertEqual(len(packet['complexes']), 46)
        self.assertEqual(packet['quality']['matched_five_observation_funds'], 300)
        self.assertEqual(packet['quality']['independent_investment_votes'], 0)
        self.assertTrue(all(packet[k] is False for k in model.PERMISSIONS))
        self.assertIsNone(packet['call']); self.assertEqual(packet['portfolio_action'], 'WAIT')
        ref = packet['funds']['SPY']['history']; self.assertEqual(native.sha(histories[ref['key']]), ref['sha256'])
        packet['replay'] = {'manifest_key': model.PREFIX + 'runs/' + 'a'*64 + '.json', 'output_sha256': native.sha(native.encoded(packet))}
        radar = model.radar(packet, STAMP, {'protected': True}, None)
        self.assertEqual(radar['complexes'], packet['complexes']); self.assertEqual(radar['source_valid_until'], packet['source_valid_until'])
        self.assertTrue(all(radar[k] is False for k in model.PERMISSIONS))


if __name__ == '__main__': unittest.main(verbosity=2)
