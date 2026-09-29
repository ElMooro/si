"""Complete feature/population regressions in both actual native auction paths."""
from pathlib import Path
from contextlib import redirect_stdout
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import io, json, runpy, sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from auction_scoring import score_auction, aggregate_indicators
support = runpy.run_path(str(ROOT / 'tests/deployment/test_auction_output_ownership.py'))
primitive = runpy.run_path(str(ROOT / 'tests/test_auction_observations.py'))['detector']()


def raw(**changes):
    return {**support['auction'](), **changes}


def score(row=None, policy=4.25):
    metrics = primitive['compute_record_metrics'](row or raw())
    return score_auction(metrics, primitive['score_indicators'](metrics, policy), policy)


def native(rows):
    store, scope = support['detector_fixture']()
    handler = scope['lambda_handler']
    handler.__globals__['fetch_fiscal_auctions'] = lambda *a, **k: rows
    with redirect_stdout(io.StringIO()), patch('urllib.request.urlopen', side_effect=AssertionError('no network')):
        assert handler({}, None)['statusCode'] == 200
    return store.docs['data/auction-crisis.json'], scope


def test_missing_dimensions_never_inflate_or_renormalize_coupon_score():
    full = raw(bid_to_cover_ratio='2.0')
    result = score(full)
    assert result['composite_score'] == 54.2  # once-rounded 54.25; existing Python rounding
    assert result['score_quality']['n_required'] == 4
    for changes in ({'allocation_pctage': None}, {'primary_dealer_accepted': None},
                    {'allocation_pctage': None, 'primary_dealer_accepted': None}, {'bid_to_cover_ratio': None}):
        out = score({**full, **changes})
        assert out['composite_score'] is None
        assert out['score_quality']['n_required'] == 4 and out['score_quality']['status'] == 'missing_inputs'
        assert out['max_indicator_score'] is out['avg_indicator_score'] is None


def test_policy_applicability_missing_quote_and_true_zero():
    bill = raw(security_type='Bill', security_term='4-Week', low_discnt_rate='4.0')
    assert score(bill)['score_quality']['n_required'] == 4
    assert score(bill, 0)['score_quality']['n_required'] == 3
    assert score(bill, 0)['score_quality']['status'] == 'complete'
    assert score(bill, None)['score_quality']['missing_features'] == ['zero_rate_floor']
    assert score({**bill, 'low_discnt_rate': None})['composite_score'] is None
    long_bill = score({**bill, 'security_term': '13-Week'}, None)
    assert long_bill['score_quality']['n_required'] == 3 and long_bill['composite_score'] is not None
    assert score()['composite_score'] == 0


def test_unsupported_instruments_and_borrowed_baselines_are_explicit():
    for changes in ({'floating_rate': 'Yes'}, {'inflation_index_security': None, 'floating_rate': None}):
        out = score(raw(**changes))
        assert out['composite_score'] is None and out['score_quality']['status'] == 'unsupported_instrument'
        assert out['score_quality']['baseline_cohort'] is None
    assert score(raw(security_term='2-Year'))['score_quality']['baseline_cohort'] == 'coupons_gt_3y'
    assert score(raw(security_type='Bill', security_term='13-Week'))['score_quality']['baseline_cohort'] == 'bills_lt_90d'


def test_invalid_scores_cannot_be_zero_or_complete():
    full = score()
    for value in (True, False, '0', float('nan'), float('inf'), -1, 101, 10**1000):
        out = score_auction(full, {**full['indicator_scores'], 'btc_extreme': value}, 4.25)
        assert out['composite_score'] is None and 'btc_extreme' in out['score_quality']['missing_features']


def test_all_observations_retained_when_ninety_percent_of_amount_lacks_btc():
    rows = [raw(), raw(cusip='MISSING01', bid_to_cover_ratio=None, total_accepted='90000000000')]
    out, _ = native(rows)
    assert len(out['auction_observations']) == len(out['recent_auctions']) == 2
    assert out['auction_population']['retained'] == 2 and out['auction_population']['row_omissions'] == 0
    assert out['composite_score'] is None and out['regime'] == 'UNAVAILABLE' and out['triggers'] == []
    assert out['weighting_quality']['known_weight_total_usd_bn'] == 100
    assert out['weighting_quality']['n_missing_scores'] == 1
    tenor = out['tenor_decomposition']['coupons_gt_3y']
    assert tenor['n_auctions'] == 2 and tenor['composite'] is tenor['max_composite'] is None
    assert out['composite_history']['current']['composite'] is None
    assert out['historical_analog']['top_matches'] == []
    assert all(row['similarity'] is None for row in out['historical_analog']['all_matches'])


def test_incomplete_zero_weight_does_not_remove_known_complete_positive_weight():
    out, _ = native([raw(), raw(cusip='ZERO00001', total_accepted='0', bid_to_cover_ratio=None)])
    assert out['composite_score'] == 0 and out['weighting_quality']['n_zero_weights'] == 1
    assert len(out['auction_observations']) == 2


def test_native_unsupported_positive_weight_withholds_without_omitting_source():
    out, _ = native([raw(), raw(cusip='FRN000001', floating_rate='Yes')])
    assert out['composite_score'] is None and len(out['auction_observations']) == 2
    assert out['auction_population']['complete_scores'] == 1


def test_full_population_survives_legacy_ten_row_sample_and_no_data_refresh():
    out, scope = native([raw(cusip=f'ROW{i:06d}') for i in range(27)])
    assert len(out['recent_auctions']) == 10 and len(out['auction_observations']) == 27
    assert out['n_recent_auctions_14d'] == 27
    handler = scope['lambda_handler']; handler.__globals__['fetch_fiscal_auctions'] = lambda *a, **k: []
    with redirect_stdout(io.StringIO()): handler({}, None)
    empty = handler.__globals__['s3'].docs['data/auction-crisis.json']
    assert empty['auction_observations'] == [] and empty['composite_score'] is None
    assert empty['auction_population']['retained'] == empty['auction_population']['complete_scores'] == 0


def test_measured_zero_missing_and_inapplicable_indicators_have_distinct_coverage():
    complete = aggregate_indicators([score()])
    assert complete['btc_extreme']['status'] == 'complete' and complete['btc_extreme']['max_score'] == 0
    assert complete['zero_rate_floor']['status'] == 'not_applicable' and complete['zero_rate_floor']['max_score'] is None
    partial = aggregate_indicators([score(), score(raw(bid_to_cover_ratio=None))])
    assert partial['btc_extreme']['status'] == 'incomplete' and partial['btc_extreme']['max_score'] == 0
    assert partial['btc_extreme']['n_missing'] == 1 and not partial['btc_extreme']['counts_complete']


def test_trigger_counts_use_seventy_not_fifty_and_missing_current_never_crashes():
    rows = [score(), score()]
    rows[0]['indicator_scores']['btc_extreme'] = 50
    rows[1]['indicator_scores']['btc_extreme'] = 70
    agg = aggregate_indicators(rows)
    assert agg['btc_extreme']['n_fired'] == 2 and agg['btc_extreme']['n_at_or_above_70'] == 1
    _, scope = native([raw()]); fn = scope['lambda_handler'].__globals__['build_triggers']
    out = fn(rows, agg, {'current': {'composite': 0}})
    btc = next(row for row in out if row['condition'].startswith('btc_extreme'))
    assert btc['auctions_already_fired_14d'] == 1
    for history in ({}, {'current': None}, {'current': {'composite': None}}, {'current': {'composite': True}}):
        assert fn(rows, agg, history) == []
    agg['btc_extreme']['status'] = 'incomplete'
    assert not any(row['condition'].startswith('btc_extreme') for row in fn(rows, agg, {'current': {'composite': 0}}))


def test_desk_keeps_same_incomplete_population_and_no_external_current_override():
    scope = support['load']('auction-desk', support['Store']())
    row = raw(); missing = raw(cusip='MISSING01', bid_to_cover_ratio=None, total_accepted='90000000000')
    day = row['auction_date']
    live = {'date': day, 'composite': 0, 'n': 1}
    out = scope['build_composite_history']({'a': row, 'b': missing}, {day: 4.25}, live)
    assert out['n_auctions_retained'] == 2 and out['n_auctions_scored'] == 1
    assert out['series'][-1]['composite'] is None and out['series'][-1]['n'] == 2
    assert out['detector_context'] == live and out['detector_context_used_in_history'] is False
    assert out['series'][-1]['weighting_quality']['known_weight_total_usd_bn'] == 100


def test_desk_matches_complete_detector_rounding_and_missing_dated_policy():
    scope = support['load']('auction-desk', support['Store']())
    row = raw(bid_to_cover_ratio='2.0'); day = row['auction_date']
    out = scope['build_composite_history']({'a': row}, {day: 4.25})
    assert out['series'][-1]['composite'] == score(row)['composite_score']
    bill = raw(security_type='Bill', security_term='4-Week', low_discnt_rate='4')
    old = (datetime.now(timezone.utc).date() - timedelta(days=6)).isoformat()
    for rates in ({}, {old: 4.25}):
        out = scope['build_composite_history']({'a': bill}, rates)
        assert out['series'][-1]['composite'] is None


def test_actual_population_permissions_never_promote_the_fixed_heuristic():
    out, _ = native([raw()])
    for key in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_eligible'):
        assert out[key] is False and out['auction_observations'][0]['score_quality'][key] is False


if __name__ == '__main__':
    tests = [value for key, value in list(globals().items()) if key.startswith('test_')]
    for test in tests: test()
    print('Auction complete scoring regressions passed:', len(tests))
