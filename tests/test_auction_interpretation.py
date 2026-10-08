"""Descriptive interpretation arithmetic over synthetic auction rows; no cloud I/O."""
from pathlib import Path
import json
import math
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/shared'))
import auction_interpretation as ai
import auction_participation as ap
from treasury_instruments import cohort_term, comparable_cohort


def row(day='2026-09-29', **changes):
    base = {'auction_date': day, 'cusip': 'T' + day.replace('-', ''), 'type': 'Note', 'term': '9-Year 10-Month', 'original_term': '10-Year', 'reopening': True,
            'instrument_kind': 'NOMINAL_COUPON', 'instrument_contract': 'treasury-instrument.v1', 'instrument_classification_status': 'verified',
            'quote_basis': 'nominal_yield_pct', 'tenor': '10Y', 'btc': 2.5, 'pd': 4e9, 'pd_tendered': 30e9, 'direct': 8e9, 'direct_tendered': 10e9,
            'indirect': 28e9, 'indirect_tendered': 33e9, 'competitive_accepted': 40e9, 'competitive_tendered': 73e9, 'noncompetitive_accepted': 1e8,
            'total_accepted': 40.1e9, 'high_yield': 4.10, 'median_yield': 4.05, 'low_yield': 3.90}
    base.update(changes)
    return base


def test_coupon_cohorts_bucket_by_original_term_and_bills_by_auctioned_term():
    a, b = row(term='9-Year 10-Month'), row(term='9-Year 11-Month')
    assert ap.cohort(a) == ap.cohort(b) == ('NOMINAL_COUPON', '10-Year', True)
    assert comparable_cohort(a) == ap.cohort(a)
    bill = row(type='Bill', instrument_kind='BILL', term='4-Week', original_term='26-Week')
    assert cohort_term(bill) == '4-Week' and ap.cohort(bill)[1] == '4-Week'
    assert cohort_term(row(original_term=None)) == '9-Year 10-Month'
    assert ap.cohort(row(reopening=False)) != ap.cohort(a)


def test_hit_ratios_dispersion_and_noncomp_are_pure_arithmetic():
    b = ai.bidder_behaviour(row())
    assert b['status'] == 'complete'
    assert b['hit_ratio_pct'] == {'pd': 13.3, 'indirect': 84.8, 'direct': 80.0, 'competitive': 54.8}
    assert b['dispersion']['high_minus_median_bp'] == 5.0 and b['dispersion']['high_minus_low_bp'] == 20.0
    assert b['noncompetitive_pct'] == 0.2
    partial = ai.bidder_behaviour(row(pd_tendered=None, median_yield=None))
    assert partial['status'] == 'partial' and partial['hit_ratio_pct']['pd'] is None and partial['dispersion']['high_minus_median_bp'] is None
    assert ai.hit_ratio(5, 4) is None and ai.hit_ratio(1, 0) is None and ai.hit_ratio(float('nan'), 2) is None
    bill = ai.bidder_behaviour(row(type='Bill', instrument_kind='BILL', high_discount_rate=4.0, median_discount_rate=3.97, low_discount_rate=3.9, high_yield=4.1, median_yield=None))
    assert bill['dispersion'] == {'high_minus_median_bp': 3.0, 'high_minus_low_bp': 10.0, 'basis': 'discount_rate_pct'}
    json.dumps(ai.bidder_behaviour(row(pd=float('inf'))), allow_nan=False)


def test_percentiles_rank_within_same_bucket_pooling_reopenings():
    rows = [row('2015-%02d-15' % m, btc=2 + m / 10, reopening=bool(m % 2)) for m in range(1, 11)]
    population = ai.build_population(rows + [row('2009-01-15', btc=9.9)])  # pre-2010 row excluded
    ctx = ai.percentile_context(row(btc=2.55), population)
    assert ctx['metrics']['btc']['n'] == 10 and ctx['metrics']['btc']['percentile'] == 50
    assert ai.percentile_context(row(btc=3.5), population)['metrics']['btc']['percentile'] == 100
    assert ai.percentile_context(row(instrument_kind='UNKNOWN', instrument_classification_status='unverified'), population)['metrics']['btc']['percentile'] is None


def test_setup_and_reaction_use_dated_par_curve_only():
    par = {'2026-09-%02d' % d: {'10Y': 4.00 + 0.01 * i} for i, d in enumerate([18, 21, 22, 23, 24, 25, 28, 29, 30])}
    out = ai.setup_and_reaction(par, row('2026-09-29'))
    assert out['dates'] == {'five_sessions_before': '2026-09-21', 'prior_close': '2026-09-28', 'auction_day': '2026-09-29', 'next_session': '2026-09-30'}
    assert out['concession_bp'] == 5.0 and out['reaction_bp'] == 1.0 and out['next_day_bp'] == 1.0 and out['status'] == 'complete'
    pending = ai.setup_and_reaction({k: v for k, v in par.items() if k < '2026-09-29'}, row('2026-09-29'))
    assert pending['reaction_bp'] is None and pending['status'] == 'partial' and 'not yet published' in pending['reason']
    assert ai.setup_and_reaction(par, row(instrument_kind='TIPS', quote_basis='real_yield_pct'))['status'] == 'unavailable'
    assert ai.setup_and_reaction({'2026-09-01': {'10Y': 4}}, row('2026-09-29'))['reason'].startswith('no par curve')


def graded_history(n=14, start_btc=2.0, grades_weak=0):
    rows = []
    for i in range(n):
        r = row('2026-%02d-%02d' % (1 + i // 3, 1 + (i % 3) * 9), btc=start_btc + i / 100, pd=4e9 + i * 1e8, indirect=28e9 - i * 1e8)
        rows.append(r)
    out = []
    for i, r in enumerate(rows):
        prior = rows[max(0, i - 12):i]
        trace = ap.grade(r, prior)
        out.append({**r, **ap.shares(r), 'grade': trace['grade'], 'demand_score': trace['score'], 'grading_inputs': trace,
                    'z': {k: trace['features'][k]['z'] for k in ap.WEIGHTS}, 'trailing12': {k: trace['features'][k]['mean'] for k in ('btc',)}})
    return out


def test_curve_map_streaks_and_consecutive_weak_alert():
    analyzed = graded_history()
    # force the last three grades weak to exercise the streak rule
    for a in analyzed[-3:]:
        a['grade'] = 'F'; a['demand_score'] = -1.5
    demand = ai.curve_map(analyzed, '2026-12-31')
    assert [r['bucket'] for r in demand['rows']] == ['10-Year']
    r = demand['rows'][0]
    assert r['consecutive_weak'] == 3 and r['streak_string'].endswith('FFF') and len(r['streak']) == 12 and r['call'] is None if 'call' in r else True
    flags = ai.alerts(analyzed, analyzed[-1]['auction_date'], window_days=400, demand_map=demand)
    ids = {x['id'] for x in flags['items']}
    assert 'three_consecutive_weak' in ids and 'grade_extreme' in ids
    assert all(x['call'] is None and x['sizing_eligible'] is False for x in flags['items'])
    assert any(rule['id'] == 'wi_tail_over_2bp_10y_30y' and rule['status'] == 'feed_unavailable' for rule in flags['rules'])
    assert ai.alerts(analyzed, '2027-06-01', window_days=14, demand_map=demand)['items'] == []


def test_two_sigma_rules_fire_on_cohort_z_only():
    analyzed = graded_history()
    last = analyzed[-1]
    last['z'] = {'btc': -2.5, 'indirect': -2.1, 'pd': 2.2}; last['trailing12'] = {'btc': 2.1, 'indirect_pct': 60.0, 'pd_pct': 10.0}
    ids = [x['id'] for x in ai.alerts(analyzed, last['auction_date'], window_days=1)['items']]
    assert {'dealer_share_plus_2sigma', 'indirect_share_minus_2sigma', 'bid_to_cover_minus_2sigma'} <= set(ids)
    wide = ai.alerts([dict(last, median_yield=3.95, z={})], last['auction_date'], window_days=1)['items']
    assert [x['id'] for x in wide if x['id'] == 'wide_stop_dispersion']


def test_crisis_fingerprints_are_descriptive_and_handle_missing_bidder_fields():
    rows = []
    for year in (2008, 2009, 2020, 2023, 2026):
        for m in range(1, 13):
            stress = (year == 2008 and m >= 9) or (year == 2020 and m == 3)
            fields = dict(btc=2.0 if stress else 2.6, median_yield=3.95 if stress else 4.06, pd=12e9 if stress else 4e9, indirect=20e9 if stress else 28e9)
            if year == 2008 and m < 9:
                fields.update(pd=None, direct=None, indirect=None)
            rows.append(row('%d-%02d-10' % (year, m), **fields))
    out = ai.crisis_fingerprints(rows, '2026-12-31')
    assert out['status'] == 'complete' and out['call'] is None and out['probability_claims'] is None and out['sizing_eligible'] is False
    byid = {e['id']: e for e in out['episodes']}
    assert byid['gfc_2008']['n_auctions'] == 4 and byid['gfc_2008']['mean_btc'] == 2.0 and byid['gfc_2008']['mean_high_minus_median_bp'] == 15.0
    assert byid['covid_2020']['n_auctions'] == 2 and byid['top_2007']['n_auctions'] == 0 and byid['top_2007']['distance_from_current'] is None
    assert out['current']['n_auctions'] == 3 and out['current']['mean_btc'] == 2.6
    assert all(e['distance_from_current'] is None or e['distance_from_current'] >= 0 for e in out['episodes'])
    assert out['timeline'] and all(set(point) == {'month', 'mean_score', 'mean_z_btc', 'mean_high_minus_median_bp', 'n'} for point in out['timeline'])
    assert len(out['caveats']) >= 4
    empty = ai.crisis_fingerprints([], '2026-12-31')
    assert empty['status'] == 'unavailable' and empty['episodes'] == []
    json.dumps(out, allow_nan=False)


def test_reads_never_carry_calls_and_withhold_without_grade():
    analyzed = graded_history()
    read = ai.auction_read(analyzed[-1])
    assert read['headline'].startswith('9-Year 10-Month note reopening:') and 'score' in read['headline']
    assert 'ealers were filled on' in read['what_it_means']
    withheld = ai.auction_read({**row(), 'grade': 'n/a'})
    assert 'withheld' in withheld['headline']
    day = ai.day_read({'headline': 'x'}, analyzed[-2:], [], demand_map={'rows': [{'bucket': '10-Year', 'consecutive_weak': 2, 'streak_string': 'CCDD'}]}, alert_items=[{'severity': 'watch'}])
    assert day['call'] is None and '10-Year' in day['headline'] and '1 watch flag' in day['watch_next'] and 'is a call' in day['watch_next']


if __name__ == '__main__':
    n = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn):
            fn(); n += 1
    print('Auction interpretation regressions passed: %d' % n)
