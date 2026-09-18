"""Economic comparability regressions; deterministic and cloud-free."""
import ast
import bisect
import json
import math
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from treasury_instruments import instrument_fields, comparable_cohort, nominal_par_eligible

SRC = HERE.parent / 'source/lambda_function.py'
tree = ast.parse(SRC.read_text(encoding='utf-8'))
env = dict(bisect=bisect, json=json, math=math, statistics=statistics, datetime=datetime,
           timedelta=timedelta, timezone=timezone, instrument_fields=instrument_fields,
           comparable_cohort=comparable_cohort, nominal_par_eligible=nominal_par_eligible,
           _PAR_DATES={}, TERM_TENOR={'10-Year': '10Y', '9-Year 10-Month': '10Y'})
functions = {'_f', '_d', '_now', '_iso', 'norm_td', 'norm_fd', 'par_prev_close', 'shares', 'z',
             'analyze_bank', 'analyze_auction', 'explain_auction', 'fmt_bn', 'auction_verdict',
             'implication_for_auction', 'analyze_buyback', 'day_verdict', 'ai_note'}
functions.update({'day_class','build_reactions','_dist','predict_today','chart_cohorts','fwd_returns'})
env.update(HORIZONS=(('same_day',0),('d1',1),('d5',5),('d20',20)),ASSETS=[('SPY','S&P 500','stocks')],CLASS_LABEL={'bills_only':'bills only'})
exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in functions], type_ignores=[]), str(SRC), 'exec'), env)


def row(tips='No', floating='No', day='2026-09-17'):
    return env['norm_td']({'cusip': '91282CRE3', 'securityType': 'Note', 'securityTerm': '9-Year 10-Month',
        'originalSecurityTerm': '10-Year', 'tips': tips, 'floatingRate': floating, 'reopening': 'Yes',
        'auctionDate': day, 'highYield': '2.653', 'bidToCoverRatio': '2.4', 'totalAccepted': '1000',
        'competitiveAccepted': '900', 'primaryDealerAccepted': '90', 'indirectBidderAccepted': '630', 'directBidderAccepted': '180'})


def test_tips_real_yield_cannot_compare_to_nominal_par():
    a = row(tips='Yes')
    out = env['analyze_auction'](a, [], {'2026-09-16': {'10Y': 5.01}})
    assert out['instrument_kind'] == 'TIPS' and out['quote_basis'] == 'real_yield_pct'
    assert out['tail_bp'] is None and out['par_prev_close'] is None and out['wi_tail_bp'] is None
    assert '235.7' not in json.dumps(out)


def test_fiscaldata_flags_and_unknown_identity_are_preserved():
    a = env['norm_fd']({'security_type': 'Note', 'security_term': '10-Year', 'inflation_index_security': 'Yes', 'floating_rate': 'No'})
    assert a['instrument_kind'] == 'TIPS'
    assert row(floating='Yes')['instrument_kind'] == 'FRN'
    missing = row(tips=None, floating=None)
    assert comparable_cohort(missing) is None and not nominal_par_eligible(missing)
    assert env['analyze_auction'](missing, [row(day='2026-08-01')], {})['grade'] == 'n/a'


def test_cohort_cannot_mix_tips_nominals_or_reopenings():
    target = row(tips='Yes')
    prior = [row(day=f'2026-08-{n:02d}') for n in range(1, 6)]
    prior += [row(tips='Yes', day=f'2026-07-{n:02d}') for n in range(1, 5)]
    prior[5]['reopening'] = False
    out = env['analyze_auction'](target, sorted(prior, key=lambda r:r['auction_date']), {})
    assert out['trailing12']['n'] == 3 and out['demand_score'] is None


def test_conflicting_security_identity_is_unverified():
    for kind, tips, floating in [('TIPS', 'No', 'No'), ('FRN', 'No', 'No'), ('Bill', 'Yes', 'Yes')]:
        fields = instrument_fields({'securityType': kind, 'tips': tips, 'floatingRate': floating}, 'treasurydirect')
        assert fields['instrument_kind'] == 'UNKNOWN' and comparable_cohort(fields) is None


def test_frn_margin_stays_separate_from_a_yield():
    a = env['norm_td']({'securityType': 'Note', 'tips': 'No', 'floatingRate': 'Yes', 'highDiscountMargin': '0.055', 'highYield': ''})
    assert a['high_discount_margin'] == 0.055 and a['high_yield'] is None
    assert a['quote_basis'] == 'discount_margin_pct' and not nominal_par_eligible(a)


def test_prior_close_gap_is_preserved_but_never_votes():
    prior = [row(day=f'2026-08-{n:02d}') for n in range(1, 6)]
    for n, r in enumerate(prior):
        r['btc'] = 2 + n / 10
    a = row()
    x = env['analyze_auction'](a, prior, {'2026-08-01': {'10Y': 2.5}, '2026-09-16': {'10Y': 5.01}})
    y = env['analyze_auction'](a, prior, {'2026-08-01': {'10Y': 2.5}, '2026-09-16': {'10Y': 1.01}})
    assert x['tail_bp'] != y['tail_bp'] and x['demand_score'] == y['demand_score']
    assert x['score_parts'][-1]['weight'] == 0 and x['wi_tail_bp'] is None
    assert 'stopped through' not in x['verdict'] and 'tailed' not in x['verdict']


def test_buyback_par_is_not_cash_or_an_easing_trade():
    op = dict(max_par=12.5e9, offered=25e9, accepted=12.5e9, maturity_bucket='10-20 years', operation_date='2026-09-17')
    b = env['analyze_buyback'](op, [op])
    assert b['fill_pct'] == 100 and b['liquidity_signal'] == 'strong'
    assert b['cash_settlement_usd'] is None and b['monetary_easing_inferred'] is False
    assert b['implication']['tone'] == 'neutral' and b['call'] is None
    assert 'LIQUIDITY INJECTION' not in b['tags'] and 'easing impulse' not in b['verdict']
    day = env['day_verdict']('2026-09-17', [], [b])
    assert day['risk_assets'] == 'neutral' and day['sizing_eligible'] is False


def test_note_is_deterministic_and_does_not_access_a_provider():
    note = env['ai_note']({'date': '2026-09-17', 'verdict': {'headline': 'Observed auction'}})
    assert note['paid_api_calls'] == 0 and note['model'] is None
    assert note['what_happened'] == 'Observed auction' and note['sizing_eligible'] is False


def test_nonfinite_input_is_unavailable():
    assert env['_f']('nan') is None and env['_f']('Infinity') is None
    assert env['_f']('0') == 0.0


def test_old_curve_cannot_be_presented_as_a_prior_close():
    out = env['analyze_auction'](row(), [], {'2025-09-16': {'10Y': 5.01}})
    assert out['tail_bp'] is None and out['par_prev_close'] is None
    # Weekend/holiday gap remains a dated context comparison.
    assert env['par_prev_close']({'2026-09-11': {'10Y': 4.1}}, '2026-09-14', '10Y') == (4.1, '2026-09-11')


def test_bill_grade_cannot_flip_coupon_participation_and_missing_is_not_weak():
    coupon = {'type': 'Note', 'term': '10-Year', 'grade': 'D', 'total_accepted': 100, 'verdict': 'Weaker coupon participation'}
    bill = {'type': 'Bill', 'term': '4-Week', 'grade': 'A', 'total_accepted': 100, 'btc': 2.0}
    result = env['day_verdict']('2026-09-17', [coupon, bill], [])
    assert 'DEMAND WEAK' in result['tags'] and 'DEMAND MIXED' not in result['tags']
    bill['grade'] = 'n/a'
    result = env['day_verdict']('2026-09-17', [bill], [])
    assert 'insufficient' in result['bullets'][0] and 'thinner' not in result['bullets'][0]


def test_chart_cohorts_keep_tips_nominals_and_reopening_status_separate():
    rows=[row(),row(tips='Yes'),row()]
    rows[2]['reopening']=False
    groups=env['chart_cohorts'](rows)
    assert len(groups)==3 and all(len(items)==1 for items in groups.values())
    assert any('TIPS' in label for label in groups) and any('new issue' in label for label in groups)


def test_partial_endpoints_and_nonfinite_values_cannot_enter_conditional_stats():
    original=env['fwd_returns']
    env['fwd_returns']=lambda ser,d:{'same_day':.01,'d1':.02,'partial':['d1'] if d=='2026-09-15' else []}
    try:
        ops={d:{'auctions':[{'type':'Bill'}],'buybacks':[]} for d in ('2026-09-15','2026-09-16','2026-09-17')}
        result=env['build_reactions'](ops,{'SPY':{}},'2026-09-18')
        assert result['baseline']['SPY']['d1']['n']==2 and result['stats']['bills_only']['SPY']['d1']['n']==2
        assert env['_dist']([.01,.02,float('nan'),float('inf'),True])['n']==2
    finally:env['fwd_returns']=original


def test_conditional_history_cannot_grant_confidence_or_impute_missing_baseline():
    reactions={'stats':{'bills_only':{'SPY':{'d1':{'n':50,'median':2,'hit':90}}}},'baseline':{}}
    result=env['predict_today'](['bills_only'],reactions,{'SPY':{}})[0]
    assert result['call'] is None and result['confidence']=='unvalidated'
    assert result['legacy_direction_hint']=='↑' and result['edge_d1'] is None
    assert not result['decision_eligible'] and not result['sizing_eligible']


if __name__ == '__main__':
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith('test_') and callable(fn)]
    for test in tests:
        test()
    print(f'Treasury comparability tests passed: {len(tests)}')
