"""Descriptive interpretation of Treasury auction results.

Everything here is arithmetic over supplied auction rows and the daily par
curve: bidder hit ratios, award dispersion, percentile context against the
full FiscalData bank, the par-curve set-up into an auction and the same-day
reaction, a per-tenor demand map with grade streaks, deterministic alert
rules and a comparison of the current auction fingerprint with past crisis
and market-top episodes. None of it is a forecast, a call or a sizing input.
"""
import math
import statistics
from datetime import date, timedelta

from auction_participation import WEIGHTS, cohort, number, shares, standardize
from auction_reactions import safe
from treasury_instruments import cohort_term, nominal_par_eligible

CONTRACT = 'auction-interpretation.v1'
COUPON_ORDER = ['2-Year', '3-Year', '5-Year', '7-Year', '10-Year', '20-Year', '30-Year']
POPULATION_START = '2010-01-01'
WEAK = ('D', 'F')

# Episode windows are calendar windows around documented market events. They
# are descriptive anchors for comparing auction behaviour, not labels of a
# validated crisis model.
EPISODES = [
    {'id': 'top_2007', 'label': 'Oct-2007 equity top', 'kind': 'market_top', 'start': '2007-08-01', 'end': '2007-11-30', 'anchor': '2007-10-09',
     'note': 'S&P 500 closing high on 9 Oct 2007; asset-backed commercial paper and interbank funding had been seizing since Aug 2007.'},
    {'id': 'gfc_2008', 'label': 'Sep-Dec 2008 Lehman / GFC', 'kind': 'crisis', 'start': '2008-09-01', 'end': '2008-12-31', 'anchor': '2008-09-15',
     'note': 'Lehman Brothers filed 15 Sep 2008; Treasury added emergency reopenings in Oct 2008 and bill yields went to zero.'},
    {'id': 'bottom_2009', 'label': 'Mar-2009 equity bottom', 'kind': 'market_bottom', 'start': '2009-02-01', 'end': '2009-04-30', 'anchor': '2009-03-09',
     'note': 'S&P 500 intraday low on 6 Mar and closing low on 9 Mar 2009; Fed announced Treasury purchases on 18 Mar 2009.'},
    {'id': 'downgrade_2011', 'label': 'Aug-2011 US downgrade', 'kind': 'crisis', 'start': '2011-07-15', 'end': '2011-09-30', 'anchor': '2011-08-05',
     'note': 'Debt-ceiling standoff; S&P lowered the US sovereign rating to AA+ on 5 Aug 2011; the 10-year yield fell below 2%.'},
    {'id': 'top_2020', 'label': 'Feb-2020 equity top', 'kind': 'market_top', 'start': '2020-01-15', 'end': '2020-02-28', 'anchor': '2020-02-19',
     'note': 'S&P 500 closing high on 19 Feb 2020 before the COVID-19 drawdown.'},
    {'id': 'covid_2020', 'label': 'Mar-2020 dash for cash', 'kind': 'crisis', 'start': '2020-03-01', 'end': '2020-04-15', 'anchor': '2020-03-16',
     'note': 'Treasury market liquidity broke in mid-March 2020; the Fed announced unlimited purchases on 23 Mar 2020.'},
    {'id': 'top_2022', 'label': 'Jan-2022 equity top', 'kind': 'market_top', 'start': '2021-11-15', 'end': '2022-02-15', 'anchor': '2022-01-03',
     'note': 'S&P 500 closing high on 3 Jan 2022 ahead of the 2022 hiking cycle.'},
    {'id': 'yield_peak_2023', 'label': 'Oct/Nov-2023 yield peak', 'kind': 'rates_stress', 'start': '2023-09-15', 'end': '2023-11-15', 'anchor': '2023-11-09',
     'note': '10-year yield touched 5% in Oct 2023; the 9 Nov 2023 30-year reopening tailed and dealers took about a quarter of it.'},
]

STRUCTURAL_CAVEATS = [
    'Bidder-class levels are not stationary: in 2007-2008 primary dealers took roughly half to two thirds of every coupon auction and indirect bidders a quarter; today dealers take a tenth. Compare the standardized (z) columns, which measure each auction against its own trailing cohort at the time, before comparing raw shares.',
    'FiscalData does not retain dealer, direct or indirect awards before 2008, so the 2007 top window compares bid-to-cover and stop-versus-median dispersion only.',
    'Treasury changed how indirect bids are counted in June 2009 (guaranteed-bid rule) and direct bidding grew after 2009; this lifts indirect and direct shares structurally.',
    'The FiscalData bank is current vintage. Rows before the listed complete-capture ranges may be incomplete; episode statistics describe the rows retained, not a certified population.',
    'Episode windows are calendar windows around documented events. Similar auction behaviour in a window does not establish that a crisis or market top is forming; it is a descriptive comparison only.',
]



def _ordinal(n):
    n = int(n)
    suffix = 'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return '%d%s' % (n, suffix)

def _finite(value):
    value = number(value)
    return value if value is not None and math.isfinite(value) else None


def _mean(values, digits=2):
    xs = [x for x in values if _finite(x) is not None]
    return round(statistics.fmean(xs), digits) if xs else None


def _median(values, digits=2):
    xs = [x for x in values if _finite(x) is not None]
    return round(statistics.median(xs), digits) if xs else None


def _pstdev(values):
    xs = [x for x in values if _finite(x) is not None]
    return statistics.pstdev(xs) if len(xs) >= 2 else None


def _day(value):
    if type(value) is not str or len(value) != 10:
        return None
    try:
        out = date.fromisoformat(value)
    except ValueError:
        return None
    return out if out.isoformat() == value else None


# ─────────────────────────── bidder behaviour ────────────────────────
def hit_ratio(accepted, tendered):
    accepted, tendered = _finite(accepted), _finite(tendered)
    if accepted is None or tendered is None or tendered <= 0 or accepted < 0 or accepted > tendered * 1.0001:
        return None
    return round(100.0 * accepted / tendered, 1)


def bidder_behaviour(row):
    """Hit ratios (accepted / tendered per bidder class), award dispersion and non-competitive share."""
    inputs = {key: row.get(key) for key in ('pd', 'pd_tendered', 'indirect', 'indirect_tendered', 'direct', 'direct_tendered',
                                             'competitive_accepted', 'competitive_tendered', 'noncompetitive_accepted', 'total_accepted',
                                             'high_yield', 'median_yield', 'low_yield', 'high_discount_rate', 'median_discount_rate', 'low_discount_rate')}
    hits = {'pd': hit_ratio(row.get('pd'), row.get('pd_tendered')),
            'indirect': hit_ratio(row.get('indirect'), row.get('indirect_tendered')),
            'direct': hit_ratio(row.get('direct'), row.get('direct_tendered')),
            'competitive': hit_ratio(row.get('competitive_accepted'), row.get('competitive_tendered'))}
    if row.get('instrument_kind') == 'BILL':
        # bills publish median/low on the discount-rate basis in both sources
        high, median, low = _finite(row.get('high_discount_rate')), _finite(row.get('median_discount_rate')), _finite(row.get('low_discount_rate'))
        basis = 'discount_rate_pct'
    else:
        high, median, low = _finite(row.get('high_yield')), _finite(row.get('median_yield')), _finite(row.get('low_yield'))
        basis = row.get('quote_basis')
    dispersion = {'high_minus_median_bp': round((high - median) * 100, 1) if high is not None and median is not None and high >= median - 1e-9 else None,
                  'high_minus_low_bp': round((high - low) * 100, 1) if high is not None and low is not None and high >= low - 1e-9 else None,
                  'basis': basis}
    noncomp, total = _finite(row.get('noncompetitive_accepted')), _finite(row.get('total_accepted'))
    noncomp_pct = round(100.0 * noncomp / total, 1) if noncomp is not None and total and total > 0 and 0 <= noncomp <= total else None
    missing = [key for key, value in hits.items() if value is None] + [key for key, value in dispersion.items() if key != 'basis' and value is None]
    return {'contract': CONTRACT, 'status': 'complete' if not missing else 'partial', 'missing': missing,
            'hit_ratio_pct': hits, 'dispersion': dispersion, 'noncompetitive_pct': noncomp_pct, 'inputs': safe(inputs),
            'reading': {
                'pd_hit': ('Dealers were filled on %.0f%% of what they bid for; a low fill means dealers were outbid by end investors, a high fill means dealers had to take the paper.' % hits['pd']) if hits['pd'] is not None else 'Dealer tendered amount unavailable.',
                'dispersion': ('The stop cleared %.1fbp above the median accepted yield; a wide gap means the last bids needed were far from the centre of demand.' % dispersion['high_minus_median_bp']) if dispersion['high_minus_median_bp'] is not None else 'Median accepted yield unavailable.'}}


# ─────────────────────────── percentile context ───────────────────────
def population_key(row):
    identity = cohort(row)
    return (identity[0], identity[1]) if identity else None


def build_population(rows, start=POPULATION_START):
    """Index of per-(kind, term bucket) metric values since `start` for percentile ranks (reopening pooled)."""
    index = {}
    for row in rows:
        key = population_key(row)
        day = row.get('auction_date')
        if key is None or type(day) is not str or day < start:
            continue
        values = shares(row)
        behaviour = bidder_behaviour(row)
        metrics = {'btc': _finite(row.get('btc')), 'indirect_pct': values.get('indirect_pct'), 'pd_pct': values.get('pd_pct'),
                   'pd_hit_pct': behaviour['hit_ratio_pct']['pd'], 'high_minus_median_bp': behaviour['dispersion']['high_minus_median_bp']}
        bucket = index.setdefault(key, {m: [] for m in metrics})
        for metric, value in metrics.items():
            if value is not None:
                bucket[metric].append(value)
    for bucket in index.values():
        for metric in bucket:
            bucket[metric].sort()
    return {'contract': CONTRACT, 'start': start, 'buckets': index, 'pooling': 'same instrument kind and term bucket; new issues and reopenings pooled'}


def _rank(value, sorted_values):
    if value is None or not sorted_values:
        return None
    below = sum(1 for x in sorted_values if x < value)
    ties = sum(1 for x in sorted_values if x == value)
    return int(round(100.0 * (below + 0.5 * ties) / len(sorted_values)))


def percentile_context(row, population):
    key = population_key(row)
    bucket = (population or {}).get('buckets', {}).get(key) if key else None
    values = shares(row)
    behaviour = bidder_behaviour(row)
    current = {'btc': _finite(row.get('btc')), 'indirect_pct': values.get('indirect_pct'), 'pd_pct': values.get('pd_pct'),
               'pd_hit_pct': behaviour['hit_ratio_pct']['pd'], 'high_minus_median_bp': behaviour['dispersion']['high_minus_median_bp']}
    out = {}
    for metric, value in current.items():
        hist = (bucket or {}).get(metric) or []
        out[metric] = {'value': value, 'percentile': _rank(value, hist), 'n': len(hist),
                       'min': hist[0] if hist else None, 'max': hist[-1] if hist else None}
    return {'contract': CONTRACT, 'since': (population or {}).get('start'), 'bucket': list(key) if key else None,
            'metrics': out, 'note': 'Percentile of the current value among retained same-bucket auctions since %s; 100 = highest on record in the bank.' % (population or {}).get('start')}


# ─────────────────────────── par-curve set-up and reaction ────────────
def setup_and_reaction(par_rows, row, sorted_dates=None):
    """Par-yield change into the auction (5 sessions) and on the auction day; constant-maturity tenor, not the CUSIP."""
    tenor, day = row.get('tenor'), row.get('auction_date')
    base = {'contract': CONTRACT, 'tenor': tenor, 'basis': 'Treasury daily par yield curve, constant-maturity tenor; not the auctioned CUSIP and not a when-issued quote',
            'concession_bp': None, 'reaction_bp': None, 'next_day_bp': None, 'dates': {}, 'status': 'unavailable', 'reason': None}
    if not par_rows or not tenor or _day(day) is None or not nominal_par_eligible(row):
        base['reason'] = ('no constant-maturity par tenor for this term' if not tenor and nominal_par_eligible(row) else
                          'TIPS real yields, FRN margins and unverified instruments have no nominal par comparison' if not nominal_par_eligible(row) else
                          'par curve or dated auction unavailable')
        return base
    dates = sorted_dates or sorted(par_rows)
    def value(d):
        return _finite((par_rows.get(d) or {}).get(tenor)) if d else None
    import bisect
    i = bisect.bisect_left(dates, day)
    prior = dates[i - 1] if i >= 1 else None
    if prior is None or (_day(day) - _day(prior)).days > 7:
        base['reason'] = 'no par curve within seven days before the auction'
        return base
    five = dates[i - 6] if i >= 6 else None
    if five is not None and (_day(prior) - _day(five)).days > 14:
        five = None
    same = dates[i] if i < len(dates) and dates[i] == day else None
    nxt = dates[i + 1] if same and i + 1 < len(dates) and (_day(dates[i + 1]) - _day(day)).days <= 7 else None
    p_prior, p_five, p_same, p_next = value(prior), value(five), value(same), value(nxt)
    base['dates'] = {'five_sessions_before': five, 'prior_close': prior, 'auction_day': same, 'next_session': nxt}
    if p_prior is not None and p_five is not None:
        base['concession_bp'] = round((p_prior - p_five) * 100, 1)
    if p_prior is not None and p_same is not None:
        base['reaction_bp'] = round((p_same - p_prior) * 100, 1)
    if p_same is not None and p_next is not None:
        base['next_day_bp'] = round((p_next - p_same) * 100, 1)
    base['status'] = 'complete' if base['concession_bp'] is not None and base['reaction_bp'] is not None else 'partial'
    base['reason'] = None if base['status'] == 'complete' else ('auction-day par curve not yet published' if p_same is None else 'par history too short for the five-session set-up')
    base['reading'] = {
        'concession': (('The %s par yield rose %.1fbp over the five sessions into the auction, so the market had cheapened the sector beforehand.' % (tenor, base['concession_bp'])) if base['concession_bp'] > 0.5 else
                       ('The %s par yield fell %.1fbp over the five sessions into the auction; the sector was bid into supply, leaving less cushion.' % (tenor, -base['concession_bp'])) if base['concession_bp'] < -0.5 else
                       'The %s par yield was roughly unchanged over the five sessions into the auction.' % tenor) if base['concession_bp'] is not None else None,
        'reaction': (('The %s par yield closed %.1fbp lower on auction day (rally after the result).' % (tenor, -base['reaction_bp'])) if base['reaction_bp'] < -0.5 else
                     ('The %s par yield closed %.1fbp higher on auction day (sell-off after the result).' % (tenor, base['reaction_bp'])) if base['reaction_bp'] > 0.5 else
                     'The %s par yield was little changed on auction day.' % tenor) if base['reaction_bp'] is not None else None}
    return base


# ─────────────────────────── curve demand map ─────────────────────────
def _score(a):
    return _finite(a.get('demand_score'))


def curve_map(analyzed, today, population=None, par_rows=None):
    """Latest graded auction per nominal coupon tenor with its 12-auction grade streak."""
    by_bucket = {}
    for a in analyzed:
        identity = cohort(a)
        if identity is None or identity[0] != 'NOMINAL_COUPON' or a.get('auction_date') > today:
            continue
        by_bucket.setdefault(identity[1], []).append(a)
    dates = sorted(par_rows) if par_rows else None
    rows = []
    for bucket in COUPON_ORDER + sorted(k for k in by_bucket if k not in COUPON_ORDER):
        items = sorted(by_bucket.get(bucket) or [], key=lambda a: a['auction_date'])
        if not items:
            continue
        latest = items[-1]
        streak = [{'d': a['auction_date'], 'grade': a.get('grade') if a.get('grade') in ('A', 'B', 'C', 'D', 'F') else 'n/a',
                   'score': _score(a), 'reopening': bool(a.get('reopening'))} for a in items[-12:]]
        consecutive_weak = 0
        for item in reversed(streak):
            if item['grade'] in WEAK:
                consecutive_weak += 1
            else:
                break
        graded = [s['score'] for s in streak if s['score'] is not None]
        zz = latest.get('z') or {}
        behaviour = bidder_behaviour(latest)
        rows.append({
            'bucket': bucket, 'tenor': latest.get('tenor'), 'auction_date': latest['auction_date'], 'cusip': latest.get('cusip'),
            'term': latest.get('term'), 'reopening': bool(latest.get('reopening')),
            'grade': latest.get('grade') if latest.get('grade') in ('A', 'B', 'C', 'D', 'F') else 'n/a', 'score': _score(latest),
            'btc': _finite(latest.get('btc')), 'indirect_pct': _finite(latest.get('indirect_pct')), 'pd_pct': _finite(latest.get('pd_pct')),
            'direct_pct': _finite(latest.get('direct_pct')), 'high_yield': _finite(latest.get('high_yield')),
            'z': {key: zz.get(key) for key in ('btc', 'indirect', 'pd')},
            'pd_hit_pct': behaviour['hit_ratio_pct']['pd'], 'indirect_hit_pct': behaviour['hit_ratio_pct']['indirect'],
            'high_minus_median_bp': behaviour['dispersion']['high_minus_median_bp'],
            'percentiles': {m: v['percentile'] for m, v in percentile_context(latest, population)['metrics'].items()} if population else None,
            'setup': setup_and_reaction(par_rows, latest, dates) if par_rows else None,
            'streak': streak, 'streak_string': ''.join(s['grade'][0] if s['grade'] != 'n/a' else '·' for s in streak),
            'consecutive_weak': consecutive_weak, 'mean_score_last_4': _mean(graded[-4:]), 'n_graded_in_streak': len(graded),
        })
    return {'contract': CONTRACT, 'as_of': today, 'rows': rows, 'scope': 'nominal coupons by original term; new issues and reopenings pooled for the streak, each graded against its own cohort',
            'call': None, 'sizing_eligible': False}


# ─────────────────────────── alerts ───────────────────────────────────
ALERT_RULES = [
    {'id': 'dealer_share_plus_2sigma', 'severity': 'watch', 'text': 'Primary dealers took a share at least two standard deviations above the comparable cohort'},
    {'id': 'indirect_share_minus_2sigma', 'severity': 'watch', 'text': 'Indirect award share at least two standard deviations below the comparable cohort'},
    {'id': 'bid_to_cover_minus_2sigma', 'severity': 'watch', 'text': 'Bid-to-cover at least two standard deviations below the comparable cohort'},
    {'id': 'three_consecutive_weak', 'severity': 'watch', 'text': 'Three consecutive D or F grades in one coupon tenor'},
    {'id': 'wide_stop_dispersion', 'severity': 'watch', 'text': 'Stop cleared at least 10bp above the median accepted yield on a coupon auction'},
    {'id': 'grade_extreme', 'severity': 'info', 'text': 'An A or F participation grade'},
    {'id': 'wi_tail_over_2bp_10y_30y', 'severity': 'watch', 'text': 'When-issued tail over 2bp on a 10-year or 30-year auction', 'status': 'feed_unavailable',
     'reason': 'No when-issued quote feed is retained; the rule is defined but cannot fire. Prior-close par gaps are not tails.'},
]


def alerts(analyzed, today, window_days=14, demand_map=None):
    start = (_day(today) - timedelta(days=window_days)).isoformat() if _day(today) else today
    out = []
    def add(rule_id, a, values, text, severity=None):
        rule = next(r for r in ALERT_RULES if r['id'] == rule_id)
        out.append({'id': rule_id, 'severity': severity or rule['severity'], 'date': a.get('auction_date'), 'cusip': a.get('cusip'),
                    'term': a.get('term'), 'type': a.get('type'), 'reopening': bool(a.get('reopening')), 'grade': a.get('grade'),
                    'text': text, 'values': values, 'call': None, 'sizing_eligible': False})
    for a in analyzed:
        day = a.get('auction_date')
        if type(day) is not str or day < start or day > today or cohort(a) is None:
            continue
        zz = a.get('z') or {}
        label = '%s %s%s' % (a.get('term'), (a.get('type') or '').lower(), ' reopening' if a.get('reopening') else '')
        t = a.get('trailing12') or {}
        if _finite(zz.get('pd')) is not None and zz['pd'] >= 2:
            add('dealer_share_plus_2sigma', a, {'pd_pct': a.get('pd_pct'), 'cohort_mean': t.get('pd_pct'), 'z': zz['pd']},
                '%s: dealers took %.0f%% vs a cohort mean of %.0f%% (z %+.1f).' % (label, a.get('pd_pct') or 0, t.get('pd_pct') or 0, zz['pd']))
        if _finite(zz.get('indirect')) is not None and zz['indirect'] <= -2:
            add('indirect_share_minus_2sigma', a, {'indirect_pct': a.get('indirect_pct'), 'cohort_mean': t.get('indirect_pct'), 'z': zz['indirect']},
                '%s: indirects took %.0f%% vs a cohort mean of %.0f%% (z %+.1f).' % (label, a.get('indirect_pct') or 0, t.get('indirect_pct') or 0, zz['indirect']))
        if _finite(zz.get('btc')) is not None and zz['btc'] <= -2:
            add('bid_to_cover_minus_2sigma', a, {'btc': a.get('btc'), 'cohort_mean': t.get('btc'), 'z': zz['btc']},
                '%s: bid-to-cover %.2f vs a cohort mean of %.2f (z %+.1f).' % (label, a.get('btc') or 0, t.get('btc') or 0, zz['btc']))
        if a.get('instrument_kind') == 'NOMINAL_COUPON':
            disp = bidder_behaviour(a)['dispersion']['high_minus_median_bp']
            if disp is not None and disp >= 10:
                add('wide_stop_dispersion', a, {'high_minus_median_bp': disp},
                    '%s: the stop cleared %.1fbp above the median accepted yield.' % (label, disp))
        if a.get('grade') in ('A', 'F'):
            add('grade_extreme', a, {'grade': a.get('grade'), 'score': a.get('demand_score')},
                '%s graded %s (score %+.2f vs its comparable cohort).' % (label, a['grade'], a.get('demand_score') or 0))
    for row in (demand_map or {}).get('rows') or []:
        if row.get('consecutive_weak', 0) >= 3 and start <= row['auction_date'] <= today:
            add('three_consecutive_weak', row, {'bucket': row['bucket'], 'consecutive_weak': row['consecutive_weak'], 'streak': row['streak_string']},
                '%s: %d consecutive D/F grades (streak %s).' % (row['bucket'], row['consecutive_weak'], row['streak_string']))
    out.sort(key=lambda x: (x['date'] or '', x['severity'] != 'watch'), reverse=True)
    return {'contract': CONTRACT, 'as_of': today, 'window_days': window_days, 'since': start, 'items': out,
            'rules': ALERT_RULES, 'n_watch': sum(1 for x in out if x['severity'] == 'watch'),
            'note': 'Deterministic threshold flags over descriptive measurements; not trade signals and not a calibrated stress model.',
            'call': None, 'sizing_eligible': False}


# ─────────────────────────── crisis / market-top fingerprints ─────────
def rolling_coupon_metrics(rows):
    """Per nominal coupon auction: cohort z-scores at the time (trailing 4-12 same cohort), hit ratio and dispersion."""
    groups = {}
    out = []
    for r in sorted((x for x in rows if type(x.get('auction_date')) is str), key=lambda x: x['auction_date']):
        identity = cohort(r)
        if identity is None or identity[0] != 'NOMINAL_COUPON':
            continue
        values = shares(r)
        behaviour = bidder_behaviour(r)
        prior = groups.setdefault(identity, [])
        item = {'auction_date': r['auction_date'], 'cusip': r.get('cusip'), 'bucket': identity[1], 'reopening': identity[2], 'term': r.get('term'),
                'btc': _finite(r.get('btc')), 'indirect_pct': values.get('indirect_pct'), 'pd_pct': values.get('pd_pct'), 'direct_pct': values.get('direct_pct'),
                'pd_hit_pct': behaviour['hit_ratio_pct']['pd'], 'high_minus_median_bp': behaviour['dispersion']['high_minus_median_bp'],
                'high_yield': _finite(r.get('high_yield')), 'total_accepted': _finite(r.get('total_accepted'))}
        # Each feature uses the last 4-12 prior same-cohort auctions that report it, so
        # bid-to-cover is standardized back to 1996 while bidder classes start when
        # FiscalData retains them (2008-2009); a feature with fewer than four priors is None.
        zs = {}
        for key, field in (('btc', 'btc'), ('indirect', 'indirect_pct'), ('pd', 'pd_pct')):
            feature_hist = [h[field] for h in prior if h.get(field) is not None][-12:]
            zs[key] = standardize(item[field], feature_hist)['z'] if len(feature_hist) >= 4 and item.get(field) is not None else None
        item['z'] = {key: zs.get(key) for key in ('btc', 'indirect', 'pd')}
        item['score'] = round(math.fsum(WEIGHTS[k] * item['z'][k] for k in WEIGHTS) / 3, 2) if all(item['z'].get(k) is not None for k in WEIGHTS) else None
        prior.append(item)
        out.append(item)
    return out


def _window_stats(items):
    scores = [i['score'] for i in items if i['score'] is not None]
    worst = min((i for i in items if i['score'] is not None), key=lambda i: i['score'], default=None)
    return {
        'n_auctions': len(items), 'n_scored': len(scores),
        'first': items[0]['auction_date'] if items else None, 'last': items[-1]['auction_date'] if items else None,
        'mean_btc': _mean([i['btc'] for i in items]), 'mean_indirect_pct': _mean([i['indirect_pct'] for i in items], 1),
        'mean_pd_pct': _mean([i['pd_pct'] for i in items], 1), 'mean_direct_pct': _mean([i['direct_pct'] for i in items], 1),
        'mean_pd_hit_pct': _mean([i['pd_hit_pct'] for i in items], 1), 'mean_high_minus_median_bp': _mean([i['high_minus_median_bp'] for i in items], 1),
        'mean_z_btc': _mean([i['z']['btc'] for i in items]), 'mean_z_indirect': _mean([i['z']['indirect'] for i in items]), 'mean_z_pd': _mean([i['z']['pd'] for i in items]),
        'mean_score': _mean(scores), 'share_weak_pct': round(100.0 * sum(1 for s in scores if s < -0.4) / len(scores), 1) if scores else None,
        'share_strong_pct': round(100.0 * sum(1 for s in scores if s >= 0.4) / len(scores), 1) if scores else None,
        'max_high_minus_median_bp': max((i['high_minus_median_bp'] for i in items if i['high_minus_median_bp'] is not None), default=None),
        'worst_auction': ({'auction_date': worst['auction_date'], 'term': worst['term'], 'bucket': worst['bucket'], 'btc': worst['btc'], 'pd_pct': worst['pd_pct'],
                           'indirect_pct': worst['indirect_pct'], 'score': worst['score'], 'high_minus_median_bp': worst['high_minus_median_bp']} if worst else None),
    }


FINGERPRINT_AXES = ('mean_z_btc', 'mean_z_indirect', 'mean_z_pd', 'mean_pd_hit_pct', 'mean_high_minus_median_bp', 'share_weak_pct')


def crisis_fingerprints(rows, today, current_days=90):
    """Compare the current auction fingerprint with documented crisis and market-top windows; descriptive only."""
    metrics = rolling_coupon_metrics(rows)
    if not metrics:
        return {'contract': CONTRACT, 'status': 'unavailable', 'reason': 'no nominal coupon rows', 'episodes': [], 'timeline': [], 'caveats': STRUCTURAL_CAVEATS}
    baseline_items = [m for m in metrics if m['auction_date'] >= POPULATION_START and m['auction_date'] <= today]
    baseline = _window_stats(baseline_items)
    spread = {axis: _pstdev([{'mean_z_btc': m['z']['btc'], 'mean_z_indirect': m['z']['indirect'], 'mean_z_pd': m['z']['pd'], 'mean_pd_hit_pct': m['pd_hit_pct'],
                              'mean_high_minus_median_bp': m['high_minus_median_bp'], 'share_weak_pct': None}[axis] for m in baseline_items]) for axis in FINGERPRINT_AXES}
    spread['share_weak_pct'] = 25.0  # percentage points; fixed scale for the weak-share axis
    start_current = (_day(today) - timedelta(days=current_days)).isoformat() if _day(today) else today
    current_items = [m for m in metrics if start_current <= m['auction_date'] <= today]
    current = _window_stats(current_items)
    current.update({'id': 'current', 'label': 'Current %d days' % current_days, 'kind': 'current', 'start': start_current, 'end': today, 'anchor': None, 'note': 'Latest retained nominal coupon auctions.'})

    def vector(stats):
        return [stats.get(axis) for axis in FINGERPRINT_AXES]

    def distance(a, b):
        parts = []
        for axis, x, y in zip(FINGERPRINT_AXES, vector(a), vector(b)):
            s = spread.get(axis)
            if x is None or y is None or not s:
                continue
            parts.append(((x - y) / s) ** 2)
        return round(math.sqrt(sum(parts) / len(parts)), 2) if len(parts) >= 3 else None

    episodes = []
    for ep in EPISODES:
        items = [m for m in metrics if ep['start'] <= m['auction_date'] <= ep['end']]
        stats = _window_stats(items)
        stats.update({k: ep[k] for k in ('id', 'label', 'kind', 'start', 'end', 'anchor', 'note')})
        stats['distance_from_current'] = distance(current, stats)
        episodes.append(stats)
    ranked = sorted((e for e in episodes if e['distance_from_current'] is not None), key=lambda e: e['distance_from_current'])
    # monthly timeline of the mean cohort score since 2003 (coupons only)
    months = {}
    for m in metrics:
        if m['auction_date'] < '2003-01-01':
            continue
        bucket = months.setdefault(m['auction_date'][:7], {'score': [], 'z_btc': [], 'disp': []})
        if m['score'] is not None:
            bucket['score'].append(m['score'])
        if m['z']['btc'] is not None:
            bucket['z_btc'].append(m['z']['btc'])
        if m['high_minus_median_bp'] is not None:
            bucket['disp'].append(m['high_minus_median_bp'])
    timeline = [{'month': k, 'mean_score': _mean(v['score']), 'mean_z_btc': _mean(v['z_btc']), 'mean_high_minus_median_bp': _mean(v['disp'], 1),
                 'n': len(v['z_btc'])} for k, v in sorted(months.items())]
    return {'contract': CONTRACT, 'status': 'complete', 'as_of': today, 'baseline': dict(baseline, id='baseline', label='All coupons since %s' % POPULATION_START[:4], kind='baseline', start=POPULATION_START, end=today),
            'current': current, 'episodes': episodes,
            'nearest': [{'id': e['id'], 'label': e['label'], 'kind': e['kind'], 'distance': e['distance_from_current']} for e in ranked[:3]],
            'axes': list(FINGERPRINT_AXES), 'axis_scale': {k: (round(v, 3) if v is not None else None) for k, v in spread.items()},
            'distance_method': 'root-mean-square of per-axis differences scaled by the since-2010 per-auction dispersion of that axis; smaller = more alike. Descriptive similarity, not a probability.',
            'timeline': timeline, 'timeline_basis': 'monthly means for nominal coupon auctions: z(bid-to-cover) since 2003 and the three-feature cohort score once bidder classes are retained (2009); each auction standardized against its trailing 4-12 same-cohort auctions at the time',
            'field_availability': 'FiscalData retains bidder-class awards (dealer / direct / indirect) and tendered amounts from 2008-2009 onward; 2007 and earlier windows compare bid-to-cover and yield dispersion only',
            'episode_windows': [{k: ep[k] for k in ('id', 'label', 'kind', 'start', 'end', 'anchor')} for ep in EPISODES],
            'caveats': STRUCTURAL_CAVEATS, 'call': None, 'sizing_eligible': False, 'probability_claims': None}


# ─────────────────────────── readable auction read ────────────────────
def auction_read(a, behaviour=None, setup=None, percentiles=None):
    """One-paragraph plain reading of a graded auction. Numbers come from the auction itself."""
    behaviour = behaviour or bidder_behaviour(a)
    g, zz, t = a.get('grade'), a.get('z') or {}, a.get('trailing12') or {}
    label = '%s %s%s' % (a.get('term') or 'unknown term', (a.get('type') or 'security').lower(), ' reopening' if a.get('reopening') else '')
    demand = {'A': 'strong demand', 'B': 'solid demand', 'C': 'average demand', 'D': 'soft demand', 'F': 'weak demand'}.get(g)
    if demand is None:
        return {'headline': '%s: participation grade withheld' % label,
                'what_happened': 'The comparable cohort or a participation input is incomplete, so no grade is published for this auction.',
                'what_it_means': 'Raw results remain in the packet; nothing is inferred from a withheld grade.', 'watch_next': 'The next auction in this cohort.'}
    bits = []
    if _finite(a.get('btc')) is not None:
        bits.append('bid-to-cover %.2f%s' % (a['btc'], ' (cohort %.2f)' % t['btc'] if _finite(t.get('btc')) is not None else ''))
    if _finite(a.get('indirect_pct')) is not None:
        bits.append('indirects %.0f%%%s' % (a['indirect_pct'], ' (%.0f%%)' % t['indirect_pct'] if _finite(t.get('indirect_pct')) is not None else ''))
    if _finite(a.get('pd_pct')) is not None:
        bits.append('dealers %.0f%%%s' % (a['pd_pct'], ' (%.0f%%)' % t['pd_pct'] if _finite(t.get('pd_pct')) is not None else ''))
    hit = behaviour['hit_ratio_pct'].get('pd')
    disp = behaviour['dispersion'].get('high_minus_median_bp')
    means = []
    if hit is not None:
        means.append(('dealers were filled on only %.0f%% of their bids, so end investors outbid them' % hit) if hit <= 15 else
                     ('dealers were filled on %.0f%% of their bids, a heavy take-down' % hit) if hit >= 35 else
                     'dealers were filled on %.0f%% of their bids' % hit)
    if disp is not None:
        means.append(('the stop cleared %.1fbp above the median bid, a wide spread' % disp) if disp >= 8 else
                     'the stop cleared %.1fbp above the median bid' % disp)
    if setup and setup.get('concession_bp') is not None:
        means.append(('the sector cheapened %.1fbp into the sale' % setup['concession_bp']) if setup['concession_bp'] > 0.5 else
                     ('the sector richened %.1fbp into the sale' % -setup['concession_bp']) if setup['concession_bp'] < -0.5 else 'the sector was flat into the sale')
    if setup and setup.get('reaction_bp') is not None:
        means.append(('the %s par yield closed %.1fbp lower afterwards' % (setup.get('tenor'), -setup['reaction_bp'])) if setup['reaction_bp'] < -0.5 else
                     ('the %s par yield closed %.1fbp higher afterwards' % (setup.get('tenor'), setup['reaction_bp'])) if setup['reaction_bp'] > 0.5 else
                     'the %s par yield was little changed afterwards' % setup.get('tenor'))
    pct = (percentiles or {}).get('metrics') or {}
    rank_bits = [('%s at the %s percentile since %s' % (name, _ordinal(pct[key]['percentile']), (percentiles or {}).get('since', '')[:4]))
                 for key, name in (('btc', 'bid-to-cover'), ('indirect_pct', 'indirect share'), ('pd_pct', 'dealer share'))
                 if pct.get(key, {}).get('percentile') is not None and (pct[key]['percentile'] >= 90 or pct[key]['percentile'] <= 10)]
    return {'headline': '%s: %s (%s, score %+.2f)' % (label, demand, g, a.get('demand_score') or 0),
            'what_happened': '%s; %s.' % (label[0].upper() + label[1:], ', '.join(bits) or 'takedown detail unavailable'),
            'what_it_means': (('; '.join(means)[0].upper() + '; '.join(means)[1:] + '.') if means else 'Hit ratios, dispersion and par set-up are unavailable for this auction.') +
                             ((' Versus every same-kind auction of this bucket since 2010: ' + '; '.join(rank_bits) + '.') if rank_bits else ''),
            'watch_next': 'Whether the next %s cohort auction confirms this read; the par curve on settlement; this is a descriptive measurement, not a forecast.' % cohort_term(a)}


def day_read(verdict, auctions, buybacks, demand_map=None, alert_items=None):
    """Readable day summary: headline plus what happened / what it means / watch next."""
    coupons = [a for a in auctions if cohort(a) and cohort(a)[0] != 'BILL']
    bills = [a for a in auctions if cohort(a) and cohort(a)[0] == 'BILL']
    graded = [a for a in coupons if a.get('grade') in ('A', 'B', 'C', 'D', 'F')]
    parts = []
    for a in sorted(graded, key=lambda x: _score(x) or 0, reverse=True):
        parts.append('%s%s %s' % (cohort_term(a) or a.get('term'), ' reopening' if a.get('reopening') else '', a.get('grade')))
    if bills:
        bill_grades = ''.join(a.get('grade') for a in bills if a.get('grade') in ('A', 'B', 'C', 'D', 'F'))
        parts.append('%d bill auction%s%s' % (len(bills), '' if len(bills) == 1 else 's', ' (%s)' % bill_grades if bill_grades else ''))
    if buybacks:
        parts.append('%d buyback%s' % (len(buybacks), '' if len(buybacks) == 1 else 's'))
    headline = ' · '.join(parts) or (verdict or {}).get('headline') or 'No operations'
    reads = [auction_read(a) for a in graded]
    happened = ' '.join(r['what_happened'] for r in reads) or (verdict or {}).get('headline') or 'No graded coupon auctions.'
    means = ' '.join(r['what_it_means'] for r in reads) or 'No coupon participation reading today; bill grades are descriptive only.'
    weak_rows = [r for r in ((demand_map or {}).get('rows') or []) if r.get('consecutive_weak', 0) >= 2]
    watch = []
    if weak_rows:
        watch.append('Tenors with two or more consecutive weak grades: %s.' % ', '.join('%s (%s)' % (r['bucket'], r['streak_string'][-4:]) for r in weak_rows))
    watching = [x for x in (alert_items or []) if x.get('severity') == 'watch']
    if watching:
        watch.append('%d watch flag%s in the last two weeks.' % (len(watching), '' if len(watching) == 1 else 's'))
    watch.append('Next cohort auctions and the auction-day par curve; nothing here is a call or a sizing input.')
    return {'headline': headline, 'what_happened': happened, 'what_it_means': means, 'watch_next': ' '.join(watch),
            'contract': CONTRACT, 'call': None, 'sizing_eligible': False}
