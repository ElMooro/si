"""Independent integer/Fraction checks for native CSLT transaction histories.

No production imports. FRED parity, original-byte binding and release calendars
are separately replayed by the reviewed source compiler, not certified here.
"""
from datetime import date
from fractions import Fraction
import math
import re

ROLES = {'total':'for_lt_total_net_99996', 'treasuries':'for_lt_treas_net_99996',
         'agency_bonds':'for_lt_agcy_net_99996', 'corporate_bonds':'for_lt_corp_net_99996',
         'equities':'for_lt_eqty_net_99996', 'short_treasury':'for_st_treas_net_99996',
         'us_abroad':'us_lt_total_net_99996', 'official':'for_lt_total_net_99990', 'private':'for_lt_total_net_99991'}
ASSETS = ('treasuries', 'agency_bonds', 'corporate_bonds', 'equities')


def require(condition, message):
    if not condition: raise ValueError(message)


def exact_number(value):
    text = str(value)
    match = re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE]([+-]?\d+))?', text)
    require(len(text) <= 60 and match is not None, 'Finite decimal notation required')
    require(match[1] is None or abs(int(match[1])) <= 60, 'Decimal exponent exceeds bound')
    return Fraction(text)


def amount(value):
    if value is None or value in ('', '.', 'NA'): return None
    require(not isinstance(value, bool) and isinstance(value, (str, int)) and len(str(value)) <= 40, 'Integer source amount required')
    try: exact = exact_number(value)
    except (ValueError, ZeroDivisionError) as exc: raise ValueError('Invalid source amount') from exc
    require(exact.denominator == 1 and abs(exact) <= 10**15, 'Source precision or magnitude differs')
    return exact.numerator


def scalar(actual, exact, label):
    if exact is None: require(actual is None, label+' must be unavailable'); return
    require(type(actual) in (int, float) and math.isfinite(actual) and actual == float(exact), label+' differs')


def decimal(actual, exact, label):
    if exact is None: require(actual is None, label+' must be unavailable'); return
    require(isinstance(actual, str) and len(actual) <= 60, label+' exact decimal required')
    try: value = exact_number(actual)
    except (ValueError, ZeroDivisionError) as exc: raise ValueError(label+' invalid decimal') from exc
    require(value == exact, label+' differs')


def months_ending(period, count):
    day = date.fromisoformat(period); require(day.day == 1, 'Monthly identity required')
    serial = day.year*12+day.month-1
    return [f'{year:04d}-{offset+1:02d}-01' for year, offset in (divmod(serial-i, 12) for i in range(count))]


def source_series(document, generated):
    require(document.get('releaseID') == '3' and document.get('version') == '2.0', 'CSLT release/schema differs')
    selected = {}; seen = set(); mapping = {value: key for key, value in ROLES.items()}
    for series_index, item in enumerate(document['series']):
        identity = item['source_id']; require(isinstance(identity, str) and identity not in seen, 'Duplicate native series identity')
        seen.add(identity)
        if identity not in mapping: continue
        role = mapping[identity]; meta = item['metadata']
        require((meta['frequency'], meta['season'], meta['units']) == ('M', 'NSA', 'Millions of Dollars'), 'Native units/frequency differ')
        require(meta['additional']['status'] == 'A', 'Native source inactive')
        rows = {}
        for index, pair in enumerate(item['observations']):
            require(isinstance(pair, list) and len(pair) == 2, 'Native observation shape differs')
            day = date.fromisoformat(pair[0]); require(day.day == 1 and day.isoformat() == pair[0] and day <= generated, 'Observation period differs')
            require(pair[0] not in rows, 'Duplicate native observation period')
            rows[pair[0]] = {'value': amount(pair[1]), 'original_row': index}
        require(rows, 'Complete native history required')
        selected[role] = {'series_index': series_index, 'source_id': identity, 'rows': dict(sorted(rows.items()))}
    require(set(selected) == set(ROLES), 'All nine core native histories required')
    return selected, len(seen)


def verify(packet, document, histories):
    """Check every core row, every rolling window and both decompositions.

    `histories` contains the complete parsed, hash-verified immutable shards.
    Missing months remain missing, including early incomplete rolling windows.
    """
    core, population = source_series(document, date.fromisoformat(packet['generated_at'][:10]))
    measurements = {m['role']: m for m in packet['measurements'].values()}
    require(len(packet['measurements']) == len(measurements) == 9 and set(measurements) == set(core), 'Core measurement population differs')
    require(packet['units'] == 'usd_bn' and packet['source_unit'] == 'usd_million', 'Reported packet units differ')
    require(packet['archive']['archive_series_count'] == population and packet['archive']['core_series_verified'] == 9 and
            packet['archive']['other_series_retained_but_not_qualified'] == population-9, 'Archive scope differs')
    counts = {'native_rows': 0, 'windows': 0, 'incomplete_windows': 0, 'reconciliations': 0, 'unreconciled': 0, 'incomplete_reconciliations': 0}

    def shard(ref, **metadata):
        doc = histories[ref['key']]
        require(doc['contract'] == 'tic-history.v1' and type(ref['observations']) is int and len(doc['rows']) == ref['observations'], 'Whole history shard differs')
        require(all(doc.get(key) == value for key, value in metadata.items()), 'History units or identity differ')
        return doc['rows']

    def window(actual, role, end, count):
        wanted = months_ending(end, count); rows = core[role]['rows']
        missing = [day for day in wanted if day not in rows or rows[day]['value'] is None]
        total = None if missing else sum(rows[day]['value'] for day in wanted)
        coordinates = [rows[day]['original_row'] for day in wanted if day in rows]
        require(actual['months'] == list(reversed(wanted)) and actual['missing_months'] == missing, 'Exact window calendar differs')
        require(actual['status'] == ('incomplete' if missing else 'complete'), 'Window completeness differs')
        require(actual['source_rows'] == coordinates and all(type(i) is int for i in actual['source_rows']), 'Window source coordinates differ')
        decimal(actual['usd_million_decimal'], total, 'Window USD millions')
        decimal(actual['usd_bn_decimal'], Fraction(total, 1000) if total is not None else None, 'Window USD billions')
        counts['windows'] += 1; counts['incomplete_windows'] += bool(missing)
        return total

    def reconciliation(actual, children, period):
        roles = ('total', *children); missing = []; values = []
        require(actual['date'] == period and set(actual['components']) == set(roles), 'Reconciliation scope differs')
        for role in roles:
            row = core[role]['rows'].get(period); item = actual['components'][role]
            value = row['value'] if row else None
            require(item['date'] == period and item['row_index'] == (row['original_row'] if row else None), 'Reconciliation row coordinate differs')
            require(item['row_index'] is None or type(item['row_index']) is int, 'Reconciliation coordinate type differs')
            decimal(item['usd_million_decimal'], value, 'Reconciliation component')
            if value is None: missing.append(role)
            else: values.append(value)
        gap = None if missing else values[0]-sum(values[1:]); bound = Fraction(len(roles), 2)
        state = 'incomplete' if missing else 'exact' if gap == 0 else 'within_reporting_rounding' if abs(gap) <= bound else 'unreconciled'
        require(actual['status'] == state and actual['missing_components'] == missing, 'Reconciliation status differs')
        decimal(actual['gap_usd_million_decimal'], gap, 'Reconciliation gap')
        decimal(actual['rounding_bound_usd_million_decimal'], bound, 'Reporting rounding bound')
        counts['reconciliations'] += 1; counts['unreconciled'] += state == 'unreconciled'; counts['incomplete_reconciliations'] += bool(missing)
        return state

    latest_windows = {}
    for role, source in core.items():
        m = measurements[role]; rows = source['rows']; latest = next(reversed(rows)); value = rows[latest]['value']
        require(m['native_source_id'] == source['source_id'] and m['series_index'] == source['series_index'] and type(m['series_index']) is int, 'Native series coordinate differs')
        require(m['id'] == source['source_id'].replace('_', '').upper(), 'Exact series identifier differs')
        require(m['unit'] == 'usd_million' and m['frequency'] == 'M' and m['as_of'] == latest, 'Measurement unit or period differs')
        decimal(m['value_decimal'], value, 'Latest source amount'); scalar(m['value'], value, 'Latest numeric amount')
        decimal(m['value_usd_bn_decimal'], Fraction(value, 1000) if value is not None else None, 'Latest USD billions')
        history = shard(m['history'], kind='native_monthly_transactions', series_id=m['id'], native_source_id=source['source_id'], unit='usd_million')
        require(len(history) == len(rows), 'Complete native row population differs')
        for actual, (period, original) in zip(history, rows.items()):
            require(actual['date'] == period and type(actual['row_index']) is int and actual['row_index'] == original['original_row'], 'Native history coordinate differs')
            decimal(actual['value_decimal'], original['value'], 'Native history amount')
            require(actual['status'] == ('missing' if original['value'] is None else 'observed'), 'Native observation status differs')
        counts['native_rows'] += len(rows)
        latest_windows[role] = {n: window(m['current_vintage_totals'][str(n)], role, latest, n) for n in (1, 3, 12)}
    asof = next(reversed(core['total']['rows'])); require(packet['data_asof'] == asof, 'Headline period differs')
    recon = shard(packet['reconciliation']['history'], kind='component_reconciliation', unit='usd_million')
    rolling = shard(packet['rolling_history'], kind='rolling_twelve_months', unit='mixed_explicit', field_units={
        'rows.*.rolling_12mo_b':'usd_bn', 'rows.*.net_cross_border_lt_12mo_b':'usd_bn',
        'rows.*.totals.*.usd_million_decimal':'usd_million', 'rows.*.totals.*.usd_bn_decimal':'usd_bn'})
    periods = list(core['total']['rows']); require(len(recon) == len(rolling) == len(periods), 'Whole historical population differs')
    states = {}
    for i, period in enumerate(periods):
        r = recon[i]; roll = rolling[i]
        require(r['date'] == roll['date'] == period, 'Historical calendar differs')
        states[period] = {name: reconciliation(r[name], children, period) for name, children in
                          (('asset_classes', ASSETS), ('official_private', ('official', 'private')))}
        require(set(roll['totals']) == set(core), 'Rolling component population differs')
        sums = {role: window(roll['totals'][role], role, period, 12) for role in core}
        scalar(roll['rolling_12mo_b'], Fraction(sums['total'], 1000) if sums['total'] is not None else None, 'Rolling total')
        net = sums['total']-sums['us_abroad'] if sums['total'] is not None and sums['us_abroad'] is not None else None
        scalar(roll['net_cross_border_lt_12mo_b'], Fraction(net, 1000) if net is not None else None, 'Historical net flow')
    require(packet['reconciliation']['latest'] == recon[-1], 'Latest reconciliation differs')
    aliases = packet['history_12mo_rolling_b']; require(len(aliases) == len(rolling), 'Whole rolling alias differs')
    for actual, row in zip(aliases, reversed(rolling)):
        require(actual['asof'] == row['date'], 'Rolling alias date differs'); scalar(actual['rolling_12mo_b'], row['rolling_12mo_b'], 'Rolling alias')
    exact = packet['exact_headline']; head = packet['headline']
    total = window(exact['foreign_net_into_us_lt_12mo'], 'total', asof, 12)
    abroad = window(exact['us_net_purchases_of_foreign_lt_12mo'], 'us_abroad', asof, 12)
    recent = window(exact['latest_three_months'], 'total', asof, 3)
    previous_end = months_ending(asof, 13)[-1]
    previous = window(exact['prior_nonoverlapping_twelve_months'], 'total', previous_end, 12)
    net = total-abroad if total is not None and abroad is not None else None
    decimal(exact['net_cross_border_lt_12mo_usd_million_decimal'], net, 'Net foreign minus domestic flow')
    expected_head = {'foreign_net_into_us_lt_12mo_b': total, 'net_cross_border_lt_12mo_b': net,
                     'latest_month_b': latest_windows['total'][1], 'run_rate_3mo_annualized_b': recent*4 if recent is not None else None,
                     'yoy_change_12mo_b': total-previous if total is not None and previous is not None else None}
    require(set(packet['by_asset_class']) == set(ASSETS), 'Asset class population differs')
    for role in ASSETS:
        item = packet['by_asset_class'][role]
        require(item['data_asof'] == asof and item['unit'] == 'usd_bn' and item['series_id'] == measurements[role]['id'], 'Asset identity differs')
        for label, n in (('latest', 1), ('rolling_12mo', 12)):
            value = window(item[label], role, asof, n)
            scalar(item['latest_month_b' if n == 1 else 'rolling_12mo_b'], Fraction(value, 1000) if value is not None else None, 'Asset amount')
    latest_roll = rolling[-1]['totals']
    short = amount(latest_roll['short_treasury']['usd_million_decimal'])
    expected_head['short_term_treasury_12mo_b'] = short
    for field, value in expected_head.items(): scalar(head[field], Fraction(value, 1000) if value is not None else None, 'Headline '+field)
    split = packet['holder_splits']['lt_total']; good = ('exact', 'within_reporting_rounding')
    split_ok = states[asof]['official_private'] in good
    split12 = all(period in states and states[period]['official_private'] in good for period in months_ending(asof, 12))
    require(split['status'] == ('OK' if split_ok else 'UNAVAILABLE') and split['rolling_twelve_months_reconciled'] is split12 and split['month'] == asof, 'Holder split qualification differs')
    gap = amount(recon[-1]['official_private']['gap_usd_million_decimal'])
    scalar(split['recon_gap_bn'], Fraction(gap, 1000) if gap is not None else None, 'Holder gap')
    for role in ('official', 'private'):
        monthly = core[role]['rows'].get(asof, {}).get('value'); twelve = amount(latest_roll[role]['usd_million_decimal'])
        scalar(split[role]['latest'], Fraction(monthly, 1000) if split_ok and monthly is not None else None, 'Latest holder amount')
        scalar(split[role]['sum_12m'], Fraction(twelve, 1000) if split12 and twelve is not None else None, 'Rolling holder amount')
    return {'contract': 'tic-independent-arithmetic.v1', 'archive_series_retained': population,
            'core_series_checked': 9, 'other_series_unqualified': population-9, **counts,
            'all_core_native_rows_checked': True, 'all_rolling_and_reconciliation_periods_checked': len(periods),
            'fred_parity_independently_verified': False, 'release_calendar_independently_verified': False,
            'historical_point_in_time_verified': False, 'predictive_edge_verified': False}
