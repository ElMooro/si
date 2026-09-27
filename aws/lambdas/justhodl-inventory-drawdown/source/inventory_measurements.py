"""Dated provider observations, without inventory-depletion or return forecasts."""
from datetime import date, timedelta
import calendar
import json
import math
import re

CONTRACT = 'inventory-observation-measurements.v1'


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)): return None
    try: return value if math.isfinite(value) else None
    except (OverflowError, ValueError): return None


def decode(raw):
    def pairs(items):
        out = {}
        for k, v in items:
            if k in out: raise ValueError('Duplicate source JSON key')
            out[k] = v
        return out
    def invalid(value): raise ValueError('Invalid source JSON number')
    def decimal(value):
        n = float(value)
        if not math.isfinite(n) or n == 0 and any(c in '123456789' for c in value.lower().split('e')[0]): invalid(value)
        return n
    if isinstance(raw, bytes): raw = raw.decode('utf-8')
    return json.loads(raw, object_pairs_hook=pairs, parse_float=decimal, parse_constant=invalid)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value): return None
    try: return date.fromisoformat(value)
    except ValueError: return None


def shift(value, months):
    year, month = divmod(value.year * 12 + value.month - 1 + months, 12); month += 1
    if not 1 <= year <= 9999: return None
    return date(year, month, min(value.day, calendar.monthrange(year, month)[1]))


def change(value, base):
    if number(value) is None or number(base) is None or value < 0 or base <= 0: return None
    return number(100 * (value / base - 1))


def provider_number(value):
    try: return number(decode(value)) if isinstance(value, str) else number(value)
    except (ValueError, TypeError): return None


def sector(sid, label, theme, acquisition, as_of):
    today = day(as_of)
    if not today: raise ValueError('Explicit check date required')
    source = acquisition.get('response'); raw = source.get('observations') if isinstance(source, dict) else None
    result = {'series': sid, 'sector': label, 'theme_etf': theme,
              'measurement_contract': CONTRACT, 'checked_as_of': as_of, 'acquisition': acquisition,
              'unit': 'ratio', 'frequency': 'monthly', 'seasonal_adjustment': 'SA',
              'definition_url': 'https://fred.stlouisfed.org/series/' + sid,
              'as_of': None, 'observation_age_days': None, 'latest_ratio': None,
              'observations': [], 'ratio_changes_pct': {}, 'percentile_5y': None,
              'percentile_method': 'Midrank in exactly 60 consecutive calendar months, including current month',
              'percentile_sample_n': 0, 'issues': [], 'status': 'source_unavailable',
              'chg_3m': None, 'chg_6m': None, 'chg_12m': None, 'drawdown_score': None, 'flag': None,
              'physical_inventory_change_verified': False, 'forecast_qualified': False,
              'legacy_signal_fields_withheld': 'Downstream consumers infer shortages from legacy changes; use dated ratio_changes_pct only as measurements.'}
    if not isinstance(raw, list): return result
    for i, row in enumerate(raw):
        d = day(row.get('date')) if isinstance(row, dict) else None
        v = provider_number(row.get('value')) if isinstance(row, dict) else None
        issues = []
        if not d or d.day != 1 or d > today: issues.append('invalid_or_future_month')
        if v is None or v < 0: issues.append('value_unavailable_or_invalid'); v = None
        result['observations'].append({'source_row': i, 'original': row, 'date': d.isoformat() if d else None,
                                       'value': v, 'issues': issues})
    observations = result['observations']
    if not observations: result['status'] = 'empty_response'; return result
    if any('invalid_or_future_month' in r['issues'] for r in observations):
        result['status'] = 'invalid_observation_calendar'; return result
    dates = [r['date'] for r in observations]
    if len(set(dates)) != len(dates): result['status'] = 'duplicate_observation_month'; return result
    ordered = sorted(observations, key=lambda r: r['date'], reverse=True)
    latest = ordered[0]; current = day(latest['date']); values = {r['date']: r for r in ordered}
    result.update(as_of=latest['date'], observation_age_days=(today-current).days,
                  latest_ratio=latest['value'], status='dated_monthly_observations' if latest['value'] is not None else 'latest_value_unavailable')
    for months in (3, 6, 12):
        previous = shift(current, -months); old = values.get(previous.isoformat()) if previous else None
        pct = change(latest['value'], old['value']) if old else None
        result['ratio_changes_pct'][str(months)+'m'] = {
            'value': pct, 'unit': 'percent', 'current_date': latest['date'],
            'prior_date': previous.isoformat() if previous else None,
            'current_source_row': latest['source_row'], 'prior_source_row': old['source_row'] if old else None,
            'status': 'exact_calendar_pair' if pct is not None else 'unavailable_calendar_pair_or_value'}
    sample = [values.get(shift(current, -i).isoformat()) for i in range(60) if shift(current, -i)]
    usable = [r for r in sample if r and r['value'] is not None]
    result['percentile_sample_n'] = len(usable)
    if len(usable) == 60 and latest['value'] is not None:
        below = sum(r['value'] < latest['value'] for r in usable)
        equal = sum(r['value'] == latest['value'] for r in usable)
        result['percentile_5y'] = 100 * (below + equal * 0.5) / 60
    return result


def stock(symbol, acquisition, context, as_of):
    today = day(as_of)
    if not today: raise ValueError('Explicit check date required')
    source = acquisition.get('response'); observations = []
    out = {'ticker': symbol, 'measurement_contract': CONTRACT, 'checked_as_of': as_of,
           'acquisition': acquisition, 'source_context': context, 'observations': observations,
           'industry': context.get('industry'), 'sector': context.get('sector'),
           'as_of': None, 'observation_age_days': None, 'dio_latest': None, 'dio_4q_ago': None,
           'dio_chg_pct': None, 'dio_change_days': None, 'unit': 'days', 'comparison': None,
           'revenue_per_share_change_pct': None, 'rev_growth_yoy': None,
           'draw_score': None, 'demand_score': None, 'boom_score': None, 'classification': None,
           'status': 'source_unavailable', 'calls_eligible': False, 'forecast_qualified': False,
           'sizing_eligible': False, 'execution_eligible': False,
           'measurement_limits': 'Provider-reported DIO; calculation inputs and methodology unverified. Per-share growth is not total revenue growth. Calendar/issuer alignment does not establish first-release availability, depletion, demand, shortage or return predictability.'}
    if not isinstance(source, list): return out
    for i, row in enumerate(source):
        d = day(row.get('date')) if isinstance(row, dict) else None
        start = day(row.get('startDate')) if isinstance(row, dict) else None
        value = number(row.get('daysOfInventoryOutstanding')) if isinstance(row, dict) else None
        issues = []
        if not isinstance(row, dict) or row.get('symbol') != symbol: issues.append('explicit_symbol_mismatch_or_missing')
        if not d or d > today: issues.append('invalid_or_future_date')
        if value is None or value < 0: issues.append('invalid_or_missing_dio'); value = None
        observations.append({'source_row': i, 'original': row, 'date': d.isoformat() if d else None,
                             'start_date': start.isoformat() if start else None, 'value': value, 'issues': issues})
    if not observations: out['status'] = 'empty_response'; return out
    if any(not r['date'] or day(r['date']) > today for r in observations):
        out['status'] = 'invalid_observation_calendar'; return out
    dates = [r['date'] for r in observations]
    if len(set(dates)) != len(dates): out['status'] = 'duplicate_observation_period'; return out
    ordered = sorted(observations, key=lambda r: r['date'], reverse=True); latest = ordered[0]
    current = day(latest['date']); original = latest['original']; value = latest['value'] if not latest['issues'] else None
    out.update(as_of=latest['date'], observation_age_days=(today-current).days, dio_latest=value,
               status='reported_dio_only' if value is not None else 'latest_value_unavailable')
    prior_date = shift(current, -12)
    prior = next((r for r in ordered if prior_date and r['date'] == prior_date.isoformat()), None)
    pair = {'status': 'explicit_comparable_calendar_quarters_unavailable', 'current_source_row': latest['source_row'],
            'prior_source_row': prior['source_row'] if prior else None, 'current_date': latest['date'],
            'prior_date': prior_date.isoformat() if prior_date else None, 'issuer_cik': None}
    out['comparison'] = pair
    if value is None or prior is None or prior['issues']: return out
    old = prior['original']; start = day(latest['start_date']); old_start = day(prior['start_date'])
    cik = str(original.get('cik', '')).lstrip('0'); old_cik = str(old.get('cik', '')).lstrip('0')
    # Do not infer statement duration from an array offset or a quarter label alone.
    following = shift(start, 3) if start else None
    if not (cik and cik.isdigit() and len(cik) <= 10 and cik == old_cik and start and start.day == 1
            and following and following-timedelta(days=1) == current and shift(start, -12) == old_start
            and original.get('period') in ('Q1','Q2','Q3','Q4') and original.get('period') == old.get('period')): return out
    pct = change(value, prior['value']); delta = number(value-prior['value'])
    pair.update(status='exact_reported_annual_quarter_pair', issuer_cik=cik)
    out.update(dio_4q_ago=prior['value'], dio_chg_pct=pct, dio_change_days=delta, status='reported_dio_calendar_pair')
    currency = original.get('reportedCurrency')
    if isinstance(currency, str) and re.fullmatch('[A-Z]{3}', currency) and currency == old.get('reportedCurrency'):
        out['revenue_per_share_change_pct'] = change(number(original.get('revenuePerShare')), number(old.get('revenuePerShare')))
    return out


def universe(sources):
    """Same three existing feeds and 130-name cap, with deterministic source order."""
    seen = {}; occurrences = []
    for key, field in (('data/bottleneck-boom.json','ranks'), ('data/chokepoint.json','all_chokepoints'),
                       ('data/scarcity-radar.json','stealth_shortage_board')):
        packet = sources.get(key)
        if packet is None: continue
        if not isinstance(packet, dict) or not isinstance(packet.get(field, []), list): raise ValueError('Malformed universe source')
        for i, row in enumerate(packet.get(field, [])):
            symbol = row.get('ticker') if isinstance(row, dict) else None
            valid = isinstance(symbol, str) and bool(re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}', symbol))
            occurrences.append({'key': key, 'field': field, 'source_row': i, 'original': row, 'valid_symbol': valid})
            if valid and symbol not in seen:
                seen[symbol] = {'industry': row.get('industry'), 'sector': row.get('sector'), 'source_key': key, 'source_row': i}
    names = list(seen)
    return {'requested': names[:130], 'not_attempted': names[130:], 'contexts': seen, 'occurrences': occurrences,
            'limit': 130, 'ordering': 'First received occurrence across declared sources; no random set slicing',
            'universe_complete': False}
