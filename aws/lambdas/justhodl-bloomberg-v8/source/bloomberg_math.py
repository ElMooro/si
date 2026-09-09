"""Dated descriptive statistics; absent observations never become neutral data."""
import calendar
from datetime import date, datetime, timedelta, timezone
import math
import statistics


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError, OverflowError):
        return None


def percent(current, previous):
    current, previous = number(current), number(previous)
    if current is None or previous in (None, 0):
        return None
    return round((current - previous) / abs(previous) * 100, 4)


def months_before(day, months):
    total = day.year * 12 + day.month - 1 - months
    year, month = divmod(total, 12); month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


def fred_statistics(observations, name):
    rows = []; seen = set(); invalid = 0
    for row in observations:
        try:
            day = date.fromisoformat(row['date'])
        except (KeyError, TypeError, ValueError):
            invalid += 1; continue
        if day in seen:
            invalid += 1; continue
        seen.add(day)
        rows.append({'date': day.isoformat(), 'value': number(row.get('value'))})
    rows.sort(key=lambda r: r['date'], reverse=True)
    result = {'name': name, 'value': rows[0]['value'] if rows else None,
              'date': rows[0]['date'] if rows else None, 'history': rows,
              'invalid_or_duplicate_rows': invalid, 'change_baselines': {},
              'change_basis': 'calendar_cutoff_with_observed_cadence_tolerance',
              'history_vintage': 'current_provider_vintage_not_point_in_time'}
    gaps = [(date.fromisoformat(a['date']) - date.fromisoformat(b['date'])).days for a, b in zip(rows, rows[1:])]
    cadence = statistics.median(gaps) if gaps else None
    for label, months in (('1w', 0), ('1m', 1), ('3m', 3), ('1y', 12)):
        value = None; baseline = None
        if rows and cadence and not invalid:
            current_day = date.fromisoformat(rows[0]['date'])
            cutoff = current_day - timedelta(days=7) if not months else months_before(current_day, months)
            if cadence <= (current_day - cutoff).days + 2:
                baseline = next((r for r in rows if r['date'] <= cutoff.isoformat()), None)
                tolerance = max(4, math.ceil(cadence * 1.5))
                if baseline and (cutoff - date.fromisoformat(baseline['date'])).days <= tolerance:
                    value = percent(result['value'], baseline['value'])
                else:
                    baseline = None
        result['chg_' + label] = value
        result['change_baselines'][label] = baseline
    return result


def stock_statistics(bars, *, today=None):
    today = today or datetime.now(timezone.utc).date()
    history = []; seen = set(); invalid = 0
    for bar in bars:
        try:
            stamp = number(bar.get('t'))
            day = datetime.fromtimestamp(stamp / 1000, timezone.utc).date() if stamp is not None else None
            close = number(bar.get('c'))
            if day is None or close is None or close <= 0 or day in seen:
                raise ValueError('invalid_bar')
        except (AttributeError, ValueError, TypeError, OverflowError, OSError):
            invalid += 1; continue
        seen.add(day)
        history.append({**bar, 'date': day.isoformat()})
    history.sort(key=lambda r: r['date'])
    completed = [r for r in history if r['date'] < today.isoformat()]
    prices = [float(r['c']) for r in completed]
    count = len(prices); current = prices[-1] if prices else None
    result = {'close': current, 'date': completed[-1]['date'] if completed else None,
              'history': list(reversed(history)), 'completed_bar_count': count,
              'invalid_or_duplicate_rows': invalid, 'adjustment_basis': 'provider_adjusted_true',
              'change_basis': 'completed_sessions_1d_1w5_1m21_3m63_1y252',
              'execution_eligible': False}
    for label, lag in (('1d', 1), ('1w', 5), ('1m', 21), ('3m', 63), ('1y', 252)):
        result['chg_' + label] = percent(current, prices[-lag-1]) if count > lag and not invalid else None
    result['change_pct'] = result['chg_1d']
    cutoff_52w = date.fromisoformat(completed[-1]['date']) - timedelta(weeks=52) if completed else today
    window = [r for r in completed if r['date'] >= cutoff_52w.isoformat()]
    covered = completed and completed[0]['date'] <= cutoff_52w.isoformat() and not invalid
    for output, field, aggregate in (('high_52w', 'h', max), ('low_52w', 'l', min)):
        values = [number(r.get(field)) for r in window]
        result[output] = aggregate(values) if covered and values and all(v is not None for v in values) else None
    cutoff = date(today.year, 1, 1) - timedelta(days=1)
    baseline = next((r for r in reversed(completed) if r['date'] <= cutoff.isoformat()), None)
    valid_ytd = baseline and (cutoff - date.fromisoformat(baseline['date'])).days <= 7
    result['chg_ytd'] = percent(current, baseline['c']) if valid_ytd and not invalid else None
    result['ytd_baseline_date'] = baseline['date'] if valid_ytd else None
    for period in (50, 200):
        sma = sum(prices[-period:]) / period if count >= period and not invalid else None
        result['sma' + str(period)] = round(sma, 4) if sma is not None else None
        result['above_sma' + str(period)] = current > sma if sma is not None else None
    rsi = None; macd = None
    if count >= 15 and not invalid:
        changes = [b-a for a, b in zip(prices, prices[1:])]
        gain = sum(max(v, 0) for v in changes[:14]) / 14
        loss = sum(max(-v, 0) for v in changes[:14]) / 14
        for change in changes[14:]:
            gain = (gain * 13 + max(change, 0)) / 14
            loss = (loss * 13 + max(-change, 0)) / 14
        rsi = 50 if gain == loss == 0 else 100 if loss == 0 else 100 - 100 / (1 + gain / loss)
    if count >= 26 and not invalid:
        ema12 = ema26 = prices[0]
        for price in prices[1:]:
            ema12 += 2 / 13 * (price - ema12); ema26 += 2 / 27 * (price - ema26)
        macd = ema12 - ema26
    result.update(rsi=round(rsi, 4) if rsi is not None else None,
                  macd=round(macd, 4) if macd is not None else None,
                  indicator_basis='Wilder_RSI14_and_EMA12_minus_EMA26_seeded_at_first_returned_close',
                  volume=number(completed[-1].get('v')) if completed else None)
    return result


def liquidity(fred):
    # FRED WALCL and WTREGEN: USD millions; RRPONTSYD: USD billions.
    # WTREGEN is a week average, WALCL a Wednesday level. This proxy is not a
    # synchronized daily balance sheet, and no growth-rate subtraction is valid.
    units = {'WALCL': 1, 'WTREGEN': 1, 'RRPONTSYD': 1000}
    values = {k: number(fred.get(k, {}).get('value')) for k in units}
    missing = [k for k, v in values.items() if v is None]
    return {'net_liquidity': round(values['WALCL'] - values['WTREGEN'] - values['RRPONTSYD'] * 1000, 4) if not missing else None,
            'fed_bs': values['WALCL'], 'tga': values['WTREGEN'],
            'rrp': values['RRPONTSYD'] * 1000 if values['RRPONTSYD'] is not None else None,
            'units': 'USD millions', 'component_dates': {k: fred.get(k, {}).get('date') for k in units},
            'basis': 'mixed_observation_dates_and_week_average_vs_point_level_descriptive_proxy',
            'net_liquidity_chg_1m': None, 'growth_status': 'ALIGNED_COMPONENT_LEVEL_HISTORY_REQUIRED',
            'missing_inputs': missing, 'execution_eligible': False}
