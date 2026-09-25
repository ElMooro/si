"""Independent Fraction checks against original Pulse provider rows; no model import."""
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
import math

EXPECTED = ('WALCL', 'WRESBAL', 'WTREGEN', 'RESPPALGUONNWW', 'RESPPNTEPNWW',
            'OTHL1690', 'SWP1690', 'BAMLH0A3HYC', 'BAMLHE00EHYIOAS', 'BAMLEMHBHYCRPIOAS', 'HQMCB10YR')


def number(value):
    if value is None or isinstance(value, bool): return None
    try: return Fraction(str(value))
    except (ValueError, ZeroDivisionError): return None


def clock(value):
    out = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if out.tzinfo is None: raise ValueError('Aware evidence clock required')
    return out.astimezone(timezone.utc)


def months_before(day, months):
    serial = day.year*12+day.month-1-months; year, zero_month = divmod(serial, 12)
    return date(year, zero_month+1, min(day.day, monthrange(year, zero_month+1)[1]))


def verify(output, source, originals):
    assert set(output['series']) == set(EXPECTED)
    for flag in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified',
                 'publication_eligible', 'point_in_time_backtest_qualified'):
        assert output[flag] is False
    assert output['call'] is None and output['decision'] == {'verb': 'WAIT', 'meaning': 'abstain'}
    assert output['composites']['credit_stress_score'] is None and output['composites']['liquidity_score'] is None
    assert output['transitions'] == []
    now = clock(output['generated_at']); source_time = clock(source['generated_at'])
    assert output['source_replay'] == source['replay'] and output['source_generated_at'] == source['generated_at']
    count = checks = comparisons = unavailable = fresh = 0
    for sid in EXPECTED:
        actual = output['series'][sid]; original = originals.get(sid)
        assert actual['signal'] is None and actual['polarity'] is None and actual['z_score'] is None
        assert actual['calls_eligible'] is False and actual['sizing_eligible'] is False
        assert actual['series_id'] == sid and actual['published_at'] is None
        assert actual['source_replay'] == source['replay']
        if sid not in source['measurements']:
            assert actual['history'] == [] and actual['latest_value'] is None and actual['quality']['status'] == 'unavailable'
            assert original is None and actual['latest_value_decimal'] is None
            assert not actual['calendar_comparisons'] and not actual['historical_calendar_comparisons']
            assert all(value is None for value in actual['deltas'].values())
            unavailable += 1; continue
        assert original is not None
        rows = original['observations']['observations']; definition = original['definition']['seriess'][0]
        assert len(actual['history']) == len(rows); count += len(rows)
        for i, (row, retained) in enumerate(zip(rows, actual['history'])):
            assert retained == {'original_row': i, 'observation_date': row['date'], 'native_value': row.get('value'),
                'realtime_start': row.get('realtime_start'), 'realtime_end': row.get('realtime_end')}
        assert actual['source_definition'] == definition and actual['unit'] == definition['units']
        assert actual['frequency'] == definition['frequency_short']
        assert actual['seasonal_adjustment'] == definition['seasonal_adjustment']
        assert actual['evidence'] == original['evidence'] and actual['acquired_at'] == original['acquired_at']
        dated = sorted(((date.fromisoformat(row['date']), i, number(row.get('value'))) for i, row in enumerate(rows)
            if row['date'] <= source_time.date().isoformat()), reverse=True)
        observed, current_index, value = dated[0]; freq = definition['frequency_short']
        expected_frequency = 'M' if sid == 'HQMCB10YR' else 'D' if sid.startswith('BAML') else 'W'
        expected_unit = 'Percent' if expected_frequency in ('D', 'M') else 'Millions of U.S. Dollars'
        expected_basis = ('Monthly' if sid == 'HQMCB10YR' else 'Daily' if sid == 'BAMLEMHBHYCRPIOAS'
            else 'Daily, Close' if sid.startswith('BAML') else 'Weekly, Ending Wednesday' if sid in ('WRESBAL', 'WTREGEN')
            else 'Weekly, As of Wednesday')
        usable = (definition['units'] == expected_unit and freq == expected_frequency and value is not None
            and definition['seasonal_adjustment'] == 'Not Seasonally Adjusted'
            and definition.get('frequency') == expected_basis
            and 0 <= (now.date()-observed).days <= {'D':10, 'W':21, 'M':100}.get(freq, -1)
            and 0 <= (now-clock(original['acquired_at'])).total_seconds() <= 26*3600
            and 0 <= (now-source_time).total_seconds() <= 26*3600
            and source['measurements'][sid]['quality']['status'] == 'fresh')
        assert (actual['quality']['status'] == 'fresh') == usable; fresh += usable
        assert actual['latest_date'] == str(observed) and actual['source_row'] == current_index
        assert number(actual['last_observed_value']) == value
        assert number(actual['latest_value_decimal']) == (value if usable else None)
        assert actual['latest_value'] == (float(value) if usable else None); checks += 3
        if not usable: assert actual['calendar_comparisons'] == {}
        else: assert actual['calendar_comparisons'] == actual['historical_calendar_comparisons']
        for label, month_count in (('week', 0), ('month', 1), ('quarter', 3), ('year', 12)):
            target = observed-timedelta(days=7) if label == 'week' else months_before(observed, month_count)
            possible = [(d, i, v) for d, i, v in dated if d <= target]
            baseline = possible[0] if possible else None
            # Even a changed definition remains inspectable as historical data,
            # but it never grants the expected Pulse series current authority.
            horizons = {'M': {1, 3, 12}, 'Q': {3, 12}, 'SA': {12}, 'A': {12}}
            if freq in horizons:
                baseline = next((r for r in dated if r[0] == target), None) if month_count in horizons[freq] else None
            elif freq == 'BW' and label == 'week': baseline = None
            elif baseline and (target-baseline[0]).days > {'D':4, 'W':6, 'BW':13}.get(freq, -1): baseline = None
            b = baseline[2] if baseline else None; difference = value-b if value is not None and b is not None else None
            relative = 100*difference/b if difference is not None and b > 0 else None
            change = actual['historical_calendar_comparisons'][label]; comparisons += 1
            assert change['target_date'] == str(target) and change['current_date'] == str(observed)
            assert change['baseline_date'] == (str(baseline[0]) if baseline else None)
            assert change['baseline_row_index'] == (baseline[1] if baseline else None)
            assert number(change['current_decimal']) == value and number(change['baseline_decimal']) == b
            assert number(change['change_decimal']) == difference
            assert change['change'] == (float(difference) if difference is not None else None)
            assert change['change_unit'] == ('percentage_points' if definition['units'].startswith('Percent') else definition['units'])
            if relative is None: assert change['pct_change'] is None
            else:
                assert type(change['pct_change']) in (int, float) and math.isfinite(change['pct_change'])
                assert abs(Fraction(str(change['pct_change']))-relative) <= Fraction(1, 1000000)
            old = {'week':'wow', 'month':'mom', 'quarter':'qoq', 'year':'yoy'}[label]+'_pct'
            assert actual['deltas'][old] == (change['pct_change'] if usable else None)
            checks += 5
    assert output['n_series_ok'] == fresh and output['n_series'] == len(EXPECTED)
    assert output['quality']['status'] == ('fresh' if fresh == len(EXPECTED) else 'degraded' if fresh else 'unavailable')
    assert output['quality']['fresh_series'] == fresh
    return {'original_rows': count, 'requested_series': len(EXPECTED), 'missing_series': unavailable,
        'fresh_series': fresh, 'calendar_comparisons': comparisons, 'rational_checks': checks,
        'forecast_qualified': False, 'sizing_qualified': False}
