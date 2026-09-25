"""Independent exact-rational verification; no production compiler import or IO.

The caller must authenticate complete originals and the canonical snapshot.
This verifies descriptive arithmetic, not a return forecast or trading edge.
"""
from calendar import monthrange
from datetime import date, datetime, timedelta
from fractions import Fraction
import math

SPECS = {'WALCL': ('Millions of U.S. Dollars', 'W', 1000, 21),
         'WTREGEN': ('Millions of U.S. Dollars', 'W', 1000, 21),
         'RRPONTSYD': ('Billions of US Dollars', 'D', 1, 10)}


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('Timezone required')
    return result


def number(value):
    if value is None or isinstance(value, bool) or value in ('', '.'): return None
    try: return Fraction(value)
    except (ValueError, TypeError, OverflowError, ZeroDivisionError): return None


def check_scalar(actual, expected):
    assert actual['unit'] == 'usd_bn'
    if expected is None:
        assert actual['exact_decimal'] is None and actual['value'] is None
    else:
        assert number(actual['exact_decimal']) == expected
        assert type(actual['value']) in (int, float) and math.isfinite(actual['value'])
        assert actual['value'] == float(expected)


def verify(output, source, originals):
    assert output['contract'] == 'liquidity-flow-research-candidate.v1'
    assert output['formula'] == 'WALCL - WTREGEN - RRPONTSYD' and output['unit'] == 'usd_bn'
    assert output['source_generated_at'] == source['generated_at'] and output['source_replay'] == source['replay']
    now, acquired = timestamp(output['generated_at']), timestamp(source['generated_at'])
    assert now >= acquired
    end = acquired.date()
    row_count, scalar_count, unavailable = 0, 0, 0
    for sid, (unit, frequency, divisor, ceiling) in SPECS.items():
        original, view = originals[sid], output['series'][sid]
        definition = original['definition']['seriess'][0]
        assert definition['id'] == sid and definition['units'] == unit
        assert definition['frequency_short'] == frequency and definition['seasonal_adjustment_short'] == 'NSA'
        assert view['definition'] == definition and view['native_unit'] == unit and view['frequency'] == frequency
        assert Fraction(view['to_usd_bn']) == Fraction(1, divisor) and view['max_observation_age_days'] == ceiling
        assert view['evidence'] == original['evidence'] and view['acquired_at'] == original['acquired_at']
        assert view['coverage'] == source['measurements'][sid]['coverage']
        assert view['source_quality'] == source['measurements'][sid]['quality']
        assert view['acquisition_age_hours'] == (now-timestamp(original['acquired_at'])).total_seconds()/3600
        expected = sorted([{'observation_date': row['date'], 'native_value': row.get('value'),
            'original_row': index, 'after_snapshot_date': date.fromisoformat(row['date']) > end}
            for index, row in enumerate(original['observations']['observations'])], key=lambda row: row['observation_date'])
        assert view['history'] == expected and view['all_original_rows_retained'] is True
        assert len({row['observation_date'] for row in expected}) == len(expected)
        row_count += len(expected)

    def leg(sid, day, actual):
        nonlocal scalar_count, unavailable
        unit, frequency, divisor, ceiling = SPECS[sid]
        rows = originals[sid]['observations']['observations']
        candidates = [(row['date'], index, row) for index, row in enumerate(rows) if row['date'] <= str(day)]
        assert actual['valuation_date'] == str(day)
        if not candidates:
            assert actual['selected'] is None and actual['carry_days'] is None and actual['status'] == 'missing_observation'
            value = None
        else:
            observed, index, row = max(candidates)
            assert actual['selected'] == {'observation_date': observed, 'native_value': row.get('value'),
                                         'original_row': index, 'after_snapshot_date': date.fromisoformat(observed) > end}
            lag = (day-date.fromisoformat(observed)).days
            assert actual['carry_days'] == lag
            value = number(row.get('value'))
            reason = 'missing_source_value' if value is None else 'invalid_domain' if value < 0 else 'age_ceiling_exceeded' if lag > ceiling else 'descriptive'
            assert actual['status'] == reason
            value = value/divisor if reason == 'descriptive' else None
        check_scalar(actual['value'], value); scalar_count += 1
        if value is None: unavailable += 1
        return value

    def sample(day, actual):
        nonlocal scalar_count
        assert actual['valuation_date'] == str(day) and set(actual['legs']) == set(SPECS)
        values = {sid: leg(sid, day, actual['legs'][sid]) for sid in SPECS}
        total = values['WALCL']-values['WTREGEN']-values['RRPONTSYD'] if all(v is not None for v in values.values()) else None
        check_scalar(actual['net'], total); scalar_count += 1
        assert actual['status'] == ('descriptive_mixed_basis' if total is not None else 'unavailable')
        assert actual['unavailable_legs'] == [sid for sid in SPECS if values[sid] is None]

    sample(end, output['last_reconstructed_snapshot'])
    assert len(output['calendar_history_180d']) == 180
    for index, actual in enumerate(output['calendar_history_180d']): sample(end-timedelta(days=179-index), actual)
    horizons = {'1d': end-timedelta(days=1), '1w': end-timedelta(days=7)}
    for label, months in (('1m', 1), ('3m', 3)):
        year, month = divmod(end.year*12+end.month-1-months, 12); month += 1
        horizons[label] = date(year, month, min(end.day, monthrange(year, month)[1]))
    assert set(output['comparisons']) == set(horizons)
    for label, start in horizons.items():
        actual = output['comparisons'][label]
        assert actual['current_valuation_date'] == str(end) and actual['baseline_valuation_date'] == str(start)
        assert actual['calendar_days'] == (end-start).days
        contributions = []
        for sid in SPECS:
            row = actual['legs'][sid]
            a, b = leg(sid, end, row['current']), leg(sid, start, row['baseline'])
            change = a-b if a is not None and b is not None else None
            contribution = change if sid == 'WALCL' or change is None else -change
            check_scalar(row['reported_level_change'], change); check_scalar(row['signed_formula_contribution'], contribution); scalar_count += 2
            carried = bool(row['current']['selected'] and row['baseline']['selected'] and row['current']['selected']['original_row'] == row['baseline']['selected']['original_row'])
            assert row['same_observation_carried'] == carried
            contributions.append(contribution)
        total = sum(contributions) if all(v is not None for v in contributions) else None
        check_scalar(actual['change'], total); scalar_count += 1
        assert actual['historical_information_availability_verified'] is False and actual['causal_flow_estimate'] is False
    age = (now-acquired).total_seconds()/3600
    eligible = age <= 26 and all(source['measurements'][sid]['quality']['status'] == 'fresh'
        and 0 <= (now-timestamp(originals[sid]['acquired_at'])).total_seconds()/3600 <= 26 for sid in SPECS)
    eligible &= output['last_reconstructed_snapshot']['status'] != 'unavailable'
    assert output['current'] == (output['last_reconstructed_snapshot'] if eligible else None)
    assert output['quality']['status'] == ('within_source_age_ceiling' if eligible else 'source_ineligible')
    assert output['quality']['source_age_hours'] == age and output['quality']['max_acquisition_age_hours'] == 26
    for field in ('release_calendar_verified', 'original_publication_time_verified'): assert output['quality'][field] is False
    assert output['quality']['publication_date'] is None
    for field in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified', 'point_in_time_backtest_qualified', 'publication_eligible'): assert output[field] is False
    assert output['candidate_only'] is True and output['call'] is None and output['decision'] == {'verb': 'WAIT', 'meaning': 'abstain'}
    assert output['portfolio_consequences'] == {'status': 'UNAVAILABLE', 'target_weights': None, 'forced_liquidation': False}
    return {'original_rows_checked': row_count, 'calendar_dates_checked': 180, 'comparison_windows_checked': 4,
            'exact_rational_comparisons': scalar_count, 'unavailable_legs_preserved': unavailable,
            'production_compiler_imported': False, 'forecast_qualified': False, 'sizing_qualified': False}
