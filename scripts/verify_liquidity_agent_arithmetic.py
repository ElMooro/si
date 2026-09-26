"""Independent rational and observation-coordinate checks; no compiler import."""
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
import math
from liquidity_agent_catalog import SPECS, SERIES, CONTEXT_KEYS
import verify_liquidity_arithmetic as flow_verifier


def clock(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    assert stamp.tzinfo is not None
    return stamp.astimezone(timezone.utc)


def number(value):
    if value in (None, '', '.') or isinstance(value, bool): return None
    return Fraction(str(value))


def scalar(actual, expected, tolerance=Fraction(0)):
    if expected is None:
        assert actual == {'value': None, 'exact_decimal': None}
    else:
        assert actual['exact_decimal'] is not None
        assert abs(number(actual['exact_decimal'])-expected) <= tolerance
        assert math.isfinite(actual['value']) and abs(actual['value']-float(expected)) <= max(1e-12, abs(float(expected))*1e-14)


def months_before(day, count):
    year, zero_month = divmod(day.year*12+day.month-1-count, 12)
    return date(year, zero_month+1, min(day.day, monthrange(year, zero_month+1)[1]))


def verify(output, source, originals):
    assert output['contract'] == 'liquidity-agent-candidate.v1' and output['candidate_only'] is True
    assert set(output['series']) == set(SERIES)
    for flag in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','publication_eligible','point_in_time_backtest_qualified'):
        assert output[flag] is False
    assert output['call'] is None and output['decision'] == {'verb':'WAIT','meaning':'abstain'}
    assert output['uncalibrated_legacy_metrics'] == {'composite_score':None,'regime':None,'credit_first_stage':None}
    assert output['portfolio_consequences']['target_weights'] is None
    assert set(output['contexts']) == set(CONTEXT_KEYS) and output['dependency_graph']['independent_votes'] == 0
    assert output['dependency_graph']['series_to_engine'] == list(SERIES)
    assert output['source_replay'] == source['replay'] and output['source_generated_at'] == source['generated_at']
    now, source_time = clock(output['generated_at']), clock(source['generated_at'])
    assert now >= source_time
    total = missing = comparisons = checks = fresh = 0
    for sid in SERIES:
        item = output['series'][sid]; original = originals.get(sid)
        assert item['series_id'] == sid and item['published_at'] is None
        assert item['calls_eligible'] is False and item['sizing_eligible'] is False
        assert item['source_replay'] == source['replay']
        if original is None:
            assert sid not in source['measurements']
            assert item['history'] == [] and item['quality']['status'] == 'unavailable'
            scalar(item['current'], None); scalar(item['last_observed'], None)
            assert item['historical_calendar_comparisons'] == {} and item['calendar_comparisons'] == {}
            assert item['definition_reviewed'] is False
            missing += 1; continue
        rows = original['observations']['observations']; definition = original['definition']['seriess'][0]
        total += len(rows)
        assert len(item['history']) == len(rows)
        for index, (row, actual) in enumerate(zip(rows, item['history'])):
            assert actual == {'original_row':index,'observation_date':row['date'],'native_value':row.get('value'),
                'realtime_start':row.get('realtime_start'),'realtime_end':row.get('realtime_end')}
        assert item['source_definition'] == definition and item['evidence'] == original['evidence']
        assert item['acquired_at'] == original['acquired_at'] and item['provider_updated_at'] == definition.get('last_updated')
        assert (item['unit'],item['frequency'],item['seasonal_adjustment']) == (definition['units'],definition['frequency_short'],definition['seasonal_adjustment'])
        defined = (definition['units'],definition['frequency_short'],definition.get('frequency'),definition['seasonal_adjustment'])
        reviewed = defined == SPECS[sid]['reviewed_definition']
        assert item['definition_reviewed'] == reviewed
        dated = sorted([(date.fromisoformat(r['date']),i,number(r.get('value'))) for i,r in enumerate(rows)
            if r['date'] <= str(source_time.date())], reverse=True)
        assert len({r[0] for r in dated}) == len(dated)
        observed, index, value = dated[0]; frequency = definition['frequency_short']
        assert item['latest_date'] == str(observed) and item['original_row'] == index
        scalar(item['last_observed'],value)
        factors = {'Millions of U.S. Dollars':Fraction(1,1000),'Billions of Dollars':Fraction(1),'Billions of US Dollars':Fraction(1)}
        factor = factors.get(definition['units']) if reviewed else None
        domain = (value is not None and (SPECS[sid]['group'] not in ('dollar','equities') or value > 0)
            and (definition['units'] not in factors or value >= 0))
        usable = (reviewed and domain and 0 <= (now.date()-observed).days <= {'D':10,'W':21,'BW':35,'M':100,'Q':200,'SA':370,'A':550}[frequency]
            and 0 <= (now-clock(original['acquired_at'])).total_seconds() <= 26*3600
            and 0 <= (now-source_time).total_seconds() <= 26*3600
            and source['measurements'][sid]['quality']['status'] == 'fresh')
        assert (item['quality']['status'] == 'within_age_ceiling') == usable; fresh += usable
        scalar(item['current'],value if usable else None); checks += 2
        normalization = item['usd_billions']
        assert number(normalization['multiplier']) == factor
        scalar({k:normalization[k] for k in ('value','exact_decimal')}, value*factor if usable and factor is not None else None); checks += 1
        assert item['calendar_comparisons'] == (item['historical_calendar_comparisons'] if usable else {})
        assert item['coverage'] == source['measurements'][sid]['coverage']
        for label, months, days in (('week',0,7),('month',1,0),('quarter',3,0),('year',12,0)):
            target = observed-timedelta(days=days) if days else months_before(observed,months)
            periodic = {'M':{1,3,12},'Q':{3,12},'SA':{12},'A':{12}}
            if frequency in periodic:
                baseline = next((r for r in dated if r[0] == target),None) if months in periodic[frequency] else None
            else:
                baseline = next((r for r in dated if r[0] <= target),None)
                if frequency == 'BW' and days: baseline = None
                if baseline and (target-baseline[0]).days > {'D':4,'W':6,'BW':13}[frequency]: baseline = None
            b = baseline[2] if baseline else None
            difference = value-b if value is not None and b is not None else None
            relative = 100*difference/b if difference is not None and b > 0 else None
            actual = item['historical_calendar_comparisons'][label]
            assert actual['target_date'] == str(target) and actual['current_date'] == str(observed)
            assert actual['baseline_date'] == (str(baseline[0]) if baseline else None)
            assert actual['baseline_row_index'] == (baseline[1] if baseline else None)
            assert number(actual['current_decimal']) == value and number(actual['baseline_decimal']) == b
            assert number(actual['change_decimal']) == difference
            assert actual['change'] == (float(difference) if difference is not None else None)
            assert actual['change_unit'] == ('percentage_points' if definition['units'].startswith('Percent') else definition['units'])
            if relative is None: assert actual['pct_change'] is None
            else: assert abs(number(actual['pct_change'])-relative) <= Fraction(1,1000000)
            comparisons += 1; checks += 5
    assert output['quality']['current_series'] == fresh
    assert output['quality']['requested_series'] == 73 and output['quality']['reconstructed_series'] == 73-missing
    assert output['quality']['missing_series'] == [sid for sid in SERIES if originals.get(sid) is None]
    assert output['quality']['status'] == ('within_age_ceiling' if fresh == 73 else 'degraded' if fresh else 'unavailable')
    net = output['derived']['net_liquidity']; reconstruction = net['reconstruction']; flow_proof = None
    if reconstruction:
        flow_proof = flow_verifier.verify(reconstruction,source,originals)
        usable = all(output['series'][sid]['quality']['status'] == 'within_age_ceiling' for sid in ('WALCL','WTREGEN','RRPONTSYD'))
        assert net['current'] == (reconstruction['current'] if usable else None)
    else:
        assert any(output['series'][s]['definition_reviewed'] is False for s in ('WALCL','WTREGEN','RRPONTSYD'))
        assert net['current'] is None
    view = output['derived']['sp500_60_calendar_days']; sp = output['series']['SP500']
    rows = sorted([r for r in sp['history'] if r['observation_date'] <= str(source_time.date())],key=lambda r:r['observation_date'])
    current = rows[-1] if rows else None
    target = date.fromisoformat(current['observation_date'])-timedelta(days=60) if current else None
    baseline = next((r for r in reversed(rows) if r['observation_date'] <= str(target)),None) if target else None
    if baseline and (target-date.fromisoformat(baseline['observation_date'])).days > 4: baseline = None
    members = [r for r in rows if target and r['observation_date'] >= str(target)]
    numeric = [r for r in members if number(r['native_value']) is not None]
    high = max(numeric,key=lambda r:number(r['native_value'])) if numeric else None
    assert view['current'] == current and view['baseline'] == baseline and view['window_high'] == high
    assert view['window_original_rows'] == [r['original_row'] for r in members]
    assert view['numeric_window_rows'] == len(numeric)
    assert view['calendar_days'] == 60 and view['target_date'] == (str(target) if target else None)
    a,b,h = (number((r or {}).get('native_value')) for r in (current,baseline,high))
    for key,denominator in (('price_return_pct',b),('distance_from_window_high_pct',h)):
        value = 100*(a-denominator)/denominator if sp['definition_reviewed'] and a is not None and a > 0 and denominator is not None and denominator > 0 else None
        scalar(view['historical_comparison'][key],value,Fraction(1,10**28)); checks += 1
    assert view['current_comparison'] == (view['historical_comparison'] if sp['quality']['status'] == 'within_age_ceiling' else None)
    for flag in ('total_return','calendar_completeness_verified','maximum_peak_to_trough_drawdown','causal_sequence_verified'):
        assert view[flag] is False
    return {'requested_series':73,'reconstructed_series':73-missing,'missing_series':missing,'current_series':fresh,
        'original_rows':total,'calendar_comparisons':comparisons,'rational_checks':checks,'flow_verification':flow_proof,
        'forecast_qualified':False,'sizing_qualified':False}
