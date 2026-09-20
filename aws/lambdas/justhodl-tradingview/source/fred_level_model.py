"""Current-vintage FRED levels; pure, source-bound arithmetic, never return forecasts."""
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re

CONTRACT = 'fred-native-level.v1'
PREFIX = 'data/fred-levels/'
AGE_CEILINGS = {'D': 7, 'W': 20, 'BW': 32, 'M': 100, 'Q': 200, 'SA': 370, 'A': 550}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def clock(stamp):
    dt = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    if dt.tzinfo is None: raise ValueError('timezone required')
    return dt


def level(value):
    if value in (None, '', '.'): return None
    if isinstance(value, bool): raise ValueError('boolean observation')
    try: result = Decimal(str(value))
    except InvalidOperation: raise ValueError('invalid observation') from None
    if not result.is_finite() or not math.isfinite(float(result)): raise ValueError('nonfinite observation')
    if result != 0 and float(result) == 0: raise ValueError('display underflow')
    return result


def compile_level(sid, observations_raw, definition_raw, collected_at):
    """Last two provider rows, including missing values; do not shift a missing baseline."""
    if not re.fullmatch(r'[A-Za-z0-9_]{1,80}', sid): raise ValueError('invalid FRED identity')
    now = clock(collected_at)
    obs, definitions = json.loads(observations_raw), json.loads(definition_raw).get('seriess')
    if not isinstance(definitions, list) or len(definitions) != 1 or definitions[0].get('id') != sid:
        raise ValueError('definition identity differs')
    definition = definitions[0]
    if any(not isinstance(definition.get(k), str) or not definition[k] for k in ('units', 'frequency_short', 'seasonal_adjustment', 'title')):
        raise ValueError('incomplete native definition')
    if any(definition.get(k) != obs.get(k) for k in ('realtime_start', 'realtime_end')):
        raise ValueError('definition and observations must share one response vintage')
    if obs.get('units') != 'lin' or obs.get('output_type') != 1 or obs.get('offset') != 0 or obs.get('sort_order') != 'desc':
        raise ValueError('native latest-window response required')
    rows = obs.get('observations')
    if not isinstance(rows, list) or not rows: raise ValueError('no provider observations')
    # A bounded latest window is sufficient for this level comparison, not a complete history.
    if len(rows) > 32 or len(rows) != min(obs.get('count', -1), obs.get('limit', -1)):
        raise ValueError('incomplete latest window')
    seen = set()
    for row in rows:
        day = date.fromisoformat(row['date'])
        if day in seen or day > now.date(): raise ValueError('duplicate or future observation')
        seen.add(day); level(row.get('value'))
        if row.get('realtime_start') != obs.get('realtime_start') or row.get('realtime_end') != obs.get('realtime_end'):
            raise ValueError('mixed response vintages')
    if [r['date'] for r in rows] != [d.isoformat() for d in sorted(seen, reverse=True)]:
        raise ValueError('unexpected observation order')
    vintage_start, vintage_end = date.fromisoformat(obs['realtime_start']), date.fromisoformat(obs['realtime_end'])
    if vintage_start != vintage_end or vintage_end != now.date(): raise ValueError('current response vintage required')
    current, previous = rows[0], rows[1] if len(rows) > 1 else None
    value, prior = level(current.get('value')), level(previous.get('value')) if previous else None
    delta = None
    if value is not None and prior is not None:
        with localcontext() as arithmetic:
            arithmetic.prec = max(32, sum(len(v.as_tuple().digits)+abs(v.as_tuple().exponent) for v in (value, prior))+2)
            delta = value-prior
        if not math.isfinite(float(delta)): raise ValueError('comparison overflow')
    age = (now.date()-date.fromisoformat(current['date'])).days
    ceiling = AGE_CEILINGS.get(definition['frequency_short'])
    status = 'unavailable' if value is None else 'frequency_unreviewed' if ceiling is None else 'stale' if age > ceiling else 'within_age_ceiling'
    return {'contract_version': CONTRACT, 'series_id': sid, 'source': 'fred:'+sid, 'resolved_via': 'fred:'+sid,
        'source_url': 'https://fred.stlouisfed.org/series/'+sid, 'definition': definition['title'],
        'unit': definition['units'], 'source_unit': definition['units'], 'frequency': definition['frequency_short'],
        'seasonal_adjustment': definition['seasonal_adjustment'], 'value': float(value) if value is not None else None,
        'prev': float(prior) if prior is not None else None, 'chg_pct': None,
        'change': float(delta) if delta is not None else None,
        'change_unit': 'percentage_points' if definition['units'] == 'Percent' else definition['units'],
        'exact': {'value': str(value) if value is not None else None, 'previous': str(prior) if prior is not None else None,
                  'change': str(delta) if delta is not None else None},
        'asof': current['date'], 'observation_date': current['date'],
        'previous_observation_date': previous['date'] if previous else None,
        'comparison_basis': 'previous_provider_observation; gaps retained, not a daily return',
        'comparison_gap_days': (date.fromisoformat(current['date'])-date.fromisoformat(previous['date'])).days if previous else None,
        'provider_rows_in_window': len(rows), 'provider_history_count': obs['count'],
        'vintage': {'realtime_start': str(vintage_start), 'realtime_end': str(vintage_end), 'basis': 'current_response; not historical availability'},
        'published_at': None, 'provider_updated_at': definition.get('last_updated'),
        'fetched_at': collected_at, 'calculated_at': collected_at,
        'quality': {'status': status, 'observation_age_days': age, 'max_observation_age_days': ceiling,
                    'release_calendar_verified': False, 'historical_availability_verified': False},
        'status': 'LIVE' if status == 'within_age_ceiling' else 'STALE' if value is not None else 'PENDING_RESOLUTION',
        'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False}


def series_for(row, aliases):
    """Explicit curated/generated transformation wins; never turn YoY into a level."""
    alias = aliases.get(row.get('symbol'))
    if alias is not None: candidate = alias
    else:
        candidate = row.get('resolved_via') or row.get('source') or ''
        candidate = candidate.replace('fred_alias:', 'fred:')
    match = re.fullmatch(r'fred:([A-Za-z0-9_]{1,80})', candidate)
    return match[1] if match else None


def merge_observation(row, observation):
    # Preserve symbol, category, notes references and all unrelated catalog fields.
    row.update(observation)
    row['cached'] = False
    row['resolution_note'] = 'One native FRED response per series; exact source window, definition and compiler retained.'


def mark_unavailable(row, sid):
    # Keep prior values as historical evidence, never relabel a failed refresh LIVE.
    row.update(status='STALE' if row.get('value') is not None else 'PENDING_RESOLUTION',
               expected_series_id=sid, cached=True,
               calls_eligible=False, sizing_eligible=False, execution_eligible=False,
               resolution_note='Native FRED refresh unavailable; previous values and observation clocks retained.')
    row['quality'] = {'status': 'refresh_unavailable', 'release_calendar_verified': False}
    if row.get('value') is None:
        row['source'], row['resolved_via'] = 'fred:'+sid, 'fred:'+sid
