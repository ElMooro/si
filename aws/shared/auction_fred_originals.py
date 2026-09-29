"""Replay the existing seventeen auction FRED inputs, without provider requests.

Every accepted response row survives in its parsed frame. Selection excludes
missing values explicitly; duplicate/future dates invalidate a history instead
of silently overwriting it. This is current-vintage descriptive source evidence,
not a historical first-release dataset or validation of the legacy risk model.
"""
from copy import deepcopy
from datetime import date
import hashlib
import json
import math
from urllib.parse import urlencode
import auction_benchmarks as benchmarks
import auction_cross_observations as cross
from auction_calendar import clock, strict_json

CONTRACT = 'auction-fred-originals.v1'
PREFIX = 'data/auction-fred-originals/'
MAX_BODY = 2 * 1024 * 1024
BENCHMARKS = tuple(sid for _, sid in benchmarks.TENORS)
LIMITS = {'DFF': 5, **cross.LIMITS, **dict.fromkeys(BENCHMARKS, 90)}
PERMISSIONS = dict.fromkeys(('source_definition_verified', 'historical_point_in_time_verified',
                            'forecast_eligible', 'calls_eligible', 'sizing_eligible', 'execution_eligible'), False)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def public_url(sid, limit):
    if LIMITS.get(sid) != limit or type(limit) is not int:
        raise ValueError('Reviewed FRED request required')
    return 'https://api.stlouisfed.org/fred/series/observations?' + urlencode([
        ('series_id', sid), ('file_type', 'json'), ('limit', limit), ('sort_order', 'desc')])


def verify_transport(raw, transport, sid, limit, read_at):
    keys = {'contract', 'source_url', 'request_started_at', 'response_received_at', 'response_status',
            'declared_content_length', 'body_bytes', 'body_sha256', 'cache_status', 'cache_age_seconds', 'cache_ttl_seconds'}
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BODY:
        raise ValueError('Bounded complete FRED body required')
    if (not isinstance(transport, dict) or set(transport) != keys or transport['contract'] != 'auction-fred-transport.v1'
        or transport['source_url'] != public_url(sid, limit) or type(transport['response_status']) is not int
        or transport['response_status'] != 200 or type(transport['body_bytes']) is not int
        or transport['body_bytes'] != len(raw) or transport['body_sha256'] != sha(raw)
        or transport['cache_status'] not in ('network', 'fresh_cache') or type(transport['cache_ttl_seconds']) is not int
        or transport['cache_ttl_seconds'] != 1800 or type(transport['cache_age_seconds']) not in (int, float)
        or not math.isfinite(transport['cache_age_seconds']) or not 0 <= transport['cache_age_seconds'] < 1800):
        raise ValueError('FRED transport does not bind the exact original')
    declared = transport['declared_content_length']
    if declared is not None and (type(declared) is not int or declared != len(raw)):
        raise ValueError('FRED declared size differs')
    received, read = clock(transport['response_received_at']), clock(read_at)
    age = (read - received).total_seconds()
    if not clock(transport['request_started_at']) <= received <= read or not 0 <= age < 1800:
        raise ValueError('FRED acquisition clock differs')
    if abs(age - transport['cache_age_seconds']) > 5:
        raise ValueError('FRED cache age differs')


def observations(frame, sid, today):
    out = {'series_id': sid, 'unit': cross.UNITS.get(sid, 'percent'), 'status': 'unavailable',
           'reason': 'source_unavailable', 'rows': [], 'excluded_rows': [], 'selected_history': {}}
    if not isinstance(frame, dict) or frame.get('read_status') != 'received':
        return out
    if frame.get('series_id') != sid or type(frame.get('requested_limit')) is not int or frame['requested_limit'] != LIMITS[sid]:
        return {**out, 'reason': 'request_identity_differs'}
    response = frame.get('response')
    raw = response.get('observations') if isinstance(response, dict) else None
    if not isinstance(raw, list) or len(raw) > LIMITS[sid]:
        return {**out, 'reason': 'observation_array_or_limit_invalid'}
    seen = set(); invalid = False
    for index, row in enumerate(raw):
        at = benchmarks.day(row.get('date')) if isinstance(row, dict) else None
        value = benchmarks.number(row.get('value')) if isinstance(row, dict) else None
        reason = ('invalid_future_or_duplicate_date' if at is None or at > today or at in seen else
                  'missing_or_nonfinite_value' if value is None else None)
        if reason == 'invalid_future_or_duplicate_date': invalid = True
        if at is not None: seen.add(at)
        measured = {'source_row_index': index, 'date': at.isoformat() if at else None,
                    'value': value, 'exclusion_reason': reason}
        out['rows'].append(measured)
        if reason: out['excluded_rows'].append(index)
    if invalid:
        return {**out, 'reason': 'invalid_future_or_duplicate_date'}
    out['selected_history'] = {r['date']: r['value'] for r in out['rows'] if r['exclusion_reason'] is None}
    return {**out, 'status': 'complete' if raw and not out['excluded_rows'] else 'partial' if raw else 'unavailable',
            'reason': 'missing_rows_excluded' if out['excluded_rows'] else None if raw else 'no_observations'}


def policy_rate(frame, today):
    source = observations(frame, 'DFF', today)
    out = {'series_id': 'DFF', 'unit': 'percent', 'value': None, 'observation_date': None,
           'source_row_index': None, 'status': 'unavailable', 'reason': source['reason'],
           'maximum_age_calendar_days': 5, 'selection': 'latest_reported_date_without_missing_value_fallback', **PERMISSIONS}
    if source['reason'] in ('invalid_future_or_duplicate_date', 'source_unavailable', 'request_identity_differs', 'observation_array_or_limit_invalid') or not source['rows']:
        return out
    latest = max(source['rows'], key=lambda r: r['date'])
    out.update(observation_date=latest['date'], source_row_index=latest['source_row_index'])
    if (today - benchmarks.day(latest['date'])).days > 5:
        return {**out, 'reason': 'latest_observation_too_old'}
    if latest['value'] is None:
        return {**out, 'reason': 'latest_observation_missing'}
    return {**out, 'value': latest['value'], 'status': 'complete', 'reason': None}


def build(records, calculation_as_of, generated_at):
    today = benchmarks.day(calculation_as_of)
    if today is None or not isinstance(records, dict) or not set(records) <= set(LIMITS):
        raise ValueError('Reviewed dated source records required')
    if today > clock(generated_at).date():
        raise ValueError('Calculation cannot be future dated')
    frames = {}; received = []; unavailable = []; not_requested = []
    for sid, limit in LIMITS.items():
        record = records.get(sid)
        if record is None:
            not_requested.append(sid); continue
        if not isinstance(record, dict) or set(record) != {'frame', 'raw'}:
            raise ValueError('Complete effective-response record required')
        frame, raw = record['frame'], record['raw']
        if not isinstance(frame, dict) or frame.get('series_id') != sid or type(frame.get('requested_limit')) is not int or frame['requested_limit'] != limit:
            raise ValueError('Source frame identity differs')
        allowed = {'series_id', 'requested_limit', 'read_status', 'response', 'adapter_read_at',
                   'http_acquisition_time_verified', 'adapter_transport', 'error', 'transport_error'}
        if not set(frame) <= allowed or frame.get('http_acquisition_time_verified', False) is not False:
            raise ValueError('Unexpected source frame authority or fields')
        if frame.get('read_status') == 'received':
            verify_transport(raw, frame.get('adapter_transport'), sid, limit, frame['adapter_read_at'])
            if clock(frame['adapter_read_at']) > clock(generated_at) or encoded(strict_json(raw)) != encoded(frame['response']):
                raise ValueError('Original response and parsed frame differ')
            if not isinstance(frame['response'], dict) or not isinstance(frame['response'].get('observations'), list):
                raise ValueError('Original observations absent')
            received.append(sid)
        elif frame.get('read_status') == 'unavailable' and raw is None and frame.get('response') is None:
            unavailable.append(sid)
        else:
            raise ValueError('Invalid source availability state')
        frames[sid] = deepcopy(frame)
    measured = {sid: observations(frames.get(sid), sid, today) for sid in LIMITS}
    cross_output = cross.build({sid: frames[sid] for sid in cross.LIMITS if sid in frames}, today)
    return {'contract': CONTRACT, 'generated_at': generated_at, 'calculation_as_of': calculation_as_of,
            'source_frames': frames, 'observations': measured,
            'fed_funds': policy_rate(frames.get('DFF'), today),
            'benchmark_histories': {sid: measured[sid]['selected_history'] for sid in BENCHMARKS},
            'cross_signals': cross_output,
            'measurement_coverage': {
                'series_status': {sid: measured[sid]['status'] for sid in LIMITS},
                'cross_status': {key: cross_output[key]['measurement_status'] for key in
                                 ('repo_stress', 'dollar_strength', 'curve_slope', 'inflation_expectations')},
                'note': 'Complete source capture does not imply complete or usable measurements.'},
            'coverage': {'expected_series': list(LIMITS), 'received_series': received, 'unavailable_series': unavailable,
                         'not_requested_series': not_requested, 'complete_request_set': len(received) == len(LIMITS),
                         'raw_rows': sum(len(f['response']['observations']) for f in frames.values() if f['read_status'] == 'received'),
                         'selected_benchmark_rows': sum(len(measured[sid]['selected_history']) for sid in BENCHMARKS),
                         'scope': 'Effective accepted responses for the existing seventeen requests, including warm cache reuse; not every HTTP retry or the complete provider history.'},
            **PERMISSIONS}
