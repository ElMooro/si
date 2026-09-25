"""Verify exact retained normalized statement responses, without acquisition.

Original vendor responses remain protected. Dates and CIKs in those responses
are provider assertions, not independently verified SEC filing histories.
"""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib, json, re
import statement_measurements as measurements

PRIVATE = 'audit-private/20260909-originals/financial-statement-research/'
MAX = 32 * 1024 * 1024
ENDPOINTS = measurements.ENDPOINTS


def sha(body):
    return hashlib.sha256(body).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def strict(body):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate original JSON key')
            out[key] = value
        return out
    def bad(_):
        raise ValueError('Nonfinite original JSON constant')
    return json.loads(body, parse_float=Decimal, parse_constant=bad, object_pairs_hook=pairs)


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Explicit source acquisition clock required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Source acquisition timezone required')
    return stamp.astimezone(timezone.utc)


def original(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref.get('sha256'), str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != PRIVATE + ref['sha256'] + '.bin'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX):
        raise ValueError('Complete protected original identity required')
    body = read(ref['key'])
    if not isinstance(body, bytes) or len(body) != ref['bytes'] or sha(body) != ref['sha256']:
        raise ValueError('Retained original bytes differ')
    return body


def spec(symbol, endpoint, period):
    if (not isinstance(symbol, str) or not re.fullmatch('[A-Z0-9][A-Z0-9.-]{0,15}', symbol)
            or endpoint not in ENDPOINTS or period not in ('annual', 'quarter')):
        raise ValueError('Explicit bounded statement request required')
    limit = 5 if period == 'annual' else 9
    return {'symbol': symbol, 'endpoint': endpoint, 'period': period, 'limit': limit,
        'url': 'https://financialmodelingprep.com/stable/' + endpoint + '?symbol=' + symbol + '&period=' + period + '&limit=' + str(limit)}


def plan(ref, read):
    manifest = strict(original(ref, read))
    if (not isinstance(manifest, dict) or manifest.get('contract') != 'financial-statement-complete-source-campaign.v1'
            or manifest.get('status') != 'complete'):
        raise ValueError('Completed whole source campaign required')
    labels = manifest.get('reported_symbols')
    if (not isinstance(labels, list) or not 1 <= len(labels) <= 600
            or not all(isinstance(v, str) for v in labels) or labels != sorted(set(labels))):
        raise ValueError('Exact unique retained universe required')
    # Membership is reconstructed from the original universe, never a caller's
    # selection or a convenient subset of successfully calculated names.
    universe = strict(original(manifest['universe'], read))
    if isinstance(universe, dict):
        choices = [v for k, v in universe.items() if k in ('rows', 'stocks', 'data') and isinstance(v, list)]
        if len(choices) != 1:
            raise ValueError('Unambiguous complete universe records required')
        universe = choices[0]
    if not isinstance(universe, list) or not all(isinstance(row, dict) for row in universe):
        raise ValueError('Complete original universe records required')
    names = [row.get('symbol') or row.get('ticker') for row in universe]
    if len(names) != len(labels) or set(names) != set(labels):
        raise ValueError('Source population differs from retained universe')
    specs = [spec(label, endpoint, period) for label in labels for period in ('annual', 'quarter') for endpoint in ENDPOINTS]
    if (set(manifest.get('captures', {})) != {v['url'] for v in specs}
            or manifest.get('planned_sources') != len(specs)
            or manifest.get('counts', {}).get('complete_sources') != len(specs)):
        raise ValueError('All six exact responses per requested name required')
    if clock(manifest['generated_at']) > clock(manifest['completed_at']):
        raise ValueError('Campaign chronology differs')
    return manifest, specs


def response(ref, request, completed_at, read):
    capsule = strict(original(ref, read))
    if (not isinstance(capsule, dict) or capsule.get('spec') != request
            or capsule.get('http_status') != 200 or capsule.get('status') != 'response_retained'):
        raise ValueError('Exact successful retained response required')
    if not clock(capsule['requested_at']) <= clock(capsule['received_at']) <= clock(completed_at):
        raise ValueError('Statement response acquisition chronology differs')
    body = original(capsule['original'], read)
    rows = strict(body)
    if not isinstance(rows, list) or len(rows) > request['limit'] or not all(isinstance(row, dict) for row in rows):
        raise ValueError('Whole bounded provider row array required')
    headers = capsule.get('headers')
    if not isinstance(headers, dict):
        raise ValueError('Retained response headers required')
    if headers.get('content-length') is not None and headers['content-length'] != str(len(body)):
        raise ValueError('Provider HTTP length differs from retained body')
    return capsule, rows
