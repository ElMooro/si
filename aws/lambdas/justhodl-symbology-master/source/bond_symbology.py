"""Optional bond identity enrichment with explicit outcomes and conditional writes.

This module does not turn mappings into trade or sizing authority. Whole prior
rows and provider candidates remain inspectable. Legacy claims are unqualified
until a new structured result exists. Normal scheduling is unchanged.
"""
from datetime import datetime, timezone, timedelta
import json
import math
import re
import time

QUEUE_KEY = 'data/_state/bond-cusip-queue.json'
MASTER_KEY = 'data/symbology/bond-cusips.json'
SCHEMA = 'openfigi-resolution.v1'
_MAX_JSON_BYTES = 16 * 1024 * 1024


def _pairs(rows):
    out = {}
    for key, value in rows:
        if key in out:
            raise ValueError('Duplicate JSON field')
        out[key] = value
    return out


def _reject(value):
    raise ValueError('Nonfinite JSON number')


def _read(client, bucket, key, *, max_bytes=_MAX_JSON_BYTES):
    if type(max_bytes) is not int or not 1 <= max_bytes <= 64 * 1024 * 1024:
        raise ValueError("Positive bounded whole-object limit required")
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('NoSuchKey', '404'):
            return None, None
        raise
    stream = response['Body']
    try:
        length = response.get('ContentLength')
        if type(length) is not int or not 0 <= length <= max_bytes:
            raise ValueError('Complete bounded storage length required')
        chunks = []
        total = 0
        while True:
            chunk = stream.read(min(65536, max_bytes + 1 - total))
            if not isinstance(chunk, bytes):
                raise ValueError('Binary storage body required')
            if not chunk:
                break
            total += len(chunk)
            if total > length:
                raise ValueError('Storage body exceeds declared length')
            chunks.append(chunk)
        if total != length:
            raise ValueError('Storage body incomplete')
        etag = response.get('ETag')
        if not isinstance(etag, str) or not etag:
            raise ValueError('Version identity required')
        value = json.loads(b''.join(chunks), object_pairs_hook=_pairs, parse_constant=_reject)
        return value, etag
    finally:
        stream.close()


def _clock(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return result.astimezone(timezone.utc) if result.tzinfo else None


def _valid_result(result, cusip):
    if (not isinstance(result, dict) or result.get('schema') != SCHEMA
            or result.get('query') != {'idType': 'ID_CUSIP', 'idValue': cusip}):
        return False
    response = result.get('response')
    if not isinstance(response, dict):
        return False
    kinds = [k for k in ('data', 'warning', 'error') if k in response]
    if len(kinds) != 1:
        return False
    if kinds == ['warning']:
        return result.get('status') == 'no_match' and isinstance(response['warning'], str) and bool(response['warning'].strip())
    if kinds == ['error']:
        return result.get('status') == 'error'
    rows = response['data']
    if not isinstance(rows, list) or not rows or any(not isinstance(r, dict) or not isinstance(r.get('figi'), str) or not re.fullmatch(r'BBG[A-Z0-9]{9}', r['figi']) for r in rows):
        return result.get('status') == 'error'
    if len(rows) > 1:
        return result.get('status') == 'ambiguous' and 'security' not in result
    return result.get('status') == 'resolved' and result.get('security') == rows[0]


def _qualified(row, now, cusip):
    if not isinstance(row, dict):
        return False
    result = row.get('resolution')
    if not _valid_result(result, cusip):
        return False
    stamp = _clock(row.get('last_attempt_at'))
    if stamp is None or stamp > now:
        return False
    if result.get('status') == 'resolved':
        return row.get('figi') == result['security']['figi']
    return result.get('status') == 'no_match' and now - stamp < timedelta(days=7)


def _remaining(deadline):
    return max(0, deadline - time.monotonic())


def enrich(client, bucket, resolver, *, deadline, limit=100, now=None):
    """Read whole state, try eligible IDs within a budget, write once with CAS.

    Failure stats contain stable reason codes, not provider exception messages.
    A failed prior read/schema check never becomes an empty master. A concurrent
    update never receives an unconditional retry. Other fields are carried on.
    """
    stats = {'resolved': 0, 'no_match': 0, 'ambiguous': 0, 'errors': 0,
             'remaining': None, 'attempted': 0, 'published': False, 'status': 'deferred'}
    if (type(limit) is not int or not 1 <= limit <= 100
            or type(deadline) not in (int, float) or not math.isfinite(deadline)):
        return {**stats, 'errors': 1, 'reason': 'invalid_work_budget'}
    now = now or datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None:
        return {**stats, 'errors': 1, 'reason': 'invalid_clock'}
    if _remaining(deadline) < 10:
        return {**stats, 'reason': 'insufficient_time'}
    try:
        queue_raw, queue_etag = _read(client, bucket, QUEUE_KEY)
    except Exception:
        return {**stats, 'errors': 1, 'reason': 'queue_read_failed'}
    if queue_raw is None and queue_etag is None:
        return {**stats, 'remaining': 0, 'reason': 'queue_missing'}
    if not isinstance(queue_raw, list):
        return {**stats, 'errors': 1, 'reason': 'queue_schema_invalid'}
    queue, seen = [], set()
    invalid = 0
    for value in queue_raw:
        normalized = value.strip().upper() if isinstance(value, str) else ''
        if not re.fullmatch(r'[A-Z0-9*@#]{8}[0-9]', normalized):
            invalid += 1
        elif normalized not in seen:
            seen.add(normalized)
            queue.append(normalized)
    stats.update(queue_rows=len(queue_raw), invalid_queue_rows=invalid,
                 distinct_valid_ids=len(queue), queue_etag=queue_etag)
    try:
        prior, etag = _read(client, bucket, MASTER_KEY)
        if prior is None and etag is None:
            prior = {'schema_version': '1.0', 'by_cusip': {}}
        if not isinstance(prior, dict) or not isinstance(prior.get('by_cusip'), dict):
            raise ValueError('Whole prior mapping required')
        if any(not isinstance(v, dict) for v in prior['by_cusip'].values()):
            raise ValueError('Whole prior rows required')
    except Exception:
        return {**stats, 'errors': 1, 'reason': 'prior_read_or_schema_failed'}
    by_cusip = dict(prior['by_cusip'])
    todo = [c for c in queue if not _qualified(by_cusip.get(c), now, c)]
    # Never attempted entries first, then oldest failed/ambiguous attempts. A
    # permanently failing head cannot starve later queued identities forever.
    todo.sort(key=lambda c: _clock(by_cusip.get(c, {}).get('last_attempt_at')) or datetime.min.replace(tzinfo=timezone.utc))
    stats['remaining'] = len(todo)
    for cusip in todo[:limit]:
        if _remaining(deadline) < 10:
            stats['reason'] = 'time_budget_reached'
            break
        try:
            result = resolver(cusip, deadline=deadline - 5)
            if not _valid_result(result, cusip):
                raise ValueError('Typed complete identified resolution required')
        except Exception:
            result = {'schema': SCHEMA, 'status': 'error', 'reason': 'resolver_failed',
                      'query': {'idType': 'ID_CUSIP', 'idValue': cusip}}
        old = by_cusip.get(cusip, {})
        value = dict(old)
        if old and 'legacy_record' not in old and not isinstance(old.get('resolution'), dict):
            value['legacy_record'] = dict(old)
        value.update(resolution=result, last_attempt_at=now.isoformat(), no_match=result['status'] == 'no_match',
                     resolution_eligible=result['status'] == 'resolved')
        if result['status'] == 'resolved':
            row = result['security']
            value.update(ticker=row.get('ticker'), figi=row['figi'], name=row.get('name'),
                         security_type=row.get('securityType'), market_sector=row.get('marketSector'),
                         coupon=row.get('coupon'), maturity=row.get('maturity'))
        by_cusip[cusip] = value
        stats['attempted'] += 1
        stats['errors' if result['status'] == 'error' else result['status']] += 1
    stats['remaining'] = sum(not _qualified(by_cusip.get(c), now, c) for c in queue)
    if not stats['attempted']:
        return {**stats, 'status': 'unchanged', 'reason': stats.get('reason', 'no_pending_ids')}
    if _remaining(deadline) <= 0:
        return {**stats, 'reason': 'publication_deadline_reached'}
    doc = {**prior, 'generated_at': now.isoformat(), 'n_cusips': len(by_cusip), 'by_cusip': by_cusip,
           'resolution_contract': SCHEMA,
           'resolution_quality': {'legacy_rows': sum(not isinstance(v.get('resolution'), dict) for v in by_cusip.values()),
                                  'remaining': stats['remaining'], 'invalid_queue_rows': invalid,
                                  'investment_authority': False}}
    try:
        client.put_object(Bucket=bucket, Key=MASTER_KEY, Body=json.dumps(doc, allow_nan=False).encode('utf-8'),
                          ContentType='application/json', CacheControl='no-cache',
                          **({'IfMatch': etag} if etag is not None else {'IfNoneMatch': '*'}))
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code'))
        return {**stats, 'status': 'not_published',
                'reason': 'concurrent_update' if code in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409') else 'write_failed'}
    return {**stats, 'published': True, 'status': 'published', 'n_cusips': len(by_cusip)}
