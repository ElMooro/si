"""Occurrence-aware Compound arithmetic; a research score is not an edge estimate.

Input selection is the producer's declared collection, not the full investable
universe. Records and their JSON pointers explain this arithmetic only. They do
not establish source freshness, independent evidence or point-in-time replay.
"""
from collections import Counter
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import json
import math
import hashlib
from context_evidence_store import strict, MAX_BYTES

CONTRACT = 'compound-numeric.v1'


class InvalidNumber(ValueError):
    pass


def number(value):
    if type(value) not in (int, float, str):
        raise InvalidNumber('finite_number_required')
    if isinstance(value, str) and (not value.strip() or len(value) > 512):
        raise InvalidNumber('finite_number_required')
    try:
        exact = Decimal(str(value).strip())
        result = float(value)
    except (ValueError, OverflowError, InvalidOperation):
        raise InvalidNumber('finite_number_required') from None
    if not exact.is_finite() or not math.isfinite(result):
        raise InvalidNumber('finite_number_required')
    if Decimal(str(result)) != exact:
        raise InvalidNumber('lossy_numeric_projection')
    return result


def symbol(value):
    if not isinstance(value, str) or not value.strip():
        return None
    candidate = value.strip().upper()
    if len(candidate) > 128 or any(ch.isspace() or ord(ch) < 32 for ch in candidate):
        return None
    return candidate


class FeedRows(list):
    def __init__(self, rows=(), *, evidence):
        super().__init__(rows)
        self.evidence = evidence


def unavailable(key, path, reason):
    return FeedRows(evidence={'source': key, 'collection': path,
        'status': 'unavailable', 'reason': reason, 'occurrences': [],
        'selected_count': None, 'usable_count': 0,
        'selection_scope': 'declared_collection_only', 'source_qualified': False})


def read_feed(client, bucket, key, path, symbol_field):
    """Read a whole bounded S3 object; failure is unavailable, never an empty feed.

    Hashes identify received bytes but are explicitly not durable originals.
    No private output, account source, provider or discovery call is introduced.
    """
    body = None
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response['Body']
        length = response.get('ContentLength')
        if type(length) is not int or not 0 <= length <= MAX_BYTES:
            raise ValueError('whole_bounded_length_required')
        data = bytearray()
        while True:
            block = body.read(min(65536, MAX_BYTES + 1 - len(data)))
            if not isinstance(block, bytes):
                raise ValueError('byte_stream_required')
            if not block:
                break
            data.extend(block)
            if len(data) > MAX_BYTES:
                raise ValueError('source_exceeds_bound')
        raw = bytes(data)
        if len(raw) != length:
            raise ValueError('whole_source_required')
        packet = strict(raw, response.get('ContentEncoding', ''))
        if not isinstance(packet, dict):
            raise ValueError('object_root_required')
        rows = select(packet, key, path, symbol_field)
        rows.evidence.update(source_sha256=hashlib.sha256(raw).hexdigest(),
            source_bytes=len(raw), whole_source_parsed=True,
            original_bytes_retained=False,
            reported_generated_at=packet.get('generated_at'),
            reported_as_of=packet.get('as_of'))
        return rows
    except Exception:
        # Never include service error messages or fragments of rejected packets.
        return unavailable(key, path, 'source_read_or_validation_failed')
    finally:
        if body is not None:
            try:
                body.close()
            except Exception:
                pass


def select(packet, key, path, symbol_field):
    """Retain every selected occurrence before resolving duplicates or numbers."""
    cursor = packet
    for field in path.split('.'):
        if not isinstance(cursor, dict) or field not in cursor:
            return unavailable(key, path, 'collection_missing')
        cursor = cursor[field]
    if not isinstance(cursor, list):
        return unavailable(key, path, 'collection_not_array')
    records = deepcopy(cursor)
    # The acquisition parser rejects nonfinite JSON before this function. A
    # direct caller must not bypass that boundary with Python-only values.
    try:
        json.dumps(records, allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        return unavailable(key, path, 'collection_not_json_safe')
    names = [symbol(r.get(symbol_field)) if isinstance(r, dict) else None for r in records]
    counts = Counter(s for s in names if s is not None)
    occurrences, rows = [], []
    root = '/' + '/'.join(p.replace('~', '~0').replace('/', '~1') for p in path.split('.'))
    for i, (record, name) in enumerate(zip(records, names)):
        entry = {'pointer': root + '/' + str(i), 'symbol': name,
                 'record': record, 'status': 'withheld'}
        occurrences.append(entry)
        if not isinstance(record, dict):
            entry['reason'] = 'record_not_object'
            continue
        if name is None:
            entry['reason'] = 'symbol_unavailable'
            continue
        if counts[name] > 1:
            entry['reason'] = 'duplicate_source_symbol'
            continue
        field = 'score' if 'score' in record else 'asymmetric_score' if 'asymmetric_score' in record else None
        entry['score_field'] = field
        if field is None:
            entry['reason'] = 'score_missing'
            continue
        try:
            value = number(record[field])
        except InvalidNumber as exc:
            entry['reason'] = str(exc)
            continue
        entry.update(status='usable', score=value)
        row = dict(record)
        row.update(_normalized_symbol=name, _compound_score=value,
                   _compound_input={'source': key, 'pointer': entry['pointer'],
                                    'score_field': field, 'record': record})
        rows.append(row)
    status = 'empty' if not records else 'usable' if len(rows) == len(records) else 'partial'
    return FeedRows(rows, evidence={'source': key, 'collection': path,
        'status': status, 'occurrences': occurrences,
        'selected_count': len(records), 'usable_count': len(rows),
        'selection_scope': 'declared_collection_only', 'source_qualified': False})


def calculate(scores, inputs):
    out = {'contract': CONTRACT, 'status': 'withheld', 'score': None,
           'formula': 'round(sum(scores) * (1 + 0.5 * (n_systems - 1)), 1)',
           'components': [], 'reason': None, 'independence_qualified': False,
           'forecast_qualified': False, 'portfolio_qualified': False}
    if not isinstance(scores, dict) or len(scores) < 2:
        out['reason'] = 'at_least_two_scored_systems_required'
        return out
    total = 0.0
    try:
        # Preserve the legacy FEEDS insertion order and arithmetic, including
        # zero-valued components in the convergence multiplier.
        for system, raw in scores.items():
            value = number(raw)
            component = {'system': system, 'score': value, 'input': inputs[system]}
            out['components'].append(component)
            total += value
            if not math.isfinite(total):
                raise InvalidNumber('sum_overflow')
        multiplier = 1 + 0.5 * (len(scores) - 1)
        result = total * multiplier
        if not math.isfinite(result):
            raise InvalidNumber('product_overflow')
        if total != 0 and result == 0:
            raise InvalidNumber('product_underflow')
        out.update(status='usable', score=round(result, 1), sum_scores=total,
                   n_systems=len(scores), multiplier=multiplier,
                   arithmetic_order=list(scores))
    except (InvalidNumber, KeyError) as exc:
        out['reason'] = str(exc) if isinstance(exc, InvalidNumber) else 'input_reference_missing'
    return out
