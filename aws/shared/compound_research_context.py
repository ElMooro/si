"""Compound is derived research context, never an independent directional vote.

Retain the complete received parsed packet and every row occurrence. Byte hashes
identify this read; they do not establish a retained original or source vintage.
"""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from context_evidence_store import strict, MAX_BYTES

BASIS = 'compound-context-without-direction.v1'
SOURCE = 'data/compound-signals.json'


def project(packet, receipt=None):
    out = {'basis': BASIS, 'source': SOURCE, 'status': 'unavailable',
        'investment_votes': 0, 'calls_eligible': False, 'sizing_eligible': False,
        'forecast_qualified': False, 'independent_evidence_eligible': False,
        'original_bytes_retained': False, 'original_source_replay_performed': False,
        'source_qualified': False, 'packet': deepcopy(packet), 'occurrences': [],
        'receipt': deepcopy(receipt),
        'reason': 'A count or heuristic composite score cannot establish direction, independence or a probability.'}
    if not isinstance(packet, dict):
        return out
    key = next((k for k in ('compound', 'ranked') if k in packet), None)
    rows = packet.get(key) if key is not None else None
    out['collection'] = key
    if not isinstance(rows, list):
        out['status'] = 'collection_unavailable'
        return out
    out['status'] = 'received_research_context'
    for i, row in enumerate(rows):
        names = [row[k] for k in ('ticker', 'symbol') if k in row] if isinstance(row, dict) else []
        valid = bool(names) and all(isinstance(v, str) and v.strip() for v in names)
        names = [v.strip().upper() for v in names] if valid else []
        name = names[0] if names and len(set(names)) == 1 else None
        out['occurrences'].append({'pointer': '/' + key + '/' + str(i), 'symbol': name,
            'record': deepcopy(row), 'vote_eligible': False})
    out['selected_count'] = len(rows)
    return out


def read(client, bucket):
    body = None
    receipt = {'source': SOURCE, 'status': 'unavailable'}
    try:
        response = client.get_object(Bucket=bucket, Key=SOURCE)
        body = response['Body']
        length = response.get('ContentLength')
        if type(length) is not int or not 0 <= length <= MAX_BYTES:
            raise ValueError('Whole bounded length required')
        raw = bytearray()
        while True:
            block = body.read(min(65536, MAX_BYTES + 1 - len(raw)))
            if not isinstance(block, bytes):
                raise ValueError('Byte stream required')
            if not block:
                break
            raw.extend(block)
            if len(raw) > MAX_BYTES:
                raise ValueError('Context exceeds bound')
        if len(raw) != length:
            raise ValueError('Whole source required')
        raw = bytes(raw)
        packet = strict(raw, response.get('ContentEncoding', ''))
        if not isinstance(packet, dict):
            raise ValueError('Object packet required')
        receipt.update(status='received', source_sha256=hashlib.sha256(raw).hexdigest(),
            source_bytes=len(raw), received_at=datetime.now(timezone.utc).isoformat(),
            whole_source_parsed=True)
        return project(packet, receipt)
    except Exception:
        receipt['reason'] = 'compound_context_read_or_validation_failed'
        return project(None, receipt)
    finally:
        if body is not None:
            try:
                body.close()
            except Exception:
                pass


def pointers(context, ticker):
    return [r['pointer'] for r in context['occurrences'] if r['symbol'] == ticker]
