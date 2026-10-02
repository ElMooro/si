"""Retain selected Fabric diagnostics without granting learning authority.

This is not source qualification or immutable replay. Both source and caller
values survive under an explicit research namespace; legacy top-level learning
keys cannot be restored by a claimed eligibility flag or failed source read.
"""
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import math
from context_evidence_store import strict, MAX_BYTES

CONTRACT = 'fabric-logging-research-only.v1'
SOURCE = 'data/feature-bus.json'
FIELDS = {'fabric_agreement': 'agreement_pct', 'fabric_score': 'fabric_score',
          'fabric_conflict': 'conflict', 'fabric_peer': 'peer_fabric_score'}


def exact(value):
    """Preserve received diagnostic precision through the SDK's older _f2d."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError('Finite research diagnostic required')
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError('Finite research diagnostic required')
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: exact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [exact(v) for v in value]
    return deepcopy(value)


def read(client, bucket):
    body = None
    try:
        response = client.get_object(Bucket=bucket, Key=SOURCE)
        body = response['Body']; length = response.get('ContentLength')
        if type(length) is not int or not 0 <= length <= MAX_BYTES:
            raise ValueError('Whole bounded Fabric source required')
        raw = bytearray()
        while True:
            block = body.read(min(65536, MAX_BYTES + 1 - len(raw)))
            if not isinstance(block, bytes):
                raise ValueError('Byte stream required')
            if not block:
                break
            raw.extend(block)
            if len(raw) > MAX_BYTES:
                raise ValueError('Source exceeds bound')
        if len(raw) != length:
            raise ValueError('Incomplete Fabric source')
        raw = bytes(raw); packet = strict(raw, response.get('ContentEncoding', ''))
        if not isinstance(packet, dict) or not isinstance(packet.get('tickers'), dict):
            raise ValueError('Declared Fabric ticker map required')
        rows = {symbol: ({field: row[field] for field in FIELDS.values() if field in row}
                         if isinstance(row, dict) else None)
                for symbol, row in packet['tickers'].items()}
        return {'status': 'received_selected_diagnostics', 'source': SOURCE,
                'source_sha256': hashlib.sha256(raw).hexdigest(), 'source_bytes': len(raw),
                'received_at': datetime.now(timezone.utc).isoformat(),
                'source_publication_clock': packet.get('generated_at'),
                'source_contract': packet.get('measurement_contract'),
                'selected_fields': list(FIELDS.values()), 'rows': rows,
                'whole_record_retained': False, 'original_bytes_retained': False,
                'source_replay_performed': False, 'source_freshness_qualified': False}
    except Exception:
        return {'status': 'source_unavailable_or_invalid', 'source': SOURCE, 'rows': {},
                'source_freshness_qualified': False}
    finally:
        if body is not None:
            try:
                body.close()
            except Exception:
                pass


def select(snapshot, symbol):
    if not isinstance(snapshot, dict):
        snapshot = {'status': 'source_unavailable_or_invalid', 'source': SOURCE,
                    'rows': {}, 'source_freshness_qualified': False}
    rows = snapshot.get('rows')
    symbol = str(symbol).upper()
    values = rows.get(symbol) if isinstance(rows, dict) else None
    present = isinstance(rows, dict) and symbol in rows
    return {**{k: deepcopy(v) for k, v in snapshot.items() if k != 'rows'},
            'ticker': symbol, 'row_present': present,
            'row_status': ('source_unavailable' if snapshot.get('status') != 'received_selected_diagnostics'
                           else 'not_reported' if not present else 'invalid_record' if not isinstance(values, dict)
                           else 'selected_diagnostics'),
            'pointer': '/tickers/' + symbol.replace('~', '~0').replace('/', '~1'),
            'selected_values': exact(values)}


def separate(metadata, received):
    if not isinstance(metadata, dict):
        raise ValueError('Metadata object required')
    out = deepcopy(metadata)
    caller = {name: exact(out.pop(name)) for name in FIELDS if name in out}
    context = {'contract': CONTRACT, 'learning_weight_eligible': False,
               'calls_eligible': False, 'sizing_eligible': False, 'forecast_qualified': False,
               'reason': 'Fabric adapters and legacy weights lack qualified independent forward edge.',
               'received': exact(received), 'caller_legacy_fields': caller}
    if 'fabric_research' in out:
        context['caller_supplied_research'] = exact(out.pop('fabric_research'))
    out['fabric_research'] = context
    return out
