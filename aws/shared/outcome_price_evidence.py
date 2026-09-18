"""Verify price marks against retained original provider responses.

A digest-shaped string is not evidence. This adapter reads a content-addressed
source object and independently selects the identified daily close. It certifies
the price measurement only, never a fill, forecast, or out-of-sample protocol.
"""
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
import hashlib
import gzip
import io
import json
import math
import re
from urllib.parse import unquote, urlsplit
from zoneinfo import ZoneInfo

from evidence_store import CONTRACT as SOURCE_CONTRACT, public_source_url
from instrument_identity import resolve_instrument

CONTRACT = 'provider-price-replay.v1'
EASTERN = ZoneInfo('America/New_York')


def finite(value):
    if isinstance(value, bool): return None
    try:
        n=float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError, OverflowError): return None


def stamp(value):
    try:
        dt=datetime.fromisoformat(str(value).replace('Z','+00:00'))
        return dt.astimezone(timezone.utc) if dt.tzinfo else None
    except (ValueError, TypeError): return None


def validate_daily_mark(mark, receipt, raw):
    """Supported evidence parser: Polygon US cash-security daily aggregates."""
    if mark.get('evidence_parser') != 'polygon-us-daily-close.v1':
        return ['unsupported_price_evidence_parser']
    identity=resolve_instrument(mark.get('symbol'), 'equity')
    if not identity or identity['instrument_id'] != mark.get('instrument_id') or mark.get('currency') != 'USD':
        return ['price_evidence_instrument_mismatch']
    source=public_source_url(receipt.get('source_url',''))
    parsed=urlsplit(source)
    match=re.fullmatch(r'/v2/aggs/ticker/([^/]+)/range/1/day/(\d{4}-\d{2}-\d{2})/(\d{4}-\d{2}-\d{2})', parsed.path)
    if parsed.hostname != 'api.polygon.io' or not match or unquote(match[1]) != identity['provider_symbols']['polygon']:
        return ['price_evidence_request_identity_mismatch']
    if receipt.get('provider') != 'polygon' or mark.get('provider') != 'polygon':
        return ['price_evidence_provider_mismatch']
    doc=json.loads(raw)
    if not isinstance(doc,dict) or doc.get('ticker') != identity['symbol'] or doc.get('adjusted') is not True:
        return ['price_evidence_response_identity_or_adjustment_mismatch']
    if doc.get('status') not in ('OK','DELAYED') or doc.get('next_url') or not isinstance(doc.get('results'),list):
        return ['price_evidence_incomplete_response']
    selected=finite(mark.get('bar_timestamp_ms'))
    rows=[r for r in doc['results'] if isinstance(r,dict) and finite(r.get('t')) == selected]
    if selected is None or len(rows) != 1:
        return ['price_evidence_missing_or_duplicate_bar']
    start=datetime.fromtimestamp(selected/1000, timezone.utc).astimezone(EASTERN)
    if start.hour or start.minute or start.second or start.microsecond or start.weekday() >= 5:
        return ['price_evidence_invalid_daily_window']
    period_end=(start+timedelta(days=1)).astimezone(timezone.utc)
    observed=stamp(mark.get('observed_at'))
    received=stamp(receipt.get('first_received_at'))
    if mark.get('observation_time_basis') != 'aggregate_period_end_not_trade_time' or observed != period_end:
        return ['price_evidence_observation_clock_mismatch']
    if received is None or received < period_end:
        return ['price_evidence_period_not_complete_at_capture']
    if not match[2] <= start.date().isoformat() <= match[3]:
        return ['price_evidence_bar_outside_request']
    if mark.get('as_of') != start.date().isoformat():
        return ['price_evidence_session_mismatch']
    price=finite(rows[0].get('c'))
    claimed=finite(mark.get('price'))
    if price is None or price <= 0 or claimed is None or not math.isclose(price,claimed,rel_tol=1e-12,abs_tol=1e-10):
        return ['price_evidence_value_mismatch']
    if mark.get('adjustment_basis') != 'split_adjusted_price' or mark.get('adjustment_vintage') != receipt.get('sha256'):
        return ['price_evidence_adjustment_vintage_mismatch']
    return []


class PriceEvidenceVerifier:
    """Bounded per-run cache. Missing/rejected archives never become verified."""
    def __init__(self, client, bucket, cache_size=128):
        self.client,self.bucket,self.cache_size=client,bucket,max(1,min(128,int(cache_size)))
        self.cache=OrderedDict()
        self.stats={'objects_read':0,'marks_verified':0,'marks_rejected':0,'archive_errors':0}

    def __call__(self, mark):
        receipt=mark.get('evidence') if isinstance(mark,dict) else None
        errors=[]
        try:
            if not isinstance(receipt,dict) or receipt.get('provider') != 'polygon':
                raise ValueError('price_archive_receipt_missing')
            if receipt.get('contract') != SOURCE_CONTRACT or receipt.get('captured') is not True:
                raise ValueError('price_archive_capture_contract_missing')
            sha=receipt.get('sha256')
            if not re.fullmatch(r'[0-9a-f]{64}',str(sha or '')) or mark.get('evidence_sha256') != sha:
                raise ValueError('price_archive_hash_identity_mismatch')
            source=public_source_url(receipt.get('source_url',''))
            request_sha=hashlib.sha256(source.encode()).hexdigest()
            key='data/evidence/polygon/'+request_sha+'/'+sha+'.bin.gz'
            if receipt.get('key') != key:
                raise ValueError('price_archive_path_mismatch')
            cache_key=(key,receipt.get('bytes'),receipt.get('first_received_at'))
            if cache_key not in self.cache:
                try:
                    obj=self.client.get_object(Bucket=self.bucket,Key=key)
                    meta=obj.get('Metadata') or {}
                    received=stamp(meta.get('received_at'))
                    stored=obj.get('LastModified')
                    if (meta.get('source_url') != source or meta.get('sha256') != sha or meta.get('provider') != 'polygon'
                            or meta.get('contract') != SOURCE_CONTRACT or received is None
                            or meta.get('received_at') != receipt.get('first_received_at')
                            or not isinstance(stored,datetime) or stored.tzinfo is None
                            or abs((stored-received).total_seconds())>300):
                        raise ValueError('source capture metadata mismatch')
                    compressed=obj['Body'].read(2_000_001)
                    if len(compressed)>2_000_000: raise ValueError('source archive too large')
                    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as archive:
                        raw=archive.read(2_000_001)
                    if len(raw)>2_000_000 or len(raw)!=receipt.get('bytes') or hashlib.sha256(raw).hexdigest()!=sha:
                        raise ValueError('source bytes mismatch')
                    self.stats['objects_read']+=1
                    self.cache[cache_key]=raw
                    while len(self.cache)>self.cache_size or sum(len(v) for v in self.cache.values())>32_000_000:
                        self.cache.popitem(last=False)
                except Exception:
                    self.stats['archive_errors']+=1
                    raise ValueError('price_archive_unavailable_or_hash_mismatch') from None
            else:
                self.cache.move_to_end(cache_key)
            errors=validate_daily_mark(mark,receipt,self.cache[cache_key])
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError) as exc:
            code=str(exc)
            errors=[code if code.startswith('price_') and ' ' not in code else 'price_archive_invalid']
        self.stats['marks_rejected' if errors else 'marks_verified']+=1
        return errors
