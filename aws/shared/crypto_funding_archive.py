"""Retain public funding originals and reviewed compilers, then replay before use.

Only content-addressed data/crypto-funding-research artifacts may be read.
No mutable pointer, account input, downloaded-code execution or decision authority.
"""
from base64 import b64decode
from datetime import datetime, timezone
from pathlib import Path
import hashlib, json, math, re
import crypto_funding_observations as model

PREFIX = 'data/crypto-funding-research/'
CONTRACT = 'crypto-funding-original-replay.v1'
MAX_BYTES = 32 * 1024 * 1024


def encode(doc):
    return (json.dumps(doc, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(',', ':')) + '\n').encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Exact acquisition timestamp required')
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, OverflowError):
        raise ValueError('Invalid acquisition timestamp') from None
    if stamp.tzinfo is None or stamp.utcoffset().total_seconds() != 0:
        raise ValueError('UTC acquisition clock required')
    return stamp


def strict(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Whole bounded archive required')
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate archive member')
            out[key] = value
        return out
    def finite(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError('Nonfinite archive number')
        return number
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=finite,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite archive')))


def reference(kind, raw):
    if kind not in ('captures', 'runs', 'compilers') or not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_BYTES:
        raise ValueError('Named bounded archive artifact required')
    suffix = '.py' if kind == 'compilers' else '.json'
    return {'key': PREFIX + kind + '/' + sha(raw) + suffix, 'bytes': len(raw), 'sha256': sha(raw)}


def validate_ref(ref, kind):
    if not isinstance(ref, dict) or set(ref) != {'key', 'bytes', 'sha256'}:
        raise ValueError('Exact archive reference required')
    if kind not in ('captures', 'runs', 'compilers') or not isinstance(ref['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256']):
        raise ValueError('Exact archive identity required')
    suffix = '.py' if kind == 'compilers' else '.json'
    if type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX_BYTES or ref['key'] != PREFIX + kind + '/' + ref['sha256'] + suffix:
        raise ValueError('Archive namespace or byte count differs')
    return ref


def complete_read(client, bucket, ref, kind):
    validate_ref(ref, kind)
    obj = client.get_object(Bucket=bucket, Key=ref['key'])
    stream, chunks, count = obj['Body'], [], 0
    try:
        if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != ref['bytes']:
            raise ValueError('Declared stored length differs')
        while count <= ref['bytes']:
            chunk = stream.read(min(65536, ref['bytes'] + 1 - count))
            if not isinstance(chunk, bytes):
                raise ValueError('Binary archive body required')
            if not chunk:
                break
            chunks.append(chunk); count += len(chunk)
    finally:
        stream.close()
    raw = b''.join(chunks)
    if len(raw) != ref['bytes'] or sha(raw) != ref['sha256']:
        raise ValueError('Complete archive bytes differ')
    return raw


def retain_bytes(client, bucket, kind, raw):
    ref = reference(kind, raw)
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*',
                          ContentType='text/plain; charset=utf-8' if kind == 'compilers' else 'application/json',
                          CacheControl='public, max-age=31536000, immutable')
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
        if code not in ('PreconditionFailed', 'ConditionalRequestConflict', '409', '412'):
            raise
    if complete_read(client, bucket, ref, kind) != raw:
        raise ValueError('Immutable archive readback differs')
    return ref


def replay_attempt(attempt, model):
    """Recompute every measurement from its original or explicitly partial body."""
    if not isinstance(attempt, dict):
        raise ValueError('Source attempt required')
    provider, instrument = attempt.get('provider'), attempt.get('instrument')
    if provider not in ('okx', 'bybit') or not isinstance(instrument, str):
        raise ValueError('Named public source required')
    url = ('https://www.okx.com/api/v5/public/funding-rate?instId=' + instrument if provider == 'okx' else
           'https://api.bybit.com/v5/market/funding/history?category=linear&symbol=' + instrument + '&limit=1')
    if attempt.get('request_url') != url:
        raise ValueError('Requested source identity differs')
    started, finished = clock(attempt.get('acquired_started_at')), clock(attempt.get('acquired_completed_at'))
    if started > finished:
        raise ValueError('Acquisition clocks reversed')
    complete = attempt.get('response_complete')
    if type(complete) is not bool:
        raise ValueError('Explicit response completeness required')
    if attempt.get('http_status') is not None and type(attempt.get('http_status')) is not int:
        raise ValueError('Typed reported HTTP status required')
    if attempt.get('transport_error') is not None and not isinstance(attempt.get('transport_error'), str):
        raise ValueError('Typed transport failure category required')
    if complete:
        try:
            raw = b64decode(attempt['original_response_base64'], validate=True)
        except (KeyError, ValueError, TypeError):
            raise ValueError('Exact original response encoding required') from None
        if len(raw) > model.MAX_RESPONSE_BYTES or type(attempt.get('original_response_bytes')) is not int or len(raw) != attempt['original_response_bytes'] or sha(raw) != attempt.get('original_response_sha256'):
            raise ValueError('Original source bytes differ')
        expected = model.point(provider, instrument, raw)
        if attempt.get('http_status') != 200 or type(attempt.get('http_status')) is not int:
            expected.update(status='unavailable', reason='http_status_not_200', funding_rate=None,
                            funding_rate_decimal=None, funding_rate_pct=None, funding_rate_pct_decimal=None)
    else:
        try:
            raw = b64decode(attempt['received_prefix_base64'], validate=True)
        except (KeyError, ValueError, TypeError):
            raise ValueError('Explicit incomplete response encoding required') from None
        if len(raw) > model.MAX_RESPONSE_BYTES + 1 or type(attempt.get('received_prefix_bytes')) is not int or len(raw) != attempt['received_prefix_bytes'] or sha(raw) != attempt.get('received_prefix_sha256'):
            raise ValueError('Incomplete response bytes differ')
        expected = model.point(provider, instrument, b'')
        for key in ('original_response_base64', 'original_response_sha256', 'original_response_bytes'):
            expected[key] = None
        expected.update(status='unavailable', reason=attempt.get('transport_error') or 'response_exceeds_capture_limit',
                        received_prefix_base64=attempt['received_prefix_base64'], received_prefix_bytes=len(raw),
                        received_prefix_sha256=sha(raw))
    expected.update(request_url=url, acquired_started_at=attempt['acquired_started_at'], acquired_completed_at=attempt['acquired_completed_at'],
                    http_status=attempt.get('http_status'), response_complete=complete, transport_error=attempt.get('transport_error'))
    if encode(expected) != encode(attempt):
        raise ValueError('Whole source-attempt replay differs')
    return expected


def replay_capture(packet, model):
    """Full deterministic replay through the real collector with frozen I/O."""
    attempts = packet.get('source_attempts') if isinstance(packet, dict) else None
    if not isinstance(attempts, list) or len(attempts) not in (10, 20):
        raise ValueError('Complete fixed-universe attempts required')
    previous = None
    for index, attempt in enumerate(attempts):
        if not isinstance(attempt, dict):
            raise ValueError('Whole typed source attempt required')
        expected_provider = 'okx' if index < 10 else 'bybit'
        symbol = model.SYMBOLS[index % 10]
        if attempt.get('provider') != expected_provider or attempt.get('instrument') != symbol + ('-USDT-SWAP' if index < 10 else 'USDT'):
            raise ValueError('Attempt order or fixed universe differs')
        replay_attempt(attempt, model)
        if previous is not None and clock(attempt['acquired_started_at']) < previous:
            raise ValueError('Sequential acquisition clocks overlap')
        previous = clock(attempt['acquired_completed_at'])
    # Independently reconstruct the collector's complete public projection.
    # No transport simulation or downloaded compiler execution is permitted.
    primary = attempts[:10]
    if any(row['status'] == 'descriptive' for row in primary) and len(attempts) != 10:
        raise ValueError('Unrequested fallback or mixed event basis')
    if not any(row['status'] == 'descriptive' for row in primary) and len(attempts) != 20:
        raise ValueError('Fallback attempts missing')
    selected = primary if len(attempts) == 10 else attempts[10:]
    rows = []
    for index, row in enumerate(selected, 0 if len(attempts) == 10 else 10):
        projected = {key: row.get(key) for key in model.POINT_FIELDS}
        projected.update(symbol=model.SYMBOLS[index % 10], evidence_index=index, sentiment='UNAVAILABLE')
        rows.append(projected)
    observed = sum(row['status'] == 'descriptive' for row in rows)
    expected = {'contract': model.PACKET_CONTRACT, 'status': 'descriptive' if observed else 'unavailable',
                'rates': rows, 'source_attempts': attempts, 'selected_provider': 'okx' if len(attempts) == 10 else 'bybit',
                'expected_instruments': len(model.SYMBOLS), 'observed_instruments': observed, 'unavailable_instruments': len(model.SYMBOLS)-observed,
                'coverage_basis': 'requested_fixed_universe_not_global_market',
                'avg_rate_pct': None, 'avg_funding': None, 'leverage_sentiment': 'UNAVAILABLE', 'bias': 'unknown',
                'long_count': None, 'short_count': None, 'positive_count': None, 'negative_count': None,
                'most_longed': [], 'most_shorted': [], 'reason': 'settlement_basis_and_cross_contract_comparability_unqualified',
                'source_timing_qualified': False, 'cross_contract_comparability_verified': False, 'annualization_qualified': False, **model.DENIED}
    if encode(expected) != encode(packet):
        raise ValueError('Complete funding capture replay differs')
    return expected


def retain_checked(client, bucket, packet, model, compilers):
    """No mutable pointer; caller publishes only after exact whole-byte readback."""
    replayed = replay_capture(packet, model)
    if set(compilers) != {'crypto_funding_observations.py', 'crypto_funding_archive.py'}:
        raise ValueError('Complete reviewed local compiler set required')
    refs = {name: retain_bytes(client, bucket, 'compilers', raw) for name, raw in compilers.items()}
    capture = retain_bytes(client, bucket, 'captures', encode(replayed))
    manifest = {'contract': CONTRACT, 'capture': capture, 'compilers': refs,
                'acquisition_started_at': packet['source_attempts'][0]['acquired_started_at'],
                'acquisition_completed_at': packet['source_attempts'][-1]['acquired_completed_at'],
                'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
                'source_qualified': False, 'point_in_time_qualified': False,
                'retention_mode': 'content_addressed_conditional_write_not_object_lock'}
    ref = retain_bytes(client, bucket, 'runs', encode(manifest))
    loaded = strict(complete_read(client, bucket, ref, 'runs'))
    for name, raw in compilers.items():
        if complete_read(client, bucket, loaded['compilers'][name], 'compilers') != raw:
            raise ValueError('Reviewed local compiler differs')
    replay_capture(strict(complete_read(client, bucket, loaded['capture'], 'captures')), model)
    return {'contract': CONTRACT, 'manifest': ref, 'capture': capture, 'complete_capture_replayed': True,
            'source_qualified': False, 'point_in_time_qualified': False, 'investment_authority': False}


def compiler_bytes():
    return {'crypto_funding_observations.py': Path(model.__file__).read_bytes(),
            'crypto_funding_archive.py': Path(__file__).read_bytes()}


def replay(client, bucket, ref):
    """Read originals in this namespace; execute only the matching local compiler."""
    manifest = strict(complete_read(client, bucket, ref, 'runs'))
    expected_keys = {'contract', 'capture', 'compilers', 'acquisition_started_at',
                     'acquisition_completed_at', 'calls_eligible', 'sizing_eligible',
                     'execution_eligible', 'source_qualified', 'point_in_time_qualified', 'retention_mode'}
    if not isinstance(manifest, dict) or set(manifest) != expected_keys or manifest['contract'] != CONTRACT:
        raise ValueError('Exact funding replay manifest required')
    if manifest['retention_mode'] != 'content_addressed_conditional_write_not_object_lock':
        raise ValueError('Exact retention assurance required')
    for key in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'source_qualified', 'point_in_time_qualified'):
        if manifest[key] is not False:
            raise ValueError('Archive cannot promote investment permission')
    sources = compiler_bytes()
    if not isinstance(manifest['compilers'], dict) or set(manifest['compilers']) != set(sources):
        raise ValueError('Whole matching local compiler set required')
    for name, raw in sources.items():
        if manifest['compilers'][name] != reference('compilers', raw) or complete_read(client, bucket, manifest['compilers'][name], 'compilers') != raw:
            raise ValueError('Compiler differs; replay with its reviewed checkout')
    packet = replay_capture(strict(complete_read(client, bucket, manifest['capture'], 'captures')), model)
    if manifest['acquisition_started_at'] != packet['source_attempts'][0]['acquired_started_at'] or manifest['acquisition_completed_at'] != packet['source_attempts'][-1]['acquired_completed_at']:
        raise ValueError('Manifest acquisition interval differs')
    return packet


def retain(client, bucket, packet):
    ref = retain_checked(client, bucket, packet, model, compiler_bytes())
    if encode(replay(client, bucket, ref['manifest'])) != encode(packet):
        raise ValueError('Pre-publication original replay differs')
    return ref


class BudgetStore:
    """Reserve final publication time; no SDK retries or new provider requests.

    Check before every storage call and body chunk. Network timeouts remain
    per operation, not a claim of an end-to-end Lambda execution guarantee.
    """
    def __init__(self, client, remaining_ms):
        self.client, self.remaining_ms = client, remaining_ms

    def check(self):
        remaining = self.remaining_ms()
        if type(remaining) is not int or remaining < 30000:
            raise ValueError('Funding archive publication budget unavailable')

    def put_object(self, **kw):
        self.check()
        return self.client.put_object(**kw)

    def get_object(self, **kw):
        self.check()
        value = self.client.get_object(**kw)
        stream, check = value['Body'], self.check
        class CheckedBody:
            def read(self, count):
                check()
                return stream.read(count)
            def close(self):
                stream.close()
        return {**value, 'Body': CheckedBody()}


def storage_client():
    import boto3
    from botocore.config import Config
    return boto3.client('s3', config=Config(connect_timeout=2, read_timeout=3,
                                          retries={'total_max_attempts': 1}))


def collect_retained(opener, client_factory, bucket, remaining_ms):
    """Return descriptive funding only after original retention and replay.

    The fixed provider universe and transport policy are unchanged. A storage,
    compiler, replay or budget failure cannot publish an unretained new value.
    """
    packet = model.collect_funding(opener)
    try:
        remaining = remaining_ms()
        if type(remaining) is not int or remaining < 30000:
            raise ValueError('Funding archive publication budget unavailable')
        client = client_factory()
        try:
            proof = retain(BudgetStore(client, remaining_ms), bucket, packet)
        finally:
            client.close()
    except Exception:
        # Do not expose credentials, signed URLs or backend exception text.
        raise ValueError('Funding originals unavailable; descriptive publication withheld') from None
    return {**packet, 'original_capture': proof}
