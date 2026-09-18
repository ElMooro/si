"""Register typed public research observations before future measurement windows.

Only a strict projection enters the public archive. This proves what the
collector observed, not a replay of an upstream engine's private reasoning.
Conditional records are application-level retention, not S3 Object Lock.
"""
import hashlib
import json
import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from instrument_identity import resolve_instrument
from private_artifact import public_source_allowed

CONTRACT = 'prospective-research-journal.v1'
PREFIX = 'data/research-forecasts/'
EASTERN = ZoneInfo('America/New_York')
PROTOCOL = {
    'contract': 'forward-daily-price-observation.v1',
    'purpose': 'Prospective descriptive measurement of explicitly published directions',
    'scope': 'USD cash equity/ETF daily price observations; no orders or simulated fills',
    'entry_policy': 'First SPY-observed session strictly after the registration date in America/New_York',
    'entry_missing_policy': 'Missing matching asset entry remains unmeasurable; never move to a later asset bar',
    'exit_policy': '5 and 20 SPY-observed sessions after entry; asset and benchmark must share endpoints',
    'horizons_sessions': [5, 20],
    'benchmark': 'equity:US:SPY',
    'price_basis': 'split_adjusted_price',
    'vintage_policy': 'Entry and exit for each instrument from one retained provider response vintage',
    'measurement_provider': 'polygon',
    'provider_parser': 'polygon-us-daily-close.v1',
    'clock': 'Daily aggregate period end is not a trade timestamp',
    'costs_policy': 'No executable fill or cost estimate; net performance and portfolio PnL are unavailable',
    'dividend_policy': 'Price returns exclude dividends; not total returns',
    'overlap_policy': 'Repeated engines, instruments and overlapping windows are not independent samples',
    'selection_policy': 'Collector candidates retained before outcomes; top-list coverage is not the whole market',
    'original_forecast_horizon': 'Unverified; these are standardized research windows, not the engine original horizon',
    'sizing_eligible': False, 'promotion_eligible': False, 'out_of_sample_validation': False,
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except (ValueError, TypeError): return None


def protocol_document():
    return json.loads(canonical(PROTOCOL))


def projection(source_key, doc, picks, raw_sha256, received_at):
    """No narrative, account, score heuristic or arbitrary object may escape."""
    if not public_source_allowed(source_key) or not re.fullmatch(r'data/[a-zA-Z0-9_-]+\.json', source_key):
        raise ValueError('public research source required')
    if not isinstance(doc, dict) or not re.fullmatch('[0-9a-f]{64}', str(raw_sha256)):
        raise ValueError('source object and byte hash required')
    now = stamp(received_at)
    if now is None: raise ValueError('source receipt clock required')
    # An as_of/date label may be an observation period, not a publication clock.
    generated = stamp(doc.get('generated_at'))
    quality = doc.get('quality') if isinstance(doc.get('quality'), dict) else {}
    state = str(quality.get('status') or '').lower()
    state = state if state in ('fresh', 'stale', 'degraded', 'error', 'unknown', 'missing') else 'unreported'
    reasons = []
    if generated is None: reasons.append('source_publication_clock_missing')
    elif not 0 <= (now-generated).total_seconds() <= 24*3600: reasons.append('source_publication_stale_or_future')
    if state in ('stale', 'error', 'missing'): reasons.append('source_quality_ineligible')
    rows = []
    for pick in picks:
        identity = pick.get('identity')
        canonical_identity = resolve_instrument((identity or {}).get('symbol'), (identity or {}).get('asset_class'))
        supported = bool(canonical_identity and canonical_identity == identity and identity['asset_class'] == 'equity')
        # Symbol text is already validated by the instrument resolver; unknown
        # inputs are counted below and never copied into a public document.
        if not supported: continue
        direction = pick.get('direction') if pick.get('direction') in ('UP', 'DOWN', 'NEUTRAL') else 'NEUTRAL'
        explicit = pick.get('prediction_origin') == 'explicit_direction' and direction in ('UP', 'DOWN')
        rows.append({'instrument': canonical_identity, 'direction': direction if explicit else 'NEUTRAL',
                     'origin': 'explicit_direction' if explicit else 'rank_observation'})
    return {'source_key': source_key, 'source_bytes_sha256': raw_sha256,
            'source_generated_at': generated.isoformat() if generated else None,
            'source_received_at': now.isoformat(), 'quality_status': state,
            'eligibility_reasons': reasons, 'observations': rows,
            'unsupported_identity_count': len(picks)-len(rows),
            'scope': 'Typed collector observation; original source bytes and upstream model replay are not retained here'}


def persist_once(client, bucket, key, value):
    body = canonical(value)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json',
                          CacheControl='public, max-age=31536000, immutable', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code', '')) not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
            raise
        if client.get_object(Bucket=bucket, Key=key)['Body'].read() != body:
            raise ValueError('immutable journal conflict') from exc
    return {'key': key, 'sha256': hashlib.sha256(body).hexdigest()}


def ensure_protocol(client, bucket):
    doc = protocol_document()
    ref = persist_once(client, bucket, PREFIX+'protocols/'+digest(doc)+'.json', doc)
    stored = client.head_object(Bucket=bucket, Key=ref['key']).get('LastModified')
    if not isinstance(stored, datetime) or stored.tzinfo is None:
        raise ValueError('protocol storage clock missing')
    return {**ref, 'first_stored_at': stored.astimezone(timezone.utc).isoformat()}


def validate_record(record):
    if record.get('contract') != CONTRACT or record.get('protocol') != protocol_document():
        raise ValueError('unsupported journal or prospective protocol')
    if set(record) != {'contract', 'forecast_id', 'registered_at', 'registration_date_et', 'source',
                       'observation', 'protocol', 'protocol_ref', 'collector', 'eligibility'}:
        raise ValueError('journal schema mismatch')
    source = record['source']
    if set(source) != {'source_key', 'source_bytes_sha256', 'source_generated_at', 'source_received_at', 'quality_status', 'scope'}:
        raise ValueError('source projection schema mismatch')
    observed = record['observation']
    if set(observed) != {'instrument', 'direction', 'origin'} or observed['origin'] != 'explicit_direction' or observed['direction'] not in ('UP', 'DOWN'):
        raise ValueError('explicit direction required')
    ident = observed['instrument']
    if resolve_instrument(ident.get('symbol'), ident.get('asset_class')) != ident or ident.get('asset_class') != 'equity':
        raise ValueError('unsupported instrument')
    receipt, generated, registered = [stamp(v) for v in (source['source_received_at'], source['source_generated_at'], record['registered_at'])]
    if not all((receipt, generated, registered)) or not 0 <= (receipt-generated).total_seconds() <= 86400 or not 0 <= (registered-receipt).total_seconds() <= 900:
        raise ValueError('prospective clocks inconsistent')
    if source['quality_status'] in ('stale', 'missing', 'error'):
        raise ValueError('source quality cannot register forecast')
    if not public_source_allowed(source['source_key']) or not re.fullmatch(r'data/[a-zA-Z0-9_-]+\.json', source['source_key']):
        raise ValueError('private or invalid source')
    if not re.fullmatch('[0-9a-f]{64}', source['source_bytes_sha256']): raise ValueError('source byte identity invalid')
    if record['registration_date_et'] != registered.astimezone(EASTERN).date().isoformat():
        raise ValueError('registration date mismatch')
    pref = record['protocol_ref']; pd = digest(record['protocol'])
    ptime = stamp(pref.get('first_stored_at'))
    if pref.get('key') != PREFIX+'protocols/'+pd+'.json' or pref.get('sha256') != pd or ptime is None or ptime > registered:
        raise ValueError('protocol was not registered before the forecast')
    if record['eligibility'] != {'measurement_only': True, 'sizing_eligible': False, 'promotion_eligible': False,
                                 'original_model_replay_verified': False, 'out_of_sample_validation': False}:
        raise ValueError('journal cannot grant authority')
    if set(record['collector']) != {'contract', 'source_sha256'} or record['collector']['contract'] != 'signal-harvester-projection.v1' or not re.fullmatch('[0-9a-f]{64}',record['collector']['source_sha256']):
        raise ValueError('collector identity missing')
    identity = {k: source[k] for k in ('source_key', 'source_bytes_sha256', 'source_generated_at')}
    expected = digest({'source': identity, 'observation': observed, 'protocol_sha256': pd})
    if record['forecast_id'] != expected: raise ValueError('forecast identity mismatch')
    return record


def register(client, bucket, source_projection, protocol_ref, collector_sha256, now=None):
    now = now or datetime.now(timezone.utc)
    if source_projection['eligibility_reasons']: return []
    source = {k: source_projection[k] for k in ('source_key', 'source_bytes_sha256', 'source_generated_at',
                                               'source_received_at', 'quality_status', 'scope')}
    refs = []
    for observed in source_projection['observations']:
        if observed['origin'] != 'explicit_direction': continue
        identity = {k: source[k] for k in ('source_key', 'source_bytes_sha256', 'source_generated_at')}
        fid = digest({'source': identity, 'observation': observed, 'protocol_sha256': digest(PROTOCOL)})
        key = PREFIX+'records/'+fid+'.json'
        record = {'contract': CONTRACT, 'forecast_id': fid, 'registered_at': now.isoformat(),
                  'registration_date_et': now.astimezone(EASTERN).date().isoformat(),
                  'source': source, 'observation': observed, 'protocol': protocol_document(), 'protocol_ref': protocol_ref,
                  'collector': {'contract': 'signal-harvester-projection.v1', 'source_sha256': collector_sha256},
                  'eligibility': {'measurement_only': True, 'sizing_eligible': False, 'promotion_eligible': False,
                                  'original_model_replay_verified': False, 'out_of_sample_validation': False}}
        validate_record(record)
        created = True
        try:
            client.put_object(Bucket=bucket, Key=key, Body=canonical(record), ContentType='application/json',
                              CacheControl='public, max-age=31536000, immutable', IfNoneMatch='*')
        except Exception as exc:
            if str(getattr(exc, 'response', {}).get('Error', {}).get('Code', '')) not in ('PreconditionFailed','412','ConditionalRequestConflict','409'): raise
            obj = client.get_object(Bucket=bucket, Key=key)
            previous = validate_record(json.loads(obj['Body'].read()))
            if previous['forecast_id'] != fid: raise ValueError('existing forecast identity mismatch') from exc
            record, created = previous, False
        refs.append({'forecast_id': fid, 'key': key, 'sha256': digest(record), 'created': created,
                     'registered_at': record['registered_at'], 'symbol': observed['instrument']['symbol'],
                     'direction': observed['direction'], 'source_key': source['source_key']})
    return refs


def read_record(client, bucket, ref):
    """Use actual S3 storage time to reject a backdated registration."""
    if not re.fullmatch('[0-9a-f]{64}', str(ref.get('forecast_id', ''))): raise ValueError('invalid forecast id')
    key = PREFIX+'records/'+ref['forecast_id']+'.json'
    if ref.get('key') != key: raise ValueError('invalid forecast path')
    obj = client.get_object(Bucket=bucket, Key=key)
    raw = obj['Body'].read(100001)
    if len(raw)>100000 or hashlib.sha256(raw).hexdigest() != ref.get('sha256'): raise ValueError('forecast bytes mismatch')
    record = validate_record(json.loads(raw))
    if record['forecast_id'] != ref['forecast_id']: raise ValueError('forecast path identity mismatch')
    stored = obj.get('LastModified'); registered = stamp(record['registered_at'])
    if not isinstance(stored, datetime) or stored.tzinfo is None or abs((stored-registered).total_seconds())>300:
        raise ValueError('forecast storage clock mismatch')
    protocol = client.get_object(Bucket=bucket, Key=record['protocol_ref']['key'])
    if hashlib.sha256(protocol['Body'].read()).hexdigest() != record['protocol_ref']['sha256'] or protocol.get('LastModified') != stamp(record['protocol_ref']['first_stored_at']):
        raise ValueError('retained protocol mismatch')
    return record
