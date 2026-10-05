"""Exact, versioned IMF public series; original responses and flags retained.

Only reviewed public definitions are requested. No warehouse/provider fallback,
monetary scaling assumption, interpolation or trading authority is introduced.
"""
import base64
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

CONTRACT = 'imf-reviewed-series.v1'
_catalogue_raw = Path(__file__).with_name('imf-series.json').read_bytes()
CATALOGUE_HASH = hashlib.sha256(_catalogue_raw).hexdigest()
CATALOGUE = json.loads(_catalogue_raw)
assert CATALOGUE['contract'] == 'imf-reviewed-catalogue.v1'
MAX_WIRE = 4000000
MAX_ROWS = 15000
MAX_AGE = 86400
ROOT = 'data/series-cache/imf-source/'
_canonical = {key.upper(): key for key in CATALOGUE['series']}
assert len(_canonical) == len(CATALOGUE['series'])
XML_NS = 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/'
_missing_status = {'K', 'O', 'M', 'L', 'H', 'Q', 'J', 'N', '_U', 'X'}


class SourceUnavailable(ValueError):
    pass


def reviewed_flow(value):
    return value.upper() if isinstance(value, str) and value.upper() in CATALOGUE['flows'] else None


def definition(sid):
    if not isinstance(sid, str) or len(sid) > 240 or sid.split(':', 1)[0].lower() != 'imf':
        raise ValueError('Select an exact reviewed IMF series identifier')
    key = _canonical.get(sid.partition(':')[2].upper())
    if key is None:
        raise ValueError('IMF flow or complete dimension key is unreviewed')
    d = dict(CATALOGUE['series'][key], id='imf:' + key)
    cfg = CATALOGUE['flows'][d['flow']]
    dimensions = {name: d['fixed_attributes'][name] for name in cfg['dimensions']}
    labels = {name: cfg['definitions'][name]['codes'][code]['labels']['Name']['en'] for name, code in dimensions.items()}
    d.update(dimensions=dimensions, labels=labels, name=' / '.join(labels.values()),
        flow_version=cfg['version'], agency=cfg['agency'],
        source_url='https://api.imf.org/external/sdmx/2.1/data/' + cfg['agency'] + ',' + d['flow'] + ',' + cfg['version'] + '/' + d['key'])
    return d


def directory(q='', limit=50, offset=0, flow=None):
    if type(limit) is not int or not 1 <= limit <= 500 or type(offset) is not int or offset < 0:
        raise ValueError('Invalid IMF directory page')
    if flow is not None and reviewed_flow(flow) is None:
        raise ValueError('Unreviewed IMF dataflow')
    terms = str(q).casefold().split()
    rows = []
    for key, entry in CATALOGUE['series'].items():
        if flow is not None and entry['flow'] != flow.upper():
            continue
        d = definition('imf:' + key)
        if not all(t in (d['id'] + ' ' + d['name'] + ' ' + d['unit']).casefold() for t in terms):
            continue
        rows.append(dict(id=d['id'], symbol=key, provider='imf', provider_name='IMF', kind='series',
            chartable=True, name=d['name'], unit=d['unit'], freq=d['freq'], geo=d['dimensions']['COUNTRY'],
            flow_version=d['flow_version'], first=d['snapshot_first'], last=d['snapshot_last'], n=None,
            live_history_verified=False, contract=CONTRACT, definition_sha256=CATALOGUE_HASH,
            seasonal_adjustment=d['seasonal_adjustment']))
    return dict(provider='imf', rows=rows[offset:offset + limit], total=len(rows), offset=offset, limit=limit,
        contract=CONTRACT, catalogue_scope='Exact reviewed public definitions; membership does not prove current observations, complete history or vendor equivalence')


def period(value, frequency):
    try:
        if frequency == 'M' and re.fullmatch(r'\d{4}-M(0[1-9]|1[0-2])', value):
            return date(int(value[:4]), int(value[-2:]), 1).isoformat()
        if frequency == 'Q' and re.fullmatch(r'\d{4}-Q[1-4]', value):
            return date(int(value[:4]), int(value[-1]) * 3 - 2, 1).isoformat()
        if frequency == 'A' and re.fullmatch(r'\d{4}', value):
            return date(int(value), 1, 1).isoformat()
    except (TypeError, ValueError):
        pass
    return None


def measured(value):
    if not isinstance(value, str) or len(value) > 80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?', value):
        return None
    try:
        number = Decimal(value)
        floating = float(number)
        # The original decimal text remains in the extract and each evidence
        # row. Plotting uses finite binary64; disclose rounding instead of
        # inventing gaps for legitimate high-precision source observations.
        return floating if number.is_finite() and math.isfinite(floating) and (floating != 0 or number == 0) else None
    except (InvalidOperation, ValueError, OverflowError):
        return None


def parsed(raw, d):
    if not isinstance(raw, bytes) or len(raw) > MAX_WIRE or b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise ValueError('IMF response bounds or XML declaration invalid')
    root = ET.fromstring(raw)
    if root.tag != '{' + XML_NS + 'message}StructureSpecificData':
        raise ValueError('IMF structure-specific response required')
    local = lambda node: node.tag.rsplit('}', 1)[-1]
    if any(local(node) == 'Footer' for node in root.iter()):
        raise ValueError('IMF response footer needs review; no potentially partial history')
    tests = [node.text for node in root.iter() if local(node) == 'Test']
    if tests != ['false']:
        raise ValueError('IMF production response required')
    for node in root.iter():
        if any(node.attrib.get(k, v) != v for k, v in (('ACCESS_SHARING_LEVEL', 'PUBLIC_OPEN'), ('SECURITY_CLASSIFICATION', 'PUB'))):
            raise ValueError('IMF response is not explicitly public; no retention or display')
    refs = [list(node)[0].attrib for node in root.iter() if local(node) == 'StructureUsage' and len(node) == 1]
    expected = {'agencyID': d['agency'], 'id': d['flow'], 'version': d['flow_version']}
    if len(refs) != 1 or any(refs[0].get(k) != v for k, v in expected.items()):
        raise ValueError('IMF dataflow/version changed')
    datasets = [node for node in root if local(node) == 'DataSet']
    if len(datasets) != 1 or datasets[0].attrib.get('action') != 'Replace':
        raise ValueError('One complete IMF replacement dataset required')
    dataset = datasets[0]
    series = [node for node in dataset if local(node) == 'Series']
    if len(series) != 1 or any(local(node) not in ('Series', 'Group') for node in dataset):
        raise ValueError('Exactly the selected IMF series required')
    selected = series[0]
    a = selected.attrib
    if a.get('ACCESS_SHARING_LEVEL') != 'PUBLIC_OPEN' or a.get('SECURITY_CLASSIFICATION') != 'PUB':
        raise ValueError('IMF response is not explicitly public; no retention or display')
    # Do not silently reinterpret a source revision to scale, base, methodology,
    # frequency or dimensions. Absent pinned attributes must remain absent.
    fields = set(CATALOGUE['flows'][d['flow']]['dimensions']) | {'SCALE', 'REFERENCE_PERIOD', 'METHODOLOGY'}
    if {k: a[k] for k in fields if k in a} != d['fixed_attributes']:
        raise ValueError('IMF units, index base, methodology or dimensions changed; review required')
    if len(selected) > MAX_ROWS or any(local(node) != 'Obs' or len(node) for node in selected):
        raise ValueError('IMF observation shape or count invalid')
    cfg = CATALOGUE['flows'][d['flow']]
    expected_groups = {g['type']: g['attributes'] for g in cfg['groups']
        if all(g['attributes'][field] == a[field] for field in cfg['group_dimensions'][g['type']])}
    received_groups = {}
    group_records = []
    for node in dataset:
        if local(node) != 'Group':
            continue
        attrs = dict(node.attrib)
        typ = attrs.pop('{http://www.w3.org/2001/XMLSchema-instance}type', '').split(':')[-1]
        if len(node) or typ not in expected_groups or typ in received_groups:
            raise ValueError('IMF metadata group identity or shape changed')
        expected = expected_groups[typ]
        fields = set(cfg['group_dimensions'][typ]) | {'UNIT', 'TRANSFORMATION', 'SCALE', 'REFERENCE_PERIOD', 'INDEX_TYPE', 'PPI_ACTIVITY'}
        if {k: attrs[k] for k in fields if k in attrs} != {k: expected[k] for k in fields if k in expected}:
            raise ValueError('IMF group dimensions, units or transformation changed; review required')
        received_groups[typ] = attrs
        group_records.append(dict(node.attrib))
    if set(received_groups) != set(expected_groups):
        raise ValueError('IMF source group metadata incomplete')
    fields = set(cfg['dimensions']) | {'SCALE', 'REFERENCE_PERIOD', 'METHODOLOGY'}
    records = []
    counts = Counter()
    for ordinal, node in enumerate(selected):
        original = dict(node.attrib)
        if any(original.get(k, v) != v for k, v in (('ACCESS_SHARING_LEVEL', 'PUBLIC_OPEN'), ('SECURITY_CLASSIFICATION', 'PUB'))):
            raise ValueError('IMF observation is not explicitly public; no retention or display')
        if any(k in original and original[k] != a.get(k) for k in fields):
            raise ValueError('IMF observation-level identity or unit override needs review')
        anchor = period(original.get('TIME_PERIOD', ''), d['freq'])
        value = measured(original.get('OBS_VALUE'))
        reason = None
        status, derivation = original.get('STATUS'), original.get('DERIVATION_TYPE')
        if anchor is None:
            reason = 'invalid_reference_period'
        elif status and status not in cfg['status_definitions']:
            reason = 'unknown_observation_status'
        elif derivation and derivation not in cfg['derivation_definitions']:
            reason = 'unknown_derivation_type'
        elif status == 'F':
            reason = 'forecast_not_historical_observation'
        elif status in _missing_status:
            reason = 'source_status_excludes_numeric_observation'
        elif value is None:
            reason = 'source_missing_or_unrepresentable_value'
        if anchor:
            counts[anchor] += 1
        records.append(dict(ordinal=ordinal, original=original, anchor=anchor, value=value if reason is None else None, rejection=reason,
            binary64_rounding=value is not None and Decimal(str(value)) != Decimal(original['OBS_VALUE'])))
    obs = {}
    for record in records:
        if record['anchor']:
            if counts[record['anchor']] > 1:
                record.update(value=None, rejection='duplicate_reference_period')
            obs[record['anchor']] = record['value']
    return sorted([t, v] for t, v in obs.items()), records, dict(a), dict(dataset.attrib), group_records


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, 'IMF redirect not followed', headers, fp)


def read_http(url):
    if not isinstance(url, str) or not re.fullmatch(r'https://api\.imf\.org/external/sdmx/2\.1/data/IMF\.STA,(?:LS|PI|PPI|MFS_IR),[0-9.]+/[A-Za-z0-9_.]+', url):
        raise ValueError('Unreviewed IMF source URL')
    req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-IMF/1.0 (+https://justhodl.ai)'})
    with urllib.request.build_opener(NoRedirect()).open(req, timeout=55) as response:
        raw = response.read(MAX_WIRE + 1)
        if response.status != 200 or len(raw) > MAX_WIRE:
            raise ValueError('IMF source status or response length invalid')
        return raw, dict(response.headers)


def prefix(d):
    return ROOT + hashlib.sha256(d['id'].encode()).hexdigest() + '/'


def _get(store, bucket, key, bound):
    try:
        response = store.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('NoSuchKey', '404', 'NotFound'):
            return None
        raise SourceUnavailable('IMF shared cache unavailable; no source request') from exc
    body = response['Body']
    try:
        raw = body.read(bound + 1)
    finally:
        body.close()
    if len(raw) > bound:
        raise SourceUnavailable('IMF retained object exceeds bound')
    return raw


def _put(store, bucket, key, raw, **kw):
    try:
        store.put_object(Bucket=bucket, Key=key, Body=raw, **kw)
    except Exception as exc:
        raise SourceUnavailable('IMF source retention failed; no unretained chart') from exc


def validate_receipt(receipt, d, raw):
    digest = hashlib.sha256(raw).hexdigest()
    key = prefix(d) + 'responses/' + digest + '.xml'
    if (not isinstance(receipt, dict) or receipt.get('contract') != CONTRACT or receipt.get('id') != d['id']
        or receipt.get('definition_sha256') != CATALOGUE_HASH or receipt.get('url') != d['source_url']
        or receipt.get('http_status') != 200 or receipt.get('sha256') != digest or receipt.get('bytes') != len(raw)
        or receipt.get('retained_key') != key or receipt.get('retained_url') != 'https://justhodl.ai/' + key
        or receipt.get('complete_response_retained') is not True):
        raise ValueError('IMF source receipt failed integrity checks')
    clock = datetime.fromisoformat(receipt['received_at'])
    if clock.tzinfo is None:
        raise ValueError('IMF receipt timezone required')


def snapshot(d, store, bucket, reader=None, now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('IMF acquisition timezone required')
    namespace = prefix(d)
    if _get(store, bucket, ROOT + 'blocked.json', 32000) is not None or _get(store, bucket, namespace + 'blocked.json', 32000) is not None:
        raise SourceUnavailable('IMF source stopped after HTTP refusal; access review required')
    current = _get(store, bucket, namespace + 'current.json', 32000)
    if current is not None:
        receipt = json.loads(current)
        digest = receipt.get('sha256', '')
        if not re.fullmatch('[0-9a-f]{64}', digest):
            raise SourceUnavailable('IMF retained identity invalid')
        raw = _get(store, bucket, namespace + 'responses/' + digest + '.xml', MAX_WIRE)
        if raw is None:
            raise SourceUnavailable('IMF retained response absent')
        validate_receipt(receipt, d, raw)
        age = (now - datetime.fromisoformat(receipt['received_at'])).total_seconds()
        if age < 0:
            raise SourceUnavailable('IMF receipt is in the future')
        if age < MAX_AGE:
            return raw, dict(receipt, snapshot_age_s=int(age))
    claim = namespace + 'request-claims/' + now.date().isoformat() + '.json'
    try:
        store.put_object(Bucket=bucket, Key=claim, Body=json.dumps({'contract': CONTRACT, 'id': d['id'], 'requested_at': now.isoformat()}).encode(), ContentType='application/json', IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'):
            raise SourceUnavailable('IMF request already claimed today; no repeat request or stale substitution') from exc
        raise SourceUnavailable('IMF shared request gate unavailable; no source request') from exc
    try:
        raw, headers = (reader or read_http)(d['source_url'])
        parsed(raw, d)  # Verify identity, access labels and units before retaining.
        digest = hashlib.sha256(raw).hexdigest()
        key = namespace + 'responses/' + digest + '.xml'
        receipt = dict(contract=CONTRACT, id=d['id'], definition_sha256=CATALOGUE_HASH, url=d['source_url'],
            http_status=200, received_at=now.isoformat(), sha256=digest, bytes=len(raw), retained_key=key,
            retained_url='https://justhodl.ai/' + key, complete_response_retained=True,
            headers={k: v for k, v in headers.items() if k.lower() in ('content-type', 'etag', 'last-modified')})
        _put(store, bucket, key, raw, ContentType='application/xml', CacheControl='public, max-age=31536000, immutable')
        _put(store, bucket, namespace + 'current.json', json.dumps(receipt).encode(), ContentType='application/json', CacheControl='no-cache')
        return raw, dict(receipt, snapshot_age_s=0)
    except urllib.error.HTTPError as exc:
        blocked = ROOT if exc.code in (401, 403, 429) else namespace
        _put(store, bucket, blocked + 'blocked.json', json.dumps({'contract': CONTRACT, 'id': d['id'], 'http_status': exc.code, 'received_at': now.isoformat(), 'retry_allowed': False}).encode(), ContentType='application/json', CacheControl='no-cache')
        raise SourceUnavailable('IMF HTTP ' + str(exc.code) + '; no fallback or retry') from exc
    except (ValueError, OSError, ET.ParseError) as exc:
        raise SourceUnavailable('IMF acquisition failed; shared daily claim retained') from exc


def packet(sid, raw, receipt):
    d = definition(sid)
    validate_receipt(receipt, d, raw)
    obs, records, attributes, dataset, groups = parsed(raw, d)
    n = sum(value is not None for _, value in obs)
    extract = json.dumps({'series_attributes': attributes, 'groups': groups, 'observations': [r['original'] for r in records]}, separators=(',', ':'), ensure_ascii=False).encode()
    cfg = CATALOGUE['flows'][d['flow']]
    flags = Counter(r['original'].get('STATUS') for r in records if r['original'].get('STATUS'))
    rejected = sum(r['rejection'] is not None for r in records)
    return dict(contract=CONTRACT, definition_sha256=CATALOGUE_HASH, id=d['id'], requested_id=sid, provider='imf',
        provider_name='IMF', name=d['name'], unit=d['unit'], freq=d['freq'], definition=d, source=d['source_url'],
        source_receipts=[receipt], acquired_at=receipt['received_at'], source_published_at=dataset.get('PUBLICATION_DATE'),
        source_updated_at=dataset.get('UPDATE_DATE'), source_dataset_attributes=dataset, source_series_attributes=attributes, source_group_metadata=groups,
        definition_source_receipt=cfg['metadata_receipt'], obs=obs, n=n, first=obs[0][0] if obs else None,
        last=obs[-1][0] if obs else None, last_valid=next((t for t, v in reversed(obs) if v is not None), None),
        source_extract={'scope': 'All original attributes of the selected public series and each observation from the complete retained XML response',
            'sha256': hashlib.sha256(extract).hexdigest(), 'bytes': len(extract), 'body_encoding': 'gzip+base64',
            'body_base64': base64.b64encode(gzip.compress(extract, mtime=0)).decode()},
        measurement_evidence={'columns': ['source_observation', 'original_period', 'chart_anchor', 'value', 'original_value', 'status', 'derivation', 'rejection', 'binary64_rounding'],
            'rows': [[r['ordinal'], r['original'].get('TIME_PERIOD'), r['anchor'], r['value'], r['original'].get('OBS_VALUE'), r['original'].get('STATUS'), r['original'].get('DERIVATION_TYPE'), r['rejection'], r['binary64_rounding']] for r in records]},
        source_status_definitions={k: cfg['status_definitions'][k] for k in flags if k in cfg['status_definitions']},
        quality={'status': 'unavailable' if not n else 'partial' if rejected or flags else 'observations', 'error': None,
            'received_rows': len(records), 'rejected_rows': rejected, 'source_status_counts': dict(flags),
            'plot_rounding_rows': sum(r['binary64_rounding'] for r in records)},
        history={'response_complete': True, 'full_upstream_history_verified': False, 'point_in_time_vintages_verified': False,
            'release_clock_verified': False, 'latest_reference_period_verified': False, 'missing_periods_filled': False,
            'observation_clock': 'Source reference period anchored to its first calendar day; not a release date or daily observation',
            'status_flags_preserved': True, 'forecasts_in_historical_plot': False,
            'numeric_representation': 'Original decimal strings remain in source evidence; chart coordinates use nearest finite binary64 values, with rounding identified per observation. No unit rescaling is applied.'},
        equivalence_to_watchlist_provider_verified=False, calls_eligible=False, sizing_eligible=False)


def fetch(sid, store, bucket, reader=None, now=None):
    d = definition(sid)
    try:
        raw, receipt = snapshot(d, store, bucket, reader, now)
        out = packet(sid, raw, receipt)
    except (SourceUnavailable, ValueError, KeyError, TypeError, UnicodeError, ET.ParseError) as exc:
        out = dict(contract=CONTRACT, definition_sha256=CATALOGUE_HASH, id=d['id'], requested_id=sid, provider='imf',
            provider_name='IMF', definition=d, name=d['name'], unit=d['unit'], freq=d['freq'], source=d['source_url'],
            obs=[], n=0, first=None, last=None, last_valid=None, acquired_at=None, source_published_at=None, source_receipts=[],
            quality={'status': 'unavailable', 'error': str(exc)[:200], 'received_rows': 0, 'rejected_rows': 0},
            history={'response_complete': False, 'full_upstream_history_verified': False, 'point_in_time_vintages_verified': False,
                'release_clock_verified': False, 'latest_reference_period_verified': False, 'missing_periods_filled': False},
            equivalence_to_watchlist_provider_verified=False, calls_eligible=False, sizing_eligible=False)
    if len(json.dumps(out).encode()) > 3900000:
        raise ValueError('IMF evidence exceeds response budget; no truncated history returned')
    return out


def cache_valid(value, sid):
    try:
        d = definition(sid)
    except ValueError:
        return False
    return (isinstance(value, dict) and value.get('contract') == CONTRACT and value.get('id') == d['id']
        and value.get('definition_sha256') == CATALOGUE_HASH and value.get('definition') == d
        and value.get('history', {}).get('response_complete') is True and not value.get('quality', {}).get('error'))
