"""Complete, retained recession research; no probability or portfolio authority.

One native run retains its inputs and calculations before publishing one CAS
head. Stored derived inputs are not historical point-in-time provider originals.
"""
from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import hashlib
import json
import math
import re
import urllib.error
import urllib.parse
import urllib.request

HEAD = 'data/global-recession.json'
INPUTS = ('data/global-business-cycle.json', 'data/oecd-cli.json',
          'data/portwatch.json', 'data/china-liquidity.json', 'data/indicator-bus.json')
PRIVATE = 'audit-private/20260909-originals/global-recession-research/'
LIMIT = 64 * 1024 * 1024
PERMISSIONS = ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')
COMPILERS = {'lambda_function.py', 'recession_research.py'}


class EvidenceError(RuntimeError):
    pass


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def strict(raw):
    def pairs(rows):
        result = {}
        for key, value in rows:
            if key in result:
                raise EvidenceError('Duplicate JSON key')
            result[key] = value
        return result
    def invalid(_):
        raise EvidenceError('Nonfinite JSON value')
    try:
        result = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid)
        encode(result)  # Reject exponent overflow too.
        return result
    except Exception:
        raise EvidenceError('Invalid complete JSON representation') from None


def clock(value):
    at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if at.tzinfo is None:
        raise EvidenceError('Aware publication clock required')
    return at.astimezone(timezone.utc)


def bounded(body):
    pieces, size = [], 0
    try:
        while True:
            part = body.read(min(65536, LIMIT + 1 - size))
            if not part:
                return b''.join(pieces)
            size += len(part)
            if size > LIMIT:
                raise EvidenceError('Whole response exceeds retention bound')
            pieces.append(part)
    finally:
        body.close()


def http_body(response):
    try:
        declared = getattr(response, 'headers', {}).get('Content-Length')
        raw = bounded(response)
        if declared is not None and (not re.fullmatch('[0-9]+', str(declared)) or int(declared) != len(raw)):
            raise EvidenceError('Incomplete HTTP representation')
        return raw
    except Exception:
        raise EvidenceError('Whole provider response could not be retained') from None


def retain(client, bucket, raw):
    if not isinstance(raw, bytes) or len(raw) > LIMIT:
        raise EvidenceError('Complete bounded bytes required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw,
                          ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise EvidenceError('Protected original retention failed') from None
    try:
        if bounded(client.get_object(Bucket=bucket, Key=ref['key'])['Body']) != raw:
            raise EvidenceError('Retained complete bytes differ')
    except Exception:
        raise EvidenceError('Protected original readback failed') from None
    return ref


def read(client, bucket, key):
    if key not in (*INPUTS, HEAD):
        raise EvidenceError('Unreviewed object read')
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('NoSuchKey', '404'):
            return None
        raise EvidenceError('Reviewed object read denied or failed') from None
    raw = bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw) or not obj.get('ETag'):
        raise EvidenceError('Complete versioned object required')
    packet = strict(raw)
    if not isinstance(packet, dict):
        raise EvidenceError('Structured input required')
    return {'raw': raw, 'packet': packet, 'etag': obj['ETag'],
            'last_modified': obj.get('LastModified').isoformat() if isinstance(obj.get('LastModified'), datetime) else None}


def projection(packet):
    output = deepcopy(packet)
    output['unqualified_legacy_calculation'] = deepcopy(packet)
    output.update(contract='global-recession-research.v1', call=None, portfolio_action='WAIT',
                  global_recession_prob_pct=None, band='UNQUALIFIED RESEARCH',
                  **{key: False for key in PERMISSIONS})
    output['decision'] = {'verb': 'WAIT', 'meaning': 'abstain',
                          'reason': 'No validated recession forecast or portfolio protocol.'}
    output['quality'] = {'status': 'unverified', 'scope': 'Retained research; definitions, vintages and predictive performance remain unqualified.'}
    output['research_limits'] = {
        'classification': 'Synthetic model labels, not official economic-cycle determinations.',
        'weights': 'Configured nominal country weights; not a validated current GDP estimate.',
        'confirmation': 'Legacy agreement rules; common inputs and stale observations are not independent forecast validation.',
        'momentum': 'six_month_change is already a percentage change. The inherited multiplier 40 and clip +/-11 are unvalidated.',
        'us_curve': 'The inherited logistic sigmoid of a latest daily spread is not the New York Fed monthly-average probit forecast.',
        'original_source_replay_verified': False, 'point_in_time_verified': False,
        'historical_provider_originals_reconstructed': False,
    }
    for row in output.get('countries', []):
        row.update(recession_prob_pct=None, contribution_pp=None,
                   model_classification_only=True, **{key: False for key in PERMISSIONS})
    for row in output.get('by_region', {}).values():
        row.update(recession_prob_pct=None, **{key: False for key in PERMISSIONS})
    if isinstance(output.get('breadth'), dict):
        output['breadth']['interpretation'] = 'Share of configured model weights carrying AT_RISK/RECESSION labels; not measured GDP in recession.'
    if isinstance(output.get('confirmation'), dict):
        block = output['confirmation']
        for key in ('unconfirmed_contribution_pp', 'divergent_contribution_pp', 'unconfirmed_share_of_global_pct'):
            block[key] = None
        for key in ('coverage_verdict', 'hard_legs', 'why'):
            block[key] = 'Unqualified agreement diagnostics; source definitions, vintages and independence have not been validated.'
    us = output.get('us_crosscheck', {})
    us['note'] = 'Separate FRED observations; capture is not model qualification.'
    if isinstance(us.get('yield_curve_probit'), dict):
        us['yield_curve_probit'].update(prob_12m_pct=None,
            method='Legacy field name retained for consumers; no qualified probability.',
            caveat='The legacy calculation uses a logistic link and latest daily spread, not the NY Fed monthly-average probit model.')
    output['equity_beta_guidance'] = {'rule': 'WAIT — abstain. No qualified equity-beta or duration instruction.',
                                    **{key: False for key in PERMISSIONS}}
    output['methodology'] = {
        'status': 'Unvalidated inherited heuristic; complete original method text and results retained under unqualified_legacy_calculation.',
        'actual_modifiers': {'cli_level': '(100 - cli_level) * 0.35, clipped to [-12,12]',
                            'momentum_6m': '-six_month_change * 40, clipped to [-11,11]',
                            'dist_200ma': '-dist_200ma_pct * 0.4, clipped to [-7,7]'},
        'calibration_verified': False,
    }
    output['not_macromicro'] = 'Independent unvalidated research; not a replication of a proprietary recession index.'
    output['caveats'] = list(output['research_limits'].values())[:5]
    if isinstance(output.get('bus_legs'), dict):
        output['bus_legs']['note'] = 'Unqualified inherited bus breadth; observation freshness and series definitions require separate validation.'
    return output


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Session:
    def __init__(self, client, bucket, started_at, now=None, opener=None):
        self.client, self.bucket, self.started_at = client, bucket, started_at
        self.started = clock(started_at)
        self.real_now = now or (lambda: datetime.now(timezone.utc))
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.prior = read(client, bucket, HEAD)
        if self.prior and clock(self.prior['packet']['generated_at']) >= self.started:
            raise EvidenceError('Predecessor is newer than this run')
        self.predecessor = retain(client, bucket, self.prior['raw']) if self.prior else None
        self.operations, self.clocks, self.pending = [], [], []
        self.inputs = set()

    def now(self, tz=None):
        value = self.real_now()
        if value.tzinfo is None or not 0 <= (value-self.started).total_seconds() <= 300:
            raise EvidenceError('Native calculation clock outside run')
        self.clocks.append(value.isoformat())
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    def feed(self, key):
        if key == HEAD:
            if len(self.pending) != 1:
                raise EvidenceError('Only the buffered first calculation may be reloaded')
            return deepcopy(self.pending[0]['packet'])
        if key not in INPUTS or key in self.inputs:
            raise EvidenceError('Unreviewed or repeated derived input')
        self.inputs.add(key)
        obj = read(self.client, self.bucket, key)
        item = {'kind': 'derived_s3', 'path': key, 'acquired_at': self.real_now().isoformat(),
                'status': 'missing' if obj is None else 'retained'}
        if obj:
            item.update(original=retain(self.client, self.bucket, obj['raw']), etag=obj['etag'], last_modified=obj['last_modified'],
                        producer_generated_at=obj['packet'].get('generated_at'))
        self.operations.append(item)
        return deepcopy(obj['packet']) if obj else None

    def fred(self, request, timeout):
        url = urllib.parse.urlsplit(request.full_url)
        query = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        if (url.scheme != 'https' or url.netloc != 'api.stlouisfed.org' or url.path != '/fred/series/observations'
                or url.fragment or request.get_method() != 'GET'
                or set(query) != {'series_id', 'api_key', 'file_type', 'sort_order', 'limit'}
                or any(len(v) != 1 for v in query.values()) or query['series_id'][0] not in ('T10Y3M', 'SAHMCURRENT')
                or query['file_type'] != ['json'] or query['sort_order'] != ['desc'] or query['limit'] != ['30']):
            raise EvidenceError('Unreviewed provider request')
        item = {'kind': 'fred_http', 'series_id': query['series_id'][0], 'acquired_at': self.real_now().isoformat(),
                'request': {'method': 'GET', 'origin': 'https://api.stlouisfed.org', 'path': url.path,
                            'query_without_credentials': {k: v[0] for k, v in query.items() if k != 'api_key'}}}
        try:
            response = self.opener(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            try:
                error_raw = http_body(exc)
            except Exception:
                raise EvidenceError('Incomplete provider error response') from None
            item.update(status='http_error', http_status=exc.code, original=retain(self.client, self.bucket, error_raw))
            self.operations.append(item)
            raise RuntimeError('FRED HTTP response unavailable') from None
        except Exception as exc:
            if isinstance(exc, EvidenceError):
                raise
            item.update(status='transport_error', error_type=type(exc).__name__)
            self.operations.append(item)
            raise RuntimeError('FRED transport unavailable') from None
        try:
            raw = http_body(response)
        except Exception:
            raise EvidenceError('Incomplete provider response cannot be published') from None
        item.update(status='retained', http_status=getattr(response, 'status', 200), original=retain(self.client, self.bucket, raw))
        self.operations.append(item)
        # Retain the original malformed response too, then fail before publication.
        packet = strict(raw)
        if not isinstance(packet, dict) or not isinstance(packet.get('observations'), list) or any(not isinstance(row, dict) for row in packet['observations']):
            raise EvidenceError('Complete provider observations structure required')
        return BytesIO(raw)

    def put_object(self, **kwargs):
        if kwargs.get('Bucket') != self.bucket or kwargs.get('Key') != HEAD or len(self.pending) >= 2:
            raise EvidenceError('Only the two reviewed native head stages are allowed')
        raw = kwargs.get('Body')
        if not isinstance(raw, bytes) or not 0 < len(raw) <= LIMIT:
            raise EvidenceError('Whole calculated output required')
        packet = strict(raw)
        # Native clock rounds to seconds; allow only that sub-second truncation.
        if not isinstance(packet, dict) or not -1 < (clock(packet['generated_at'])-self.started).total_seconds() <= 300:
            raise EvidenceError('Calculated output clock outside run')
        self.pending.append({'raw': raw, 'packet': packet})
        return {'publication_state': 'buffered_not_published'}

    def finish(self, compilers):
        if (len(self.pending) != 2 or self.inputs != set(INPUTS) or set(compilers) != COMPILERS
                or any(not isinstance(v, str) or not re.fullmatch('[a-f0-9]{64}', v) for v in compilers.values())):
            raise EvidenceError('Complete native stages, inputs and compiler closure required')
        final = self.pending[-1]['packet']
        if not isinstance(final.get('bus_legs'), dict):
            raise EvidenceError('Native bus stage did not complete')
        calculations = [retain(self.client, self.bucket, p['raw']) for p in self.pending]
        requested = [r['series_id'] for r in self.operations if r['kind'] == 'fred_http']
        if requested not in ([], ['T10Y3M', 'SAHMCURRENT']):
            raise EvidenceError('Incomplete native provider acquisition sequence')
        manifest = {'contract': 'recession-native-acquisition.v1', 'started_at': self.started_at,
                    'compiler_sha256': compilers, 'predecessor': self.predecessor,
                    'operations': self.operations, 'processing_clocks': self.clocks,
                    'complete_native_stages': calculations, 'derived_inputs_atomic': False,
                    'fred_acquisition': 'native_requests_retained' if requested else 'not_requested_by_native_configuration',
                    'historical_point_in_time_verified': False, 'forecast_qualified': False}
        manifest_ref = retain(self.client, self.bucket, encode(manifest))
        public = projection(final)
        public['publication_context'] = {
            'contract': 'recession-retained-publication.v1', 'compiler_sha256': compilers,
            'acquisition_manifest': manifest_ref, 'predecessor': self.predecessor,
            'complete_native_stages': calculations, 'source_count': len(self.operations),
            'input_versions': [{k: v for k, v in row.items() if k != 'etag'} for row in self.operations if row['kind'] == 'derived_s3'],
            'fred_acquisition': manifest['fred_acquisition'],
            'single_conditional_head_write': True, 'original_source_replay_verified': False,
            'point_in_time_verified': False,
        }
        raw = encode(public)
        intended = retain(self.client, self.bucket, raw)
        attempt = retain(self.client, self.bucket, encode({'contract': 'recession-publication-attempt.v1',
            'status': 'planned_bytes_only', 'predecessor': self.predecessor, 'intended': intended}))
        actual = read(self.client, self.bucket, HEAD)
        if (actual is None) != (self.prior is None) or actual and (actual['etag'] != self.prior['etag'] or actual['raw'] != self.prior['raw']):
            raise EvidenceError('Head changed during compilation')
        self.client.put_object(Bucket=self.bucket, Key=HEAD, Body=raw, ContentType='application/json',
            CacheControl='public, max-age=600, s-maxage=600',
            **({'IfMatch': self.prior['etag']} if self.prior else {'IfNoneMatch': '*'}))
        return {'manifest': manifest_ref, 'output': intended, 'attempt': attempt}


def compiler_hashes():
    return {name: sha((Path(__file__).parent/name).read_bytes()) for name in COMPILERS}


def replay_native(module, manifest, fetch):
    """Replay only retained native calculations offline with their exact clocks.

    Caller imports the verified compiler with AWS construction stubbed. `fetch`
    retrieves only hash-bound protected evidence. No provider/network acquisition
    or public write is available through this adapter.
    """
    if manifest.get('contract') != 'recession-native-acquisition.v1' or manifest.get('compiler_sha256') != compiler_hashes():
        raise EvidenceError('Replay requires the exact complete compiler closure')
    operations = deepcopy(manifest['operations'])
    clocks = list(manifest['processing_clocks'])
    stages = list(manifest['complete_native_stages'])
    originals = (module.s3, module.datetime, module._research_session, module.FRED_KEY)
    def retained(ref):
        if (not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256')))
                or ref.get('key') != PRIVATE + ref['sha256'] + '.bin'
                or type(ref.get('bytes')) is not int or not 0 <= ref['bytes'] <= LIMIT):
            raise EvidenceError('Invalid retained replay identity')
        raw = fetch(ref['key'])
        if len(raw) != ref['bytes'] or sha(raw) != ref['sha256']:
            raise EvidenceError('Replay retained body differs')
        return raw
    class ReplayClock(originals[1]):
        @classmethod
        def now(cls, tz=None):
            if not clocks:
                raise EvidenceError('Replay calculation clock exhausted')
            value = clock(clocks.pop(0))
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    class Replay:
        pending = []
        def feed(self, key):
            if key == HEAD:
                if len(self.pending) != 1:
                    raise EvidenceError('Replay head stage mismatch')
                return strict(self.pending[0])
            if not operations:
                raise EvidenceError('Replay input exhausted')
            item = operations.pop(0)
            if key not in INPUTS or (item.get('kind'), item.get('path')) != ('derived_s3', key):
                raise EvidenceError('Replay input order differs')
            if item.get('status') == 'missing':
                return None
            if item.get('status') != 'retained':
                raise EvidenceError('Unsupported replay input state')
            return strict(retained(item['original']))
        def fred(self, request, timeout):
            if not operations:
                raise EvidenceError('Replay provider exhausted')
            item = operations.pop(0)
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
            safe = {k: v[0] for k, v in query.items() if k != 'api_key' and len(v) == 1}
            if item.get('kind') != 'fred_http' or safe != item.get('request', {}).get('query_without_credentials'):
                raise EvidenceError('Replay provider request differs')
            if item.get('status') in ('http_error', 'transport_error'):
                if 'original' in item:
                    retained(item['original'])
                raise RuntimeError('Retained provider unavailable')
            if item.get('status') != 'retained':
                raise EvidenceError('Unsupported provider replay state')
            return BytesIO(retained(item['original']))
        def put_object(self, **kwargs):
            if kwargs.get('Bucket') != module.BUCKET or kwargs.get('Key') != HEAD or len(self.pending) >= 2:
                raise EvidenceError('Unreviewed replay write')
            self.pending.append(kwargs['Body'])
            return {'publication_state': 'offline_not_published'}
    adapter = Replay()
    module.s3, module.datetime, module._research_session = adapter, ReplayClock, adapter
    module.FRED_KEY = 'offline-replay-only' if any(r.get('kind') == 'fred_http' for r in operations) else ''
    try:
        module._produce_with_bus(None, None)
        if operations or clocks or len(adapter.pending) != 2 or len(stages) != 2:
            raise EvidenceError('Incomplete acquisition/clock/stage replay')
        for raw, ref in zip(adapter.pending, stages):
            if raw != retained(ref):
                raise EvidenceError('Whole native calculation differs')
        return {'contract': 'recession-native-replay.v1', 'complete_native_stages': 2,
                'input_operations': len(manifest['operations']), 'processing_clocks': len(manifest['processing_clocks']),
                'final_calculation_sha256': sha(adapter.pending[-1]),
                'point_in_time_verified': False, 'forecast_qualified': False}
    finally:
        module.s3, module.datetime, module._research_session, module.FRED_KEY = originals
