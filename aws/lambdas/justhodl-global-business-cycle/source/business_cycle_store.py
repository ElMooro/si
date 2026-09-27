"""Whole predecessor/output retention and unqualified business-cycle research.

These are derived model outputs, not a qualified recession forecast, validated
allocation protocol, original-provider archive or point-in-time reconstruction.
"""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re

HEAD = 'data/global-business-cycle.json'
HISTORY = 'data/global-business-cycle-history.json'
COMPOSITE = 'data/global-business-cycle-composite-history.json'
KEYS = (HEAD, HISTORY, COMPOSITE)
PRIVATE = 'audit-private/20260909-originals/global-business-cycle-research/'
LIMIT = 64 * 1024 * 1024
PERMISSIONS = ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')
COMPILERS = {'lambda_function.py', 'cycle_composite.py', '_fred_shim.py',
             'business_cycle_store.py', 'business_cycle_acquisition.py', 'managed_secret.py'}


class PublicationError(RuntimeError):
    pass


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def strict(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise PublicationError('Duplicate business-cycle JSON key')
            value[key] = item
        return value
    def invalid(value):
        raise PublicationError('Nonfinite business-cycle JSON')
    value = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid)
    encode(value)
    return value


def clock(value):
    at = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if at.tzinfo is None:
        raise PublicationError('Aware business-cycle clock required')
    return at.astimezone(timezone.utc)


def bounded(stream):
    parts, count = [], 0
    try:
        while True:
            part = stream.read(min(65536, LIMIT+1-count))
            if not part:
                break
            count += len(part)
            if count > LIMIT:
                raise PublicationError('Complete business-cycle representation exceeds bound')
            parts.append(part)
        return b''.join(parts)
    finally:
        stream.close()


def read(client, bucket, key):
    if key not in KEYS:
        raise PublicationError('Only fixed business-cycle public predecessors allowed')
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('NoSuchKey', '404'):
            return None
        raise
    raw = bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw) or not obj.get('ETag'):
        raise PublicationError('Complete versioned business-cycle predecessor required')
    packet = strict(raw)
    if not isinstance(packet, dict):
        raise PublicationError('Whole structured business-cycle packet required')
    clock(packet['generated_at'])
    return {'raw': raw, 'packet': packet, 'etag': obj['ETag']}


def retain(client, bucket, raw):
    if not isinstance(raw, bytes) or not 0 <= len(raw) <= LIMIT:
        raise PublicationError('Complete bounded business-cycle bytes required')
    ref = {'key': PRIVATE+sha(raw)+'.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    if bounded(client.get_object(Bucket=bucket, Key=ref['key'])['Body']) != raw:
        raise PublicationError('Retained business-cycle bytes differ')
    return ref


def research_projection(packet):
    """Preserve complete computed model fields, withhold unsupported directions."""
    output = deepcopy(packet)
    output.update(contract='global-business-cycle-research.v1', call=None, portfolio_action='WAIT',
                  **{key: False for key in PERMISSIONS})
    output['quality'] = {'status': 'unverified', 'scope': 'Derived model research only; source identity, publication vintages and predictive qualification remain open.'}
    output['decision'] = {'verb': 'WAIT', 'meaning': 'abstain',
                          'reason': 'No validated cycle forecast or portfolio protocol is available.'}
    output['research_limits'] = {
        'phases': 'Synthetic model classifications, not official economic-cycle determinations.',
        'confidence': 'Legacy coverage/agreement heuristic, not a calibrated probability.',
        'weights': 'Configured country and pillar weights; GDP vintage and investment optimality are unverified.',
        'history': 'Current-vintage model calculations; historical release availability, source adjustments and overlapping returns are unqualified.',
        'physical': 'Legacy port comparison including carried observations; agreement is not independent forecast validation.',
        'original_source_replay_verified': False, 'point_in_time_verified': False,
    }
    interpretation = output.get('interpretation')
    if isinstance(interpretation, dict):
        output['unqualified_legacy_interpretation'] = deepcopy(interpretation)
        output['interpretation'] = {
            **interpretation,
            'decisive_call': 'WAIT — abstain. The synthetic cycle readings have no validated allocation authority.',
            'cross_asset': {name: {**row, 'signal': None, 'rationale': 'No qualified cycle allocation signal.',
                                   **{key: False for key in PERMISSIONS}}
                            for name, row in (interpretation.get('cross_asset') or {}).items()},
            'country_tilts': {name: {**row, 'tilt': None, **{key: False for key in PERMISSIONS}}
                              for name, row in (interpretation.get('country_tilts') or {}).items()},
            **{key: False for key in PERMISSIONS},
        }
    fit = output.get('downturn_probability_6m')
    if isinstance(fit, dict):
        output['unqualified_in_sample_fit'] = deepcopy(fit)
        output['downturn_probability_6m'] = {
            **fit, 'ok': False, 'probability_now': None,
            'reason': 'In-sample fit has no validated forecast probability.',
            **{key: False for key in PERMISSIONS},
        }
    for row in (output.get('by_country') or {}).values():
        if isinstance(row, dict):
            row.update(model_classification_only=True, **{key: False for key in PERMISSIONS})
    return output


class PublicationClient:
    """Buffer only the three existing writes; preserve other native read behavior."""
    def __init__(self, client, bucket, started_at):
        self.client, self.bucket, self.started_at = client, bucket, started_at
        self.started = clock(started_at)
        self.prior = {key: read(client, bucket, key) for key in KEYS}
        if any(value and clock(value['packet']['generated_at']) >= self.started for value in self.prior.values()):
            raise PublicationError('Business-cycle predecessor is newer than this run')
        self.predecessors = {key: retain(client, bucket, value['raw']) if value else None for key, value in self.prior.items()}
        self.pending = {}

    def __getattr__(self, name):
        # Existing producer reads/paginators keep their original scope. Only the
        # reviewed put_object output path is intercepted; no extra input is read.
        if name not in ('get_object', 'get_paginator'):
            raise PublicationError('Unreviewed business-cycle client operation')
        return getattr(self.client, name)

    def put_object(self, **kwargs):
        key = kwargs.get('Key')
        if kwargs.get('Bucket') != self.bucket or key not in KEYS or key in self.pending:
            raise PublicationError('Only one complete write to each reviewed output allowed')
        raw = kwargs.get('Body')
        if not isinstance(raw, bytes) or not 0 < len(raw) <= LIMIT:
            raise PublicationError('Whole business-cycle output bytes required')
        packet = strict(raw)
        if not isinstance(packet, dict) or not 0 <= (clock(packet['generated_at'])-self.started).total_seconds() <= 900:
            raise PublicationError('Output clock outside native business-cycle run')
        self.pending[key] = {'raw': raw, 'packet': packet}
        return {'ResponseMetadata': {'HTTPStatusCode': 200}, 'publication_state': 'buffered_not_published'}

    def finish(self, compilers, acquisition=None):
        if (not {HEAD, HISTORY}.issubset(self.pending) or not isinstance(compilers, dict)
                or set(compilers) != COMPILERS
                or any(not isinstance(v, str) or not re.fullmatch(r'[0-9a-f]{64}', v) for v in compilers.values())):
            raise PublicationError('Complete current/history outputs and compiler closure required')
        raw_outputs = {key: retain(self.client, self.bucket, value['raw']) for key, value in self.pending.items()}
        context = {'contract': 'business-cycle-derived-publication.v1', 'started_at': self.started_at,
                   'compiler_sha256': compilers, 'predecessors': self.predecessors,
                   'complete_unmodified_calculations': raw_outputs,
                   'public_projections_atomic': False, 'original_source_replay_verified': False,
                   'unchanged_output_keys': [key for key in KEYS if key not in self.pending]}
        if acquisition is not None:
            context['native_acquisition'] = acquisition
        outputs = {key: encode({**research_projection(value['packet']), 'publication_context': context}) for key, value in self.pending.items()}
        refs = {key: retain(self.client, self.bucket, raw) for key, raw in outputs.items()}
        attempt = retain(self.client, self.bucket, encode({'contract': 'business-cycle-publication-attempt.v1',
            'status': 'planned_bytes_only', 'started_at': self.started_at, 'public_projections_atomic': False,
            'predecessors': self.predecessors, 'intended': refs}))
        for key, prior in self.prior.items():
            actual = read(self.client, self.bucket, key)
            if (actual is None) != (prior is None) or actual and (actual['etag'] != prior['etag'] or actual['raw'] != prior['raw']):
                raise PublicationError('Business-cycle head changed during compilation')
        # Histories first, headline last. S3 objects remain explicitly non-atomic;
        # readers must use bound retained references, never mix mutable heads.
        for key in (HISTORY, COMPOSITE, HEAD):
            if key not in outputs:
                continue
            prior = self.prior[key]
            self.client.put_object(Bucket=self.bucket, Key=key, Body=outputs[key], ContentType='application/json',
                CacheControl='public, max-age=3600, s-maxage=3600',
                **({'IfMatch': prior['etag']} if prior else {'IfNoneMatch': '*'}))
        return {'attempt': attempt, 'outputs': refs}


def compiler_hashes():
    root = Path(__file__).parent
    import managed_secret
    result = {name: sha((root/name).read_bytes()) for name in (
        'lambda_function.py', 'cycle_composite.py', '_fred_shim.py', 'business_cycle_store.py', 'business_cycle_acquisition.py')}
    result['managed_secret.py'] = sha(Path(managed_secret.__file__).read_bytes())
    return result
