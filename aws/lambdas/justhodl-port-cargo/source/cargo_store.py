"""Retain complete Port Cargo inputs and replay native calculations privately.

This contract preserves the inherited model without endorsing its definitions,
missing-value policy or forecasts. Conditional writes never undo a newer writer.
"""
from datetime import datetime, timezone
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from copy import deepcopy
import hashlib, json, math, re, time, urllib.request, urllib.error, urllib.parse

HEAD = 'data/port-cargo.json'
CHOICE = 'data/warm/portwatch/layer-choice.json'
PORTWATCH = 'data/portwatch.json'
GRAPH = 'data/impact/exposure-graph.json'
BETAS = 'data/impact/betas.json'
KEYS = (HEAD, CHOICE, PORTWATCH, GRAPH, BETAS)
READS = (CHOICE, PORTWATCH, GRAPH, BETAS)
PRIVATE = 'audit-private/20260909-originals/port-cargo-research/'
BASE = 'https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services'
LIMIT = 64 * 1024 * 1024
CONTRACT = 'port-cargo-preserved-calculation.v1'
COMPILERS = ('lambda_function.py', 'cargo_store.py', 'cargo_measurements.py', 'impact_mapper.py')
sha = lambda raw: hashlib.sha256(raw).hexdigest()
encode = lambda v: json.dumps(v, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')
class CaptureError(ValueError): pass


def strict(raw):
    def pairs(items):
        d={}
        for k,v in items:
            if k in d:raise CaptureError('Duplicate JSON key')
            d[k]=v
        return d
    def invalid(x):raise CaptureError('Nonfinite JSON number')
    def number(x):
        n=float(x)
        if not math.isfinite(n):invalid(x)
        return n
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=invalid)


def whole(stream,length=None):
    chunks=[];size=0
    try:
        while True:
            raw=stream.read(min(65536,LIMIT+1-size))
            if not raw:break
            if not isinstance(raw,bytes):raise CaptureError('Bytes required')
            size+=len(raw);chunks.append(raw)
            if size>LIMIT:raise CaptureError('Whole input exceeds byte bound')
    finally:stream.close()
    raw=b''.join(chunks)
    if length is not None and (not str(length).isdigit() or int(length)!=len(raw)):raise CaptureError('Whole input length differs')
    return raw


def hashes():
    import impact_mapper
    return {name: sha((Path(impact_mapper.__file__) if name == 'impact_mapper.py' else Path(__file__).parent/name).read_bytes()) for name in COMPILERS}


def retained(s3,bucket,ref):
    if (not isinstance(ref,dict) or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256')))
        or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<=ref['bytes']<=LIMIT):
        raise CaptureError('Exact protected identity required')
    obj=s3.get_object(Bucket=bucket,Key=ref['key']);raw=whole(obj['Body'],obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise CaptureError('Complete retained body differs')
    return raw


def retain(s3,bucket,raw):
    if not isinstance(raw,bytes) or len(raw)>LIMIT:raise CaptureError('Bounded original required')
    ref={'key':PRIVATE+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}
    try:s3.put_object(Bucket=bucket,Key=ref['key'],Body=raw,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
    except Exception as e:
        if str(getattr(e,'response',{}).get('Error',{}).get('Code')) not in ('409','412','ConditionalRequestConflict','PreconditionFailed'):raise
    if retained(s3,bucket,ref)!=raw:raise CaptureError('Retained readback differs')
    return ref


def decode(raw):
    value = strict(raw)
    if not isinstance(value, dict):
        raise CaptureError('Complete JSON object required')
    return value


def identity(req, timeout):
    url = req.full_url
    parts = urllib.parse.urlsplit(url)
    method, body = req.get_method(), req.data
    if (parts.scheme != 'https' or parts.netloc != 'services9.arcgis.com' or parts.fragment
            or method not in ('GET', 'POST') or (method == 'GET' and body is not None)
            or (method == 'POST' and (parts.query or not isinstance(body, bytes)))):
        raise CaptureError('Only reviewed keyless provider query requests allowed')
    root = urllib.parse.urlsplit(BASE).path
    suffix = parts.path[len(root):] if parts.path.startswith(root) else None
    metadata = {'/Daily_Ports_Data/FeatureServer/0', '/PortWatch_ports_database/FeatureServer/0'}
    if suffix != '' and suffix not in metadata and (not isinstance(suffix, str) or not re.fullmatch(r'/[A-Za-z0-9_]*[Pp]orts?[A-Za-z0-9_]*/FeatureServer/[012]/query', suffix)):
        raise CaptureError('Unreviewed provider layer path')
    pairs = urllib.parse.parse_qsl(body.decode('utf-8') if body is not None else parts.query, keep_blank_values=True)
    params = dict(pairs)
    allowed = {'where', 'outFields', 'resultRecordCount', 'resultOffset', 'orderByFields',
               'returnCountOnly', 'returnIdsOnly', 'objectIds', 'returnGeometry', 'outStatistics', 'groupByFieldsForStatistics', 'f'}
    if len(pairs) != len(params) or set(params) - allowed or params.get('f') != 'pjson':
        raise CaptureError('Unreviewed provider parameters')
    if type(timeout) not in (int, float) or not 0 < timeout <= 60:
        raise CaptureError('Bounded native timeout required')
    return {'method': method, 'url': url, 'body_utf8': body.decode('utf-8') if body is not None else None, 'timeout': timeout}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs): return None


class Response(BytesIO):
    def __init__(self, raw, code=200, headers=None):
        super().__init__(raw)
        self.headers = headers or {}
        self.status = self.code = code
    def getcode(self): return self.code


class Session:
    def __init__(self, client, bucket, at, opener=None):
        self.client, self.bucket, self.at = client, bucket, at
        self.inputs, self.pending = {}, {}
        self.reads, self.http = [], []
        self.failure = None
        self.started = time.monotonic()
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        # These five existing public inputs were preserved in the full baseline.
        # A denied or malformed input cannot silently become an empty model.
        for key in KEYS:
            obj = client.get_object(Bucket=bucket, Key=key)
            raw = whole(obj['Body'], obj.get('ContentLength'))
            if type(obj.get('ContentLength')) is not int or not obj.get('ETag'):
                raise CaptureError('Whole versioned input required')
            original = retain(client, bucket, raw)
            decode(raw)
            self.inputs[key] = {'raw': raw, 'original': original, 'etag': obj['ETag'],
                                'last_modified': obj['LastModified'].isoformat()}

    def ready(self):
        if self.failure:
            raise CaptureError(self.failure)

    def get_object(self, **kw):
        self.ready()
        key = kw.get('Key')
        if kw != {'Bucket': self.bucket, 'Key': key} or key not in READS:
            self.failure = 'Unexpected input read'; self.ready()
        self.reads.append(key)
        return {'Body': BytesIO(self.inputs[key]['raw'])}

    def put_object(self, **kw):
        self.ready()
        key, raw = kw.get('Key'), kw.get('Body')
        if (kw.get('Bucket') != self.bucket or key not in (HEAD, CHOICE)
                or not isinstance(raw, bytes) or len(raw) > LIMIT or key in self.pending):
            self.failure = 'Unexpected native output'; self.ready()
        decode(raw)
        self.pending[key] = raw
        return {}

    def urlopen(self, req, timeout=None):
        self.ready()
        try:
            request = identity(req, timeout)
            remaining = 600 - (time.monotonic() - self.started)
            if remaining <= 0 or len(self.http) >= 292:
                raise CaptureError('Complete acquisition exceeds original runtime budget')
            at = datetime.now(timezone.utc).isoformat()
            try:
                response = self.opener(req, timeout=min(timeout, remaining))
            except urllib.error.HTTPError as exc:
                response = exc
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                row = {'request': request, 'acquired_at': at, 'status': 'transport_error', 'error_type': type(exc).__name__}
                row['attempt_manifest'] = retain(self.client, self.bucket, encode(row))
                self.http.append(row)
                raise CaptureError('Provider transport failed') from None
            headers = getattr(response, 'headers', {}) or {}
            code = response.getcode()
            raw = whole(response, headers.get('Content-Length'))
            row = {'request': request, 'acquired_at': at, 'status': 'http_response', 'http_status': code,
                   'headers': {k: headers[k] for k in ('Content-Type', 'Content-Length', 'Content-Encoding', 'Date', 'Last-Modified', 'ETag', 'Retry-After') if k in headers},
                   'original': retain(self.client, self.bucket, raw)}
            row['attempt_manifest'] = retain(self.client, self.bucket, encode(row))
            self.http.append(row)
            if code != 200 or decode(raw).get('error'):
                raise CaptureError('Provider error retained; partial calculation cannot publish')
            return Response(raw, code, headers)
        except Exception:
            self.failure = 'Whole provider acquisition failed'
            raise


def calculate(module, session):
    names = ('s3', 'datetime', 'urllib', 'time', 'LAYER', 'DATEFIELD', 'RESOLVER_PATH', '_REQUEST_COUNT', '_CARGO_REVIEW', '_CARGO_ACQUISITION')
    original = {k: getattr(module, k) for k in names}
    impact = module.impact_mapper
    previous = {k: getattr(impact, k) for k in ('_S3', '_CACHE', 'datetime')}
    stamp = datetime.fromisoformat(session.at)
    if stamp.tzinfo is None:
        raise CaptureError('Aware deterministic calculation clock required')
    class Frozen(original['datetime']):
        @classmethod
        def now(cls, tz=None): return stamp.astimezone(tz) if tz else stamp.replace(tzinfo=None)
    try:
        module.s3 = impact._S3 = session
        module.datetime = impact.datetime = Frozen
        # Warm Lambda memory must not reuse uncaptured graph/beta inputs.
        impact._CACHE = {}
        module.time = SimpleNamespace(time=lambda: stamp.timestamp())
        module.urllib = SimpleNamespace(parse=urllib.parse, request=SimpleNamespace(Request=urllib.request.Request, urlopen=session.urlopen))
        module.LAYER, module.DATEFIELD, module.RESOLVER_PATH = BASE + '/Daily_Ports_Data/FeatureServer/0/query', 'date', 'unresolved'
        module._REQUEST_COUNT, module._CARGO_REVIEW, module._CARGO_ACQUISITION = 0, None, None
        result = module._native_calculation()
        session.ready()
        if HEAD not in session.pending:
            raise CaptureError('Complete native result required')
        if not isinstance(module._CARGO_REVIEW, dict) or not isinstance(module._CARGO_ACQUISITION, dict):
            raise CaptureError('Complete source membership and calendar review required')
        session.measurements, session.acquisition = module._CARGO_REVIEW, module._CARGO_ACQUISITION
        return result
    finally:
        for key, value in original.items(): setattr(module, key, value)
        for key, value in previous.items(): setattr(impact, key, value)


def projection(calculation, context, measurements, acquisition):
    packet = deepcopy(calculation)
    packet.update(contract=CONTRACT, publication_context=context, forecast_qualified=False,
                  calls_eligible=False, sizing_eligible=False, execution_eligible=False, portfolio_action='WAIT',
                  research_limits='Source query membership and the separate measurement_review reproduce exact dated shipment comparisons over matched port cohorts. Legacy fields retain their inherited missing-as-zero arithmetic, ragged trimming, seasonal alignment, name joins and unqualified impact models. Neither field set is a customs-value, company-revenue or portfolio forecast.')
    packet['measurement_review'], packet['acquisition_review'] = measurements, acquisition
    packet['duration_s'] = context['capture_elapsed_s']
    packet['duration_basis'] = 'Complete retained input acquisition and calculation; excludes final conditional publication'
    return packet


def publication_context(manifest, ref):
    return {'contract': CONTRACT, 'manifest': ref, 'compiler_sha256': hashes(),
            'capture_elapsed_s': manifest['capture_elapsed_s'], 'original_source_replay_verified': False,
            'publication_atomic': False, 'point_in_time_verified': False}


def run(module, event=None, context=None, opener=None, at=None):
    client, bucket = module.s3, module.BUCKET
    at = at or datetime.now(timezone.utc).isoformat()
    session = Session(client, bucket, at, opener)
    result = calculate(module, session)
    if session.reads != list(READS):
        raise CaptureError('Complete original input sequence required')
    calculation = decode(session.pending[HEAD])
    if (calculation.get('fetch_status') != 'OK' or not calculation.get('n_rows_window')
            or any(not isinstance(gap, str) or not gap.startswith('ragged edge: trimmed ') for gap in calculation.get('gaps', []))):
        raise CaptureError('Incomplete native calculation cannot replace previous publication')
    previous = decode(session.inputs[HEAD]['raw'])
    if datetime.fromisoformat(previous['generated_at'].replace('Z', '+00:00')) >= datetime.fromisoformat(at):
        raise CaptureError('New publication cannot move the accepted clock backward')
    inputs = {k: {x: y for x, y in v.items() if x != 'raw'} for k, v in session.inputs.items()}
    outputs = {k: retain(client, bucket, raw) for k, raw in session.pending.items()}
    manifest = {'contract': CONTRACT, 'compiler_sha256': hashes(), 'calculation_at': at, 'inputs': inputs,
                'read_order': session.reads, 'http_attempts': session.http, 'complete_native_outputs': outputs,
                'native_return': result, 'inputs_atomic': False, 'point_in_time_verified': False,
                'duration_is_frozen_for_replay': True, 'capture_elapsed_s': round(time.monotonic() - session.started, 3)}
    manifest_ref = retain(client, bucket, encode(manifest))
    context = publication_context(manifest, manifest_ref)
    planned = {k: raw for k, raw in session.pending.items() if k != HEAD}
    planned[HEAD] = encode(projection(calculation, context, session.measurements, session.acquisition))
    retain(client, bucket, encode({'manifest': manifest_ref, 'publication_order': list(planned),
                                  'planned_outputs': {k: retain(client, bucket, raw) for k, raw in planned.items()}}))
    completed = []
    for key, raw in planned.items():
        try:
            # Explicit output keys keep the public-writer registry inspectable.
            if key == HEAD:
                client.put_object(Bucket=bucket, Key='data/port-cargo.json', Body=raw, IfMatch=inputs[key]['etag'],
                                  ContentType='application/json', CacheControl='public, max-age=1800')
            else:
                client.put_object(Bucket=bucket, Key='data/warm/portwatch/layer-choice.json', Body=raw, IfMatch=inputs[key]['etag'],
                                  ContentType='application/json', CacheControl='no-store')
        except Exception as exc:
            if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict'):
                return {'published': False, 'state': 'concurrent_writer_preserved', 'completed_paths': completed}
            raise
        completed.append(key)
    return {'published': True, 'generated_at': at, 'http_attempts': len(session.http), 'portfolio_action': 'WAIT'}


def replay(module, client, bucket, packet):
    context = packet.get('publication_context') or {}
    if (context.get('contract') != CONTRACT or context.get('compiler_sha256') != hashes()
            or any(context.get(k) is not False for k in ('original_source_replay_verified', 'publication_atomic', 'point_in_time_verified'))):
        raise CaptureError('Exact compiler and limitations required')
    manifest = decode(retained(client, bucket, context['manifest']))
    if manifest.get('contract') != CONTRACT or manifest.get('compiler_sha256') != hashes() or set(manifest['inputs']) != set(KEYS):
        raise CaptureError('Whole original manifest differs')
    if context != publication_context(manifest, context['manifest']):
        raise CaptureError('Public preservation context differs from retained manifest')
    class Replay:
        def __init__(self):
            self.at = manifest['calculation_at']; self.pending = {}; self.failure = None
            self.reads = list(manifest['read_order']); self.http = list(manifest['http_attempts'])
            self.inputs = {k: {**v, 'raw': retained(client, bucket, v['original'])} for k, v in manifest['inputs'].items()}
        ready = Session.ready
        put_object = Session.put_object
        bucket = module.BUCKET
        def get_object(self, **kw):
            key = kw.get('Key')
            if kw != {'Bucket': bucket, 'Key': key} or not self.reads or self.reads.pop(0) != key:
                self.failure = 'Native input read order differs'; self.ready()
            return {'Body': BytesIO(self.inputs[key]['raw'])}
        def urlopen(self, req, timeout=None):
            if not self.http:
                self.failure = 'Unexpected provider replay'; self.ready()
            row = self.http.pop(0)
            if row['request'] != identity(req, timeout) or row.get('status') != 'http_response' or row.get('http_status') != 200:
                self.failure = 'Native provider request differs'; self.ready()
            if decode(retained(client, bucket, row['attempt_manifest'])) != {k: v for k, v in row.items() if k != 'attempt_manifest'}:
                raise CaptureError('Whole attempt identity differs')
            return Response(retained(client, bucket, row['original']), 200, row['headers'])
    session = Replay()
    result = calculate(module, session)
    if session.reads or session.http or encode(result) != encode(manifest['native_return']) or set(session.pending) != set(manifest['complete_native_outputs']):
        raise CaptureError('Incomplete deterministic replay')
    for key, raw in session.pending.items():
        if raw != retained(client, bucket, manifest['complete_native_outputs'][key]):
            raise CaptureError('Whole native calculation differs')
    if encode(packet) != encode(projection(decode(session.pending[HEAD]), context, session.measurements, session.acquisition)):
        raise CaptureError('Public projection differs')
    return {'status': 'whole_native_calculation_replayed', 'http_attempts': len(manifest['http_attempts']),
            'complete_stored_inputs': len(KEYS), 'original_native_rows': packet['n_rows_window'],
            'calendar_port_rows': len(session.measurements['ports']), 'source_catalog_ports': session.measurements['catalog_ports'],
            'complete_query_membership_replayed': True, 'complete_calendar_measurements_replayed': True,
            'provider_requests': 0, 'public_writes': 0, 'point_in_time_verified': False, 'model_qualified': False}
