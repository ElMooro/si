"""Retain all original Macro Leads inputs and replay the complete calculation."""
from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO
from copy import deepcopy
from types import SimpleNamespace
import hashlib, json, math, re, time, urllib.request, urllib.error, urllib.parse
import macro_measurements as measurements
HEAD='data/macro-leads.json'
PRIVATE='audit-private/20260909-originals/macro-leads-research/'
CONTRACT='macro-leads-research.v1'
LIMIT=64*1024*1024
sha=lambda raw:hashlib.sha256(raw).hexdigest()
encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
COMPILERS=['lambda_function.py', 'macro_measurements.py', 'macro_store.py', 'xlrd-2.0.1.dist-info/INSTALLER', 'xlrd-2.0.1.dist-info/LICENSE', 'xlrd-2.0.1.dist-info/METADATA', 'xlrd-2.0.1.dist-info/RECORD', 'xlrd-2.0.1.dist-info/REQUESTED', 'xlrd-2.0.1.dist-info/WHEEL', 'xlrd-2.0.1.dist-info/top_level.txt', 'xlrd/__init__.py', 'xlrd/biffh.py', 'xlrd/book.py', 'xlrd/compdoc.py', 'xlrd/formatting.py', 'xlrd/formula.py', 'xlrd/info.py', 'xlrd/sheet.py', 'xlrd/timemachine.py', 'xlrd/xldate.py']


class EvidenceError(ValueError):
    pass


def strict(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise EvidenceError('Duplicate JSON key')
            out[key] = value
        return out
    def invalid(value):
        raise EvidenceError('Nonfinite JSON number')
    def number(value):
        result = float(value)
        if not math.isfinite(result):
            invalid(value)
        return result
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=number, parse_constant=invalid)


def whole(stream, length, limit=LIMIT, deadline=None):
    parts = []; size = 0
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                raise EvidenceError('Complete response deadline exhausted')
            part = stream.read(min(65536, limit + 1 - size))
            if not part:
                break
            if not isinstance(part, bytes):
                raise EvidenceError('Byte stream required')
            parts.append(part); size += len(part)
            if size > limit:
                raise EvidenceError('Complete body exceeds bound; never truncated')
    finally:
        stream.close()
    if length is not None and (not str(length).isdigit() or int(length) != size):
        raise EvidenceError('Whole response length differs')
    return b''.join(parts)


def get(client, bucket, key):
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey', 'NotFound'):
            return None
        raise EvidenceError('Object read failed') from None
    raw = whole(obj['Body'], obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or not isinstance(obj.get('ETag'), str) or not obj['ETag']:
        raise EvidenceError('Complete conditional object identity required')
    return {'raw': raw, 'etag': obj['ETag']}


def retained(client, bucket, ref):
    if (not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256')))
            or ref.get('key') != PRIVATE + ref['sha256'] + '.bin' or type(ref.get('bytes')) is not int or not 0 <= ref['bytes'] <= LIMIT):
        raise EvidenceError('Exact protected original identity required')
    found = get(client, bucket, ref['key'])
    if found is None or len(found['raw']) != ref['bytes'] or sha(found['raw']) != ref['sha256']:
        raise EvidenceError('Retained whole body differs')
    return found['raw']


def retain(client, bucket, raw):
    if not isinstance(raw, bytes) or len(raw) > LIMIT:
        raise EvidenceError('Bounded whole original required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*', ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict'):
            raise
    if retained(client, bucket, ref) != raw:
        raise EvidenceError('Retained readback differs')
    return ref


class Budget:
    def __init__(self, client, deadline):
        self.client, self.deadline = client, deadline
    def check(self):
        if time.monotonic() + 15 >= self.deadline:
            raise EvidenceError('Acquisition/publication runtime budget exhausted')
    def get_object(self, **kw):
        self.check(); return self.client.get_object(**kw)
    def put_object(self, **kw):
        self.check(); return self.client.put_object(**kw)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


COUNTRIES = ('US', 'EZ', 'JP', 'GB', 'CA', 'AU', 'CH', 'SE', 'NO', 'KR', 'MX', 'BR', 'IN', 'CN', 'ZA', 'PL', 'TR', 'ID', 'NZ', 'CL')
SERIES = {'HTRUCKSSAAR', 'FRGSHPUSM649NCIS', 'TSIFRGHT', 'RAILFRTINTERMODAL', 'TRUCKD11'} | {'IRSTCB01' + c + 'M156N' for c in COUNTRIES}
GPR = 'https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls'


def compiler_hashes():
    return {name: sha((Path(__file__).parent / name).read_bytes()) for name in COMPILERS}


def identity(request):
    req = urllib.request.Request(request) if isinstance(request, str) else request
    p = urllib.parse.urlsplit(req.full_url); pairs = urllib.parse.parse_qsl(p.query, keep_blank_values=True); query = dict(pairs)
    if p.scheme != 'https' or p.username or p.password or p.fragment or len(pairs) != len(query) or req.get_method() != 'GET' or req.data is not None:
        raise EvidenceError('Exact public GET source required')
    if p.netloc == 'api.stlouisfed.org':
        if query.get('series_id') not in SERIES or query.get('file_type') != 'json':
            raise EvidenceError('Undeclared FRED series')
        basic = {'series_id', 'api_key', 'file_type'}
        if p.path == '/fred/series/observations':
            if set(query) == basic | {'sort_order', 'limit'}:
                expected = '240' if query['series_id'] == 'HTRUCKSSAAR' else '60' if query['series_id'].startswith('IRSTCB01') else '40'
                if query['sort_order'] != 'desc' or query['limit'] != expected:
                    raise EvidenceError('Legacy declared observation request differs')
            elif set(query) == basic | {'realtime_start', 'realtime_end', 'observation_start', 'observation_end', 'sort_order', 'limit', 'offset', 'units', 'output_type'}:
                if (query['series_id'] != 'HTRUCKSSAAR' or query['observation_start'] != '1776-07-04' or query['sort_order'] != 'asc'
                        or query['limit'] != '100000' or query['offset'] != '0' or query['units'] != 'lin' or query['output_type'] != '1'):
                    raise EvidenceError('Complete truck observation request differs')
            else:
                raise EvidenceError('Undeclared observation transformation')
        elif p.path != '/fred/series' or set(query) != basic | {'realtime_start', 'realtime_end'} or query['series_id'] != 'HTRUCKSSAAR':
            raise EvidenceError('Undeclared FRED metadata request')
    elif p.netloc == 'query1.finance.yahoo.com':
        if p.path.removeprefix('/v8/finance/chart/') not in ('GC=F', 'SI=F', 'HG=F', 'xauusd', 'xagusd') or not p.path.startswith('/v8/finance/chart/') or query != {'interval': '1d', 'range': '2y'}:
            raise EvidenceError('Undeclared futures request')
    elif req.full_url != GPR:
        raise EvidenceError('Undeclared source host or path')
    return {'method': 'GET', 'endpoint': p.scheme + '://' + p.netloc + p.path,
            'parameters': {k: v for k, v in query.items() if k not in ('api_key', 'apikey')}}


class Response(BytesIO):
    def __init__(self, raw, headers=None):
        super().__init__(raw); self.headers = headers or {}; self.status = 200
    def getcode(self):
        return 200


class Session:
    def __init__(self, client, bucket, at, opener=None):
        self.client, self.bucket, self.at = client, bucket, at
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.http = []; self.pending = None; self.failure = None; self.rate_limited = set()
        self.started = time.monotonic(); self.raws = {}; self.total_bytes = 0
    def ready(self):
        if self.failure:
            raise EvidenceError(self.failure)
    def record(self, row):
        try:
            row['attempt_manifest'] = retain(self.client, self.bucket, encode(row)); self.http.append(row)
        except Exception:
            self.failure = 'Complete source-attempt retention failed'; self.ready()
    def put_object(self, **kw):
        self.ready()
        if kw.get('Bucket') != self.bucket or kw.get('Key') != HEAD or self.pending is not None or not isinstance(kw.get('Body'), bytes):
            self.failure = 'Unexpected native writer'; self.ready()
        raw = kw['Body']; doc = strict(raw)
        if len(raw) > LIMIT or not isinstance(doc, dict) or doc.get('generated_at') != self.at:
            self.failure = 'Incomplete native publication'; self.ready()
        self.pending = raw
        return {}
    def get_object(self, **kw):
        self.failure = 'No undeclared stored input allowed'; self.ready()
    def urlopen(self, req, timeout=None):
        self.ready()
        try:
            request = identity(req)
            if type(timeout) not in (int, float) or not 0 < timeout <= 45 or len(self.http) >= 34:
                raise EvidenceError('Native request population or timeout differs')
        except Exception:
            self.failure = 'Undeclared provider request'; self.ready()
        row = {'request': request, 'requested_at': datetime.now(timezone.utc).isoformat()}
        host = urllib.parse.urlsplit(request['endpoint']).netloc
        remaining = 100 - (time.monotonic() - self.started)
        if host in self.rate_limited or remaining <= 1:
            row['status'] = 'rate_limit_not_retried' if host in self.rate_limited else 'budget_not_attempted'
            self.record(row); raise EvidenceError(row['status'])
        phase = 'connect'
        try:
            try:
                response = self.opener(req, timeout=min(timeout, 12, remaining))
            except urllib.error.HTTPError as exc:
                response = exc
            phase = 'body'; code = response.getcode(); headers = response.headers or {}
            url = req if isinstance(req, str) else req.full_url
            if hasattr(response, 'geturl') and response.geturl() != url:
                response.close(); raise EvidenceError('Redirect refused')
            raw = whole(response, headers.get('Content-Length'), 16 * 1024 * 1024, self.started + 100)
            self.total_bytes += len(raw)
            if self.total_bytes > LIMIT:
                raise EvidenceError('Complete acquisition exceeds population bound')
            row.update(status='http_response', http_status=code,
                       headers={k: headers.get(k) for k in ('Content-Type', 'Content-Length', 'Date', 'Last-Modified', 'ETag', 'Retry-After')},
                       original=retain(self.client, self.bucket, raw))
            self.raws[len(self.http)] = raw
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            if phase != 'connect':
                self.failure = 'Incomplete source body or failed retention'; self.ready()
            row.update(status='transport_error', error_type=type(exc).__name__)
        except Exception:
            self.failure = 'Whole source acquisition/retention refused'; self.ready()
        self.record(row)
        if row['status'] != 'http_response' or row['http_status'] != 200:
            if row.get('http_status') == 429:
                self.rate_limited.add(host)
            raise EvidenceError('Source response unavailable; original attempt retained')
        return Response(raw, row['headers'])


def calculate(module, session, credential):
    original = {k: getattr(module, k) for k in ('S3', 'datetime', 'urllib', 'FRED_KEY', 'FMP_KEY')}
    at = measurements.clock(session.at)
    class Frozen(original['datetime']):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz) if tz else at.replace(tzinfo=None)
    try:
        module.S3 = session; module.datetime = Frozen; module.FRED_KEY = credential; module.FMP_KEY = ''
        module.urllib = SimpleNamespace(request=SimpleNamespace(Request=urllib.request.Request, urlopen=session.urlopen))
        result = module.handler({}, None); session.ready()
        if session.pending is None:
            raise EvidenceError('Complete native output missing')
        params = {'series_id': 'HTRUCKSSAAR', 'api_key': credential, 'file_type': 'json', 'realtime_start': at.date().isoformat(), 'realtime_end': at.date().isoformat()}
        packets = []
        for endpoint in ('series', 'series/observations'):
            if endpoint.endswith('observations'):
                params.update(observation_start='1776-07-04', observation_end=at.date().isoformat(), sort_order='asc', limit='100000', offset='0', units='lin', output_type='1')
            try:
                response = session.urlopen('https://api.stlouisfed.org/fred/' + endpoint + '?' + urllib.parse.urlencode(params), timeout=12)
                packets.append(strict(response.read())); response.close()
            except EvidenceError:
                session.ready(); packets.append(None)
        truck = measurements.truck(*packets, session.at)
        gpr = None
        for i, row in enumerate(session.http):
            if row['request']['endpoint'] == GPR and row.get('http_status') == 200:
                gpr = measurements.gpr(session.raws[i], session.at)
        session.review = {'contract': measurements.CONTRACT, 'calculation_at': session.at, 'heavy_truck': truck,
                          'gpr': gpr, 'gpr_status': gpr['status'] if gpr else 'source_unavailable', 'sources_atomic': False,
                          'original_vintage_verified': False, **measurements.FLAGS}
        return result
    finally:
        for key, value in original.items():
            setattr(module, key, value)


def projection(previous, native, review, at, compilers):
    def overlay(old, new):
        if not isinstance(old, dict) or not isinstance(new, dict):
            return deepcopy(new)
        result = deepcopy(old)
        for key, value in new.items():
            result[key] = overlay(result.get(key), value)
        return result
    out = overlay(previous, native)
    out.update(version='1.1.0', contract=CONTRACT, generated_at=at, measurement_review=review, compiler_sha256=compilers,
               legacy_calculation=deepcopy(native), portfolio_action='WAIT', **measurements.FLAGS)
    truck = review['heavy_truck']; field = deepcopy(out.get('heavy_truck_sales', {}))
    field.update(saar_millions=truck['level'], asof=truck['latest_month'] + '-01' if truck['latest_month'] else None,
                 yoy_pct=truck.get('yoy', {}).get('percent'), z_1y=truck.get('prior_12_months', {}).get('z'),
                 z_basis='Prior 12 exact calendar months, excluding current; population standard deviation.',
                 note=truck['interpretation'], status=truck['status'], **measurements.FLAGS)
    out['heavy_truck_sales'] = field
    gpr = review['gpr']; field = deepcopy(out.get('geopolitical_risk', {}))
    field.update(gpr=gpr['level'] if gpr else None, asof=gpr['latest_month'] + '-01' if gpr else None,
                 z_5y=gpr['prior_60_months']['z'] if gpr else None, unit='Index 1985:2019=100',
                 z_basis='Prior 60 exact calendar months, excluding current; population standard deviation.',
                 status=review['gpr_status'], **measurements.FLAGS)
    out['geopolitical_risk'] = field
    for key in ('copper_gold_silver', 'rate_cut_diffusion', 'freight_activity'):
        if isinstance(out.get(key), dict):
            out[key].update(research_status='retained_legacy_calculation_unqualified', **measurements.FLAGS)
    out['quality'] = {'status': 'descriptive_measurements' if truck['status'] == 'measured' and gpr and gpr['status'] == 'measured' else 'partial_measurements',
                      'legacy_populated_is_not_verified_coverage': True, 'observation_freshness_verified': False}
    out['source'] = 'Retained FRED observation requests, Yahoo futures-chart responses and the original Caldara-Iacoviello GPR workbook.'
    out['research_limits'] = 'Calendar checks apply to heavy-truck sales and the source GPR matrix. Legacy metal-ratio units/quote clocks, central-bank monthly-rate classifications and mixed-frequency freight averages remain unqualified. Source replay is not first-release availability, independent evidence or predictive validation.'
    return out


def publication_context(ref):
    return {'manifest': ref, 'original_vintage_verified': False, 'original_source_replay_verified': False, 'sources_atomic': False, 'public_write_count': 1}


def run(module, event=None, context=None, at=None, opener=None):
    if isinstance(event, dict) and ('httpMethod' in event or 'requestContext' in event):
        return {'statusCode': 409, 'body': 'Stored research; HTTP does not run the producer.'}
    at = at or datetime.now(timezone.utc).isoformat(); measurements.clock(at)
    remaining = context.get_remaining_time_in_millis() / 1000 if context and hasattr(context, 'get_remaining_time_in_millis') else 150
    client = Budget(module.S3, time.monotonic() + min(135, remaining - 10)); bucket = module.BUCKET
    old = get(client, bucket, HEAD)
    if old is None:
        raise EvidenceError('Existing complete macro predecessor required')
    ref_old = retain(client, bucket, old['raw']); previous = strict(old['raw'])
    if not isinstance(previous, dict) or measurements.clock(previous.get('generated_at')) >= measurements.clock(at):
        raise EvidenceError('Exact older predecessor required')
    session = Session(client, bucket, at, opener); result = calculate(module, session, module.FRED_KEY)
    compilers = compiler_hashes(); native = strict(session.pending); out = projection(previous, native, session.review, at, compilers)
    plan = {'contract': CONTRACT, 'generated_at': at, 'predecessor': {'key': HEAD, 'original': ref_old, 'etag': old['etag']},
            'compiler_sha256': compilers, 'http_attempts': session.http, 'native_return': result,
            'native_output': retain(client, bucket, session.pending), 'projection': retain(client, bucket, encode(out)),
            'credential_present': bool(module.FRED_KEY), 'sources_atomic': False}
    ref = retain(client, bucket, encode(plan)); out['publication_context'] = publication_context(ref)
    raw = encode(out); retain(client, bucket, raw)
    client.put_object(Bucket=bucket, Key=HEAD, Body=raw, IfMatch=old['etag'], ContentType='application/json', CacheControl='public, max-age=3600')
    check = get(client, bucket, HEAD)
    if check is None or check['raw'] != raw:
        raise EvidenceError('Public readback differs; no rollback')
    return {'published': True, 'contract': CONTRACT, 'provider_attempts': len(session.http), 'quality': out['quality']['status'], 'portfolio_action': 'WAIT'}


def replay(module, client, bucket, packet):
    ref = packet.get('publication_context', {}).get('manifest'); plan = strict(retained(client, bucket, ref))
    if (plan.get('contract') != CONTRACT or plan.get('compiler_sha256') != compiler_hashes()
            or plan.get('predecessor', {}).get('key') != HEAD or packet.get('publication_context') != publication_context(ref)
            or plan.get('sources_atomic') is not False or type(plan.get('credential_present')) is not bool
            or not isinstance(plan.get('http_attempts'), list) or not 1 <= len(plan['http_attempts']) <= 34):
        raise EvidenceError('Exact retained compiler and publication required')
    previous = strict(retained(client, bucket, plan['predecessor']['original']))
    if not isinstance(previous, dict) or measurements.clock(previous.get('generated_at')) >= measurements.clock(plan.get('generated_at')):
        raise EvidenceError('Exact older predecessor required')
    class Replay:
        def __init__(self):
            self.at = plan['generated_at']; self.bucket = bucket; self.http = []; self.raws = {}; self.pending = None; self.failure = None
            self.expected = list(plan['http_attempts'])
        ready = Session.ready
        put_object = Session.put_object
        get_object = Session.get_object
        def urlopen(self, req, timeout=None):
            try:
                if not self.expected:
                    raise EvidenceError('Unrecorded request')
                row = self.expected.pop(0)
                measurements.clock(row.get('requested_at'))
                if row.get('status') not in ('http_response', 'transport_error', 'rate_limit_not_retried', 'budget_not_attempted'):
                    raise EvidenceError('Unknown source-attempt state')
                if row.get('request') != identity(req) or strict(retained(client, bucket, row['attempt_manifest'])) != {k: v for k, v in row.items() if k != 'attempt_manifest'}:
                    raise EvidenceError('Exact source request/attempt required')
                raw = retained(client, bucket, row['original']) if row.get('original') else None
                length = row.get('headers', {}).get('Content-Length')
                if raw is not None and length is not None and (not str(length).isdigit() or int(length) != len(raw)):
                    raise EvidenceError('Source length differs')
                if row.get('status') == 'http_response' and raw is None:
                    raise EvidenceError('Complete source body absent')
            except Exception:
                self.failure = 'Recorded original source integrity failed'; self.ready()
            if raw is not None:
                self.raws[len(self.http)] = raw
            self.http.append(row)
            if row.get('status') != 'http_response' or row.get('http_status') != 200:
                if row.get('status') in ('rate_limit_not_retried', 'budget_not_attempted'):
                    raise EvidenceError(row['status'])
                raise EvidenceError('Source response unavailable; original attempt retained')
            return Response(raw, row.get('headers'))
    session = Replay(); result = calculate(module, session, 'offline-present' if plan['credential_present'] else '')
    if session.expected or encode(result) != encode(plan['native_return']) or session.pending != retained(client, bucket, plan['native_output']):
        raise EvidenceError('Complete native calculation differs')
    out = projection(previous, strict(session.pending), session.review, plan['generated_at'], plan['compiler_sha256'])
    if encode(out) != retained(client, bucket, plan['projection']):
        raise EvidenceError('Exact calendar projection differs')
    out['publication_context'] = publication_context(ref)
    if encode(out) != encode(packet):
        raise EvidenceError('Whole publication differs')
    gpr = session.review['gpr']
    return {'status': 'complete_native_and_monthly_research_replayed', 'provider_attempts': len(plan['http_attempts']),
            'truck_observations': session.review['heavy_truck']['returned_rows'], 'gpr_series': gpr['series_count'] if gpr else 0,
            'gpr_monthly_positions': gpr['monthly_positions'] if gpr else 0, 'provider_requests': 0, 'public_writes': 0,
            'original_vintage_verified': False, 'forecast_qualified': False}
