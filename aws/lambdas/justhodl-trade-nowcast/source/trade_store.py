"""Whole trade-source capture, retained predecessors and one conditional head.

No native/provider request occurs during replay. The protected byte chain is
an acquisition record, not a claim to historical first-release vintages.
"""
from pathlib import Path
from datetime import datetime, timezone
from copy import deepcopy
import hashlib, json, math, re, time, urllib.request, urllib.error, urllib.parse
import trade_measurements as measurements

HEAD = 'data/trade-nowcast.json'
PRIVATE = 'audit-private/20260909-originals/trade-nowcast-research/'
CONTRACT = 'trade-nowcast-research.v1'
LIMIT = 64 * 1024 * 1024
HTTP_LIMIT = 16 * 1024 * 1024
CPB = 'https://www.cpb.nl/sitemap.xml'
BALTIC = 'https://tradingeconomics.com/commodity/baltic'
COMPILERS = ('lambda_function.py', 'trade_store.py', 'trade_measurements.py', 'managed_secret.py')
sha = lambda raw: hashlib.sha256(raw).hexdigest()
encode = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


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


def compiler_hashes():
    import managed_secret
    return {name: sha((Path(managed_secret.__file__) if name == 'managed_secret.py' else Path(__file__).parent / name).read_bytes()) for name in COMPILERS}


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


def identity(url):
    p = urllib.parse.urlsplit(url); pairs = urllib.parse.parse_qsl(p.query, keep_blank_values=True); query = dict(pairs)
    if p.scheme != 'https' or p.fragment or p.username or p.password or len(pairs) != len(query):
        raise EvidenceError('Exact HTTPS source identity required')
    if p.netloc == 'api.stlouisfed.org':
        allowed = {'series_id', 'file_type', 'api_key', 'realtime_start', 'realtime_end'}
        if p.path == '/fred/series/observations':
            allowed |= {'observation_start', 'observation_end', 'sort_order', 'limit', 'offset', 'units', 'output_type'}
            if (query.get('observation_start') != '1776-07-04' or query.get('sort_order') != 'asc' or query.get('limit') != '100000'
                    or query.get('offset') != '0' or query.get('units') != 'lin' or query.get('output_type') != '1'):
                raise EvidenceError('Complete untransformed FRED population required')
        elif p.path != '/fred/series':
            raise EvidenceError('Unreviewed FRED endpoint')
        if set(query) != allowed or query.get('series_id') not in measurements.PROFILES or query.get('file_type') != 'json' or not query.get('api_key'):
            raise EvidenceError('Unreviewed FRED series request')
    elif p.netloc == 'www.cpb.nl':
        if p.query or not (url == CPB or re.fullmatch(r'/wereldhandelsmonitor/cpb-wereldhandelsmonitor-[a-z]+-\d{4}', p.path)
                           or re.fullmatch(r'/system/files/cpbmedia/CPB-world-trade-monitor-[a-z]+-\d{4}\.xlsx', p.path)):
            raise EvidenceError('Unreviewed CPB source path')
    elif url != BALTIC:
        raise EvidenceError('Unreviewed source host')
    return {'method': 'GET', 'endpoint': p.scheme + '://' + p.netloc + p.path, 'parameters': {k: v for k, v in query.items() if k != 'api_key'}}


class Acquisition:
    def __init__(self, client, bucket, opener=None):
        self.client, self.bucket = client, bucket; self.attempts = []; self.total_bytes = 0
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.deadline = time.monotonic() + 120
    def fetch(self, url, optional=False):
        request = identity(url)
        if len(self.attempts) >= 12:
            raise EvidenceError('Declared source request population exceeded')
        remaining = self.deadline - time.monotonic()
        if remaining <= 1:
            raise EvidenceError('Whole source acquisition budget exhausted')
        row = {'request': request, 'requested_at': datetime.now(timezone.utc).isoformat()}
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-Trade-Research/1.1', 'Accept-Encoding': 'identity'})
        phase = 'connect'
        try:
            try:
                response = self.opener(req, timeout=min(12, remaining))
            except urllib.error.HTTPError as exc:
                response = exc
            phase = 'whole_response'
            headers = response.headers or {}; code = response.getcode()
            # The opener refuses redirects; test/openers must enforce the same identity.
            if hasattr(response, 'geturl') and response.geturl() != url:
                response.close(); raise EvidenceError('Unreviewed response redirect')
            raw = whole(response, headers.get('Content-Length'), HTTP_LIMIT, self.deadline)
            self.total_bytes += len(raw)
            if self.total_bytes > LIMIT:
                raise EvidenceError('Complete source population exceeds bound')
            row.update(status='http_response', http_status=code, received_at=datetime.now(timezone.utc).isoformat(),
                       headers={k: headers.get(k) for k in ('Content-Type', 'Content-Length', 'Date', 'Last-Modified', 'ETag', 'Retry-After')},
                       original=retain(self.client, self.bucket, raw))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if phase != 'connect':
                row.update(status='acquisition_refused', error_type=type(exc).__name__)
                row['attempt_manifest'] = retain(self.client, self.bucket, encode(row)); self.attempts.append(row)
                raise EvidenceError('Incomplete response or failed retention') from None
            row.update(status='transport_error', error_type=type(exc).__name__)
        except Exception as exc:
            row.update(status='acquisition_refused', error_type=type(exc).__name__)
            row['attempt_manifest'] = retain(self.client, self.bucket, encode(row)); self.attempts.append(row)
            raise EvidenceError('Whole source acquisition/retention refused') from None
        row['attempt_manifest'] = retain(self.client, self.bucket, encode(row)); self.attempts.append(row)
        if row['status'] != 'http_response' or row['http_status'] != 200:
            if optional:
                return None
            raise EvidenceError('Source unavailable; exact failed attempt retained, no immediate retry')
        return raw


def calculate(fetch, credential, at):
    current = measurements.clock(at).date().isoformat(); series = {}
    for sid in measurements.PROFILES:
        params = {'series_id': sid, 'file_type': 'json', 'api_key': credential, 'realtime_start': current, 'realtime_end': current}
        meta = strict(fetch('https://api.stlouisfed.org/fred/series?' + urllib.parse.urlencode(params)))
        params.update(observation_start='1776-07-04', observation_end=current, sort_order='asc', limit='100000', offset='0', units='lin', output_type='1')
        obs = strict(fetch('https://api.stlouisfed.org/fred/series/observations?' + urllib.parse.urlencode(params)))
        series[sid] = measurements.fred(sid, meta, obs, at)
    discovery = measurements.cpb_candidates(fetch(CPB), at)
    report = measurements.cpb_report(fetch(discovery['selected_url']), discovery, at)
    cpb = measurements.cpb_workbook(fetch(report['workbook_url']), report, at)
    baltic = fetch(BALTIC, optional=True)
    bdi = measurements.bdi(baltic) if baltic is not None else {
        'status': 'source_unavailable', 'level': None, 'quote_at': None, 'unit': 'index_points', 'candidates': [],
        'read': 'Source unavailable; previous quote is not substituted.', **measurements.FLAGS}
    return {'contract': measurements.CONTRACT, 'calculation_at': at, 'series': series, 'cpb_discovery': discovery, 'cpb': cpb, 'bdi': bdi,
            'sources_atomic': False, 'original_vintage_verified': False, **measurements.FLAGS,
            'interpretation': 'Prices, merchandise trade volumes and dry-bulk freight quotes are different measurements. No mixed-unit composite, demand direction or portfolio permission follows from them.'}


def projection(previous, review, at, compilers):
    out = deepcopy(previous)
    # Preserve the old public claims once, with their own old clock. Every later
    # complete head is also retained before replacement, without recursive copies.
    if 'legacy_pre_research_fields' not in out:
        out['legacy_pre_research_fields'] = {k: deepcopy(previous[k]) for k in ('generated_at', 'version', 'series', 'bdi', 'cpb_wtm', 'rate_pressure', 'verdict', 'plain', 'errors', 'ok') if k in previous}
    series = deepcopy(previous.get('series', {}))
    if not isinstance(series, dict):
        raise EvidenceError('Prior series object differs')
    for sid, row in review['series'].items():
        name = row['key']; item = deepcopy(series.get(name, {}))
        if not isinstance(item, dict):
            raise EvidenceError('Prior series record differs')
        item.update(series_id=sid, name=row['name'], unit=row['unit'], seasonal_adjustment='NSA', status=row['status'],
                    date=row.get('latest_month') + '-01' if row.get('latest_month') else None, level=row.get('level'),
                    yoy_pct=row.get('yoy', {}).get('percent'), q_ann_pct=None,
                    three_month_pct=row.get('three_month', {}).get('percent'), annualization_status='not_seasonally_adjusted', **measurements.FLAGS)
        series[name] = item
    cpb = deepcopy(previous.get('cpb_wtm', {})); bdi = deepcopy(previous.get('bdi', {}))
    if not isinstance(cpb, dict) or not isinstance(bdi, dict):
        raise EvidenceError('Prior source objects differ')
    cpb.update(latest_url=review['cpb']['report']['report_url'], period=review['cpb']['report']['period'],
               pages_n=len(review['cpb_discovery']['candidates']), published_at=review['cpb']['report']['published_at'],
               workbook_url=review['cpb']['report']['workbook_url'], series_id='tgz_w1_qnmi_sn',
               trade_volume_chg_pct=review['cpb']['world_trade']['mom']['percent'], change_basis='Exact consecutive calendar-month seasonally adjusted volume indexes',
               excerpt=review['cpb']['report']['source_description'], excerpt_used_for_numbers=False, err=None, **measurements.FLAGS)
    bdi.update(review['bdi'])
    measured = sum(r['status'] == 'measured' for r in review['series'].values())
    cpb_measured = review['cpb']['world_trade']['status'] == 'measured'
    errors = [sid + ': ' + r['status'] for sid, r in review['series'].items() if r['status'] != 'measured']
    if not cpb_measured:
        errors.append('CPB world merchandise volume: ' + review['cpb']['world_trade']['status'])
    out.update(version='1.1.0', contract=CONTRACT, generated_at=at, series=series, cpb_wtm=cpb, bdi=bdi,
               rate_pressure=None, verdict='UNQUALIFIED', plain='Source-specific monthly prices and merchandise trade volumes. Mixed-unit rate-pressure and scraped demand claims are unqualified.',
               ok=measured == 4 and cpb_measured, errors=errors,
               measurement_review=review, compiler_sha256=compilers, portfolio_action='WAIT', **measurements.FLAGS)
    out['quality'] = {'status': 'descriptive_measurements' if measured == 4 and cpb_measured else 'partial_measurements', 'measured_fred_series': measured,
                      'cpb_month': review['cpb']['report']['period'], 'bdi_quote_qualified': False, 'publication_time_is_observation_time': False}
    return out


def publication_context(ref):
    return {'manifest': ref, 'original_vintage_verified': False, 'original_source_replay_verified': False,
            'sources_atomic': False, 'public_write_count': 1,
            'replay_scope': 'Whole predecessor, full provider bodies and exact calendar compilation. Not first-release-vintage, forecast or portfolio validation.'}


def run(client, bucket, credential, event=None, context=None, at=None, opener=None):
    if isinstance(event, dict) and ('httpMethod' in event or 'requestContext' in event):
        return {'statusCode': 409, 'body': 'Stored research only; HTTP requests do not generate publications.'}
    at = at or datetime.now(timezone.utc).isoformat(); measurements.clock(at)
    remaining = context.get_remaining_time_in_millis() / 1000 if context and hasattr(context, 'get_remaining_time_in_millis') else 180
    client = Budget(client, time.monotonic() + min(165, remaining - 10))
    old = get(client, bucket, HEAD)
    if old is None:
        raise EvidenceError('Existing predecessor required; no empty bootstrap')
    original = retain(client, bucket, old['raw']); previous = strict(old['raw'])
    if not isinstance(previous, dict) or measurements.clock(previous.get('generated_at')) >= measurements.clock(at):
        raise EvidenceError('Complete older predecessor required')
    if not isinstance(credential, str) or not credential:
        raise EvidenceError('Existing managed FRED credential unavailable')
    acquisition = Acquisition(client, bucket, opener)
    review = calculate(acquisition.fetch, credential, at); compilers = compiler_hashes()
    out = projection(previous, review, at, compilers)
    plan = {'contract': CONTRACT, 'generated_at': at, 'predecessor': {'key': HEAD, 'original': original, 'etag': old['etag']},
            'http_attempts': acquisition.attempts, 'compiler_sha256': compilers, 'projection': retain(client, bucket, encode(out)),
            'sources_atomic': False, 'public_write_count': 1}
    ref = retain(client, bucket, encode(plan)); out['publication_context'] = publication_context(ref)
    raw = encode(out); retain(client, bucket, raw)
    # One compare-and-swap publication: no timestamp-only update or partial pair.
    client.put_object(Bucket=bucket, Key=HEAD, Body=raw, IfMatch=old['etag'], ContentType='application/json', CacheControl='public, max-age=3600')
    check = get(client, bucket, HEAD)
    if check is None or check['raw'] != raw:
        raise EvidenceError('Publication differs; concurrent data is not rolled back')
    return {'published': True, 'contract': CONTRACT, 'generated_at': at, 'cpb_month': review['cpb']['report']['period'],
            'cpb_series': review['cpb']['series_count'], 'provider_attempts': len(acquisition.attempts), 'portfolio_action': 'WAIT'}


def replay(client, bucket, packet):
    context = packet.get('publication_context', {}); ref = context.get('manifest'); plan = strict(retained(client, bucket, ref))
    if (plan.get('contract') != CONTRACT or plan.get('compiler_sha256') != compiler_hashes() or context != publication_context(ref)
            or plan.get('sources_atomic') is not False or plan.get('public_write_count') != 1 or plan.get('predecessor', {}).get('key') != HEAD):
        raise EvidenceError('Exact compiler, predecessor and publication context required')
    previous = strict(retained(client, bucket, plan['predecessor']['original'])); attempts = list(plan['http_attempts'])
    if not isinstance(previous, dict) or measurements.clock(previous.get('generated_at')) >= measurements.clock(plan['generated_at']):
        raise EvidenceError('Older complete predecessor required')
    def fetch(url, optional=False):
        if not attempts:
            raise EvidenceError('Missing complete provider attempt')
        row = attempts.pop(0)
        if row.get('request') != identity(url) or strict(retained(client, bucket, row['attempt_manifest'])) != {k: v for k, v in row.items() if k != 'attempt_manifest'}:
            raise EvidenceError('Original request/attempt identity differs')
        raw = retained(client, bucket, row['original']) if 'original' in row else None
        if row.get('status') != 'http_response' or row.get('http_status') != 200:
            if optional and row.get('status') in ('http_response', 'transport_error'):
                return None
            raise EvidenceError('Failed required provider response')
        if raw is None:
            raise EvidenceError('Whole provider body missing')
        length = row.get('headers', {}).get('Content-Length')
        if length is not None and (not str(length).isdigit() or int(length) != len(raw)):
            raise EvidenceError('Provider response length differs')
        return raw
    review = calculate(fetch, 'offline-credential-presence', plan['generated_at'])
    out = projection(previous, review, plan['generated_at'], plan['compiler_sha256'])
    if attempts or encode(out) != retained(client, bucket, plan['projection']):
        raise EvidenceError('Complete original calculation differs')
    out['publication_context'] = publication_context(ref)
    if encode(out) != encode(packet):
        raise EvidenceError('Whole public packet differs')
    return {'status': 'complete_original_sources_and_calendar_replayed', 'provider_requests': 0, 'public_writes': 0,
            'provider_attempts': len(plan['http_attempts']), 'fred_observations': sum(r['returned_rows'] for r in review['series'].values()),
            'cpb_series': review['cpb']['series_count'], 'cpb_monthly_positions': review['cpb']['monthly_positions'],
            'original_vintage_verified': False, 'forecast_qualified': False}
