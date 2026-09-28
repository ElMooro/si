"""Retain original provider bytes; no additional requests or research authority."""
from datetime import datetime, timezone
from io import BytesIO
from threading import Lock
from urllib.parse import urlsplit, parse_qsl, urlencode, unquote
import gzip
import hashlib
import re
import urllib.error
import urllib.request
import uuid
import zlib
from backlog_measurements import decode, compile_concept

CONTRACT = 'backlog-provider-originals.v1'
PREFIX = 'data/backlog/sources/'
WIRE_BOUND = 8 * 1024 * 1024
DECODED_BOUND = 16 * 1024 * 1024


def endpoint(url):
    p = urlsplit(url)
    if p.scheme != 'https' or p.fragment or p.username or p.password or p.port:
        raise ValueError('Declared provider endpoint required')
    if p.netloc == 'www.sec.gov' and p.path == '/files/company_tickers.json' and not p.query:
        return url
    if (p.netloc == 'data.sec.gov' and not p.query and
            re.fullmatch(r'/api/xbrl/companyconcept/CIK[0-9]{1,10}/us-gaap/[A-Za-z0-9]+\.json', p.path)):
        return url
    pairs = parse_qsl(p.query, keep_blank_values=True)
    if len(dict(pairs)) != len(pairs):
        raise ValueError('Repeated endpoint parameter')
    query = dict(pairs)
    query.pop('apikey', None)
    if (p.netloc != 'financialmodelingprep.com' or p.path not in ('/stable/income-statement', '/stable/key-metrics-ttm')
            or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,24}', query.get('symbol', ''))):
        raise ValueError('Declared Backlog provider endpoint required')
    expected = {'symbol'} if p.path.endswith('key-metrics-ttm') else {'symbol', 'period', 'limit'}
    if set(query) != expected or ('period' in query and (query['period'] != 'quarter' or query['limit'] != '6')):
        raise ValueError('Original Backlog enrichment scope required')
    return 'https://' + p.netloc + p.path + '?' + urlencode(sorted(query.items()))


def reference(raw):
    if not isinstance(raw, bytes) or len(raw) > WIRE_BOUND:
        raise ValueError('Whole bounded original required')
    digest = hashlib.sha256(raw).hexdigest()
    return {'key': PREFIX + digest + '.bin', 'bytes': len(raw), 'sha256': digest}


def validate_ref(ref):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'bytes', 'sha256'} or
            not isinstance(ref['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256']) or
            type(ref['bytes']) is not int or not 0 <= ref['bytes'] <= WIRE_BOUND or
            ref['key'] != PREFIX + ref['sha256'] + '.bin'):
        raise ValueError('Declared Backlog original required')
    return ref


def expanded(raw, encoding):
    if encoding == 'gzip':
        with gzip.GzipFile(fileobj=BytesIO(raw)) as stream:
            out = stream.read(DECODED_BOUND + 1)
    elif encoding == 'deflate':
        stream = zlib.decompressobj()
        out = stream.decompress(raw, DECODED_BOUND + 1)
        if not stream.eof or stream.unused_data:
            raise ValueError('Whole bounded deflate response required')
    elif encoding in ('', 'identity'):
        out = raw
    else:
        raise ValueError('Unsupported content encoding')
    if len(out) > DECODED_BOUND:
        raise ValueError('Expanded provider response exceeds bound')
    return out


def result(raw, encoding, status, url):
    if status == 404 and url.startswith('https://data.sec.gov/api/xbrl/companyconcept/'):
        return {'_source_status': 404}, 'http_not_found'
    if not 200 <= status < 300:
        return None, 'http_error'
    try:
        return decode(expanded(raw, encoding)), 'received'
    except (ValueError, UnicodeError, OSError, EOFError, RecursionError, zlib.error):
        return None, 'invalid_response'


def read_original(s3, bucket, ref):
    validate_ref(ref)
    obj = s3.get_object(Bucket=bucket, Key=ref['key'])
    raw = obj['Body'].read(WIRE_BOUND + 1)
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw) or reference(raw) != ref:
        raise ValueError('Whole original readback differs')
    return raw


class Capture:
    def __init__(self, s3, bucket, ua, secret, opener=None):
        self.s3, self.bucket, self.ua, self.secret = s3, bucket, ua, secret
        self.opener = opener or urllib.request.urlopen
        self.lock, self.attempts, self.failed = Lock(), [], False
        self.run_id = uuid.uuid4().hex

    def acquire(self, url, t=15):
        clean = endpoint(url)
        with self.lock:
            attempt = {'run_id': self.run_id, 'request_index': len(self.attempts), 'endpoint': clean,
                       'started_at': datetime.now(timezone.utc).isoformat(), 'status': 'incomplete'}
            self.attempts.append(attempt)
        parsed = None
        try:
            req = urllib.request.Request(url, headers={'User-Agent': self.ua, 'Accept-Encoding': 'gzip, deflate'})
            try:
                response = self.opener(req, timeout=t)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                status = response.code
                encoding = (response.headers.get('Content-Encoding') or '').lower().strip()
                length = response.headers.get('Content-Length')
                raw = response.read(WIRE_BOUND + 1)
            attempt.update(http_status=status, content_encoding=encoding, observed_wire_bytes=len(raw))
            if type(status) is not int or not 100 <= status <= 599:
                raise ValueError('Invalid HTTP status')
            if len(raw) > WIRE_BOUND:
                attempt['status'] = 'response_bound_exceeded'
                return None, attempt
            if length is not None and (not re.fullmatch('[0-9]+', length) or int(length) != len(raw)):
                attempt['status'] = 'incomplete_response'
                return None, attempt
            # Never persist request headers, a credentialed URL or a credential echo.
            # Invalid compressed FMP bodies cannot be checked safely, so fail closed.
            try:
                decoded = expanded(raw, encoding)
                text = decoded.decode('utf-8')
                try:
                    import json
                    text += json.dumps(decode(decoded), ensure_ascii=False)
                except (ValueError, RecursionError):
                    if clean.startswith('https://financialmodelingprep.com/'):
                        raise PermissionError('Invalid provider JSON cannot be checked for credential material')
                if self.secret and (self.secret in text or self.secret in unquote(text)):
                    raise PermissionError('Provider body contains credential material')
            except (ValueError, UnicodeError, OSError, EOFError, zlib.error):
                if clean.startswith('https://financialmodelingprep.com/'):
                    raise PermissionError('Provider body cannot be checked for credential material')
            ref = reference(raw)
            try:
                self.s3.put_object(Bucket=self.bucket, Key=ref['key'], Body=raw,
                                   ContentType='application/octet-stream', IfNoneMatch='*',
                                   CacheControl='public, max-age=31536000, immutable')
            except Exception as error:
                if str(getattr(error, 'response', {}).get('Error', {}).get('Code')) not in ('412', 'PreconditionFailed'):
                    raise
            if read_original(self.s3, self.bucket, ref) != raw:
                raise ValueError('Original readback mismatch')
            attempt['original_ref'] = ref
            parsed, attempt['status'] = result(raw, encoding, status, clean)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            attempt.update(status='transport_error', error_type=type(error).__name__)
        except Exception as error:
            with self.lock:
                self.failed = True
            attempt.update(status='evidence_failure', error_type=type(error).__name__)
            raise
        finally:
            attempt['received_at'] = datetime.now(timezone.utc).isoformat()
        return parsed, attempt

    def finish(self):
        if self.failed or any(a['status'] in ('incomplete', 'evidence_failure') for a in self.attempts):
            raise ValueError('Incomplete Backlog source evidence; retain prior publication')
        return {'contract': CONTRACT, 'run_id': self.run_id, 'attempts': self.attempts,
                'selection_context_replayed': False, 'private_coverage_cache_published': False,
                'point_in_time_availability_verified': False, 'investment_authority': False}


def replay(packet, read):
    """Replay this publication's received provider responses, not private selection."""
    manifest = packet.get('provider_sources')
    if not isinstance(manifest, dict) or manifest.get('contract') != CONTRACT:
        return {'status': 'pending_original_schedule_source_capture', 'whole_provider_http_replay_verified': False}
    if any(manifest.get(k) is not False for k in ('selection_context_replayed', 'private_coverage_cache_published',
                                                'point_in_time_availability_verified', 'investment_authority')):
        raise ValueError('Explicit limits on provider replay required')
    run_id = manifest.get('run_id')
    if not isinstance(run_id, str) or not re.fullmatch('[a-f0-9]{32}', run_id):
        raise ValueError('Acquisition identity required')
    attempts = manifest.get('attempts')
    if not isinstance(attempts, list) or not attempts:
        raise ValueError('Complete provider attempt population required')
    values, verified, wire_bytes = {}, 0, 0
    for i, attempt in enumerate(attempts):
        if (not isinstance(attempt, dict) or type(attempt.get('request_index')) is not int or
                attempt['request_index'] != i or attempt.get('run_id') != run_id or
                endpoint(attempt.get('endpoint', '')) != attempt.get('endpoint')):
            raise ValueError('Contiguous credential-free provider attempt identities required')
        clocks = [datetime.fromisoformat(attempt[k]) for k in ('started_at', 'received_at')]
        if any(t.tzinfo is None for t in clocks) or clocks[0] > clocks[1]:
            raise ValueError('Ordered aware acquisition clocks required')
        if 'original_ref' in attempt:
            ref = validate_ref(attempt['original_ref']); raw = read(ref)
            if reference(raw) != ref or type(attempt.get('observed_wire_bytes')) is not int or attempt['observed_wire_bytes'] != len(raw):
                raise ValueError('Whole received wire bytes differ')
            code = attempt.get('http_status')
            if type(code) is not int or not 100 <= code <= 599:
                raise ValueError('Literal HTTP status required')
            value, status = result(raw, attempt.get('content_encoding'), code, attempt['endpoint'])
            if status != attempt.get('status'):
                raise ValueError('Response classification differs')
            values[i] = value; verified += 1; wire_bytes += len(raw)
        elif attempt.get('status') not in ('transport_error', 'incomplete_response', 'response_bound_exceeded'):
            raise ValueError('Missing original response')
    def source(attempt, expected):
        if (not isinstance(attempt, dict) or type(attempt.get('request_index')) is not int or
                not 0 <= attempt['request_index'] < len(attempts) or attempt != attempts[attempt['request_index']] or
                attempt['endpoint'] != endpoint(expected)):
            raise ValueError('Published measurement is not bound to its exact request')
        return values.get(attempt['request_index'])
    def concept(c, cik):
        count = 0
        if c.get('contract') == 'backlog-measurements.v1':
            tag = c['tag']; url = f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{tag}.json'
            original = source(c.get('source_attempt'), url)
            if original is None:
                raise ValueError('A measurement requires a complete usable original')
            rebuilt = compile_concept(original, cik, tag, c['checked_as_of'])
            if any(c.get(k) != v for k, v in rebuilt.items()):
                raise ValueError('Original concept replay differs')
            units = original.get('units') or {}
            selected = units.get('USD') or units.get('USD/shares') or next(iter(units.values()), [])
            ends = set()
            for row in selected:
                if row.get('val') is not None and row.get('end'):
                    float(row['val']); ends.add(row['end'])
            if type(c.get('legacy_request_stop_count')) is not int or c['legacy_request_stop_count'] != len(ends):
                raise ValueError('Original fallback stopping rule differs')
            count += len(c['observations'])
        elif (c.get('latest') is not None or c.get('observations') or c.get('qoq') is not None or c.get('yoy') is not None):
            raise ValueError('Unreviewed concept cannot supply measurements')
        for other in c.get('alternate_concepts', []):
            count += concept(other, cik)
        return count
    def current(c):
        return c.get('source_attempt', {}).get('run_id') == run_id or any(current(a) for a in c.get('alternate_concepts', []))
    rows = observations = 0
    mapping_attempts = [a for a in attempts if a['endpoint'] == 'https://www.sec.gov/files/company_tickers.json']
    if len(mapping_attempts) != 1:
        raise ValueError('Exactly the existing issuer mapping request required')
    mapping = source(mapping_attempts[0], mapping_attempts[0]['endpoint'])
    cik_map = {}
    if isinstance(mapping, dict):
        for row in mapping.values():
            ticker = (row.get('ticker') or '').upper()
            if ticker: cik_map[ticker] = str(row.get('cik_str')).zfill(10)
    for ticker, row in packet['by_ticker'].items():
        concepts = row.get('measurements', {})
        if not any(current(c) for c in concepts.values()):
            continue
        if cik_map.get(ticker) != row.get('cik'):
            raise ValueError('Provider issuer mapping differs')
        observations += sum(concept(c, row['cik']) for c in concepts.values())
        enrichment = row.get('provider_enrichment', {})
        refs = enrichment.get('source_attempts', {})
        for name, path in (('income_statement', 'income-statement?period=quarter&limit=6&'), ('key_metrics_ttm', 'key-metrics-ttm?')):
            attempt = refs.get(name)
            original = source(attempt, 'https://financialmodelingprep.com/stable/' + path + 'symbol=' + ticker) if attempt is not None else None
            expected = original if isinstance(original, (list, dict)) else None
            if enrichment.get(name) != expected:
                raise ValueError('Complete enrichment differs')
        rows += 1
    if type(packet.get('slice_this_run')) is not int or rows != packet['slice_this_run']:
        raise ValueError('Whole refreshed issuer population must replay')
    return {'status': 'current_provider_responses_replayed', 'whole_provider_http_replay_verified': verified == len(attempts),
            'retained_http_responses_replayed': True,
            'request_attempts': len(attempts), 'retained_responses': verified, 'wire_bytes_with_repeats': wire_bytes,
            'current_issuers': rows, 'current_source_observations': observations,
            'carried_issuers_not_replayed': len(packet['by_ticker']) - rows,
            'selection_context_replayed': False, 'point_in_time_availability_verified': False, 'investment_authority': False}
