"""Retain original provider bytes; no additional requests or research authority."""
from datetime import date, datetime, timezone, timedelta
from io import BytesIO
from threading import Lock
from urllib.parse import urlsplit, parse_qsl, urlencode, unquote
import gzip
import hashlib
import json
import re
import urllib.error
import urllib.request
import uuid
import zlib
from buyback_measurements import decode, dossier

CONTRACT = 'buyback-provider-originals.v1'
PREFIX = 'data/buyback-engine/sources/'
WIRE_BOUND = 8 * 1024 * 1024
DECODED_BOUND = 16 * 1024 * 1024


def same(left, right):
    return json.dumps(left, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':')) == json.dumps(right, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def endpoint(url):
    p = urlsplit(url)
    if p.scheme != 'https' or p.netloc != 'financialmodelingprep.com' or p.fragment or p.username or p.password or p.port:
        raise ValueError('Declared provider endpoint required')
    pairs = parse_qsl(p.query, keep_blank_values=True)
    if len(dict(pairs)) != len(pairs): raise ValueError('Repeated endpoint parameter')
    query = dict(pairs); query.pop('apikey', None)
    name = p.path.removeprefix('/stable/')
    if name == 'earnings-calendar':
        if set(query) != {'from', 'to', 'limit'} or query['limit'] != '3000':
            raise ValueError('Original calendar request required')
        first, last = [datetime.fromisoformat(query[k]).date() for k in ('from', 'to')]
        if first.isoformat() != query['from'] or last.isoformat() != query['to'] or not 0 <= (last-first).days <= 6:
            raise ValueError('Original seven-day calendar window required')
    else:
        limits = {'profile': None, 'cash-flow-statement': '5', 'key-metrics': '1', 'enterprise-values': '5'}
        if name not in limits or not re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,15}', query.get('symbol', '')):
            raise ValueError('Declared Buyback provider endpoint required')
        expected = {'symbol'} if name == 'profile' else {'symbol', 'period', 'limit'}
        if set(query) != expected or name != 'profile' and (query['period'] != 'quarter' or query['limit'] != limits[name]):
            raise ValueError('Original Buyback statement request required')
    return 'https://financialmodelingprep.com/stable/' + name + '?' + urlencode(sorted(query.items()))


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
        raise ValueError('Declared Buyback original required')
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
    if not 200 <= status < 300:
        return None, 'http_error'
    try:
        return decode(expanded(raw, encoding).decode('utf-8')), 'received'
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
            req = urllib.request.Request(url, headers={'User-Agent': self.ua})
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
            raise ValueError('Incomplete Buyback source evidence; retain prior publication')
        return {'contract': CONTRACT, 'run_id': self.run_id, 'attempts': self.attempts,
                'selection_context_replayed': False, 'private_selection_inputs_published': False,
                'point_in_time_availability_verified': False, 'investment_authority': False}


def replay(packet, read):
    """Replay provider responses and statement arithmetic, never upstream selection."""
    manifest = packet.get('provider_sources')
    if not isinstance(manifest, dict) or manifest.get('contract') != CONTRACT:
        return {'status': 'pending_original_schedule_source_capture', 'whole_provider_http_replay_verified': False}
    if any(manifest.get(k) is not False for k in ('selection_context_replayed', 'private_selection_inputs_published',
                                                'point_in_time_availability_verified', 'investment_authority')):
        raise ValueError('Explicit source replay limits required')
    run_id = manifest.get('run_id'); attempts = manifest.get('attempts')
    if not isinstance(run_id, str) or not re.fullmatch('[a-f0-9]{32}', run_id) or not isinstance(attempts, list) or not attempts:
        raise ValueError('Complete identified provider attempts required')
    values = {}; retained = 0; size = 0
    for i, attempt in enumerate(attempts):
        if (not isinstance(attempt, dict) or type(attempt.get('request_index')) is not int or attempt['request_index'] != i
                or attempt.get('run_id') != run_id or endpoint(attempt.get('endpoint', '')) != attempt.get('endpoint')):
            raise ValueError('Contiguous credential-free provider attempt identities required')
        clocks = [datetime.fromisoformat(attempt[k]) for k in ('started_at', 'received_at')]
        if any(t.tzinfo is None for t in clocks) or clocks[0] > clocks[1]:
            raise ValueError('Ordered aware receipt clocks required')
        if 'original_ref' in attempt:
            ref = validate_ref(attempt['original_ref']); raw = read(ref)
            if reference(raw) != ref or type(attempt.get('observed_wire_bytes')) is not int or len(raw) != attempt['observed_wire_bytes']:
                raise ValueError('Original wire bytes differ')
            code = attempt.get('http_status')
            if type(code) is not int or not 100 <= code <= 599:
                raise ValueError('Literal HTTP status required')
            value, status = result(raw, attempt.get('content_encoding'), code, attempt['endpoint'])
            if status != attempt.get('status'): raise ValueError('Provider response classification differs')
            values[i] = value; retained += 1; size += len(raw)
        elif attempt.get('status') not in ('transport_error', 'incomplete_response', 'response_bound_exceeded'):
            raise ValueError('Required provider original is missing')
    referenced = set()
    def logical(records, path):
        if not isinstance(records, list) or not 1 <= len(records) <= 2:
            raise ValueError('Original one/two attempt retry chain required')
        output = None; previous = -1
        for ordinal, attempt in enumerate(records):
            index = attempt.get('request_index') if isinstance(attempt, dict) else None
            if (type(index) is not int or not 0 <= index < len(attempts) or index <= previous or index in referenced
                    or not same(attempt, attempts[index]) or attempt['endpoint'] != endpoint('https://financialmodelingprep.com/stable/'+path)):
                raise ValueError('Exact original request binding required')
            if ordinal < len(records)-1 and attempt['status'] == 'received':
                raise ValueError('A successful original request cannot trigger another retry')
            referenced.add(index); previous = index
            output = values.get(index) if attempt['status'] == 'received' else None
        if records[-1]['status'] != 'received' and len(records) != 2:
            raise ValueError('Original failed-request retry population differs')
        return output
    calendars = packet.get('calendar_sources')
    if not isinstance(calendars, list) or len(calendars) != 7:
        raise ValueError('All seven original calendar windows required')
    first = datetime.fromisoformat(calendars[0]['from']).date(); end = first+timedelta(days=45); current = first; next_earn = {}
    for calendar in calendars:
        last = min(current+timedelta(days=6), end)
        if calendar.get('from') != current.isoformat() or calendar.get('to') != last.isoformat():
            raise ValueError('Original contiguous calendar requests differ')
        value = logical(calendar.get('attempts'), f'earnings-calendar?from={current.isoformat()}&to={last.isoformat()}&limit=3000')
        for row in value if isinstance(value, list) else []:
            ticker = (row.get('symbol') or '').upper().strip(); day = row.get('date')
            if ticker and day and (ticker not in next_earn or day < next_earn[ticker]): next_earn[ticker] = day
        current = last+timedelta(days=1)
    observations = 0
    for ticker, row in packet['tickers'].items():
        refs = row.get('provider_attempts')
        if not isinstance(refs, dict) or set(refs) not in ({'profile','cash_flow'}, {'profile','cash_flow','key_metrics','enterprise_values'}):
            raise ValueError('Original issuer request population required')
        profile = logical(refs['profile'], f'profile?symbol={ticker}')
        cash = logical(refs['cash_flow'], f'cash-flow-statement?symbol={ticker}&period=quarter&limit=5')
        if not isinstance(cash, list) or not cash: raise ValueError('Actual received statement rows required')
        if (len(cash) >= 2) != ('key_metrics' in refs): raise ValueError('Original statement request stopping rule differs')
        metrics = logical(refs['key_metrics'], f'key-metrics?symbol={ticker}&period=quarter&limit=1') if len(cash) >= 2 else None
        shares = logical(refs['enterprise_values'], f'enterprise-values?symbol={ticker}&period=quarter&limit=5') if len(cash) >= 2 else None
        expected = dossier(ticker, profile, cash, metrics, shares, row['checked_as_of'])
        if any(k not in row or not same(row[k], v) for k, v in expected.items()):
            raise ValueError('Whole original issuer calculations differ')
        proxy = {}; reported = next_earn.get(ticker)
        if reported:
            try:
                e = date.fromisoformat(reported); start = e-timedelta(days=30)
                proxy = {'next_earnings': reported}
                if start <= first <= e+timedelta(days=2): proxy['in_blackout'] = True
                elif first < start: proxy['days_to_blackout'] = (start-first).days
            except ValueError:
                pass
        if any((k in row) != (k in proxy) or k in proxy and not same(row[k], proxy[k]) for k in ('next_earnings','in_blackout','days_to_blackout')):
            raise ValueError('Published earnings-calendar proxy differs')
        observations += len(expected['measurements']['cashflow_observations'])
    exclusions = packet.get('excluded')
    if not isinstance(exclusions, list) or type(packet.get('n_excluded')) is not int or packet['n_excluded'] != len(exclusions):
        raise ValueError('Complete excluded issuer population required')
    for excluded in exclusions:
        refs = excluded.get('provider_attempts')
        if excluded.get('reason') == 'denylist':
            if refs is not None or excluded.get('profile_response') is not None:
                raise ValueError('Denylist exclusion cannot imply a provider profile request')
        else:
            if not isinstance(refs, dict) or set(refs) != {'profile'}:
                raise ValueError('Exact profile original required for a provider-based exclusion')
            original = logical(refs['profile'], 'profile?symbol='+excluded['ticker'])
            if not same(original, excluded.get('profile_response')): raise ValueError('Excluded profile original differs')
    if type(packet.get('n_research_rows')) is not int or packet['n_research_rows'] != len(packet['tickers']):
        raise ValueError('Whole current issuer population required')
    return {'status':'current_provider_responses_replayed','whole_provider_http_replay_verified':retained==len(attempts),
            'retained_http_responses_replayed':True,'request_attempts':len(attempts),'retained_responses':retained,
            'wire_bytes_with_repeats':size,'current_issuers':len(packet['tickers']),'cashflow_observations':observations,
            'calendar_windows':len(calendars),'attempts_without_published_issuer_projection':len(attempts)-len(referenced),
            'selection_context_replayed':False,'upstream_context_replayed':False,'point_in_time_availability_verified':False,'investment_authority':False}
