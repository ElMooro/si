"""Whole SEC Atom capture, conditional publication and exact offline replay.

Only the declared public filing head and content-addressed filing originals are
read. The original one/four feed requests are retained without retries or any
provider expansion. No account, notification, order or paid-model capability.
"""
from datetime import datetime, timezone
from pathlib import Path
import time
import urllib.error
import urllib.request
import re
import sec_atom_model as model

PRIVATE = 'audit-private/20260909-originals/sec-filing-research/'
LIMIT = 64 * 1024 * 1024
COMPILERS = ('lambda_function.py', 'sec_atom_model.py', 'sec_atom_store.py')


class EvidenceError(ValueError):
    pass


def whole(stream, length=None, deadline=None):
    parts = []; size = 0
    try:
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                raise EvidenceError('Whole-response time bound exhausted')
            part = stream.read(min(65536, LIMIT + 1 - size))
            if not part: break
            if not isinstance(part, bytes): raise EvidenceError('Byte stream required')
            size += len(part)
            if size > LIMIT: raise EvidenceError('Whole response exceeds bound; never truncated')
            parts.append(part)
    finally:
        stream.close()
    if length is not None and (not str(length).isdigit() or int(length) != size):
        raise EvidenceError('Whole-response length differs')
    return b''.join(parts)


def allowed(key, kind):
    return key == model.HEADS[kind] or isinstance(key, str) and bool(re.fullmatch(re.escape(PRIVATE) + r'[a-f0-9]{64}\.bin', key))


def get(client, bucket, key, kind):
    if kind not in model.HEADS or not allowed(key, kind):
        raise EvidenceError('Unreviewed object read')
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as error:
        if str(getattr(error, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey', 'NotFound'):
            return None
        raise
    raw = whole(obj['Body'], obj.get('ContentLength'))
    if type(obj.get('ContentLength')) is not int or not isinstance(obj.get('ETag'), str) or not obj['ETag']:
        raise EvidenceError('Complete conditional object identity required')
    return {'raw': raw, 'etag': obj['ETag']}


def retained(client, bucket, ref, kind):
    if (not isinstance(ref, dict) or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256')))
            or ref.get('key') != PRIVATE + ref['sha256'] + '.bin' or type(ref.get('bytes')) is not int or not 0 <= ref['bytes'] <= LIMIT):
        raise EvidenceError('Exact retained original identity required')
    found = get(client, bucket, ref['key'], kind)
    if found is None or len(found['raw']) != ref['bytes'] or model.sha(found['raw']) != ref['sha256']:
        raise EvidenceError('Retained whole original differs')
    return found['raw']


def retain(client, bucket, raw, kind):
    if not isinstance(raw, bytes) or len(raw) > LIMIT:
        raise EvidenceError('Whole bounded original required')
    ref = {'key': PRIVATE + model.sha(raw) + '.bin', 'sha256': model.sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*',
                          ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as error:
        if str(getattr(error, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'PreconditionFailed', 'ConditionalRequestConflict'):
            raise
    if retained(client, bucket, ref, kind) != raw: raise EvidenceError('Immutable readback differs')
    return ref


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Budget:
    def __init__(self, client, deadline): self.client, self.deadline = client, deadline
    def check(self):
        if time.monotonic() + 5 >= self.deadline: raise EvidenceError('Complete-publication budget exhausted')
    def get_object(self, **kwargs): self.check(); return self.client.get_object(**kwargs)
    def put_object(self, **kwargs): self.check(); return self.client.put_object(**kwargs)


def compiler_bytes(native_path):
    result = {'lambda_function.py': Path(native_path).read_bytes()}
    for name in COMPILERS[1:]: result[name] = Path(__file__).with_name(name).read_bytes()
    return result


def context(ref):
    return {'manifest': ref, 'contract': 'sec-atom-original-replay.v1',
            'provider_originals_retained': True, 'original_vintage_verified': False,
            'public_head_write': 'conditional_on_exact_predecessor',
            'history': 'Every publication retains its complete predecessor; no prior head is retrospectively rewritten.'}


def run(client, bucket, native_path, recipe, user_agent, event=None, runtime_context=None, opener=None, now=None, sleep=None):
    if isinstance(event, dict) and ('httpMethod' in event or 'requestContext' in event):
        return {'statusCode': 409, 'body': 'Stored filing research; HTTP does not invoke source acquisition.'}
    model.recipe_check(recipe); kind = recipe['kind']; head = model.HEADS[kind]
    if bucket != 'justhodl-dashboard-live' or not isinstance(user_agent, str) or not user_agent.strip() or '\r' in user_agent or '\n' in user_agent:
        raise EvidenceError('Declared research bucket and existing SEC user agent required')
    now = now or (lambda: datetime.now(timezone.utc).isoformat())
    sleep = sleep or time.sleep
    remaining = runtime_context.get_remaining_time_in_millis() / 1000 if runtime_context is not None else 300
    deadline = time.monotonic() + min(280, remaining - 10)
    guarded = Budget(client, deadline); guarded.check()
    started = now()
    if model.clock(started) is None: raise EvidenceError('Explicit UTC acquisition clock required')
    prior = get(guarded, bucket, head, kind)
    prior_ref = {'key': head, 'original': retain(guarded, bucket, prior['raw'], kind), 'etag': prior['etag']} if prior else None
    if prior:
        previous = model.strict(prior['raw'])
        if not isinstance(previous, dict) or not isinstance(previous.get('filings'), list):
            raise EvidenceError('Malformed predecessor retained; replacement refused')
    open_request = opener or urllib.request.build_opener(NoRedirect()).open
    attempts = []; blocked = False; total_bytes = 0
    for form in model.FORMS[kind]:
        requested = now(); url = model.feed_url(form)
        attempt = {'requested_form': form, 'url': url, 'requested_at': requested}
        if blocked or time.monotonic() + 40 >= deadline:
            attempt['status'] = 'rate_limit_not_attempted' if blocked else 'budget_not_attempted'
        else:
            request = urllib.request.Request(url, headers={'User-Agent': user_agent, 'Accept-Encoding': 'identity'})
            try:
                try:
                    response = open_request(request, timeout=min(20 if kind == '8k' else 25, deadline - time.monotonic() - 30))
                except urllib.error.HTTPError as error:
                    response = error
            except (OSError, TimeoutError, urllib.error.URLError) as error:
                attempt.update(status='transport_error', error_type=type(error).__name__)
            else:
                headers = response.headers or {}
                status = response.getcode()
                if hasattr(response, 'geturl') and response.geturl() != url:
                    response.close(); raise EvidenceError('Redirect refused')
                raw = whole(response, headers.get('Content-Length'), deadline - 30)
                total_bytes += len(raw)
                if total_bytes > LIMIT: raise EvidenceError('Whole source population exceeds bound')
                attempt.update(status='http_response', http_status=status,
                               headers={k: headers.get(k) for k in ('Content-Type', 'Content-Length', 'Content-Encoding', 'Date', 'Last-Modified', 'ETag', 'Retry-After')},
                               original=retain(guarded, bucket, raw, kind))
                if headers.get('Content-Encoding', 'identity').lower() not in ('', 'identity'):
                    attempt['status'] = 'unsupported_encoding'
                blocked = status == 429
        attempt['received_at'] = now()
        attempt['attempt_manifest'] = retain(guarded, bucket, model.encode(attempt), kind)
        attempts.append(attempt)
        if kind == '10kq' and form != model.FORMS[kind][-1] and not blocked: sleep(0.3)
    generated = now()
    inputs = {'contract': model.CONTRACT, 'recipe': recipe, 'started_at': started,
              'generated_at': generated, 'prior': prior_ref, 'attempts': attempts}
    input_ref = retain(guarded, bucket, model.encode(inputs), kind)
    read = lambda ref: retained(guarded, bucket, ref, kind)
    # Even malformed/failed attempts are retained before the calculation is
    # allowed to fail. A failure never refreshes a last-good publication date.
    output = model.build(inputs, read)
    sources = compiler_bytes(native_path)
    compilers = {name: retain(guarded, bucket, raw, kind) for name, raw in sources.items()}
    output['compiler_sha256'] = {name: ref['sha256'] for name, ref in compilers.items()}
    output_ref = retain(guarded, bucket, model.encode(output), kind)
    plan = {'contract': 'sec-atom-original-replay.v1', 'kind': kind, 'input': input_ref,
            'output': output_ref, 'compilers': compilers}
    plan_ref = retain(guarded, bucket, model.encode(plan), kind)
    output['publication_context'] = context(plan_ref)
    raw = model.encode(output); retain(guarded, bucket, raw, kind)
    condition = {'IfMatch': prior['etag']} if prior else {'IfNoneMatch': '*'}
    guarded.put_object(Bucket=bucket, Key=head, Body=raw, ContentType='application/json', CacheControl='no-store', **condition)
    found = get(guarded, bucket, head, kind)
    if found is None or found['raw'] != raw:
        raise EvidenceError('Published readback differs; another writer is never rolled back')
    return {'statusCode': 200, 'body': model.encode({'ok': True, 'contract': model.CONTRACT,
            'filings': len(output['filings']), 'requested_feeds': len(attempts), 'calls_eligible': False}).decode('utf-8')}


def replay(client, bucket, native_path, kind, packet):
    if kind not in model.FORMS or packet.get('contract') != model.CONTRACT or packet.get('engine_family') != kind:
        raise EvidenceError('Expected native filing research publication required')
    ref = packet.get('publication_context', {}).get('manifest')
    plan = model.strict(retained(client, bucket, ref, kind))
    if plan.get('contract') != 'sec-atom-original-replay.v1' or plan.get('kind') != kind or set(plan.get('compilers', {})) != set(COMPILERS):
        raise EvidenceError('Whole original publication plan required')
    actual = compiler_bytes(native_path)
    for name, identity in plan['compilers'].items():
        if retained(client, bucket, identity, kind) != actual[name]:
            raise EvidenceError('Matching archived compiler required; current code cannot reinterpret old output')
    inputs = model.strict(retained(client, bucket, plan['input'], kind))
    if inputs.get('contract') != model.CONTRACT or inputs.get('recipe', {}).get('kind') != kind:
        raise EvidenceError('Exact native input family required')
    for attempt in inputs['attempts']:
        expected = {k: v for k, v in attempt.items() if k != 'attempt_manifest'}
        if model.strict(retained(client, bucket, attempt['attempt_manifest'], kind)) != expected:
            raise EvidenceError('Recorded attempt differs')
        if 'original' in attempt:
            raw = retained(client, bucket, attempt['original'], kind)
            length = attempt.get('headers', {}).get('Content-Length')
            if length is not None and (not str(length).isdigit() or int(length) != len(raw)):
                raise EvidenceError('Complete retained HTTP body differs')
    output = model.build(inputs, lambda identity: retained(client, bucket, identity, kind))
    output['compiler_sha256'] = {name: model.sha(raw) for name, raw in actual.items()}
    if model.encode(output) != retained(client, bucket, plan['output'], kind):
        raise EvidenceError('Whole compiler output differs')
    output['publication_context'] = context(ref)
    if model.encode(output) != model.encode(packet):
        raise EvidenceError('Whole current public packet differs')
    return {'status': 'complete_sec_atom_originals_replayed', 'kind': kind, 'requested_feeds': len(inputs['attempts']),
            'source_entries': sum(r.get('returned_entries', 0) for r in packet['source_responses']),
            'filing_versions': len(packet['filing_versions']), 'headline_filings': len(packet['filings']),
            'provider_requests': 0, 'public_writes': 0, **model.FLAGS}
