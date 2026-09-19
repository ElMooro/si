"""Original exchange capture, correction-preserving ledger and atomic snapshot publication."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import time
import urllib.error
import urllib.request
import evidence_store
import hot_native as native
import hot_research as model
import pd_fails_context

PREFIX = 'data/hot-money-research/'
CURRENT = 'data/hot-money.json'
STATE = PREFIX + 'ledger.json'
LEDGERS = {'twse': 'data/providers/twse/bfi82u-foreign.json', 'tpex': 'data/providers/tpex/insti-foreign.json'}
COMPILERS = (model, native, pd_fails_context)
MAX_BYTES = 16*1024*1024


def now(): return datetime.now(timezone.utc).isoformat()
def code(exc): return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
def missing(exc): return code(exc) in ('NoSuchKey', '404')
def conflict(exc): return code(exc) in ('PreconditionFailed', 'ConditionalRequestConflict', '412', '409')


def bounded(body):
    try: raw = body.read(MAX_BYTES+1)
    finally: body.close()
    if len(raw) > MAX_BYTES: raise ValueError('exchange evidence size bound')
    return raw


def raw_reader(client, bucket):
    def read(key):
        if not isinstance(key, str) or not re.fullmatch(r'data/[A-Za-z0-9_./-]+', key) or '..' in key:
            raise ValueError('public evidence path required')
        raw = bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        if raw[:2] == b'\x1f\x8b': raw = bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        return raw
    return read


def immutable(client, bucket, key, raw, kind='application/json'):
    try: client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc): raise
    if raw_reader(client, bucket)(key) != raw: raise ValueError('retained exchange bytes differ')


def read_state(client, bucket):
    try:
        obj = client.get_object(Bucket=bucket, Key=STATE)
        state = json.loads(bounded(obj['Body']))
        if state.get('contract') != 'exchange-original-ledger.v1': raise ValueError('invalid exchange ledger contract')
        if set(state.get('boards', {})) != set(model.BOARDS): raise ValueError('invalid exchange ledger boards')
        native.clock(state['generated_at'])
        return state, obj['ETag']
    except Exception as exc:
        if not missing(exc): raise
        return {'contract': 'exchange-original-ledger.v1', 'generated_at': '1970-01-01T00:00:00+00:00',
                'boards': {b: {} for b in model.BOARDS}, 'legacy_context': {}, 'legacy_rows': {}}, None


def preserve(client, bucket, state):
    read = raw_reader(client, bucket); refs = deepcopy(state['legacy_context']); rows = deepcopy(state['legacy_rows'])
    for board, key in {**LEDGERS, 'packet': CURRENT}.items():
        if key in refs: continue
        try: raw = read(key)
        except Exception as exc:
            if missing(exc): continue
            raise
        try: old = json.loads(raw)
        except (ValueError, UnicodeDecodeError): old = None
        sha = hashlib.sha256(raw).hexdigest(); dest = PREFIX+'legacy-unvalidated/'+sha+('.json' if old is not None else '.bin')
        immutable(client, bucket, dest, raw, 'application/json' if old is not None else 'application/octet-stream')
        refs[key] = {'key': dest, 'sha256': sha, 'bytes': len(raw), 'status': 'UNQUALIFIED_LEGACY'}
        if board in model.BOARDS:
            valid = {}
            for date, value in (old.get('rows', {}) if isinstance(old, dict) else {}).items():
                try:
                    native.day(date)
                    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value: continue
                    valid[date] = str(int(value))
                except (ValueError, TypeError, OverflowError): continue
            rows[board] = valid
    return refs, rows


def acquire(client, bucket, board, requested=None):
    url = native.requested_url(board, requested)
    request = urllib.request.Request(url, headers={'User-Agent': 'JustHodl source research', 'Accept': 'application/json'})
    # TLS stays verified. A fresh opener bypasses any provider-shaped legacy urlopen shim.
    for attempt in range(2):
        try:
            with urllib.request.build_opener().open(request, timeout=20) as response: raw = response.read(2*1024*1024+1)
            break
        except urllib.error.HTTPError as exc:
            if attempt or exc.code not in (429, 500, 502, 503, 504, 520): raise
            time.sleep(2)
    if len(raw) > 2*1024*1024: raise ValueError('exchange response exceeds capture bound')
    stamp = now()
    receipt = evidence_store.capture(client, bucket, 'twse' if board == 'twse' else 'tpex', url.split('?')[0], raw,
                                     received_at=native.clock(stamp))
    descriptor = {'board': board, 'url': url, 'requested_day': requested, 'acquired_at': stamp, 'evidence': receipt}
    return descriptor, native.parse(board, raw, descriptor)


def merge(state, updates, refs, rows, stamp):
    out = deepcopy(state)
    out['legacy_context'].update(refs)
    for board in model.BOARDS:
        out['legacy_rows'].setdefault(board, {}).update(rows.get(board, {}))
    for board, date, descriptor in updates:
        old = out['boards'][board].get(date)
        if old:
            prior = old['descriptor']; a, b = native.clock(prior['acquired_at']), native.clock(descriptor['acquired_at'])
            if a > b: continue
            if a == b and prior != descriptor: raise ValueError('conflicting captures at identical acquisition clock')
            history = list(old.get('revisions', []))
            if prior['evidence']['sha256'] != descriptor['evidence']['sha256']: history.append(prior)
        else: history = []
        out['boards'][board][date] = {'descriptor': descriptor, 'revisions': history}
    out['generated_at'] = max(state['generated_at'], stamp, key=native.clock)
    return out


def originals_for(state, probe, read):
    descriptors = [point['descriptor'] for board in state['boards'].values() for point in board.values()]
    descriptors += [d for board in state['boards'].values() for point in board.values() for d in point.get('revisions', [])]
    if probe: descriptors.append(probe)
    keys = sorted({d['evidence']['key'] for d in descriptors})
    if len(keys) > 4000: raise ValueError('exchange ledger requires partitioning before more originals')
    with ThreadPoolExecutor(max_workers=8) as pool: return dict(zip(keys, pool.map(read, keys)))


def publish(client, bucket, key, packet):
    for _ in range(4):
        try:
            obj = client.get_object(Bucket=bucket, Key=key); etag = obj['ETag']
            try: old = json.loads(bounded(obj['Body']))
            except (ValueError, UnicodeDecodeError): old = {}
            if not isinstance(old, dict): old = {}
        except Exception as exc:
            if not missing(exc): raise
            old = {}; etag = None
        if old.get('contract') in (model.CONTRACT, 'exchange-ledger-projection.v1'):
            if native.clock(old['generated_at']) >= native.clock(packet['generated_at']): return False
            if native.clock(old['source_generated_at']) > native.clock(packet['source_generated_at']): return False
        try:
            client.put_object(Bucket=bucket, Key=key, Body=model.encoded(packet), ContentType='application/json', CacheControl='no-store',
                              **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            return True
        except Exception as exc:
            if not conflict(exc): raise
    raise RuntimeError('exchange publication contention')


def run(client, bucket, event=None, context=None):
    event = event or {}; state, etag = read_state(client, bucket); refs, legacy_rows = preserve(client, bucket, state)
    read = raw_reader(client, bucket); updates = []; errors = {}; backfill_errors = {}; probe = None
    latest = {}; started = time.monotonic()
    def error_label(exc): return 'HTTP_'+str(exc.code) if isinstance(exc, urllib.error.HTTPError) else type(exc).__name__
    for board in ('twse', 'tpex_openapi'):
        try:
            descriptor, row = acquire(client, bucket, board)
            if board == 'twse': updates.append(('twse', row['date'], descriptor)); latest['twse'] = row['date']
            else:
                probe = descriptor; descriptor, dated = acquire(client, bucket, 'tpex', row['date'])
                if any(dated[k] != row[k] for k in ('buy_twd', 'sell_twd', 'net_twd')): raise ValueError('TPEx source copies disagree')
                updates.append(('tpex', dated['date'], descriptor)); latest['tpex'] = dated['date']
        except Exception as exc: errors['twse' if board == 'twse' else 'tpex'] = error_label(exc)
    interim = merge(state, updates, refs, legacy_rows, now())
    candidates = sorted({date for rows in legacy_rows.values() for date in rows} | {d for b in interim['boards'].values() for d in b}, reverse=True)
    today = native.clock(now()).astimezone(native.TZ).date()
    candidates = [d for d in candidates if 0 <= (today-native.day(d)).days <= 180][:86]
    budget = event.get('max_backfill_requests', 12)
    if isinstance(budget, bool) or not isinstance(budget, int): raise ValueError('integer acquisition budget required')
    budget = max(0, min(45, budget)); attempts = 0
    # Recheck two previous known dates for revisions; remaining requests recover originals for legacy dates.
    jobs = [(b, d) for d in candidates for b in model.BOARDS if d != latest.get(b) and
            (d not in interim['boards'][b] or d in candidates[:3])]
    for board, date in jobs:
        if attempts >= budget or time.monotonic()-started > 190 or (context and context.get_remaining_time_in_millis() < 75000): break
        attempts += 1; time.sleep(2.2)
        try:
            descriptor, row = acquire(client, bucket, board, date); updates.append((board, row['date'], descriptor))
        except Exception as exc: backfill_errors[board+':'+date] = error_label(exc)
    print(json.dumps({'stage':'exchange_capture','elapsed_s':round(time.monotonic()-started,2),
                      'updates':len(updates),'backfill_attempts':attempts,'current_errors':errors,'backfill_errors':backfill_errors}))
    # Validate merged originals before the first mutable write, and retain every prior ledger version.
    for _ in range(4):
        merged = merge(state, updates, refs, legacy_rows, now()); originals = originals_for(merged, probe, read)
        try: fails = json.loads(read('data/settlement-fails.json'))
        except Exception as exc:
            if not missing(exc): raise
            fails = {}
        inputs = {'state': merged, 'generated_at': now(), 'openapi': probe, 'legacy_rows': merged['legacy_rows'],
                  'legacy_context': merged['legacy_context'], 'fails_context': fails,
                  'current_errors': errors, 'backfill_errors': backfill_errors}
        output = model.build(inputs, originals)
        previous = model.encoded(state)
        immutable(client, bucket, PREFIX+'ledger-versions/'+model.digest(state)+'.json', previous)
        immutable(client, bucket, PREFIX+'ledger-versions/'+model.digest(merged)+'.json', model.encoded(merged))
        try:
            client.put_object(Bucket=bucket, Key=STATE, Body=model.encoded(merged), ContentType='application/json', CacheControl='no-store',
                              **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            break
        except Exception as exc:
            if not conflict(exc): raise
            state, etag = read_state(client, bucket)
    else: raise RuntimeError('exchange ledger contention')
    raw = model.encoded(inputs); input_sha = model.digest(inputs); key = PREFIX+'inputs/'+input_sha+'.json'
    immutable(client, bucket, key, raw); compilers = {}
    for module in COMPILERS:
        body = Path(module.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest(); dest = PREFIX+'compilers/'+sha+'.py'
        immutable(client, bucket, dest, body, 'text/plain'); compilers[module.__name__] = {'key': dest, 'sha256': sha}
    body = model.encoded(output); sha = model.digest(output); dest = PREFIX+'outputs/'+sha+'.json'
    immutable(client, bucket, dest, body)
    manifest = {'contract': 'hot-money-replay.v1', 'generated_at': inputs['generated_at'], 'compilers': compilers,
                'input': {'key': key, 'sha256': input_sha, 'bytes': len(raw)}, 'output_sha256': sha,
                'output': {'key': dest, 'sha256': sha, 'bytes': len(body)}}
    manifest_key = PREFIX+'runs/'+model.digest(manifest)+'.json'; immutable(client, bucket, manifest_key, model.encoded(manifest))
    retained = json.loads(read(key))
    if model.build(retained, originals_for(retained['state'], retained['openapi'], read)) != output: raise ValueError('exchange original replay differs')
    output['replay'] = {'manifest_key': manifest_key, 'output_sha256': sha, 'compilers': compilers}
    published = publish(client, bucket, CURRENT, output)
    if published:
        for board, ledger_key in LEDGERS.items():
            rows = {d: int(v) for d, v in merged['legacy_rows'].get(board, {}).items()}
            verified = {row['date']: int(row['net_twd']) for row in (output['countries']['taiwan'] if board == 'twse' else output['countries']['taiwan']['otc'])['history']}
            rows.update(verified)
            projection = {'contract': 'exchange-ledger-projection.v1', 'generated_at': output['generated_at'],
                'source_generated_at': output['source_generated_at'], 'rows': rows, 'original_verified_dates': sorted(verified),
                'legacy_unverified_dates': sorted(set(rows)-set(verified)), 'replay': output['replay'],
                'source': native.SCOPES[board], 'unit': 'TWD', 'calls_eligible': False}
            publish(client, bucket, ledger_key, projection)
    return {'published': published, 'generated_at': output['generated_at'], 'quality': output['quality'],
            'replay': output['replay'], 'backfill_attempts': attempts, 'backfill_errors': backfill_errors,
            'paid_ai_calls': 0, 'notifications_sent': 0, 'private_account_reads': 0, 'portfolio_writes': 0}
