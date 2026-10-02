"""Dependency-free full-handler differential tests, with invented data only.

The byte fixture is the complete predecessor, SHA-256 pinned below. Neither
handler uses a real SDK, network, credentials, private state or retained data.
"""
import ast
import copy
import gzip
import hashlib
import io
import json
import sys
import traceback
import types
from concurrent.futures import Future
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
SOURCE = HERE.parent / 'source/lambda_function.py'
PREDECESSOR = HERE / 'fixtures/predecessor.py.txt'
PREDECESSOR_SHA = '9c82d0046498e7de75d5f24a8346047e0a59635972d438e65d1613331250a312'
BUCKET = 'invented-test-bucket'
STATE = 'data/_state/series-extract-{}.json'
STAMP = 1790899200
PRIVATE = 'invented-sensitive-checkpoint-do-not-log'


class ClientError(Exception):
    """Match botocore's service-error response/operation constructor contract."""
    def __init__(self, response, operation_name):
        self.response = response
        self.operation_name = operation_name
        super().__init__(response['Error'].get('Message', PRIVATE))


def service_error(code, status=404):
    return ClientError({'Error': {'Code': code, 'Message': PRIVATE},
                        'ResponseMetadata': {'HTTPStatusCode': status}}, 'GetObject')


class SpoofedError(Exception):
    response = {'Error': {'Code': 'NoSuchKey'}}


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls.fromtimestamp(STAMP, tz)


class InlinePool:
    def __init__(self, *args, **kwargs):
        pass
    def submit(self, function, *args, **kwargs):
        future = Future()
        try:
            future.set_result(function(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future
    def shutdown(self, **kwargs):
        pass


def load(path):
    module = types.ModuleType('series_fixture')
    sdk = types.ModuleType('boto3')
    sdk.client = lambda *a, **kw: None
    botocore = types.ModuleType('botocore')
    cfg = types.ModuleType('botocore.config')
    cfg.Config = lambda **kw: None
    exceptions = types.ModuleType('botocore.exceptions')
    exceptions.ClientError = ClientError
    with patch.dict(sys.modules, {'boto3': sdk, 'botocore': botocore,
                                 'botocore.config': cfg,
                                 'botocore.exceptions': exceptions}), \
            patch.dict('os.environ', {'S3_BUCKET': BUCKET}):
        exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    module.datetime = FrozenDateTime
    module.time = types.SimpleNamespace(time=lambda: STAMP)
    module.ThreadPoolExecutor = InlinePool
    return module


class Body:
    def __init__(self, storage, key, value, failure=None):
        self.storage, self.key, self.value, self.failure = storage, key, value, failure
    def read(self):
        self.storage.trace.append(('read', self.key))
        if self.failure:
            raise self.failure
        return self.value


class Storage:
    def __init__(self, objects, failure=None, provider='eurostat', fail_put=None):
        self.objects = copy.deepcopy(objects)
        self.trace = []
        self.failure, self.provider, self.fail_put = failure, provider, fail_put
    def head_object(self, **kwargs):
        self.trace.append(('head', kwargs))
        if kwargs['Key'] not in self.objects:
            raise service_error('NoSuchKey')
        return {'ContentLength': len(self.objects[kwargs['Key']])}
    def get_object(self, **kwargs):
        self.trace.append(('get', kwargs))
        key = kwargs['Key']
        if key == STATE.format(self.provider) and self.failure:
            stage, problem = self.failure
            if stage == 'get':
                raise problem
            if stage == 'missing_body':
                return {}
            if stage == 'body':
                return {'Body': Body(self, key, b'', problem)}
        if key not in self.objects:
            raise service_error('NoSuchKey')
        return {'Body': Body(self, key, self.objects[key])}
    def list_objects_v2(self, **kwargs):
        self.trace.append(('list', kwargs))
        keys = sorted(k for k in self.objects if k.startswith(kwargs['Prefix']))
        # Force pagination without modifying the handler's page-size control.
        start = int(kwargs.get('ContinuationToken', 0))
        size = min(kwargs['MaxKeys'], 2)
        result = {'Contents': [{'Key': k, 'Size': len(self.objects[k])}
                               for k in keys[start:start + size]],
                  'IsTruncated': start + size < len(keys)}
        if result['IsTruncated']:
            result['NextContinuationToken'] = str(start + size)
        return result
    def put_object(self, **kwargs):
        self.trace.append(('put', copy.deepcopy(kwargs)))
        if self.fail_put and self.fail_put in kwargs['Key']:
            raise RuntimeError('invented failed write response')
        self.objects[kwargs['Key']] = kwargs['Body']
        if self.fail_put == 'lost_checkpoint_response' and kwargs['Key'].startswith('data/_state/'):
            raise TimeoutError('invented lost checkpoint write response')


def warm(provider, rows=1001, flows=1, correction=0):
    result = {'data/audit/engine-writes-overrides.json': json.dumps(
        {'writes': {'test-engine': [provider + ':FLOW0']}}).encode()}
    for flow in range(flows):
        if provider == 'eurostat':
            value = 'freq,unit,geo\\TIME_PERIOD\t2025\t2026\n'
            value += ''.join(f'A,EUR,G{i}\t{i}\t{i + correction}\n' for i in range(rows))
            key = f'data/warm/eurostat/data/FLOW{flow}.dat.gz'
            result[key] = gzip.compress(value.encode(), mtime=STAMP)
        else:
            for year in (2025, 2026):
                value = 'KEY,FREQ,REF_AREA,UNIT,TIME_PERIOD,OBS_VALUE\n'
                value += ''.join(f'M.G{i}.EUR,M,G{i},EUR,{year}-01,{i + correction}\n'
                                 for i in range(rows))
                key = f'data/warm/ecb/data/FLOW{flow}__{year}_{year}.dat.gz'
                result[key] = gzip.compress(value.encode(), mtime=STAMP)
    return result


def checkpoint(**changes):
    result = {'flows_done': [], 'series_count': 1500, 'n_pages': 3,
              'pages_objects': 3, 'pages_bytes': 17000, 'pages_seeded': 'legacy',
              'buffer': [], 'page_hashes': {}}
    result.update(changes)
    return result


def objects_with(state, provider='eurostat', rows=1001, flows=1, correction=0):
    objects = warm(provider, rows, flows, correction)
    if state is not None:
        objects[STATE.format(provider)] = json.dumps(state).encode()
    objects[f'data/providers/{provider}/series/page-0000.json'] = b'{"invented_old_page":true}'
    return objects


def run(path, objects, event=None, failure=None, budget=None, fail_put=None, storage=None):
    module = load(path)
    provider = (event or {}).get('provider', 'eurostat')
    storage = storage or Storage(objects, failure, provider, fail_put)
    start_trace = len(storage.trace)
    module.s3 = storage
    context_calls = []
    def remaining():
        context_calls.append(True)
        return 0 if budget is not None and len(context_calls) >= budget else 900000
    stdout = io.StringIO()
    returned = error = None
    formatted = ''
    with redirect_stdout(stdout):
        try:
            returned = module.lambda_handler(event, types.SimpleNamespace(
                get_remaining_time_in_millis=remaining))
        except Exception as exc:
            error = (type(exc).__name__, str(exc))
            formatted = ''.join(traceback.format_exception(exc))
    return {'return': returned, 'error': error, 'stdout': stdout.getvalue(),
            'objects': copy.deepcopy(storage.objects), 'trace': copy.deepcopy(storage.trace[start_trace:]),
            'context_calls': len(context_calls), 'traceback': formatted}


def equivalent(objects, **kwargs):
    old, new = (run(path, objects, **kwargs) for path in (PREDECESSOR, SOURCE))
    assert {k: v for k, v in new.items() if k != 'traceback'} == {
        k: v for k, v in old.items() if k != 'traceback'}, (kwargs, new['error'], old['error'])
    return new


def rejected(objects, failure=None, provider='eurostat'):
    result = run(SOURCE, objects, {'provider': provider}, failure)
    assert result['error'] and result['error'][0] == 'RuntimeError', result
    assert result['return'] is None and result['stdout'] == ''
    assert result['objects'] == objects
    assert all(t[0] in ('get', 'read') for t in result['trace'])
    assert result['trace'][0] == ('get', {'Bucket': BUCKET, 'Key': STATE.format(provider)})
    assert len([t for t in result['trace'] if t[0] == 'get']) == 1
    assert result['context_calls'] == 0
    assert PRIVATE not in result['traceback'], result['traceback']
    return result


def scope_test():
    assert hashlib.sha256(PREDECESSOR.read_bytes()).hexdigest() == PREDECESSOR_SHA
    old, new = (ast.parse(p.read_bytes()) for p in (PREDECESSOR, SOURCE))
    new.body = [n for n in new.body if not (
        isinstance(n, ast.FunctionDef) and n.name == '_validate_allocation_counters'
        or isinstance(n, ast.ImportFrom) and n.module == 'botocore.exceptions')]
    a, b = (next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name == 'lambda_handler') for tree in (old, new))
    ai = next(i for i, n in enumerate(a.body) if isinstance(n, ast.Try))
    bi = next(i for i, n in enumerate(b.body) if isinstance(n, ast.Try))
    # Replace only admission plus its validation call with exact old admission.
    assert isinstance(b.body[bi + 1], ast.Expr)
    assert b.body[bi + 1].value.func.id == '_validate_allocation_counters'
    b.body[bi:bi + 2] = [a.body[ai]]
    assert ast.dump(old, include_attributes=False) == ast.dump(new, include_attributes=False)


def healthy_tests():
    count = 0
    for provider in ('eurostat', 'ecb'):
        empty = equivalent({}, event={'provider': provider})
        assert json.loads(empty['return']['body'])['n_pages'] == 0
        assert json.loads(empty['return']['body'])['series_extracted'] == 0
        first_page = equivalent(warm(provider, rows=500), event={'provider': provider})
        assert json.loads(first_page['return']['body'])['n_pages'] == 1
        count += 2
        for rows in (0, 1, 499, 500, 501, 1001):
            for state in (checkpoint(), checkpoint(flows_done=['FLOW0']), None):
                result = equivalent(objects_with(state, provider, rows), event={'provider': provider})
                assert result['return']['statusCode'] == 200
                count += 1
        for changes in ({'buffer': [{'id': 'invented-buffer', 'flow': 'OLD'}]},
                        {'flow_progress': {'FLOW0': {'rows_done': 500, 'attempts': 1}}},
                        {'flow_progress': {'FLOW0': {'slice_idx': 1, 'attempts': 1}}},
                        {'series_count': 1500.0, 'pages_objects': ' +003 ', 'pages_bytes': '017000'},
                        {'series_count': -0.0, 'pages_objects': '1_000', 'pages_bytes': '+0'},
                        {'series_count': 1500.0, 'pages_objects': 3.0, 'pages_bytes': 17000.0}):
            equivalent(objects_with(checkpoint(**changes), provider, flows=3),
                       event={'provider': provider})
            count += 1
        legacy = checkpoint()
        for key in ('buffer', 'page_hashes', 'pages_objects', 'pages_bytes', 'pages_seeded'):
            legacy.pop(key, None)
        equivalent(objects_with(legacy, provider), event={'provider': provider})
        count += 1
        for optional in (None, ''):
            # These are successful old manifest defaults on an idle checkpoint.
            equivalent(objects_with(checkpoint(flows_done=['FLOW0'], pages_objects=optional,
                                               pages_bytes=optional), provider),
                       event={'provider': provider})
            count += 1
        for budget in (1, 2, 3, 5):
            equivalent(objects_with(checkpoint(), provider, flows=3),
                       event={'provider': provider}, budget=budget)
            count += 1
        for state in (checkpoint(), None):
            equivalent(objects_with(state, provider), event={'provider': provider}, fail_put='page-')
            count += 1
    equivalent({}, event={'provider': 'unsupported'})
    # Tier1 stays outside admission; exercise its unchanged empty build branch.
    equivalent({'data/index/eurostat/flows.json.gz': gzip.compress(
        b'{"flows":{}}', mtime=STAMP)}, event={'provider': 'eurostat', 'mode': 't1'})
    return count + 2


def failure_tests():
    count = 0
    failures = [('get', service_error(c, s)) for c, s in (
        ('AccessDenied', 403), ('InternalError', 500), ('NoSuchBucket', 404),
        ('404', 404), ('NotFound', 404), ('RequestTimeout', 400),
        ('SlowDown', 503), ('nosuchkey', 404))]
    failures += [('get', TimeoutError(PRIVATE)), ('get', ConnectionError(PRIVATE)),
                 ('get', RuntimeError(PRIVATE)), ('get', SpoofedError(PRIVATE)),
                 ('missing_body', None), ('body', TimeoutError(PRIVATE)),
                 ('body', OSError(PRIVATE)), ('body', service_error('NoSuchKey'))]
    for provider in ('eurostat', 'ecb'):
        objects = objects_with(checkpoint(), provider)
        for failure in failures:
            rejected(objects, failure, provider)
            count += 1
        for raw in (b'', b'{', b'{"n_pages":', b'\xff', b'null', b'[]', b'1',
                    b'"' + PRIVATE.encode() + b'"', b'{}'):
            broken = copy.deepcopy(objects)
            broken[STATE.format(provider)] = raw
            rejected(broken, provider=provider)
            count += 1
        for field in ('n_pages', 'series_count', 'pages_objects', 'pages_bytes'):
            values = [True, False, -1, -1.0, 1.25, float('nan'), float('inf'),
                      -float('inf'), [], {}, 'invalid', '-1']
            if field in ('n_pages', 'series_count'):
                values += [None, '', '3']
            if field == 'n_pages':
                values += [0.0, 3.0]
            for value in values:
                rejected(objects_with(checkpoint(**{field: value}), provider), provider=provider)
                count += 1
        # A subsequent scheduled run against the readable checkpoint is normal.
        failed = rejected(objects, ('get', TimeoutError(PRIVATE)), provider)
        recovered = equivalent(failed['objects'], event={'provider': provider})
        assert recovered['return']['statusCode'] == 200
        assert recovered['objects'][f'data/providers/{provider}/series/page-0000.json'] == objects[
            f'data/providers/{provider}/series/page-0000.json']
        count += 1
        store = Storage(objects, ('get', TimeoutError(PRIVATE)), provider)
        first = run(SOURCE, objects, event={'provider': provider}, storage=store)
        assert first['error'] == ('RuntimeError', 'checkpoint retrieval failed')
        assert store.objects == objects
        store.failure = None
        next_run = run(SOURCE, objects, event={'provider': provider}, storage=store)
        expected = run(PREDECESSOR, objects, event={'provider': provider})
        assert {k: v for k, v in next_run.items() if k != 'traceback'} == {
            k: v for k, v in expected.items() if k != 'traceback'}
        assert json.loads(store.objects[STATE.format(provider)])['n_pages'] == 5
        count += 1
    return count


def replay_and_corruption_tests():
    objects = objects_with(checkpoint(n_pages=0, series_count=0), rows=500)
    completed = equivalent(objects)
    page = 'data/providers/eurostat/series/page-0000.json'
    assert completed['objects'][page] != objects[page]
    # Crash after page PUT, before checkpoint commit: old checkpoint replays tail.
    uncommitted = copy.deepcopy(objects)
    uncommitted[page] = completed['objects'][page]
    replayed = equivalent(uncommitted)
    assert any(t[0] == 'put' and t[1]['Key'] == page for t in replayed['trace'])
    # An already recorded identical tail hash suppresses the same PUT as before.
    saved = checkpoint(n_pages=0, series_count=0, page_hashes={
        page: hashlib.sha256(completed['objects'][page]).hexdigest()[:32]})
    skipped = equivalent(objects_with(saved, rows=500))
    assert not any(t[0] == 'put' and t[1]['Key'] == page for t in skipped['trace'])
    corrected = equivalent(objects_with(saved, rows=500, correction=10))
    assert corrected['objects'][page] != completed['objects'][page]
    # Lost checkpoint response, NOT lost data: synthetic original corruption.
    valid = objects_with(checkpoint(), rows=500)
    original = run(PREDECESSOR, valid, failure=('get', TimeoutError(PRIVATE)))
    assert original['return']['statusCode'] == 200
    assert original['objects'][page] != valid[page]
    assert json.loads(original['objects'][STATE.format('eurostat')])['n_pages'] == 1
    after_retry = run(PREDECESSOR, original['objects'])
    assert after_retry['objects'][page] != valid[page]
    rejected(valid, ('get', TimeoutError(PRIVATE)))
    # Exact NoSuchKey remains bootstrap even with existing pages: unresolved risk.
    missing = objects_with(None, rows=500)
    boot = equivalent(missing)
    assert boot['objects'][page] != missing[page]
    lost_write = equivalent(objects, fail_put='lost_checkpoint_response')
    # The server persisted this checkpoint; a retry reads its real progress.
    recovered = equivalent(lost_write['objects'])
    assert recovered['objects'][page] == completed['objects'][page]
    assert json.loads(recovered['objects'][STATE.format('eurostat')])['n_pages'] == 1
    # Retrying a successful checkpoint must not rewrite committed pages.
    idle = equivalent(completed['objects'])
    assert not any(t[0] == 'put' and '/series/page-' in t[1]['Key'] for t in idle['trace'])
    return 10


def consumer_tests():
    # Run the actual consumer functions on synthetic complete-handler output.
    symdir = ast.parse((ROOT / 'aws/lambdas/justhodl-symdir/source/lambda_function.py').read_bytes())
    catalog = ast.parse((ROOT / 'aws/lambdas/justhodl-provider-catalog/source/lambda_function.py').read_bytes())
    counts = 0
    for provider in ('eurostat', 'ecb'):
        result = equivalent(objects_with(checkpoint(), provider, rows=500), event={'provider': provider})
        objects = result['objects']
        namespace = {'json': json, 'PROV_NAME': {provider: 'Test'},
                     '_get_json': lambda k: json.loads(objects[k])}
        nodes = [n for n in symdir.body if isinstance(n, ast.FunctionDef)
                 and n.name in ('_page_rows', '_page_row')]
        exec(compile(ast.Module(nodes, type_ignores=[]), '<actual-symdir-consumer>', 'exec'), namespace)
        rows = namespace['_page_rows'](provider, 'FLOW0', 3)
        projected = [namespace['_page_row'](provider, 'FLOW0', row) for row in rows]
        assert len(projected) == 500 and projected[0]['chartable'] is True
        fn = next(n for n in catalog.body if isinstance(n, ast.FunctionDef) and n.name == '_series_list')
        namespace['_get_doc'] = lambda k: json.loads(objects[k])
        exec(compile(ast.Module([fn], type_ignores=[]), '<actual-catalog-consumer>', 'exec'), namespace)
        summary = namespace['_series_list']((f'data/providers/{provider}/series-manifest.json', 'series_extracted'))
        assert summary == {'count': 2000, 'ids': [], 'counted': True}, summary
        counts += 2
    return counts


if __name__ == '__main__':
    scope_test()
    healthy = healthy_tests()
    failures = failure_tests()
    replay = replay_and_corruption_tests()
    consumers = consumer_tests()
    print(f'Series admission PASS: {healthy} healthy/legacy full-handler differentials; '
          f'{failures} rejected/recovery cases; {replay} replay/corruption cases; '
          f'{consumers} actual consumer cases; complete source scope identity')
