"""Full-handler storage uncertainty/recovery tests; invented objects, fake SDK only."""
import ast
import copy
import hashlib
import json
import socket
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
BASELINE = HERE / 'fixtures/storage-predecessor.py.txt'
BASELINE_SHA = 'bcd80ce358aa433d9de33c45b9fb2900987c63046d343462b3c359b7c3724867'


def restore_scope(source):
    """Verify the repair's AST footprint, then restore PR75 for its old proof.

    Only collection, submission, checkpoint PUT handling, the two fatal
    bypasses and worker cleanup change. Admission, parsing, output and all
    other modes remain covered by the original predecessor scope check.
    """
    assert hashlib.sha256(BASELINE.read_bytes()).hexdigest() == BASELINE_SHA
    old, new = ast.parse(BASELINE.read_bytes()), ast.parse(source.read_bytes())
    cls = next(n for n in new.body if isinstance(n, ast.ClassDef) and n.name == 'StorageWriteError')
    assert [n.id for n in cls.bases] == ['RuntimeError']
    new.body.remove(cls)
    a, b = (next(n for n in t.body if isinstance(n, ast.FunctionDef) and n.name == 'lambda_handler')
            for t in (old, new))
    for name in ('_collect', '_flush', '_checkpoint'):
        i = next(i for i, n in enumerate(b.body) if isinstance(n, ast.FunctionDef) and n.name == name)
        b.body[i] = copy.deepcopy(next(n for n in a.body if isinstance(n, ast.FunctionDef) and n.name == name))
    outer = next(n for n in b.body if isinstance(n, ast.Try) and n.finalbody)
    assert not outer.handlers and not outer.orelse
    assert ast.unparse(outer.finalbody) == ('try:\n    _collect(True)\nfinally:\n    pool.shutdown(wait=True)')
    fatal = [n for n in ast.walk(outer) if isinstance(n, ast.ExceptHandler)
             and isinstance(n.type, ast.Name) and n.type.id == 'StorageWriteError']
    assert len(fatal) == 2
    for h in fatal:
        assert len(h.body) == 1 and isinstance(h.body[0], ast.Raise) and h.body[0].exc is None
    for n in ast.walk(outer):
        if isinstance(n, ast.Try):
            n.handlers = [h for h in n.handlers if h not in fatal]
    i = b.body.index(outer)
    shutdown = next(n for n in a.body if isinstance(n, ast.Expr)
                    and ast.unparse(n) == 'pool.shutdown(wait=True)')
    b.body[i:i + 1] = outer.body + [copy.deepcopy(shutdown)]
    i = next(i for i, n in enumerate(b.body) if isinstance(n, ast.Assign)
             and any(isinstance(t, ast.Name) and t.id == 'body' for t in n.targets))
    wrapped = b.body[i + 1]
    assert isinstance(wrapped, ast.Try) and len(wrapped.handlers) == 1
    assert ast.unparse(wrapped.handlers[0]) == ("except Exception:\n    raise StorageWriteError('checkpoint write failed or uncertain') from None")
    old_put = next(n for n in a.body if isinstance(n, ast.Expr)
                   and isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Attribute)
                   and n.value.func.attr == 'put_object'
                   and any(k.arg == 'Key' and isinstance(k.value, ast.Name) and k.value.id == 'state_key'
                           for k in n.value.keywords))
    b.body[i:i + 2] = [copy.deepcopy(old_put)]
    assert ast.dump(new, include_attributes=False) == ast.dump(old, include_attributes=False)
    return new


class FaultStore:
    """Wrap the original fake store with numbered precommit/uncertain outcomes."""
    def __init__(self, base, objects, provider, faults=None):
        self.inner = base.Storage(objects, provider=provider)
        self.provider = provider
        self.faults = faults or {}
        self.counts = {'page': 0, 'checkpoint': 0, 'manifest': 0}
        self.attempts = []
        self.checkpoints = []
        self.lock = threading.RLock()
        self.base = base
    @property
    def objects(self):
        return self.inner.objects
    @property
    def trace(self):
        return self.inner.trace
    def __getattr__(self, name):
        return getattr(self.inner, name)
    def put_object(self, **kw):
        with self.lock:
            key = kw['Key']
            kind = ('checkpoint' if key == self.base.STATE.format(self.provider)
                    else 'manifest' if key.endswith('series-manifest.json') else 'page')
            self.counts[kind] += 1
            fault = self.faults.get((kind, self.counts[kind]))
            self.attempts.append((kind, key, fault))
            if not fault:
                result = self.inner.put_object(**kw)
            else:
                self.trace.append(('put', copy.deepcopy(kw)))
                if fault == 'lost':
                    self.objects[key] = kw['Body']
                elif fault != 'before':
                    raise AssertionError(fault)
                result = None
            if kind == 'checkpoint' and fault != 'before':
                self.checkpoints.append(kw['Body'])
            if fault:
                raise TimeoutError(self.base.PRIVATE)
            return result


class DeferredPool:
    """Futures finish on collection. Alternate results arrive only in cleanup."""
    instances = []
    reject_at = None
    def __init__(self, *args, **kwargs):
        self.entries = []
        self.shutdown_called = False
        type(self).instances.append(self)
    def submit(self, function, *args, **kwargs):
        if len(self.entries) + 1 == self.reject_at:
            raise RuntimeError('invented submission uncertainty')
        owner = self
        class Deferred(Future):
            def __init__(self):
                super().__init__()
                self.harvested = False
            def done(self):
                # Some successes stay outstanding until the fatal cleanup.
                return len(owner.entries) % 2 == 0 if self is owner.entries[-1] else False
            def result(self, timeout=None):
                if not super().done():
                    try:
                        self.set_result(function(*args, **kwargs))
                    except Exception as e:
                        self.set_exception(e)
                self.harvested = True
                return super().result(timeout)
        future = Deferred()
        self.entries.append(future)
        # First writer responds early; others are deliberately late.
        if len(self.entries) == 1:
            try:
                future.set_result(function(*args, **kwargs))
            except Exception as e:
                future.set_exception(e)
            future.done = lambda: True
        return future
    def shutdown(self, wait=True):
        assert wait
        assert all(f.harvested for f in self.entries), 'unharvested worker result'
        self.shutdown_called = True


def invoke(base, source, store, provider, pool=None, budget=None, writers=None):
    original = base.load
    def load(path):
        module = original(path)
        if pool:
            module.ThreadPoolExecutor = pool
        if writers is not None:
            module.WRITERS = writers
        return module
    with patch.object(base, 'load', load):
        return base.run(source, {}, {'provider': provider}, storage=store, budget=budget)


def puts(result, kind):
    return [t[1] for t in result['trace'] if t[0] == 'put' and
            (t[1]['Key'].startswith('data/_state/') if kind == 'checkpoint' else
             t[1]['Key'].endswith('series-manifest.json') if kind == 'manifest' else
             '/series/page-' in t[1]['Key'])]


def initial(base, provider, rows=500, flows=1, state=None):
    state = state or base.checkpoint()
    objects = base.objects_with(state, provider, rows=rows, flows=flows)
    objects[f'data/providers/{provider}/series-manifest.json'] = b'{"invented_previous_manifest":true}'
    return objects


def refused(base, result, store, provider, old_manifest, reason):
    assert result['return'] is None and result['stdout'] == ''
    assert result['error'] == ('StorageWriteError', reason), result['error']
    assert base.PRIVATE not in result['traceback']
    assert not puts(result, 'manifest')
    assert store.objects[f'data/providers/{provider}/series-manifest.json'] == old_manifest
    saved = json.loads(store.objects[base.STATE.format(provider)])
    assert not any('TimeoutError' in e for e in saved.get('errors', {}).values())
    assert not saved.get('missing_pages')


def healthy(base):
    count = 0
    # Check actual asynchronous executor in addition to deterministic completion.
    for provider in ('eurostat', 'ecb'):
        for pool in (base.InlinePool, DeferredPool, ThreadPoolExecutor):
            for rows, flows in ((0, 2), (501, 3), (3501, 2)):
                objects = initial(base, provider, rows=rows, flows=flows)
                old = invoke(base, BASELINE, FaultStore(base, objects, provider), provider, pool)
                new = invoke(base, base.SOURCE, FaultStore(base, objects, provider), provider, pool)
                # Parallel PUT ordering can differ. Bytes and all other behavior must agree.
                for result in (old, new):
                    result['trace'] = sorted((repr(t) for t in result['trace']))
                    result.pop('traceback')
                assert old == new
                count += 1
        # Retirement, stall, carried buffer, progress/budget and normal repeat.
        for state in (base.checkpoint(buffer=[{'id': 'carried', 'flow': 'OLD'}]),
                      base.checkpoint(flow_progress={'FLOW0': {'rows_done': 500, 'slice_idx': 1}}),
                      base.checkpoint(flow_progress={'FLOW0': {'rows_done': 0, 'slice_idx': 0,
                                      'attempts': 4, 'rows_at_last_check': 0, 'slice_at_last_check': 0}})):
            objects = initial(base, provider, rows=1001, flows=3, state=state)
            for budget in (None, 1, 3, 5):
                old = base.run(BASELINE, objects, {'provider': provider}, budget=budget)
                new = base.run(base.SOURCE, objects, {'provider': provider}, budget=budget)
                assert old == new
                count += 1
        objects = initial(base, provider, rows=500)
        for _ in range(3):
            old = base.run(BASELINE, objects, {'provider': provider})
            new = base.run(base.SOURCE, objects, {'provider': provider})
            assert old == new
            objects = new['objects']
            count += 1
    return count


def baseline_reproductions(base):
    count = 0
    for provider in ('eurostat', 'ecb'):
        for kind in ('page', 'checkpoint'):
            for fault in ('before', 'lost'):
                objects = initial(base, provider, rows=1500, flows=2)
                results = []
                for source in (base.PREDECESSOR, BASELINE):
                    store = FaultStore(base, objects, provider, {(kind, 1): fault})
                    result = invoke(base, source, store, provider)
                    assert json.loads(result['return']['body'])['ok'] is True
                    assert json.loads(store.objects[base.STATE.format(provider)])['flows_done'] == ['FLOW0', 'FLOW1']
                    repeat = invoke(base, source, store, provider)
                    assert not puts(repeat, 'page')
                    results.append(result)
                assert results[0] == results[1]
                count += 1
    return count


def failure_recovery(base):
    count = 0
    for provider in ('eurostat', 'ecb'):
        for kind, nth, rows, flows in (('page', 1, 500, 1), ('page', 2, 1500, 1),
                                      ('page', 4, 1500, 3), ('checkpoint', 1, 500, 1),
                                      ('checkpoint', 2, 500, 3), ('checkpoint', 4, 500, 3)):
            for fault in ('before', 'lost'):
                for correction in (0, 17):
                    objects = initial(base, provider, rows=rows, flows=flows)
                    store = FaultStore(base, objects, provider, {(kind, nth): fault})
                    manifest_key = f'data/providers/{provider}/series-manifest.json'
                    failed = invoke(base, base.SOURCE, store, provider)
                    refused(base, failed, store, provider, objects[manifest_key], f'{kind} write failed or uncertain')
                    # Actual object is the last durable checkpoint, never rolled back after uncertainty.
                    durable = store.checkpoints[-1] if store.checkpoints else objects[base.STATE.format(provider)]
                    assert store.objects[base.STATE.format(provider)] == durable
                    saved = json.loads(durable)
                    assert not saved.get('write_errors') and not saved.get('failed_flows')
                    assert len(puts(failed, 'checkpoint')) == (nth if kind == 'checkpoint' else (0 if nth <= 3 else 1))
                    retained = {k: v for k, v in objects.items() if '/series/page-' in k}
                    assert all(store.objects[k] == v for k, v in retained.items())
                    store.faults.clear()
                    store.objects.update(base.warm(provider, rows=rows, flows=flows, correction=correction))
                    retry_objects = copy.deepcopy(store.objects)
                    expected = base.run(BASELINE, retry_objects, {'provider': provider})
                    retried = invoke(base, base.SOURCE, store, provider)
                    assert retried == expected, (provider, kind, nth, fault, correction)
                    if len(saved['flows_done']) < flows:
                        assert puts(retried, 'page'), 'durable unfinished work was skipped'
                        assert puts(retried, 'page')[0]['Key'].endswith(f"page-{saved['n_pages']:04d}.json")
                    else:
                        assert not puts(retried, 'page'), 'committed progress was replayed'
                    # A third scheduled invocation is also byte-identical and does not rewrite pages.
                    expected_repeat = base.run(BASELINE, store.objects, {'provider': provider})
                    repeat = invoke(base, base.SOURCE, store, provider)
                    assert repeat == expected_repeat and not puts(repeat, 'page')
                    count += 1
        # Multiple checkpoints carry incomplete page buffers from earlier completed flows.
        for fault in ('before', 'lost'):
            objects = initial(base, provider, rows=501, flows=3)
            store = FaultStore(base, objects, provider, {('checkpoint', 2): fault})
            failed = invoke(base, base.SOURCE, store, provider)
            refused(base, failed, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'checkpoint write failed or uncertain')
            saved = json.loads(store.objects[base.STATE.format(provider)])
            assert len(saved['buffer']) == (1 if fault == 'before' else 2)
            assert saved['flows_done'] == (['FLOW0'] if fault == 'before' else ['FLOW0', 'FLOW1'])
            store.faults.clear()
            expected = base.run(BASELINE, store.objects, {'provider': provider})
            assert invoke(base, base.SOURCE, store, provider) == expected
            count += 1
        # Restore a carried buffer and partial-flow budget checkpoint after page failure.
        for budget in (3, 5):
            objects = initial(base, provider, rows=1500, flows=3,
                              state=base.checkpoint(buffer=[{'id': 'carried', 'flow': 'OLD'}]))
            store = FaultStore(base, objects, provider, {('page', 1): 'before'})
            failed = invoke(base, base.SOURCE, store, provider, budget=budget)
            refused(base, failed, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'page write failed or uncertain')
            assert store.objects[base.STATE.format(provider)] == objects[base.STATE.format(provider)]
            store.faults.clear()
            for _ in range(8):
                expected = base.run(BASELINE, store.objects, {'provider': provider}, budget=budget)
                retry = invoke(base, base.SOURCE, store, provider, budget=budget)
                assert retry == expected
                if len(json.loads(store.objects[base.STATE.format(provider)])['flows_done']) == 3:
                    break
            assert len(json.loads(store.objects[base.STATE.format(provider)])['flows_done']) == 3
            count += 1
    return count


def workers(base):
    count = 0
    for provider in ('eurostat', 'ecb'):
        for fault in ('before', 'lost'):
            for rows in (1500, 3500):
                DeferredPool.instances.clear()
                objects = initial(base, provider, rows=rows, flows=2)
                store = FaultStore(base, objects, provider, {('page', 1): fault})
                failed = invoke(base, base.SOURCE, store, provider, DeferredPool, writers=1)
                refused(base, failed, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                        'page write failed or uncertain')
                pool = DeferredPool.instances[-1]
                assert pool.shutdown_called and len(pool.entries) == 3
                assert all(f.harvested for f in pool.entries)
                assert len(puts(failed, 'page')) == 3
                assert not puts(failed, 'checkpoint')
                count += 1
        DeferredPool.instances.clear()
        DeferredPool.reject_at = 2
        try:
            objects = initial(base, provider, rows=1500)
            store = FaultStore(base, objects, provider)
            failed = invoke(base, base.SOURCE, store, provider, DeferredPool)
            refused(base, failed, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'page write failed or uncertain')
            assert DeferredPool.instances[-1].shutdown_called
            assert not puts(failed, 'checkpoint')
            count += 1
        finally:
            DeferredPool.reject_at = None
        # A real worker is held until a different worker has failed; cleanup joins both.
        entered, failed_event = threading.Event(), threading.Event()
        class LateStore(FaultStore):
            def put_object(self, **kw):
                if kw['Key'].endswith('page-0003.json'):
                    entered.set()
                    assert failed_event.wait(5), 'late worker did not settle'
                elif kw['Key'].endswith('page-0004.json'):
                    assert entered.wait(5)
                    try:
                        return super().put_object(**kw)
                    finally:
                        failed_event.set()
                return super().put_object(**kw)
        objects = initial(base, provider, rows=1500)
        # second page reaches the fake PUT first; its numbered fault releases the first.
        store = LateStore(base, objects, provider, {('page', 1): 'before'})
        failed = invoke(base, base.SOURCE, store, provider, ThreadPoolExecutor)
        refused(base, failed, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                'page write failed or uncertain')
        assert entered.is_set() and failed_event.is_set() and len(puts(failed, 'page')) == 3
        assert not puts(failed, 'checkpoint')
        count += 1
    return count


def parser_errors(base):
    count = 0
    for provider in ('eurostat', 'ecb'):
        for prior in (0, 2):
            objects = initial(base, provider, rows=500, flows=2,
                              state=base.checkpoint(flow_progress={'FLOW0': {'error_count': prior}}))
            for k in list(objects):
                if k.startswith(f'data/warm/{provider}/data/FLOW0'):
                    objects[k] = b'invented invalid gzip'
            old = base.run(BASELINE, objects, {'provider': provider})
            new = base.run(base.SOURCE, objects, {'provider': provider})
            assert old == new
            count += 1
        # Checkpoint raised from inside generic parser-error handling is fatal too.
        for fault in ('before', 'lost'):
            store = FaultStore(base, objects, provider, {('checkpoint', 1): fault})
            result = invoke(base, base.SOURCE, store, provider)
            refused(base, result, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'checkpoint write failed or uncertain')
            assert len(puts(result, 'checkpoint')) == 1
            count += 1
    return count


def exception_boundaries(base):
    count = 0
    for provider in ('eurostat', 'ecb'):
        # Parser errors after yielding a page retain the normal retry/retirement
        # behavior; a writer failure during that catch must instead escape.
        original = base.load
        def load(path):
            module = original(path)
            def parse(*args, **kwargs):
                for i in range(500):
                    yield {'id': f'parser-row-{i}', 'flow': 'FLOW0'}
                raise ValueError('invented late parser error')
            if provider == 'ecb':
                module.GROUP_EXTRACTORS['ecb'] = parse
            else:
                module.EXTRACTORS['eurostat'] = parse
            return module
        objects = initial(base, provider, state=base.checkpoint(
            flow_progress={'FLOW0': {'error_count': 2}}))
        with patch.object(base, 'load', load):
            old = base.run(BASELINE, objects, {'provider': provider})
            new = base.run(base.SOURCE, objects, {'provider': provider})
            assert old == new
            store = FaultStore(base, objects, provider, {('page', 1): 'lost'})
            result = base.run(base.SOURCE, {}, {'provider': provider}, storage=store)
            refused(base, result, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'page write failed or uncertain')
            assert not puts(result, 'checkpoint')
        count += 2
        # A checkpoint inside the stall path, outside the parser try, also aborts.
        for fault in ('before', 'lost'):
            objects = initial(base, provider, state=base.checkpoint(flow_progress={'FLOW0': {
                'attempts': 4, 'rows_done': 0, 'rows_at_last_check': 0,
                'slice_idx': 0, 'slice_at_last_check': 0}}))
            store = FaultStore(base, objects, provider, {('checkpoint', 1): fault})
            result = invoke(base, base.SOURCE, store, provider)
            refused(base, result, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'checkpoint write failed or uncertain')
            assert len(puts(result, 'checkpoint')) == 1 and not puts(result, 'page')
            count += 1
        # Idle final checkpoint failure must not publish a new manifest.
        for fault in ('before', 'lost'):
            objects = initial(base, provider, state=base.checkpoint(flows_done=['FLOW0']))
            store = FaultStore(base, objects, provider, {('checkpoint', 1): fault})
            result = invoke(base, base.SOURCE, store, provider)
            refused(base, result, store, provider, objects[f'data/providers/{provider}/series-manifest.json'],
                    'checkpoint write failed or uncertain')
            assert len(puts(result, 'checkpoint')) == 1 and not puts(result, 'page')
            count += 1
    return count


def limitations(base):
    count = 0
    for provider in ('eurostat', 'ecb'):
        # Manifest failure has no transaction with the checkpoint; state can be newer.
        for fault in ('before', 'lost'):
            objects = initial(base, provider)
            old_store = FaultStore(base, objects, provider, {('manifest', 1): fault})
            new_store = FaultStore(base, objects, provider, {('manifest', 1): fault})
            old = invoke(base, BASELINE, old_store, provider)
            new = invoke(base, base.SOURCE, new_store, provider)
            assert {k: v for k, v in old.items() if k != 'traceback'} == {k: v for k, v in new.items() if k != 'traceback'}
            assert new['return'] is None
            assert json.loads(new_store.objects[base.STATE.format(provider)])['flows_done'] == ['FLOW0']
            count += 1
        # No changed-input detection for a flow already done in an accepted checkpoint.
        objects = initial(base, provider)
        store = FaultStore(base, objects, provider, {('checkpoint', 1): 'lost'})
        invoke(base, base.SOURCE, store, provider)
        pages = {k: v for k, v in store.objects.items() if '/series/page-' in k}
        store.objects.update(base.warm(provider, rows=500, correction=88))
        store.faults.clear()
        resumed = invoke(base, base.SOURCE, store, provider)
        assert not puts(resumed, 'page') and all(store.objects[k] == v for k, v in pages.items())
        count += 1
        # Missing-checkpoint bootstrap is unchanged, including replacement risk.
        objects = initial(base, provider)
        objects.pop(base.STATE.format(provider))
        assert base.run(BASELINE, objects, {'provider': provider}) == base.run(base.SOURCE, objects, {'provider': provider})
        count += 1
        # Bookkeeping errors share the old collector catch: refuse rather than claim a missing page.
        objects = initial(base, provider, state=base.checkpoint(pages_objects=None))
        result = invoke(base, base.SOURCE, FaultStore(base, objects, provider), provider)
        assert result['error'] == ('StorageWriteError', 'page write failed or uncertain')
        count += 1
    return count


def run_tests(base):
    with patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')), \
         patch.object(socket, 'create_connection', side_effect=AssertionError('network forbidden')):
        counts = {'baseline false-success reproductions': baseline_reproductions(base),
                  'healthy full-handler comparisons': healthy(base),
                  'fault/recovery scenarios': failure_recovery(base),
                  'worker drain/submission scenarios': workers(base),
                  'parser-error scenarios': parser_errors(base),
                  'exception-boundary scenarios': exception_boundaries(base),
                  'remaining risk scenarios': limitations(base)}
    print('Series storage PASS: ' + '; '.join(f'{n} {name}' for name, n in counts.items()))
    return counts
