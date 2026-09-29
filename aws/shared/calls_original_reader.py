"""Bounded, invocation-local reader for reviewed public original archives."""
from concurrent.futures import Future
import gzip
import hashlib
import io
import re
from threading import Lock
import calls_liquidity_originals as liquidity
import calls_fails_originals as fails
import calls_ciss_originals as ciss


def allowed(key):
    return isinstance(key, str) and (
        liquidity.store.allowed(key) and key not in (liquidity.store.SOURCE, liquidity.store.SETTLEMENT, liquidity.store.model.CURRENT)
        or any(re.fullmatch(pattern, key) for pattern in fails.IMMUTABLE+ciss.IMMUTABLE))


class ImmutableReader:
    def __init__(self, read):
        self.read = read; self.cache = {}; self.bytes = 0; self.lock = Lock(); self.pending = {}
        self._verified = {}; self._verified_bytes = 0

    def __call__(self, key):
        if not allowed(key): raise ValueError('Reviewed immutable original path required before transport')
        with self.lock:
            if key in self.cache: return self.cache[key]
            owner = key not in self.pending
            if owner:
                if len(self.cache)+len(self.pending) >= 256: raise ValueError('Complete original archive count exceeds bound')
                self.pending[key] = Future()
            future = self.pending[key]
        if not owner: return future.result()
        try:
            raw = self.read(key)
            if not isinstance(raw, bytes) or not 0 < len(raw) <= liquidity.store.MAX:
                raise ValueError('Complete bounded original artifact required')
            match = re.search(r'/([a-f0-9]{64})\.(?:json|py|bin\.gz)$', key)
            if not match or hashlib.sha256(raw).hexdigest() != match[1]: raise ValueError('Original artifact hash differs')
            with self.lock:
                if self.bytes+len(raw) > 256*1024*1024: raise ValueError('Complete original archive bytes exceed bound')
                self.cache[key] = raw; self.bytes += len(raw)
            future.set_result(raw); return raw
        except BaseException as exc:
            future.set_exception(exc); raise
        finally:
            with self.lock: del self.pending[key]

    def ciss_snapshot(self, identity, build):
        """Reuse immutable verified bytes only inside this reader's invocation.

        Freshness is deliberately absent from this cache. The binding reevaluates
        original clocks for every requested decision time. No cache is persisted.
        """
        if not isinstance(identity, str) or not re.fullmatch('[a-f0-9]{64}', identity):
            raise ValueError('Exact CISS snapshot identity required')
        saved = self._verified.get(identity)
        if saved is None:
            if len(self._verified) >= 8: raise ValueError('Verified snapshot count exceeds bound')
            raw = build()
            if not isinstance(raw, bytes) or not 0 < len(raw) <= 64*1024*1024 or self._verified_bytes+len(raw) > 64*1024*1024:
                raise ValueError('Complete verified snapshot exceeds bound')
            saved = (hashlib.sha256(raw).hexdigest(), raw)
            self._verified[identity] = saved; self._verified_bytes += len(raw)
        if not isinstance(saved, tuple) or len(saved) != 2 or not isinstance(saved[1], bytes) or hashlib.sha256(saved[1]).hexdigest() != saved[0]:
            raise ValueError('Invocation-local verified snapshot differs')
        return saved[1]


def reader(client, bucket):
    def read(key):
        raw = liquidity.store.bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        return liquidity.store.bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return ImmutableReader(read)
