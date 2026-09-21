"""Bounded descriptive access; legacy Massive ranks cannot regain authority."""
from copy import deepcopy
from threading import Lock
import json, time
import boto3
from massive_research_context import CURRENT, context

_BUCKET = 'justhodl-dashboard-live'
_KEY = CURRENT
_CACHE = {}
_LOCK = Lock()
_SUCCESS_TTL = 30.0
_FAILURE_TTL = 5.0
_MAX_BYTES = 16 * 1024 * 1024


def _load():
    with _LOCK:
        tick = time.monotonic()
        if _CACHE and 0 <= tick - _CACHE['read_at'] < _CACHE['ttl']:
            return deepcopy(_CACHE['packet'])
        try:
            obj = boto3.client('s3', region_name='us-east-1').get_object(Bucket=_BUCKET, Key=_KEY)
            stream = obj['Body']
            try: raw = stream.read(_MAX_BYTES + 1)
            finally: stream.close()
            if not 0 < len(raw) <= _MAX_BYTES: raise ValueError('Bounded composite publication required')
            packet = context(json.loads(raw))
            ttl = _SUCCESS_TTL if packet['native_reference_available'] else _FAILURE_TTL
        except Exception:
            packet = context(None); ttl = _FAILURE_TTL
        _CACHE.clear(); _CACHE.update(read_at=time.monotonic(), ttl=ttl, packet=packet)
        return deepcopy(packet)


def massive_research(): return _load()
def massive_market(): return _load()['market']
def massive_ticker(sym): return _load()['tickers'].get(sym, {})
def massive_prepump(): return _load()['top_prepump']
def sector_flow_z(sector_etf): return None
