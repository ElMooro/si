"""Auction-local FRED transport: bounded bodies and acquisition-aware warm cache.

The original four-attempt transient retry policy and 30-minute cache remain.
Expired cache cannot replace a failed refresh. This records transport metadata;
it does not retain immutable originals or verify provider publication vintages.
Other Lambdas and the fleet-wide shared shim are unchanged.
"""
from datetime import datetime, timezone
import hashlib
import io
import json
import math
import time as _time
import urllib.parse as _parse
import urllib.request as _ur

_FRED_HOST = 'api.stlouisfed.org'
_CACHE = {}
_CACHE_TTL = 1800
_MAX_BODY = 2 * 1024 * 1024
_PUBLIC_PARAMETERS = frozenset(('series_id','file_type','limit','sort_order','observation_start','observation_end',
                               'realtime_start','realtime_end','units','frequency','aggregation_method','output_type','offset'))


class FredResponseUnavailable(ValueError):
    """Fixed diagnostic only; never propagate credential-bearing request text."""


def _utc(stamp):
    return datetime.fromtimestamp(stamp, timezone.utc).isoformat()


def _public_url(target):
    parsed = _parse.urlsplit(target)
    public = [(key,value) for key,value in _parse.parse_qsl(parsed.query) if key in _PUBLIC_PARAMETERS]
    return _parse.urlunsplit(('https', _FRED_HOST, parsed.path, _parse.urlencode(public), ''))


def _json_pairs(pairs):
    result = {}
    for key,value in pairs:
        if key in result:raise FredResponseUnavailable('fred_duplicate_json_key')
        result[key] = value
    return result


def _json_number(text):
    value = float(text)
    if not math.isfinite(value):raise FredResponseUnavailable('fred_nonfinite_json_number')
    return value


def _json_constant(_):
    raise FredResponseUnavailable('fred_nonfinite_json_constant')


def _complete_response(response, target, started):
    try:
        if getattr(response, 'status', None) != 200:
            raise FredResponseUnavailable('fred_non_success_or_partial_response')
        if response.geturl() != target:
            raise FredResponseUnavailable('fred_response_identity_changed')
        headers = response.headers
        encoding = headers.get('Content-Encoding')
        if encoding not in (None, '', 'identity'):
            raise FredResponseUnavailable('fred_unsupported_content_encoding')
        declared = headers.get('Content-Length')
        if declared is not None:
            if not isinstance(declared, str) or not declared.isascii() or not declared.isdigit() or len(declared)>10:
                raise FredResponseUnavailable('fred_invalid_content_length')
            declared = int(declared)
            if not 0 < declared <= _MAX_BODY:raise FredResponseUnavailable('fred_body_size_invalid')
        raw = response.read(_MAX_BODY+1)
        if not isinstance(raw, bytes) or not 0 < len(raw) <= _MAX_BODY:
            raise FredResponseUnavailable('fred_body_size_invalid')
        if declared is not None and len(raw) != declared:
            raise FredResponseUnavailable('fred_incomplete_declared_body')
        try:
            parsed = json.loads(raw.decode('utf-8'), object_pairs_hook=_json_pairs,
                                parse_float=_json_number, parse_constant=_json_constant)
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise FredResponseUnavailable('fred_invalid_json_body') from None
        if not isinstance(parsed, dict) or not isinstance(parsed.get('observations'), list):
            raise FredResponseUnavailable('fred_observation_array_missing')
        received = _time.time()
        if received < started:raise FredResponseUnavailable('fred_acquisition_clock_reversed')
        metadata = {'contract':'auction-fred-transport.v1','source_url':_public_url(target),
                    'request_started_at':_utc(started),'response_received_at':_utc(received),
                    'response_status':200,'declared_content_length':declared,
                    'body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest()}
        return received, raw, metadata
    finally:
        try:response.close()
        except Exception:pass


class _CachedResponse(io.BytesIO):
    """Independent normal file cursor; original acquisition time survives reuse."""
    def __init__(self, body, metadata, target, cache_status, cache_age):
        super().__init__(body)
        self.status = self.code = 200
        self._url = target
        self.headers = {'Content-Length':str(len(body)), 'Content-Type':'application/json'}
        self.jh_fred_transport = dict(metadata, cache_status=cache_status,
                                      cache_age_seconds=round(cache_age,6), cache_ttl_seconds=_CACHE_TTL)

    def getcode(self):return self.status
    def geturl(self):return self._url


def _install():
    if getattr(_ur, '_jh_fred_shim_installed', False):return
    original = _ur.urlopen

    def patched(url, *args, **kwargs):
        target = url.full_url if hasattr(url,'full_url') else url if isinstance(url,str) else None
        try:parsed = _parse.urlsplit(target) if target else None
        except ValueError:parsed = None
        if not parsed or parsed.scheme!='https' or parsed.hostname!=_FRED_HOST or parsed.path!='/fred/series/observations':
            return original(url, *args, **kwargs)
        now = _time.time();hit = _CACHE.get(target)
        if hit and 0 <= now-hit[0] < _CACHE_TTL:
            return _CachedResponse(hit[1],hit[2],target,'fresh_cache',now-hit[0])
        for attempt in range(4):
            try:
                started = _time.time()
                response = original(url,*args,**kwargs)
                received,raw,metadata = _complete_response(response,target,started)
                _CACHE[target] = (received,raw,metadata)
                return _CachedResponse(raw,metadata,target,'network',0)
            except FredResponseUnavailable:
                raise
            except Exception as exc:
                code = getattr(exc,'code',None)
                if code==429 or code in (500,502,503,504) or code is None:
                    _time.sleep(min(8,0.6*(2**attempt)))
                    continue
                raise FredResponseUnavailable('fred_nonretryable_request_failure') from None
        raise FredResponseUnavailable('fred_refresh_failed_expired_cache_withheld' if hit else 'fred_request_attempts_exhausted') from None

    _ur.urlopen = patched
    _ur._jh_fred_shim_installed = True


try:
    _install()
except Exception:
    # Import failures still leave the ordinary urllib transport available.
    pass
