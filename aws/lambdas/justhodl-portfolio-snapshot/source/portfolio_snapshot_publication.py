"""Complete snapshot byte serialization and acknowledged private publication.

Only the snapshot producer imports this adapter. The current value compatibility
guard runs before encoding. Both sinks reuse the resulting immutable body.
These remain separate writes; no atomicity or replica consistency is implied.
"""
import hashlib
import io
import json
import math
import os
import time
import urllib.error
import urllib.request

from private_artifact import service_headers

MAX_BYTES = 20_000_000
MAX_IDENTITY_BYTES = 32 * 1024 * 1024
MAX_DEPTH = 128
STRING_CHUNK = 4096
ACK_MAX_BYTES = 4096
ORIGIN = 'https://justhodl-data-proxy.raafouis.workers.dev'
URL = ORIGIN + '/private-artifact?kind=portfolio-snapshot'
PROTOCOL = 'portfolio-snapshot-bytes.v1'


class SnapshotPublicationUnavailable(RuntimeError):
    """Fixed diagnostics never quote account data, tokens or response text."""


def encode_snapshot(value, max_bytes=None):
    """Match the existing compact ASCII JSON exactly, in bounded chunks.

    BytesIO avoids a second whole text representation. An individual string is
    escaped in chunks instead of allocating its entire expanded JSON form.
    The bound applies before every buffer write. No field is dropped or rounded.
    """
    if max_bytes is None: max_bytes = MAX_BYTES
    if type(max_bytes) is not int or not 0 < max_bytes <= MAX_BYTES:
        raise ValueError('Reviewed snapshot byte bound required')
    output = io.BytesIO()
    def write(raw):
        if output.tell() + len(raw) > max_bytes:
            raise ValueError('Complete snapshot exceeds private mirror byte bound')
        output.write(raw)
    def text(item):
        write(b'"')
        for start in range(0, len(item), STRING_CHUNK):
            part = item[start:start + STRING_CHUNK]
            try: part.encode('utf-8', 'strict')
            except UnicodeError: raise ValueError('Complete snapshot contains unsupported Unicode') from None
            write(json.encoder.encode_basestring_ascii(part)[1:-1].encode('ascii'))
        write(b'"')
    def visit(item, depth=0):
        if depth > MAX_DEPTH:
            raise ValueError('Complete snapshot exceeds consumer nesting bound')
        if item is None: write(b'null')
        elif type(item) is bool: write(b'true' if item else b'false')
        elif type(item) in (int, float):
            if type(item) is int and abs(item) > 2**53-1:
                raise ValueError('Complete snapshot contains a consumer-unsafe number')
            number = float(item)
            if not math.isfinite(number) or (number.is_integer() and abs(number) > 2**53-1):
                raise ValueError('Complete snapshot contains a consumer-unsafe number')
            # Python JSON's finite float formatting is repr; integers use str.
            write(repr(item).encode('ascii'))
        elif type(item) is str: text(item)
        elif type(item) is list:
            write(b'[')
            for index, child in enumerate(item):
                if index: write(b',')
                visit(child, depth + 1)
            write(b']')
        elif type(item) is dict:
            write(b'{')
            for index, (key, child) in enumerate(item.items()):
                if type(key) is not str: raise ValueError('Complete snapshot object key is unsupported')
                if index: write(b',')
                text(key); write(b':'); visit(child, depth + 1)
            write(b'}')
        else: raise ValueError('Complete snapshot contains an unsupported value type')
    try:
        visit(value)
        return output.getvalue()
    finally:
        output.close()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        try: fp.close()
        finally: raise SnapshotPublicationUnavailable('Snapshot publication redirect refused')


def _within(deadline):
    if time.monotonic() >= deadline:
        raise SnapshotPublicationUnavailable('Snapshot publication acknowledgement deadline')


def _deadline(context):
    available = 25.0
    if context is not None:
        try: remaining = context.get_remaining_time_in_millis()
        except Exception: raise SnapshotPublicationUnavailable('Snapshot publication runtime budget unavailable') from None
        if (type(remaining) not in (int, float) or
                (type(remaining) is int and abs(remaining) > 2**53-1) or not math.isfinite(remaining)):
            raise SnapshotPublicationUnavailable('Snapshot publication runtime budget unavailable')
        available = min(available, remaining / 1000 - 5)
        if available < 1:
            raise SnapshotPublicationUnavailable('Snapshot publication runtime reserve unavailable')
    return time.monotonic() + available


def _header(headers, name, default=None):
    values = headers.get_all(name, [])
    if not values: return default
    if len(values) != 1 or type(values[0]) is not str:
        raise SnapshotPublicationUnavailable('Snapshot acknowledgement header is ambiguous')
    return values[0]


def _acknowledgement(response, deadline):
    if response.status != 200 or response.geturl() != URL:
        raise SnapshotPublicationUnavailable('Snapshot publication response not accepted')
    headers = response.headers
    if _header(headers, 'Content-Encoding', 'identity') not in ('', 'identity'):
        raise SnapshotPublicationUnavailable('Snapshot acknowledgement encoding refused')
    if _header(headers, 'Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
        raise SnapshotPublicationUnavailable('Snapshot acknowledgement type refused')
    declared = _header(headers, 'Content-Length')
    if declared is not None:
        if not declared.isascii() or not declared.isdecimal() or len(declared) > 8 or int(declared) > ACK_MAX_BYTES:
            raise SnapshotPublicationUnavailable('Snapshot acknowledgement length refused')
        declared = int(declared)
    parts, size = [], 0
    while True:
        _within(deadline)
        chunk = response.read(min(1024, ACK_MAX_BYTES + 1 - size))
        _within(deadline)
        if type(chunk) is not bytes:
            raise SnapshotPublicationUnavailable('Snapshot acknowledgement bytes unavailable')
        if not chunk: break
        size += len(chunk)
        if size > ACK_MAX_BYTES:
            raise SnapshotPublicationUnavailable('Snapshot acknowledgement byte bound exceeded')
        parts.append(chunk)
    if declared is not None and size != declared:
        raise SnapshotPublicationUnavailable('Snapshot acknowledgement is incomplete')
    def pairs(rows):
        out = {}
        for key, item in rows:
            if key in out: raise ValueError('duplicate field')
            out[key] = item
        return out
    def constant(_): raise ValueError('nonfinite value')
    try:
        value = json.loads(b''.join(parts).decode('utf-8', 'strict'), object_pairs_hook=pairs, parse_constant=constant)
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8', 'strict')
    except (ValueError, UnicodeError, TypeError, RecursionError):
        raise SnapshotPublicationUnavailable('Complete snapshot acknowledgement is invalid') from None
    return value


def publish_snapshot(body, identity_bytes, context=None):
    if type(body) is not bytes or not 0 < len(body) <= MAX_BYTES:
        raise SnapshotPublicationUnavailable('Complete encoded snapshot required')
    if type(identity_bytes) is not int or not 0 < identity_bytes <= MAX_IDENTITY_BYTES:
        raise SnapshotPublicationUnavailable('Complete snapshot identity size required')
    base = os.environ.get('PRIVATE_ARTIFACT_PROXY', ORIGIN).rstrip('/')
    if base != ORIGIN:
        raise SnapshotPublicationUnavailable('Reviewed snapshot publication origin required')
    deadline = _deadline(context)
    digest = hashlib.sha256(body).hexdigest()
    try:
        headers = {**service_headers(), 'Content-Type': 'application/json', 'X-JH-Body-SHA256': digest}
        _within(deadline)
        request = urllib.request.Request(URL, data=body, method='PUT', headers=headers)
        opener = urllib.request.build_opener(NoRedirect())
        timeout = min(8, deadline-time.monotonic())
        if timeout <= 0: raise SnapshotPublicationUnavailable('Snapshot publication acknowledgement deadline')
        with opener.open(request, timeout=timeout) as response:
            value = _acknowledgement(response, deadline)
    except SnapshotPublicationUnavailable:
        raise
    except urllib.error.HTTPError as error:
        try: error.close()
        except Exception: pass
        raise SnapshotPublicationUnavailable('Snapshot publication was not acknowledged') from None
    except Exception:
        raise SnapshotPublicationUnavailable('Snapshot publication acknowledgement unavailable') from None
    _within(deadline)
    if (type(value) is not dict or value.get('ok') is not True or value.get('protocol') != PROTOCOL or
            type(value.get('body_bytes')) is not int or value['body_bytes'] != len(body) or
            value.get('body_sha256') != digest or type(value.get('identity_bytes')) is not int or value['identity_bytes'] != identity_bytes):
        raise SnapshotPublicationUnavailable('Snapshot publication acknowledgement does not match the complete body')
    return value
