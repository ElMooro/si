"""Ordered private risk publication; S3 and the mirror are separate commits.

Pure protocol/transport adapter. No provider requests, messages or trade authority.
All prior raw risk bytes must be retained before replacing the S3 current object.
"""
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request

from portfolio_risk_model import canonical, snapshot_value_identity

PROTOCOL = 'portfolio-risk-publication.v1'
MAX_BYTES = 20_000_000
MAX_REVISION = 2**53 - 1
ORIGIN = 'https://justhodl-data-proxy.raafouis.workers.dev'
TOKEN = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\Z')


class PublicationUnavailable(RuntimeError):
    """Fixed public diagnostics never include private response text."""


class SupersededMirror(PublicationUnavailable):
    pass


def error_code(error):
    return str(getattr(error, 'response', {}).get('Error', {}).get('Code', ''))


def revision(value, allow_zero=False):
    return type(value) is int and (0 if allow_zero else 1) <= value <= MAX_REVISION


def opaque_etag(value):
    if type(value) is not str or not re.fullmatch(r'"[^"\r\n]{1,200}"', value):
        raise PublicationUnavailable('Quoted source representation identity required')
    return value


def utc_stamp(value):
    if type(value) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)', value):
        raise PublicationUnavailable('UTC acquisition timestamp required')
    try:
        datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise PublicationUnavailable('Valid acquisition timestamp required') from None
    return value


def document(raw):
    if type(raw) is not bytes or len(raw) > MAX_BYTES:
        raise PublicationUnavailable('Complete publication body exceeds bound')
    def pairs(rows):
        out = {}
        for key, value in rows:
            if key in out:
                raise PublicationUnavailable('Duplicate publication JSON field')
            out[key] = value
        return out
    def constant(_):
        raise PublicationUnavailable('Nonfinite publication JSON value')
    try:
        value = json.loads(raw.decode('utf-8', errors='strict'), object_pairs_hook=pairs, parse_constant=constant)
        if type(value) is not dict:
            raise ValueError('object required')
        snapshot_value_identity(value)  # complete depth/Unicode/finite typed values
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        raise PublicationUnavailable('Invalid complete publication JSON') from None
    return value


def read_bytes(body, limit, deadline, declared=None, read_method='read'):
    """Bounded complete EOF read; deadline is acceptance, not SDK cancellation."""
    parts, size = [], 0
    try:
        if declared is not None and (type(declared) is not int or declared < 0 or declared > limit):
            raise PublicationUnavailable('Invalid publication body length')
        while True:
            if time.monotonic() >= deadline:
                raise PublicationUnavailable('Publication body deadline exceeded')
            part = getattr(body, read_method)(min(65536, limit + 1 - size))
            if time.monotonic() >= deadline:
                raise PublicationUnavailable('Publication body deadline exceeded')
            if type(part) is not bytes:
                raise PublicationUnavailable('Publication body must contain bytes')
            if not part:
                break
            size += len(part)
            if size > limit:
                raise PublicationUnavailable('Complete publication body exceeds bound')
            parts.append(part)
        if declared is not None and size != declared:
            raise PublicationUnavailable('Incomplete publication body')
        return b''.join(parts)
    finally:
        body.close()


def read_current(s3, bucket, key):
    deadline = time.monotonic() + 20
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except Exception as error:
        if error_code(error) in ('404', 'NoSuchKey', 'NotFound'):
            return None
        raise
    try:
        etag = opaque_etag(response.get('ETag'))
        if response.get('ContentEncoding', 'identity') not in ('', 'identity'):
            raise PublicationUnavailable('Encoded current publication is unsupported')
    except Exception:
        response['Body'].close()
        raise
    raw = read_bytes(response['Body'], MAX_BYTES, deadline, response.get('ContentLength'))
    value = document(raw)
    rank = 0
    if 'publication' in value:
        pub = value['publication']
        if type(pub) is not dict or pub.get('schema_version') != PROTOCOL or not revision(pub.get('revision')):
            raise PublicationUnavailable('Current publication revision is invalid')
        rank = pub['revision']
    return {'raw': raw, 'document': value, 'etag': etag, 'revision': rank}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


def publication_request(method, raw, token=None, sha256=None):
    """Fixed HTTPS origin, existing service identity, exact bytes, strict ack."""
    from private_artifact import service_headers
    base = os.environ.get('PRIVATE_ARTIFACT_PROXY', ORIGIN).rstrip('/')
    if base != ORIGIN or method not in ('POST', 'PUT'):
        raise PublicationUnavailable('Reviewed private publication origin required')
    url = base + '/private-artifact?kind=portfolio-risk' + ('&action=reserve' if method == 'POST' else '')
    headers = {**service_headers(), 'Content-Type': 'application/json', 'Accept-Encoding': 'identity'}
    if token is not None:
        headers['X-JH-Publication-Token'] = token
    if sha256 is not None:
        headers['X-JH-Body-SHA256'] = sha256
    request = urllib.request.Request(url, data=raw, method=method, headers=headers)
    response = None
    try:
        deadline = time.monotonic() + 20
        try:
            response = urllib.request.build_opener(NoRedirect()).open(request, timeout=20)
        except urllib.error.HTTPError as error:
            response = error
        if response.geturl() != url or response.status not in (200, 409):
            raise PublicationUnavailable('Private publication HTTP response unavailable')
        encoding = response.headers.get('Content-Encoding', 'identity')
        lengths = response.headers.get_all('Content-Length') or []
        if encoding not in ('', 'identity') or len(lengths) > 1:
            raise PublicationUnavailable('Invalid private acknowledgement framing')
        declared = None
        if lengths:
            text = lengths[0].strip(' \t')
            if not re.fullmatch(r'[0-9]+', text):
                raise PublicationUnavailable('Invalid private acknowledgement length')
            declared = int(text)
        ack = document(read_bytes(response, 65536, deadline, declared, 'read1'))
        if response.status == 409:
            if method == 'PUT' and ack.get('ok') is False and ack.get('error') == 'superseded_publication':
                raise SupersededMirror('Private mirror already has a newer publication')
            raise PublicationUnavailable('Private publication revision conflict')
        return ack
    except PublicationUnavailable:
        raise
    except Exception:
        raise PublicationUnavailable('Private publication transport unavailable') from None
    finally:
        if response is not None:
            response.close()


def reserve_publication(s3, bucket, key, request):
    current = read_current(s3, bucket, key)
    minimum = current['revision'] if current else 0
    ack = request('POST', canonical({'minimum_revision': minimum}))
    if (type(ack) is not dict or ack.get('ok') is not True or ack.get('protocol') != PROTOCOL or
            not revision(ack.get('revision')) or ack['revision'] <= minimum or
            type(ack.get('token')) is not str or not TOKEN.fullmatch(ack['token'])):
        raise PublicationUnavailable('Private publication reservation not acknowledged')
    return {'revision': ack['revision'], 'token': ack['token'], 'started_at': datetime.now(timezone.utc).isoformat()}


def source_matches(s3, bucket, key, expected):
    try:
        actual = s3.head_object(Bucket=bucket, Key=key)
    except Exception as error:
        if error_code(error) in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise
    return opaque_etag(actual.get('ETag')) == opaque_etag(expected)


def publish_ordered(payload, attempt, s3, bucket, key, snapshot_key, request, retain_previous):
    """CAS the complete S3 object; acknowledge the exact same mirror bytes.

    Source HEAD checks fence known changes, but do not create a cross-key S3
    transaction. Consumers must still verify the complete snapshot binding.
    """
    rank = attempt.get('revision')
    if not revision(rank) or type(attempt.get('token')) is not str or not TOKEN.fullmatch(attempt['token']):
        raise PublicationUnavailable('Issued private publication identity required')
    source_etag = opaque_etag(attempt.get('source_etag'))
    source_sha = attempt.get('source_value_sha256')
    binding = payload.get('snapshot_binding')
    if (type(source_sha) is not str or not re.fullmatch(r'[a-f0-9]{64}', source_sha) or type(binding) is not dict or
            binding.get('encoding') != 'typed-json-binary64.v1' or binding.get('value_sha256') != source_sha or 'publication' in payload):
        raise PublicationUnavailable('Complete snapshot value binding required')
    candidate = {**payload, 'publication': {'schema_version': PROTOCOL, 'revision': rank,
        'source_etag': source_etag, 'source_value_sha256': source_sha, 'started_at': utc_stamp(attempt.get('started_at'))}}
    raw = canonical(candidate)
    document(raw)
    sha256 = hashlib.sha256(raw).hexdigest()
    def result(status):
        return {'published': status == 'published', 'status': status, 'revision': rank}
    for _ in range(4):
        current = read_current(s3, bucket, key)
        if current and current['revision'] > rank:
            return result('superseded')
        if current and current['revision'] == rank and current['raw'] != raw:
            raise PublicationUnavailable('S3 publication revision body conflict')
        if not source_matches(s3, bucket, snapshot_key, source_etag):
            return result('source_changed')
        if current and current['raw'] == raw:
            break
        if current:
            retain_previous(current['raw'])
        condition = {'IfMatch': current['etag']} if current else {'IfNoneMatch': '*'}
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json',
                          CacheControl='private, no-store', **condition)
            break
        except Exception as error:
            if error_code(error) not in ('412', 'PreconditionFailed', '409', 'ConditionalRequestConflict'):
                raise
    else:
        raise PublicationUnavailable('Private publication contention exceeded retry bound')
    # The source or another risk writer may have advanced after our conditional
    # write. Never advertise our packet as current in that case.
    if not source_matches(s3, bucket, snapshot_key, source_etag):
        return result('source_changed_after_s3')
    current = read_current(s3, bucket, key)
    if current is None or current['raw'] != raw:
        return result('superseded_after_s3')
    try:
        ack = request('PUT', raw, token=attempt['token'], sha256=sha256)
    except SupersededMirror:
        return result('superseded_at_mirror')
    if (type(ack) is not dict or ack.get('ok') is not True or ack.get('protocol') != PROTOCOL or
            type(ack.get('revision')) is not int or ack['revision'] != rank or
            ack.get('body_sha256') != sha256 or ack.get('status') not in ('published', 'unchanged')):
        raise PublicationUnavailable('Exact private publication not acknowledged')
    current = read_current(s3, bucket, key)
    if current is None or current['raw'] != raw:
        return result('superseded_after_mirror')
    return result('published')
