"""Read only one Worker's control-plane code/settings; never call its routes.

This captures a predecessor. A matching source build and intended commit are
separate requirements before a future deployment can receive a release receipt.
"""
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
import base64
import hashlib
import json
import math
import re
import time
import urllib.error
import urllib.request

WORKER = 'justhodl-data-proxy'
LIMIT = 64 * 1024 * 1024
UUID = re.compile(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}')


class EvidenceError(ValueError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise EvidenceError('Control-plane redirects refused')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def document(raw):
    if type(raw) is not bytes or len(raw) > LIMIT: raise EvidenceError('Complete control-plane JSON bytes required')
    def pairs(rows):
        out = {}
        for key, value in rows:
            if key in out: raise EvidenceError('Duplicate control-plane JSON field')
            out[key] = value
        return out
    def constant(_): raise EvidenceError('Nonfinite control-plane JSON value')
    try:
        value = json.loads(raw.decode('utf-8', errors='strict'), object_pairs_hook=pairs, parse_constant=constant)
        pending = [(value, 0)]
        while pending:
            item, depth = pending.pop()
            if depth > 128: raise EvidenceError('Control-plane JSON nesting exceeds bound')
            if type(item) is float and not math.isfinite(item): raise EvidenceError('Nonfinite control-plane JSON value')
            if isinstance(item, str): item.encode('utf-8', errors='strict')
            if isinstance(item, dict):
                for k, v in item.items():
                    k.encode('utf-8', errors='strict'); pending.append((v, depth + 1))
            elif isinstance(item, list): pending.extend((v, depth + 1) for v in item)
        return value
    except (RecursionError, UnicodeError, json.JSONDecodeError) as exc:
        raise EvidenceError('Invalid complete control-plane JSON') from None


def complete(response, deadline, monotonic=time.monotonic):
    if response.status != 200: raise EvidenceError('Control-plane response must be HTTP 200')
    if response.headers.get('Content-Encoding', 'identity').lower() not in ('identity', ''):
        raise EvidenceError('Unexpected control-plane content encoding')
    lengths = response.headers.get_all('Content-Length') or []
    if len(lengths) > 1: raise EvidenceError('Ambiguous control-plane body length')
    size, declared, parts = 0, None, []
    if lengths:
        if type(lengths[0]) is not str: raise EvidenceError('Invalid control-plane body length')
        value = lengths[0].strip(' \t')
        if not value or any(c not in '0123456789' for c in value): raise EvidenceError('Invalid control-plane body length')
        declared = int(value)
        if declared > LIMIT: raise EvidenceError('Complete control-plane body exceeds bound')
    while True:
        if monotonic() >= deadline: raise EvidenceError('Control-plane acquisition deadline exceeded')
        part = response.read1(min(65536, LIMIT + 1 - size))
        if monotonic() >= deadline: raise EvidenceError('Control-plane acquisition deadline exceeded')
        if type(part) is not bytes: raise EvidenceError('Control-plane body must be bytes')
        if not part: break
        size += len(part)
        if size > LIMIT: raise EvidenceError('Complete control-plane body exceeds bound')
        parts.append(part)
    if declared is not None and size != declared: raise EvidenceError('Incomplete control-plane body')
    return b''.join(parts)


class ControlPlane:
    """The exact allowlist excludes KV, DO storage, logs, routes and mutations."""
    def __init__(self, account, token, opener=None, monotonic=time.monotonic):
        if type(account) is not str or not re.fullmatch(r'[a-f0-9]{32}', account) or type(token) is not str or not token:
            raise EvidenceError('Existing Cloudflare control-plane identity unavailable')
        self.base = 'https://api.cloudflare.com/client/v4/accounts/'+account+'/workers/scripts/'+WORKER
        self.token, self.opener, self.monotonic = token, opener or urllib.request.build_opener(NoRedirect()).open, monotonic
        self.read_count = 0

    def get(self, suffix):
        if suffix not in ('/deployments', '/settings', '/schedules', '/content/v2'):
            if not suffix.startswith('/versions/') or not UUID.fullmatch(suffix.removeprefix('/versions/')):
                raise EvidenceError('Unreviewed control-plane path refused')
        request = urllib.request.Request(self.base+suffix, method='GET', headers={
            'Authorization': 'Bearer '+self.token, 'Accept-Encoding': 'identity',
            'User-Agent': 'JustHodl-WorkerSourceEvidence/1.0', 'Cache-Control': 'no-cache'})
        deadline = self.monotonic()+60
        try:
            self.read_count += 1
            with self.opener(request, timeout=30) as response:
                raw = complete(response, deadline, self.monotonic)
                headers = {key: response.headers.get(key, '') for key in ('Content-Type', 'ETag', 'cf-entrypoint')}
        except Exception as exc:
            status = '_HTTP_'+str(exc.code) if isinstance(exc, urllib.error.HTTPError) else ''
            if isinstance(exc, urllib.error.HTTPError): exc.close()
            raise EvidenceError('Control-plane GET failed: '+type(exc).__name__+status) from None
        if self.token.encode() in raw or self.token.encode() in encoded(headers):
            raise EvidenceError('Control-plane response echoed credential')
        if suffix == '/content/v2': return raw, headers
        value = document(raw)
        if not isinstance(value, dict) or value.get('success') is not True or value.get('errors'):
            raise EvidenceError('Control-plane JSON request was not acknowledged')
        return value.get('result')


def active_deployment(value):
    rows = value.get('deployments') if isinstance(value, dict) else None
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise EvidenceError('Current Worker deployment is unavailable')
    row = rows[0]; versions = row.get('versions')
    if not isinstance(versions, list) or len(versions) != 1 or not isinstance(versions[0], dict):
        raise EvidenceError('One fully deployed Worker version required')
    version = versions[0]
    if type(version.get('percentage')) not in (int, float) or version['percentage'] != 100:
        raise EvidenceError('One fully deployed Worker version required')
    if not UUID.fullmatch(str(row.get('id', ''))) or not UUID.fullmatch(str(version.get('version_id', ''))):
        raise EvidenceError('Exact Worker deployment/version identity required')
    return {'deployment_id': row['id'], 'version_id': version['version_id'], 'percentage': 100}


def source_bodies(raw, headers):
    """Split complete inert source without executing or saving extracted paths."""
    if type(raw) is not bytes or len(raw) > LIMIT: raise EvidenceError('Complete Worker source bytes required')
    content_type = headers.get('Content-Type', '')
    if type(content_type) is not str or not content_type.isascii() or '\r' in content_type or '\n' in content_type:
        raise EvidenceError('Invalid source content type')
    try:
        message = BytesParser(policy=policy.default).parsebytes(b'Content-Type: '+content_type.encode('ascii')+b'\r\nMIME-Version: 1.0\r\n\r\n'+raw)
    except Exception:
        raise EvidenceError('Malformed Worker source package') from None
    if message.is_multipart():
        boundary = message.get_boundary()
        if not boundary or not boundary.isascii() or not raw.rstrip(b'\r\n').endswith(('--'+boundary+'--').encode('ascii')):
            raise EvidenceError('Complete Worker multipart closing boundary required')
        parts = list(message.iter_parts())
    elif message.get_content_type() in ('application/javascript', 'text/javascript', 'application/javascript+module'):
        parts = [message]
    else:
        raise EvidenceError('Unrecognized Worker source content type')
    members = {}
    for part in parts:
        if part.is_multipart() or part.defects: raise EvidenceError('Malformed Worker source member')
        name = part.get_filename() or part.get_param('name', header='Content-Disposition')
        if not message.is_multipart(): name = headers.get('cf-entrypoint') or '$entrypoint'
        if not isinstance(name, str) or not name or name in members: raise EvidenceError('Unique Worker source member identity required')
        body = part.get_payload(decode=True)
        if part.defects or type(body) is not bytes: raise EvidenceError('Complete Worker source member required')
        members[name] = (body, part.get_content_type())
    if message.defects or not members: raise EvidenceError('Complete Worker source package required')
    return members


def source_members(raw, headers):
    return {name: {'bytes': len(body), 'sha256': digest(body), 'content_type': kind}
        for name, (body, kind) in source_bodies(raw, headers).items()}


def configuration(settings, schedules):
    if not isinstance(settings, dict) or not isinstance(settings.get('bindings'), list):
        raise EvidenceError('Complete Worker settings/bindings required')
    bindings = []
    for binding in settings['bindings']:
        if not isinstance(binding, dict) or not isinstance(binding.get('name'), str) or not isinstance(binding.get('type'), str):
            raise EvidenceError('Typed Worker binding identity required')
        bindings.append({'name': binding['name'], 'type': binding['type'], 'configuration_sha256': digest(encoded(binding))})
    if len({v['name'] for v in bindings}) != len(bindings): raise EvidenceError('Duplicate Worker binding')
    if not isinstance(schedules, dict) or not isinstance(schedules.get('schedules'), list):
        raise EvidenceError('Complete Worker cron settings required')
    # Values and secret material never enter reports. These hashes bind complete
    # configuration objects; volatile release annotations are separate from it.
    stable = {k:v for k,v in settings.items() if k != 'annotations'}
    stable['bindings'] = sorted(settings['bindings'], key=lambda v:v['name'])
    return {'configuration_sha256': digest(encoded(stable)), 'bindings': sorted(bindings, key=lambda v:v['name']),
        'compatibility_date': settings.get('compatibility_date'), 'compatibility_flags': settings.get('compatibility_flags', []),
        'schedule_count': len(schedules['schedules']), 'schedules_sha256': digest(encoded(schedules))}


def capture(client):
    before = active_deployment(client.get('/deployments'))
    version = client.get('/versions/'+before['version_id'])
    if not isinstance(version, dict) or version.get('id') != before['version_id']:
        raise EvidenceError('Worker version response identity differs')
    config = configuration(client.get('/settings'), client.get('/schedules'))
    raw, headers = client.get('/content/v2'); members = source_members(raw, headers)
    repeated, repeated_headers = client.get('/content/v2')
    if source_members(repeated, repeated_headers) != members or headers.get('cf-entrypoint') != repeated_headers.get('cf-entrypoint'):
        raise EvidenceError('Worker code changed during capture')
    if configuration(client.get('/settings'), client.get('/schedules')) != config:
        raise EvidenceError('Worker configuration changed during capture')
    if active_deployment(client.get('/deployments')) != before:
        raise EvidenceError('Worker deployment changed during capture')
    resources = version.get('resources'); script = resources.get('script') if isinstance(resources, dict) else None
    version_etag = script.get('etag') if isinstance(script, dict) else None
    if version_etag is not None and type(version_etag) is not str:
        raise EvidenceError('Invalid Worker version script identity')
    # Record the available identities without treating an opaque provider ETag
    # as our SHA-256 or assuming this endpoint is pinned to that version.
    return {'contract': 'worker-control-plane-predecessor.v1', 'worker': WORKER,
        'captured_at': datetime.now(timezone.utc).isoformat(), **before, 'configuration': config,
        'source_members': members, 'transport': {'headers': headers, 'bytes': len(raw), 'sha256': digest(raw),
            'raw_body_base64': base64.b64encode(raw).decode()},
        'version_script_etag': version_etag, 'source_active_version_binding_verified': False,
        'intended_repo_build_verified': False, 'worker_invocations': 0, 'private_reads': 0, 'mutations': 0,
        'control_plane_gets': client.read_count}
