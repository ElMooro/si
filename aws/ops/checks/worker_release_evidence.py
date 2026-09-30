"""Verify complete deployed Worker bytes against a fixed Wrangler build.

Provider ETags are opaque representation identities, never our content hashes.
No deployed script is executed and no Worker route or private data is read.
"""
import base64
import re
from pathlib import Path
from worker_source_evidence import EvidenceError, WORKER, encoded, digest, source_members

WRANGLER_VERSION = '4.144.0'


def representation_identity(value, quoted):
    if type(value) is not str or not value or value.startswith('W/'):
        raise EvidenceError('Strong provider script identity required')
    if quoted:
        if not (value.startswith('"') and value.endswith('"')):
            raise EvidenceError('Quoted HTTP source ETag required')
        value = value[1:-1]
    elif value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if not value or any(ord(c)<33 or ord(c)>126 or c=='"' for c in value):
        raise EvidenceError('Unambiguous provider script identity required')
    return value


def verify(captured, build_dir, intended_commit, baseline_configuration, wrangler_version):
    if type(intended_commit) is not str or not re.fullmatch(r'[a-f0-9]{40}', intended_commit):
        raise EvidenceError('Exact intended Worker source commit required')
    if wrangler_version != WRANGLER_VERSION:
        raise EvidenceError('Reviewed pinned Worker build tool required')
    if captured.get('worker') != WORKER or captured.get('contract') != 'worker-control-plane-predecessor.v1':
        raise EvidenceError('Exact reviewed Worker capture required')
    if encoded(captured.get('configuration')) != encoded(baseline_configuration):
        raise EvidenceError('Original Worker configuration or schedules differ')
    try:
        transport = captured['transport']
        raw = base64.b64decode(transport['raw_body_base64'], validate=True)
        members = source_members(raw, transport['headers'])
    except (KeyError, TypeError, ValueError):
        raise EvidenceError('Complete original Worker source transport required') from None
    if type(transport.get('bytes')) is not int or len(raw) != transport['bytes'] or digest(raw) != transport.get('sha256'):
        raise EvidenceError('Worker source transport digest differs')
    if encoded(members) != encoded(captured.get('source_members')):
        raise EvidenceError('Worker source member inventory differs')
    source_etag = representation_identity(transport['headers'].get('ETag'), True)
    active_etag = representation_identity(captured.get('version_script_etag'), False)
    if source_etag != active_etag:
        raise EvidenceError('Source representation does not match the active version')
    if transport['headers'].get('cf-entrypoint') != 'index.js':
        raise EvidenceError('Exact reviewed Worker entrypoint required')
    # This Worker's current Wrangler configuration uploads one bundled module.
    # The map and README are dry-run auxiliaries, not executable uploaded modules.
    # A future module/asset addition needs an explicit inventory update.
    root = Path(build_dir)
    files = {p.relative_to(root).as_posix(): p for p in root.rglob('*') if p.is_file()}
    if set(files)-{'index.js','index.js.map','README.md'} or 'index.js' not in files:
        raise EvidenceError('Unexpected Worker dry-run build inventory')
    expected = files['index.js'].read_bytes()
    wanted = {'index.js': {'bytes':len(expected), 'sha256':digest(expected), 'content_type':'application/javascript+module'}}
    if encoded(members) != encoded(wanted):
        raise EvidenceError('Deployed Worker bytes differ from the complete intended build')
    return {'contract':'worker-release.v1', 'status':'matched', 'worker':WORKER,
        'commit':intended_commit, 'captured_at':captured['captured_at'],
        'deployment_id':captured['deployment_id'], 'version_id':captured['version_id'],
        'provider_script_etag':active_etag, 'source_files':members,
        'build_tool':{'name':'wrangler','version':wrangler_version},
        'configuration':captured['configuration'], 'source_active_version_binding_verified':True,
        'intended_repo_build_verified':True, 'complete_source_transport_sha256':digest(raw),
        'private_reads':0,'worker_invocations':0,'configuration_changes':0,
        'normal_private_publication_verified':False,
        'verification_scope':'Complete built module bytes, provider representation identity, stable deployment/configuration; no live route invocation'}
