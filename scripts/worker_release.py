"""Capture before deploy, then issue an exact-source receipt after deploy.

Runs on the existing Worker deploy runner. Never invokes Worker routes or reads
account/consumer state. It publishes no receipt until all comparisons succeed.
"""
from pathlib import Path
import argparse
import base64
import json
import os
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/ops/checks'), str(ROOT/'scripts')]
from worker_source_evidence import ControlPlane, EvidenceError, capture, encoded, digest, source_bodies
from worker_release_evidence import WRANGLER_VERSION, verify
from check_secrets import findings

WORKER_PATH = 'cloudflare/workers/justhodl-data-proxy'
BASELINE = ROOT/'aws/ops/reports/worker-source/6354-36658515237.json'
TOOLS = ['.github/workflows/deploy-workers.yml', 'scripts/worker_release.py', 'scripts/publish_worker_evidence.py',
    'aws/ops/checks/worker_release_evidence.py', 'aws/ops/checks/worker_source_evidence.py']

# One-time allowance (ops 6551, 2026-10-20).  Release 39461888087 put a plain_text Worker var, JH_EDGE_REVALIDATE, on
# the live Worker and then failed its configuration check.  The feature needs no var (it keys on request.cf) and
# wrangler.toml does not declare it, so the next `wrangler deploy` removes it again.  Until that deploy has happened the
# live configuration is the reviewed baseline PLUS exactly this one binding.  The `before` phase accepts that and
# nothing else; the `after` phase always demands the exact baseline, so the var is gone, and the Worker is back to the
# reviewed configuration, before any receipt exists.
STRAY_BINDING = ('JH_EDGE_REVALIDATE', 'plain_text')


class HidingStrayBinding:
    """Read-only wrapper: /settings is returned without the one allowed stray binding (and that is recorded)."""
    def __init__(self, client):
        self.client, self.stray_seen = client, False

    @property
    def read_count(self):
        return self.client.read_count

    def get(self, suffix):
        value = self.client.get(suffix)
        if suffix == '/settings' and isinstance(value, dict) and isinstance(value.get('bindings'), list):
            kept = [b for b in value['bindings'] if not (isinstance(b, dict) and (b.get('name'), b.get('type')) == STRAY_BINDING)]
            if len(kept) != len(value['bindings']):
                self.stray_seen = True
                value = dict(value, bindings=kept)
        return value


def configuration_difference(current, baseline):
    """Which parts of the configuration differ - binding and field NAMES only, never a value or a digest."""
    try:
        parts = [k for k in sorted(set(current) | set(baseline)) if k != 'bindings' and current.get(k) != baseline.get(k)]
        now = {b['name']: b['configuration_sha256'] for b in current.get('bindings', [])}
        was = {b['name']: b['configuration_sha256'] for b in baseline.get('bindings', [])}
        for label, names in (('bindings added', set(now) - set(was)), ('bindings removed', set(was) - set(now)),
                             ('bindings changed', {n for n in set(now) & set(was) if now[n] != was[n]})):
            if names:
                parts.append(label + ' ' + ','.join(sorted(names)))
        return '; '.join(parts) or 'no difference found in the named parts'
    except Exception:  # noqa: BLE001 - a diagnostic must never hide the real refusal
        return 'difference not describable'


def build_identity(build_dir, commit, root=None):
    root = ROOT if root is None else Path(root)
    actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    if not re.fullmatch(r'[a-f0-9]{40}', commit) or actual != commit:
        raise EvidenceError('Worker checkout differs from intended commit')
    subprocess.run(['git', 'diff', '--exit-code', 'HEAD', '--', WORKER_PATH, *TOOLS], cwd=root, check=True, stdout=subprocess.DEVNULL)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard', '-z', '--', WORKER_PATH, *TOOLS], cwd=root)
    ignored_source = subprocess.check_output(['git', 'ls-files', '--others', '--ignored', '--exclude-standard', '-z', '--', WORKER_PATH+'/src'], cwd=root)
    if untracked or ignored_source:
        raise EvidenceError('Uncommitted Worker build input refused')
    names = subprocess.check_output(['git', 'ls-files', '-z', '--', WORKER_PATH, *TOOLS], cwd=root).decode().split('\0')
    if any((root/name).is_symlink() for name in names if name):
        raise EvidenceError('External symlinked Worker input refused')
    sources = {name: {'bytes':len((root/name).read_bytes()), 'sha256':digest((root/name).read_bytes())} for name in names if name}
    raw = (Path(build_dir)/'index.js').read_bytes()
    return {'commit':commit, 'repository_files':sources, 'built_index':{'bytes':len(raw),'sha256':digest(raw)},
        'build_tool':{'name':'wrangler','version':WRANGLER_VERSION}}


def safe_capture(tolerate_stray=False):
    client = ControlPlane(os.environ.get('CLOUDFLARE_ACCOUNT_ID'),os.environ.get('CLOUDFLARE_API_TOKEN'))
    if tolerate_stray: client = HidingStrayBinding(client)
    result = capture(client)
    if tolerate_stray and client.stray_seen: result['tolerated_stray_bindings'] = [STRAY_BINDING[0]]
    retired = set(json.loads((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())['sha256'])
    bodies = source_bodies(base64.b64decode(result['transport']['raw_body_base64'],validate=True),result['transport']['headers'])
    if any(findings(raw.decode('utf-8',errors='replace'),retired) for raw,_ in bodies.values()) or findings(encoded(result).decode('utf-8'),retired):
        raise EvidenceError('Worker capture contains a credential pattern; retention refused')
    return result


def execute(phase, build_dir, evidence_dir, commit, run_id):
    if not re.fullmatch(r'[0-9]+',run_id): raise EvidenceError('Exact runner identity required')
    evidence_dir = Path(evidence_dir)
    identity = build_identity(build_dir,commit)
    baseline = json.loads(BASELINE.read_bytes())
    if phase == 'before':
        value = safe_capture()
        if encoded(value['configuration']) != encoded(baseline['configuration']):
            difference = configuration_difference(value['configuration'], baseline['configuration'])
            # The one known leftover (see STRAY_BINDING) is accepted; anything else still stops the deploy.
            relaxed = safe_capture(tolerate_stray=True)
            if relaxed.get('tolerated_stray_bindings') != [STRAY_BINDING[0]] or encoded(relaxed['configuration']) != encoded(baseline['configuration']):
                raise EvidenceError('Original Worker configuration or schedules differ before deploy ('+difference+')')
            value = relaxed
        evidence_dir.mkdir(parents=True,exist_ok=True)
        with (evidence_dir/'before.json').open('xb') as out: out.write(encoded(value)+b'\n')
        with (evidence_dir/'build.json').open('xb') as out: out.write(encoded(identity)+b'\n')
        return {'phase':'before','source_members':len(value['source_members']),'commit':commit,'worker_invocations':0,'private_reads':0}
    before = json.loads((evidence_dir/'before.json').read_bytes())
    if encoded(before['configuration']) != encoded(baseline['configuration']):
        raise EvidenceError('Recorded predecessor configuration differs from the original baseline')
    if encoded(identity) != encoded(json.loads((evidence_dir/'build.json').read_bytes())):
        raise EvidenceError('Worker build or source changed during deploy')
    after = safe_capture()
    if encoded(after['configuration']) != encoded(before['configuration']):
        raise EvidenceError('Worker configuration differs after deploy ('+configuration_difference(after['configuration'], before['configuration'])+')')
    receipt = verify(after,build_dir,commit,before['configuration'],WRANGLER_VERSION)
    receipt.update(workflow_run_id=run_id,repository_files=identity['repository_files'])
    bundle = {'contract':'worker-release-capture.v1','before':before,'after':after,'build':identity,'receipt':receipt}
    # Preserve complete originals before making the release receipt available.
    artifact = ROOT/'aws/ops/reports/worker-source'/('release-'+run_id+'.json')
    artifact.parent.mkdir(parents=True,exist_ok=True)
    with artifact.open('xb') as out: out.write(encoded(bundle)+b'\n')
    receipt['capture_artifact'] = {'path':artifact.relative_to(ROOT).as_posix(),'sha256':digest(artifact.read_bytes())}
    target = ROOT/'data/ops/releases/worker-justhodl-data-proxy.json'
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes(encoded(receipt)+b'\n')
    return {'phase':'after','status':receipt['status'],'commit':commit,'version_id':receipt['version_id'],
        'source_files':receipt['source_files'],'worker_invocations':0,'private_reads':0}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=['before','after'])
    parser.add_argument('--build-dir',required=True)
    parser.add_argument('--evidence-dir',required=True)
    parser.add_argument('--commit',required=True)
    args=parser.parse_args()
    print(json.dumps(execute(args.phase,args.build_dir,args.evidence_dir,args.commit,os.environ.get('GITHUB_RUN_ID',''))))


if __name__=='__main__':
    try:main()
    except Exception as exc:
        message='Worker release evidence failed: '+type(exc).__name__+(': '+str(exc) if isinstance(exc,EvidenceError) else '')
        print(message,file=sys.stderr)
        # also as a workflow annotation, so the reason shows on the run page and through the checks API
        print('::error title=Worker release evidence::'+message.replace('%','%25').replace('\r','%0D').replace('\n','%0A'))
        sys.exit(1)
