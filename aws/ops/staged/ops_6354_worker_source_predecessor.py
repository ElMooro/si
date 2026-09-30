"""Capture only the data proxy's inert source and control-plane metadata.

Run exclusively in worker-source-evidence.yml with the existing Cloudflare
control-plane secrets. No AWS session, Worker invocation or private data read.
"""
from pathlib import Path
import base64
import json
import os
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'scripts')]
from worker_source_evidence import ControlPlane, EvidenceError, capture, encoded, source_bodies, digest
from check_secrets import findings


def prepare(result, retired):
    """Inspect decoded original code before it can become a repository artifact."""
    raw = base64.b64decode(result['transport']['raw_body_base64'], validate=True)
    bodies = source_bodies(raw, result['transport']['headers'])
    for body, _ in bodies.values():
        if findings(body.decode('utf-8', errors='replace'), retired):
            raise EvidenceError('Worker source contains a credential pattern; source retention refused')
    # Also scan metadata/headers and all source members as one serialized document.
    if findings(encoded(result).decode('utf-8'), retired):
        raise EvidenceError('Worker evidence contains a credential pattern; retention refused')
    return encoded(result) + b'\n'


def main():
    from ops_report import report
    with report('ops_6354_worker_source_predecessor') as r:
        intended = os.environ.get('EXPECTED_COMMIT', '')
        run_id = os.environ.get('GITHUB_RUN_ID', '')
        actual = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        if not re.fullmatch(r'[a-f0-9]{40}', intended) or actual != intended or not re.fullmatch(r'[0-9]+', run_id):
            raise EvidenceError('Exact intended runner commit and run identity required')
        client = ControlPlane(os.environ.get('CLOUDFLARE_ACCOUNT_ID'), os.environ.get('CLOUDFLARE_API_TOKEN'))
        result = capture(client)
        result.update(inspection_commit=actual, workflow_run_id=run_id)
        retired = set(json.loads((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())['sha256'])
        raw = prepare(result, retired)
        target = ROOT/'aws/ops/reports/worker-source'/('6354-'+run_id+'.json')
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as out:
            out.write(raw)
        r.kv(artifact=str(target.relative_to(ROOT)).replace('\\', '/'), bytes=len(raw), sha256=digest(raw),
            worker=result['worker'], deployment_id=result['deployment_id'], version_id=result['version_id'],
            source_members=len(result['source_members']), control_plane_gets=result['control_plane_gets'],
            source_active_version_binding_verified=False, intended_repo_build_verified=False,
            worker_invocations=0, private_reads=0, mutations=0, inspection_commit=actual)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
