# Requeued after Bloomberg destination and dated-calculation recovery.
"""Refresh only the two reviewed public research publishers after cadence repair."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops/checks'), str(ROOT / 'aws/shared')]
from audit_20260909_release import ReleaseVerifier, PRIMARY, utcnow

FUNCTIONS = ('justhodl-bloomberg-v8', 'justhodl-options-flow')


def main():
    report = {'ops': 5285, 'ok': False, 'status': 'NOT_STARTED', 'private_payloads_reported': 0,
              'raw_lambda_responses_reported': 0,
              'checkout_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()}
    destination = ROOT / 'aws/ops/reports/5285_cadence_publications.json'
    def checkpoint():
        destination.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    try:
        if os.environ.get('GITHUB_ACTIONS') != 'true':
            raise RuntimeError('runner_only')
        import boto3
        from botocore.config import Config
        clients = {name: boto3.client(name, region_name='us-east-1', config=Config(
            connect_timeout=15, read_timeout=45, retries={'total_max_attempts': 1 if name == 'lambda' else 4}))
            for name in ('lambda', 's3')}
        verifier = ReleaseVerifier(ROOT, clients, parity_seconds=180, publication_seconds=420)
        report.update(verifier.report, ops=5285)
        verifier.report = report; verifier.checkpoint = checkpoint
        report['artifact_scope'] = {name: {'primary_keys': PRIMARY[name]} for name in FUNCTIONS}
        if not verifier.parity(FUNCTIONS):
            raise RuntimeError('source_parity_failed')
        for name in FUNCTIONS:
            if not verifier.inspect_function(name):
                verifier.invoke_once(name)
        deadline = time.monotonic() + 420
        while True:
            remaining = [name for name in FUNCTIONS if not verifier.inspect_function(name)]
            checkpoint()
            if not remaining or time.monotonic() >= deadline:
                break
            print(json.dumps({'ops': 5285, 'phase': 'publication', 'pending': remaining}), flush=True)
            time.sleep(min(20, max(0, deadline - time.monotonic())))
        report.update(ok=not remaining, status='VERIFIED' if not remaining else 'PUBLICATION_PENDING',
                      finished_at=utcnow().isoformat())
    except Exception as exc:
        report.update(ok=False, status='VERIFICATION_FAILED', error_type=type(exc).__name__)
    checkpoint()
    print(json.dumps({k: report[k] for k in ('ops', 'ok', 'status')}))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
