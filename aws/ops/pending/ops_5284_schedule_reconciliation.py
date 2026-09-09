"""Disable guarded duplicate Options Flow EventBridge rules; runner only."""
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops/checks'), str(ROOT / 'aws/shared')]
from audit_20260909_schedules import FUNCTION, consolidate, require
from release_package_evidence import check_packages


def main():
    report = {'ops': 5284, 'ok': False, 'status': 'NOT_STARTED', 'input_bodies_reported': 0,
              'checkout_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()}
    try:
        require(os.environ.get('GITHUB_ACTIONS') == 'true', 'runner_only')
        import boto3
        from botocore.config import Config
        clients = {name: boto3.client(name, region_name='us-east-1', config=Config(
            connect_timeout=15, read_timeout=45, retries={'mode': 'adaptive', 'max_attempts': 6}))
            for name in ('sts', 'lambda', 'events', 'scheduler')}
        require(clients['sts'].get_caller_identity()['Account'] == '857687956942', 'wrong_account')
        report['source_package'] = check_packages(clients['lambda'], ROOT, [FUNCTION])[0]
        require(report['source_package']['pass'], 'source_package_mismatch')
        consolidate(clients['events'], clients['scheduler'], report)
        report.update(ok=True, status='DUPLICATE_RULES_DISABLED_SCHEDULERS_PRESERVED')
    except Exception as exc:
        report.update(status='RECONCILIATION_BLOCKED', error_type=type(exc).__name__)
        # Only our fixed guard codes may be published, never SDK exception text.
        if isinstance(exc, ValueError) and len(exc.args) == 1 and str(exc).replace('_', '').isalpha():
            report['guard'] = str(exc)
    (ROOT / 'aws/ops/reports/5284_schedule_reconciliation.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
    print(json.dumps({k: report[k] for k in ('ops', 'ok', 'status')}))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
