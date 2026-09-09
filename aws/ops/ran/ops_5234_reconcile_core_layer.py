"""Reconcile after the reviewed 70-function batch and all three successful follow-ups.

The original batch's sole failure was research-backtest; its exact recovery run
is now a required successful dependency. Per-function source parity is checked
again by operation5231. No cloud calls occur while these follow-ups are active.
"""
import json
import subprocess
import sys
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops'), str(ROOT / 'aws/ops/checks')]
from ops_report import report
from core_layer_reconciliation import reconcile
from audit_release_dependencies import await_releases

DESTINATION = ROOT / 'aws/ops/reports/5234_core_layer_reconciliation.json'


def checkpoint(document):
    document['checker_source_sha'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)
    DESTINATION.write_text(json.dumps(document, indent=2) + '\n')


with report('ops_5234_reconcile_core_layer') as rep:
    rep.heading('Current core layer and qualified runtime reconciliation')
    try:
        def dependency_checkpoint(rows):
            path=ROOT/'aws/ops/reports/5234_release_dependencies.json'
            path.write_text(json.dumps({'releases':rows,'scope':'GITHUB_METADATA_ONLY_BEFORE_AWS_CALLS'},indent=2)+'\n')
        await_releases(checkpoint=dependency_checkpoint)
        config = Config(connect_timeout=15, read_timeout=45, retries={'max_attempts': 2})
        clients = [boto3.client(service, region_name='us-east-1', config=config) for service in ('lambda', 'ssm')]
        result = reconcile(*clients, ROOT, checkpoint)
        rep.kv(status=result['status'], consumer_count=result.get('consumer_count'), unresolved_count=result.get('unresolved_count'),
               prior_5232_configuration_safety=result['prior_5232_configuration_safety'])
        if not result['ok']:
            rep.fail('Current layer/configuration gaps remain; metadata report preserves unresolved states')
            sys.exit(1)
        rep.ok('Current discovered bindings verified; historical pre-migration configuration safety remains unverified')
    except Exception as error:
        rep.fail('Core layer reconciliation stopped: ' + type(error).__name__)
        sys.exit(1)
