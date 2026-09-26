"""Read-only actual Signal Board package/receipt and preserved trigger proof."""
from pathlib import Path
import hashlib, json, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import liquidity_agent_triggers as triggers

FUNCTION = 'justhodl-signal-board'
BUCKET = 'justhodl-dashboard-live'
BASELINE = '746cfc5ea9abc8dc933c7c1bbefc36c1adebe5db00d78c2f38cfe97031ef35d6'

def main():
    subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')], cwd=ROOT, check=True)
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/'+FUNCTION+'/tests/run_tests.py'], cwd=ROOT, text=True).strip()
    if len(expected) != 40: raise ValueError('Full intended release commit required')
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6148_signal_board_donor_regression_verify') as r:
        raw = bounded(s3.get_object(Bucket=BUCKET, Key='audit-private/20260909-originals/signal-board-research/'+BASELINE+'.bin')['Body'])
        if len(raw) != 192798 or hashlib.sha256(raw).hexdigest() != BASELINE: raise ValueError('Accepted complete baseline differs')
        old = json.loads(raw)
        actual = runtime(lam, s3, events, scheduler, FUNCTION)
        if actual['receipt'] != {'status': 'matched', 'commit': expected}: raise ValueError('Exact native receipt required')
        if actual['memory_mb'] != 1024 or actual['timeout'] != 600: raise ValueError('Whole-source runtime reserve differs')
        if actual['schedules'] != old['native_predecessor']['runtime']['schedules']: raise ValueError('Classic trigger differs')
        arn = lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
        inventory = triggers.collect(lam, scheduler, s3, arn, BUCKET)
        for key in ('matching_schedules', 'event_source_mappings', 'direct_bucket_notifications'):
            if inventory[key] != old['trigger_inventory'][key]: raise ValueError('Preserved trigger inventory differs: '+key)
        r.kv(expected_commit=expected, runtime=actual, trigger_inventory=inventory,
             code_and_receipt_verified=True, trigger_bindings_preserved=True,
             producer_invocations=0, consumer_invocations=0, provider_requests=0, private_account_reads=0,
             public_writes=0, notifications_sent=0, schedules_changed=0, normal_publication_verified=False,
             scope='Exact deployed compiler/source package and preserved direct bindings; normal publication and complete public replay still required.')

if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
