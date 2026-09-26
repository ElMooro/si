"""Read-only package, exact receipt and normal schedule evidence for two producers."""
from pathlib import Path
import json, subprocess, sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops', 'aws/ops/checks', 'aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime
import liquidity_agent_triggers as triggers

FUNCTIONS = ('justhodl-liquidity-pulse', 'justhodl-yield-curve')
BUCKET = 'justhodl-dashboard-live'
CLOCKS = {'justhodl-liquidity-pulse': 'cron(3 16 * * ? *)', 'justhodl-yield-curve': 'cron(52 13 * * ? *)'}


def main():
    for function in FUNCTIONS:
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/function/'tests/run_tests.py')], cwd=ROOT, check=True)
    expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--',
        'aws/lambdas/justhodl-liquidity-pulse/source/liquidity_pulse_store.py'], cwd=ROOT, text=True).strip()
    if len(expected) != 40: raise ValueError('Full deployment commit required')
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6150_pulse_curve_publication_verify') as r:
        rows = []
        for function in FUNCTIONS:
            actual = runtime(lam, s3, events, scheduler, function)
            config = json.loads((ROOT/'aws/lambdas'/function/'config.json').read_bytes())
            if actual['receipt'] != {'status': 'matched', 'commit': expected}: raise ValueError('Exact receipt required: '+function)
            if actual['memory_mb'] != config.get('memory', config.get('memory_mb')) or actual['timeout'] != config.get('timeout', config.get('timeout_s')): raise ValueError('Runtime reserve differs')
            arn = lam.get_function_configuration(FunctionName=function)['FunctionArn']
            discovered = triggers.collect(lam, scheduler, s3, arn, BUCKET)
            clocks = [(row['expression'], row['state'], row.get('timezone', 'UTC'))
                      for row in actual['schedules'] if row.get('expression')]
            clocks.extend((row['ScheduleExpression'], row['State'], row['ScheduleExpressionTimezone'])
                          for row in discovered['matching_schedules'])
            if not clocks or any(row != (CLOCKS[function], 'ENABLED', 'UTC') for row in clocks):
                raise ValueError('Reviewed normal publication timing differs: '+function)
            rows.append({'runtime': actual, 'trigger_inventory': discovered, 'reviewed_publication_timing_matches': True})
        r.kv(expected_commit=expected, packages=rows, code_and_receipt_verified=True,
             producer_invocations=0, consumer_invocations=0, provider_requests=0,
             private_account_reads=0, public_writes=0, notifications_sent=0, schedules_changed=0,
             normal_publication_verified=False, original_provider_replay_performed=False,
             scope='Actual code, receipt and normal publication timing only. The next scheduled output still requires complete source replay.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
