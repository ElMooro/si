"""Read-only complete-population and package prerequisites for recurring refresh."""
from pathlib import Path
import json, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared', 'aws/ops', 'aws/ops/checks')]
from ops_report import report
import capital_structure_refresh as refresh


def main():
    subprocess.run([sys.executable, str(ROOT/'tests/test_capital_structure_refresh.py')], cwd=ROOT, check=True)
    s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6147_capital_structure_refresh_preflight') as r:
        plan = refresh.baseline_plan(s3, refresh.now)
        state, etag, raw = refresh.load_control(s3)
        ready_for_first_cycle = state is None or (state.get('contract') == refresh.CONTRACT and state.get('status') == 'complete')
        r.kv(population_names=len(plan['reported_symbols']), planned_sources=plan['planned_sources'], batches=plan['batches'],
             previous_control_status=state.get('status') if state else 'absent', first_cycle_may_start=ready_for_first_cycle,
             actual_request_interval_seconds=refresh.INTERVAL, source_refresh_clock_utc='20:15 daily',
             provider_requests=0, public_writes=0, ready_writes=0, producer_invocations=0, consumer_invocations=0,
             private_account_reads=0, notifications_sent=0, paid_ai_calls=0, native_schedule_changes=0,
             complete_refresh_executed=False, recurring_runtime_capacity_verified=False)
        if not ready_for_first_cycle: raise ValueError('Existing refresh requires review before another acquisition')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
