"""Verify complete direct schedule discovery; retain the existing target config.

No invocation, provider request, schedule mutation or public/history write.
"""
from pathlib import Path
from datetime import datetime, timezone
import json
import subprocess
import sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/ops/staged')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
import ops_6189_global_recession_baseline as baseline
import retained_access_evidence as access
FN = 'justhodl-global-recession'
BASELINE_SHA = '863712562cb23f3ac62fb46aaea39d14a98f1ce48afe1ae1f01ab5e37520016a'


def main():
    subprocess.run([sys.executable, str(ROOT / 'tests/test_offexchange_alias_runtime.py')], cwd=ROOT, check=True)
    lam, s3, events, scheduler = (boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler'))
    with report('ops_6190_recession_schedule_acceptance') as r:
        raw = bounded(s3.get_object(Bucket=baseline.BUCKET, Key=baseline.PRIVATE + BASELINE_SHA + '.bin')['Body'])
        if len(raw) != 12219 or baseline.sha(raw) != BASELINE_SHA:
            raise ValueError('Complete original baseline differs')
        prior = json.loads(raw)
        before = runtime(lam, s3, events, scheduler, FN)
        if before['code_sha256'] != prior['predecessor']['runtime']['CodeSha256'] or (before['memory_mb'], before['timeout']) != (512, 300):
            raise ValueError('Original producer changed before schedule review')
        expected = {'kind': 'EventBridge Scheduler', 'name': 'global-recession-sched', 'state': 'ENABLED',
                    'expression': 'cron(40 12 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}
        if expected not in before['schedules']:
            raise ValueError('Existing 12:40 native schedule differs or was omitted')
        actual = scheduler.get_schedule(Name='global-recession-sched', GroupName='default')
        def encode(value):
            return json.dumps(value, default=lambda item: item.isoformat() if isinstance(item, datetime) else (_ for _ in ()).throw(TypeError('Unsupported schedule value')),
                              sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
        saved = encode(actual)
        ref = baseline.retain(s3, saved)
        head = baseline.capture(s3, 'data/global-recession.json')
        if encode(scheduler.get_schedule(Name='global-recession-sched', GroupName='default')) != saved:
            raise ValueError('Schedule changed during read-only verification')
        if runtime(lam, s3, events, scheduler, FN) != before:
            raise ValueError('Runtime or complete direct schedule list changed')
        privacy = access.summarize([access.check(ref['key'])])
        if not privacy['all_denied']:
            raise ValueError('Retained schedule configuration must remain private')
        r.kv(actual_runtime=before, retained_schedule_configuration=ref,
             schedule_representation='Canonical complete get_schedule response; timestamps serialized in ISO format; not an original wire response',
             scan_scope='Every paginated Scheduler summary across all groups; only exact target Lambda schedules are retrieved. Indirect orchestration is outside this direct-target inventory.',
             prior_baseline_sha256=BASELINE_SHA,
             baseline_schedule_erratum='The predecessor audit filtered schedule names by function prefix and missed global-recession-sched. An empty result did not mean no schedule existed.',
             complete_current_output=head, **privacy,
             native_invocations=0, provider_requests=0, public_writes=0, history_writes=0,
             account_reads=0, notifications_sent=0, schedules_changed=0,
             first_possible_new_code_publication='Original daily 12:40 UTC, after a separately verified code release; no code release occurs in this operation.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
