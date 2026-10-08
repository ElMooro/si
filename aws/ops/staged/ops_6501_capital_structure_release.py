"""Review and release the failed capital-structure refresh control (2026-10-08 audit, item 7).

Evidence: workflow run 36269293338 (2026-09-26) planned successfully, then part-1 failed with a
ValueError after provider batch errors; the control has sat at status=failed since, and every daily
run (0 successes in the last 40) stops at 'Previous refresh incomplete or failed; explicit review is
required'. This is that review. It prints the non-secret failure fields, retains the failed control
bytes unchanged, and marks the control 'released' so the next scheduled plan can start a NEW cycle.
Nothing is retried and no provider is called.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / 'aws/ops/checks'), str(ROOT / 'aws/shared')]
import capital_structure_refresh as refresh  # noqa: E402
import share_structure_campaign as campaign  # noqa: E402
import share_structure_sources as capture  # noqa: E402

REPORT = ROOT / 'aws/ops/reports/latest/ops_6501_capital_structure_release.md'
client = boto3.client('s3', region_name='us-east-1', config=Config(retries={'max_attempts': 2}))
NOW = datetime.now(timezone.utc)
out = {'ran_at': NOW.isoformat()}
ok = True
try:
    state, etag, raw = refresh.load_control(client)
    fields = ('contract', 'status', 'request_id', 'run_id', 'started_at', 'error_type', 'failed_at', 'active_phase',
              'phase_started_at', 'completed_phases', 'updated_at')
    out['failed_control'] = {k: state.get(k) for k in fields} if state else None
    if state and state.get('active_phase'):
        try:
            j = campaign.read_journal(client, capture.request_key(state['request_id'], 'phase:' + state['active_phase']))
            out['phase_journal'] = {k: j.get(k) for k in ('phase', 'status', 'started_at')}
        except Exception as e:
            out['phase_journal'] = {'error': type(e).__name__}
        try:
            b = campaign.read_journal(client, capture.request_key(state['request_id'], 'batch:' + state['active_phase'].removeprefix('part-')))
            out['batch_journal'] = {'status': b.get('status'), 'n_captures': len(b.get('captures') or {}),
                                    'counts': b.get('counts'), 'error_type': b.get('error_type')}
        except Exception as e:
            out['batch_journal'] = {'error': type(e).__name__}
    if state and state.get('status') == 'failed':
        out['release'] = refresh.release_failed_control(
            client, 6501, 'audit 2026-10-08: run 36269293338 part-1 failure reviewed; failed control retained; '
                          'new cycle permitted, failed cycle never retried')
    else:
        out['release'] = {'skipped': 'control status is %r' % (state or {}).get('status')}
    after, _, _ = refresh.load_control(client)
    out['after'] = {k: after.get(k) for k in ('status', 'request_id', 'updated_at')} | {'review': after.get('review')}
except Exception as e:
    ok = False
    out['error'] = type(e).__name__ + ': ' + str(e)[:200]
lines = ['# ops 6501 - capital-structure refresh: review and release', '', '```json',
         json.dumps(out, indent=1, default=str), '```', '', 'VERDICT: %s' % ('PASS' if ok else 'FAIL')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines))
if not ok:
    sys.exit(1)
