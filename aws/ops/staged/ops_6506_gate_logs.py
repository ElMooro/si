"""Read-only: contract-gate log tail for the last 2 hours (why did v1.5.1 learn not complete?)."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import boto3
ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6506_gate_logs.md'
logs = boto3.client('logs'); lam = boto3.client('lambda')
NOW = datetime.now(timezone.utc)
g = '/aws/lambda/justhodl-contract-gate'
c = lam.get_function_configuration(FunctionName='justhodl-contract-gate')
out = {'ran_at': NOW.isoformat(), 'config': {k: c.get(k) for k in ('Timeout', 'MemorySize', 'LastModified', 'CodeSha256')}}
evs = []
token = None
while True:
    kw = dict(logGroupName=g, startTime=int((NOW - timedelta(hours=2)).timestamp() * 1000), limit=10000)
    if token: kw['nextToken'] = token
    r = logs.filter_log_events(**kw)
    evs += r.get('events', [])
    token = r.get('nextToken')
    if not token or len(evs) > 20000: break
keep = [e for e in evs if any(s in e['message'] for s in ('REPORT', 'START', '[contracts]', 'Error', 'error', 'Traceback', 'killed', 'timed out', 'mode'))]
out['n_events'] = len(evs)
out['lines'] = [datetime.fromtimestamp(e['timestamp'] / 1000, timezone.utc).strftime('%H:%M:%S ') + e['message'].rstrip()[:300] for e in keep][-150:]
lines = ['# ops 6506 - contract-gate logs (read-only)', '', '- config: %s' % json.dumps(out['config'], default=str), '- events: %d' % len(evs), '', '```'] + out['lines'] + ['```', '', 'VERDICT: PASS']
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines))
