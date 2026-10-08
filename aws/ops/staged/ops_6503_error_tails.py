"""Read-only: raw log tails + REPORT lines for the functions erroring in the last 7 days whose
errors matched no Traceback/[ERROR] pattern in ops 6498 (likely timeouts or memory kills).
No invocations, no writes."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re

import boto3

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6503_error_tails.md'
FNS = ['justhodl-ticker-360', 'justhodl-outcome-checker', 'justhodl-auction-crisis-ai', 'justhodl-analytics-snapshot',
       'justhodl-event-flow-monitor', 'justhodl-morning-intelligence', 'justhodl-apac-leadlag', 'justhodl-ecb-derived',
       'justhodl-strategist', 'justhodl-imf-full', 'justhodl-signal-genealogy', 'justhodl-fedwatch-rate-probability',
       'justhodl-ecb-deep', 'justhodl-forensic-screen', 'justhodl-short-interest', 'justhodl-ecb-detail', 'justhodl-ici-flows']
lam = boto3.client('lambda')
logs = boto3.client('logs')
NOW = datetime.now(timezone.utc)
start = int((NOW - timedelta(days=7)).timestamp() * 1000)
out = {'ran_at': NOW.isoformat(), 'functions': {}}
for fn in FNS:
    rec = {}
    try:
        c = lam.get_function_configuration(FunctionName=fn)
        rec['config'] = {'timeout_s': c['Timeout'], 'memory_mb': c['MemorySize'], 'runtime': c['Runtime'],
                         'last_modified': c['LastModified'], 'handler': c['Handler']}
    except Exception as e:
        rec['config'] = {'error': str(e)[:160]}
    g = '/aws/lambda/' + fn
    try:
        reports = logs.filter_log_events(logGroupName=g, startTime=start, filterPattern='REPORT', limit=200).get('events', [])
        durs = []
        for e in reports:
            m = re.search(r'Duration: ([\d.]+) ms.*?Memory Size: (\d+) MB\s+Max Memory Used: (\d+) MB', e['message'], re.S)
            if m:
                durs.append((float(m.group(1)), int(m.group(2)), int(m.group(3)), 'Status: timeout' in e['message'] or 'timed out' in e['message']))
        if durs:
            rec['report'] = {'n': len(durs), 'max_duration_s': round(max(d[0] for d in durs) / 1000, 1),
                             'median_duration_s': round(sorted(d[0] for d in durs)[len(durs) // 2] / 1000, 1),
                             'max_mem_used_mb': max(d[2] for d in durs), 'mem_size_mb': durs[0][1]}
        streams = logs.describe_log_streams(logGroupName=g, orderBy='LastEventTime', descending=True, limit=3).get('logStreams', [])
        tails = []
        for s in streams[:2]:
            evs = logs.get_log_events(logGroupName=g, logStreamName=s['logStreamName'], limit=40, startFromHead=False).get('events', [])
            tails.append([e['message'].rstrip()[:400] for e in evs])
        rec['tails'] = tails
        bad = logs.filter_log_events(logGroupName=g, startTime=start, limit=30,
                                     filterPattern='?"Task timed out" ?"Runtime exited" ?"signal: killed" ?"Error" ?"error" ?"Traceback" ?"Exception"').get('events', [])
        rec['bad_lines'] = sorted({e['message'].rstrip()[:400] for e in bad})[:12]
    except Exception as e:
        rec['logs_error'] = type(e).__name__ + ': ' + str(e)[:160]
    out['functions'][fn] = rec
lines = ['# ops 6503 - raw error tails (read-only)', '']
for fn, rec in out['functions'].items():
    lines += ['## %s' % fn, '', '- config: %s' % json.dumps(rec.get('config'), default=str),
              '- report: %s' % json.dumps(rec.get('report')), '', '```']
    lines += rec.get('bad_lines') or ['(no matching bad lines)']
    lines += ['--- latest stream tail ---'] + ((rec.get('tails') or [[]])[0][-14:] or ['(empty)']) + ['```', '']
lines += ['## Full data', '', '```json', json.dumps(out, default=str, separators=(',', ':')), '```', '', 'VERDICT: PASS']
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines[:60]))
