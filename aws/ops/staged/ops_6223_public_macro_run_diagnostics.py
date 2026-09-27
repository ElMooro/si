"""Read-only bounded scheduled-run diagnosis; never return raw log messages."""
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
import ast, re, sys, subprocess
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
from ops_report import report

TARGETS = {'justhodl-portwatch': 'data/portwatch.json', 'justhodl-geopolitical-risk': 'data/geopolitical-risk.json'}
START = datetime(2026, 9, 27, 11, 15, tzinfo=timezone.utc)


def reviewed_literals(fn):
    allowed = set()
    for path in (ROOT / 'aws/lambdas' / fn / 'source').glob('*.py'):
        for node in ast.walk(ast.parse(path.read_bytes())):
            if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call) and node.exc.args:
                arg = node.exc.args[0]
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and len(arg.value) <= 180:
                    allowed.add(arg.value)
    return allowed


def diagnose(logs, fn, end):
    literals = reviewed_literals(fn); names = {p.name for p in (ROOT / 'aws/lambdas' / fn / 'source').glob('*.py')}
    messages = 0; errors = Counter(); types = Counter(); locations = Counter(); reports = []; next_token = None; seen = set()
    for _ in range(50):
        args = dict(logGroupName='/aws/lambda/' + fn, startTime=int(START.timestamp()*1000), endTime=int(end.timestamp()*1000), limit=1000)
        if next_token: args['nextToken'] = next_token
        result = logs.filter_log_events(**args)
        for event in result.get('events', []):
            if event['eventId'] in seen: continue
            seen.add(event['eventId']); messages += 1
            if messages > 5000: raise ValueError('Complete bounded run-log population exceeded')
            text = event.get('message', '')
            for literal in literals:
                if literal in text: errors[literal] += 1
            for kind in re.findall(r'\[ERROR\]\s+([A-Za-z_][A-Za-z0-9_.]{0,80})(?=[:\s])', text): types[kind] += 1
            for file, line in re.findall(r'File "/var/task/([^/"\r\n]+\.py)", line ([0-9]{1,6})', text):
                if file in names: locations[file + ':' + line] += 1
            if 'Task timed out' in text or 'Runtime.Timeout' in text: types['Runtime.Timeout'] += 1
            if 'REPORT RequestId:' in text:
                row = {'at': datetime.fromtimestamp(event['timestamp']/1000, timezone.utc).isoformat()}
                for label in ('Duration', 'Billed Duration', 'Memory Size', 'Max Memory Used', 'Init Duration'):
                    found = re.search(r'(?:^|\t|\s{2,})' + re.escape(label) + r':\s*([0-9.]+)\s*(ms|MB)', text)
                    if found: row[label] = {'value': float(found[1]), 'unit': found[2]}
                found = re.search(r'Status:\s*(success|error|timeout)', text)
                if found: row['status'] = found[1]
                reports.append(row)
        new = result.get('nextToken')
        if not new or new == next_token: break
        next_token = new
    else: raise ValueError('Complete run-log pagination required')
    return {'events_examined': messages, 'reviewed_error_counts': dict(errors), 'exception_type_counts': dict(types),
            'source_locations': dict(locations), 'execution_reports': reports, 'raw_messages_returned': 0}


def main():
    end = datetime.now(timezone.utc)
    if not START < end < datetime(2026, 9, 27, 14, tzinfo=timezone.utc):
        raise ValueError('This diagnostic is bound to the original September 27 morning runs')
    subprocess.run([sys.executable, str(ROOT / 'tests/ops/test_public_macro_run_diagnostics.py')], cwd=ROOT, check=True)
    logs = boto3.client('logs', region_name='us-east-1'); s3 = boto3.client('s3', region_name='us-east-1')
    with report('ops_6223_public_macro_run_diagnostics') as r:
        results = {}
        for fn, key in TARGETS.items():
            result = diagnose(logs, fn, end)
            obj = s3.head_object(Bucket='justhodl-dashboard-live', Key=key)
            result['stored_public_head'] = {'key': key, 'bytes': obj['ContentLength'], 'last_modified': obj['LastModified'].isoformat()}
            results[fn] = result
        r.kv(window_start=START.isoformat(), window_end=end.isoformat(), public_producer_diagnostics=results,
             native_invocations=0, schedule_changes=0, provider_requests=0, account_reads=0, public_writes=0,
             scope='Reviewed error literals, exception classes, source locations and runtime counters only. No raw messages, credentials, environment values or unrelated log groups.')


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
