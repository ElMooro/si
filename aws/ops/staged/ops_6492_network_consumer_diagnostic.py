"""Read-only schedules/metrics and bounded, sanitized public-engine errors.

Never invoke engines, retrieve application objects, print raw logs/target inputs,
or change cloud resources. HTTP-denied research packets remain unprobed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
from ops_6490_network_schedule_diagnostic import collect as collect_schedules

FUNCTIONS = tuple('justhodl-' + name for name in (
    'industry-rotation', 'earnings-confluence', 'fx-intelligence',
    'stock-valuations', 'master-ranker', 'alpha-council', 'signal-harvester'))
ERROR_FUNCTIONS = ('justhodl-master-ranker', 'justhodl-signal-harvester')
ERROR_CLASSES = ('TypeError', 'ValueError', 'NameError', 'KeyError', 'IndexError',
                 'AttributeError', 'ImportError', 'ModuleNotFoundError',
                 'RuntimeError', 'ClientError', 'MemoryError', 'TimeoutError',
                 'SyntaxError', 'UnboundLocalError', 'Runtime.ExitError')


def error_projection(event):
    """Only fixed error class labels and Python file/line locations may leave."""
    message = event.get('message', '')
    if not isinstance(message, str):
        message = ''
    types = [name for name in ERROR_CLASSES
             if re.search(r'(?<![\w.])' + re.escape(name) + r'(?![\w.])', message)]
    frames = [{'file': match[0], 'line': int(match[1])} for match in re.findall(
        r'File "/var/task/([a-zA-Z_][a-zA-Z0-9_]*\.py)", line ([0-9]{1,6})', message)]
    return {'at_unix_ms': event.get('timestamp'), 'error_classes': types,
            'frames': frames[:20], 'raw_message_retained': False}


def collect(events, scheduler, cloudwatch, logs, now):
    result = collect_schedules(events, scheduler, cloudwatch, now, FUNCTIONS)
    result['application_error_log_reads'] = 0
    result['error_observations'] = {}
    for fn in ERROR_FUNCTIONS:
        # One page only: this is a sample, never a complete error count.
        packet = logs.filter_log_events(logGroupName='/aws/lambda/' + fn,
            startTime=int((now - timedelta(hours=24)).timestamp() * 1000),
            endTime=int(now.timestamp() * 1000), filterPattern='"[ERROR]"', limit=50)
        result['application_error_log_reads'] += 1
        result['error_observations'][fn] = {
            'events': [error_projection(row) for row in packet.get('events', [])],
            'page_has_continuation': bool(packet.get('nextToken')),
            'scope': 'At most 50 [ERROR] events from 24 hours; raw messages discarded. '
                     'Timeout/OOM logs without this marker may not appear.'}
    result.pop('application_log_reads', None)
    return result


def main():
    import boto3
    from ops_report import report
    with report('ops_6492_network_consumer_diagnostic') as output:
        try:
            clients = [boto3.client(name, region_name='us-east-1')
                       for name in ('events', 'scheduler', 'cloudwatch', 'logs')]
            evidence = collect(*clients, datetime.now(timezone.utc))
        except Exception as exc:
            output.fail('Read-only consumer diagnostic failed: ' + type(exc).__name__)
            raise RuntimeError('consumer_diagnostic_unavailable') from None
        output.log(json.dumps(evidence, sort_keys=True, separators=(',', ':')))


if __name__ == '__main__':
    main()
