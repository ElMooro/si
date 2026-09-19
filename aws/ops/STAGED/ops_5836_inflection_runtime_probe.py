"""Read only runtime capacity and sanitized Lambda REPORT metrics; no invoke."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import sys
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    lam = boto3.client('lambda', region_name='us-east-1')
    logs = boto3.client('logs', region_name='us-east-1')
    with report('ops_5836_inflection_runtime_probe') as r:
        for fn in ('justhodl-liquidity-inflection', 'justhodl-daily-report-v3'):
            conf = lam.get_function_configuration(FunctionName=fn)
            fields = {k: conf.get(k) for k in ('State', 'LastUpdateStatus', 'MemorySize', 'Timeout', 'CodeSha256')}
            fields['reserved_concurrency'] = lam.get_function_concurrency(FunctionName=fn).get('ReservedConcurrentExecutions')
            r.kv(function=fn, runtime=fields)
            events = logs.filter_log_events(logGroupName='/aws/lambda/'+fn,
                startTime=int((datetime.now(timezone.utc)-timedelta(hours=1)).timestamp()*1000),
                filterPattern='"REPORT RequestId:"', limit=20).get('events', [])
            for event in events:
                # Do not emit arbitrary application logs, environment or secrets.
                message = event.get('message', '')
                metrics = {name: re.search(pattern, message).group(1) for name, pattern in (
                    ('duration_ms', r'(?<!Billed )Duration: ([0-9.]+) ms'),
                    ('billed_ms', r'Billed Duration: ([0-9.]+) ms'),
                    ('memory_mb', r'Memory Size: ([0-9.]+) MB'),
                    ('max_memory_mb', r'Max Memory Used: ([0-9.]+) MB'),
                    ('status', r'Status: ([a-z]+)'),
                ) if re.search(pattern, message)}
                r.kv(function=fn, timestamp_ms=event['timestamp'], metrics=metrics)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('Runtime probe failed; inspect sanitized runner report.')
        sys.exit(1)
