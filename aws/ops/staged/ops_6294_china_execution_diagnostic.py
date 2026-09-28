"""Read-only diagnosis of China's original September 28 scheduled execution."""
from pathlib import Path
from datetime import datetime, timezone
import json, re, subprocess, sys
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-china-liquidity/source')]
from ops_report import report
from market_runtime_evidence import runtime
import china_store as store

FN = 'justhodl-china-liquidity'
BUCKET = 'justhodl-dashboard-live'
START = datetime(2026, 9, 28, 14, 30, tzinfo=timezone.utc)
END = datetime(2026, 9, 28, 14, 40, tzinfo=timezone.utc)


def system_reports(logs):
    rows = []
    for page in logs.get_paginator('filter_log_events').paginate(
            logGroupName='/aws/lambda/' + FN, startTime=int(START.timestamp() * 1000),
            endTime=int(END.timestamp() * 1000), filterPattern='"REPORT RequestId:"'):
        for event in page.get('events', []):
            message = event['message']
            identity = re.match(r'REPORT RequestId:\s+([a-f0-9-]{36})(?:\s|$)', message)
            if not identity or not START.timestamp() * 1000 <= event['timestamp'] <= END.timestamp() * 1000:
                raise ValueError('Only bounded native system REPORT records allowed')
            row = {'request_id': identity[1], 'timestamp_ms': event['timestamp']}
            for key, label, unit in [('duration_ms', 'Duration', 'ms'), ('billed_duration_ms', 'Billed Duration', 'ms'),
                                     ('memory_mb', 'Memory Size', 'MB'), ('max_memory_mb', 'Max Memory Used', 'MB')]:
                match = re.search(r'(?:^|\t)' + label + r':\s*([0-9]+(?:\.[0-9]+)?)\s*' + unit, message)
                if match:
                    row[key] = float(match[1])
            for key, label in [('status', 'Status'), ('error_type', 'Error Type')]:
                match = re.search(r'(?:^|\t)' + label + r':\s*([A-Za-z0-9_.-]+)', message)
                if match:
                    row[key] = match[1]
            rows.append(row)
    return rows


def own_source_attempts(s3):
    """Hash-check complete own objects created in the fixed schedule window.

    No account/consumer namespace or body is read. Report only bounded acquisition
    metadata; retained provider bodies never enter the operation report.
    """
    selected = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=store.PRIVATE):
        for obj in page.get('Contents', []):
            if START <= obj['LastModified'] <= END:
                if not re.fullmatch(re.escape(store.PRIVATE) + r'[a-f0-9]{64}\.bin', obj['Key']):
                    raise ValueError('Only own content-addressed originals allowed')
                selected.append(obj)
    if len(selected) > 160 or sum(o['Size'] for o in selected) > store.LIMIT:
        raise ValueError('Whole scheduled evidence exceeds diagnostic bound')
    attempts = []
    for obj in sorted(selected, key=lambda x: (x['LastModified'], x['Key'])):
        digest = obj['Key'].removeprefix(store.PRIVATE).removesuffix('.bin')
        raw = store.retained(s3, BUCKET, {'key': obj['Key'], 'sha256': digest, 'bytes': obj['Size']})
        try:
            row = store.strict(raw)
        except (ValueError, UnicodeError):
            continue  # Complete original HTML or other provider body, never emitted.
        if not isinstance(row, dict) or not {'request', 'requested_at', 'status'} <= row.keys():
            continue
        request = row['request']
        stamp = datetime.fromisoformat(row['requested_at'])
        if stamp.tzinfo is None or not START <= stamp <= END or not isinstance(request, dict):
            raise ValueError('Own acquisition attempt clock or identity differs')
        host = request.get('source_host')
        if host not in {'api.stlouisfed.org', 'api.db.nomics.world', 'www.pbc.gov.cn'}:
            raise ValueError('Declared public provider required')
        state = row['status']
        if state not in {'http_response', 'transport_error', 'rate_limit_not_retried', 'budget_not_attempted'}:
            raise ValueError('Declared acquisition outcome required')
        summary = {'manifest_sha256': digest, 'requested_at': row['requested_at'], 'source_host': host, 'status': state}
        if type(row.get('http_status')) is int:
            summary['http_status'] = row['http_status']
        series = request.get('parameters', {}).get('series_id')
        if series is not None:
            if not isinstance(series, str) or not re.fullmatch(r'[A-Za-z0-9_]{1,80}', series):
                raise ValueError('Literal public series identity required')
            summary['series_id'] = series
        if row.get('original') is not None:
            body = store.retained(s3, BUCKET, row['original'])
            summary['original_bytes'] = len(body)
            summary['original_sha256'] = store.sha(body)
        attempts.append(summary)
    return {'objects_checked': len(selected), 'bytes_checked': sum(o['Size'] for o in selected), 'attempts': attempts}


def main():
    if datetime.now(timezone.utc) < END:
        raise ValueError('Wait for the original execution window to end')
    clients = [boto3.client(n, region_name='us-east-1') for n in ('lambda', 's3', 'events', 'scheduler')]
    with report('ops_6294_china_execution_diagnostic') as r:
        before = runtime(*clients, FN)
        expected = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/' + FN], cwd=ROOT, text=True).strip()
        if before['receipt'] != {'status': 'matched', 'commit': expected} or before['source_files_checked'] != 4:
            raise ValueError('Exact four-source deployed package required')
        baseline = json.loads((ROOT / 'docs/audit/2026-09-27/china-original-baseline.json').read_bytes())['actual_producer']
        mapping = {'runtime': 'Runtime', 'handler': 'Handler', 'timeout': 'Timeout', 'memory_mb': 'MemorySize', 'architectures': 'Architectures'}
        if any(before[k] != baseline['runtime'][v] for k, v in mapping.items()) or before['schedules'] != baseline['schedules']:
            raise ValueError('Original producer resources or cadence differs')
        reports = system_reports(boto3.client('logs', region_name='us-east-1'))
        evidence = own_source_attempts(clients[1])
        head = store.get(clients[1], BUCKET, store.HEAD)
        if head is None:
            raise ValueError('Complete existing public head required')
        packet = store.strict(head['raw'])
        if runtime(*clients, FN) != before:
            raise ValueError('Runtime changed during diagnosis')
        r.kv(actual_runtime=before,system_reports=reports,own_source_evidence=evidence,
             public_head={'bytes':len(head['raw']),'sha256':store.sha(head['raw']),'generated_at':packet.get('generated_at'),'contract':packet.get('contract')},
             native_invocations=0,provider_requests=0,account_reads=0,consumer_reads=0,application_log_reads=0,public_writes=0,history_writes=0,schedule_changes=0,
             scope='Only the fixed public producer package, September 28 14:30–14:40 UTC system REPORT records and its content-addressed original acquisition evidence. Provider bodies are hash-checked, never emitted. No native invocation or downstream/owner state.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
