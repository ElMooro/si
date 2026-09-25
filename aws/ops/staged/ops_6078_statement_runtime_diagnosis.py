"""Read the failed accounting publication journal and its bounded runtime error.

No invocation, provider request, account read, schedule or engine-data write.
Only the runner report is published. Logs are confined to the new public-source
publisher execution; unrelated application logs and environment are not read.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, re, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared', 'aws/ops', 'aws/ops/staged')]
from ops_report import report
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
import statement_producer as producer
import statement_research_source as source

BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-forensic-screen'
COMMIT = '41efe217e24cd3572ea5f58622de6ea7c41d12a4'
REQUEST = 'chatgpt-statement-native-' + COMMIT[:12] + '-1'


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1')
    logs = boto3.client('logs', region_name='us-east-1')
    with report('ops_6078_statement_runtime_diagnosis') as r:
        state = source.strict(producer.raw(s3, BUCKET, producer.request_key(REQUEST)))
        dispatch = source.strict(producer.raw(s3, BUCKET, producer.request_key(REQUEST+'-dispatch')))
        assert state['request_id'] == REQUEST and dispatch['status'] == 'accepted_async'
        execution = state['execution_id']; assert re.fullmatch('[a-f0-9-]{36}', execution)
        cfg = lam.get_function_configuration(FunctionName=FUNCTION)
        receipt = source.strict(producer.raw(s3, BUCKET, 'data/ops/releases/'+FUNCTION+'.json'))
        assert receipt['commit'] == COMMIT and receipt['code_sha256'] == cfg['CodeSha256']
        start = source.clock(state['started_at'])
        assert 0 < (datetime.now(timezone.utc)-start).total_seconds() < 7200
        args = {'logGroupName':'/aws/lambda/'+FUNCTION, 'filterPattern':'"'+execution+'"',
            'startTime':int(start.timestamp()*1000)-3000, 'endTime':int(start.timestamp()*1000)+900000, 'limit':50}
        rows, seen = [], set()
        for _ in range(4):
            page = logs.filter_log_events(**args); rows.extend(page.get('events', []))
            token = page.get('nextToken')
            if not token or token in seen: break
            seen.add(token); args['nextToken'] = token
        streams = {row['logStreamName'] for row in rows}
        assert len(streams) == 1, 'One exact execution stream required'
        events = logs.get_log_events(logGroupName='/aws/lambda/'+FUNCTION, logStreamName=next(iter(streams)),
            startTime=int(start.timestamp()*1000)-1000, endTime=int(start.timestamp()*1000)+60000,
            startFromHead=True, limit=100)['events']
        errors = []
        for event in events:
            message = event.get('message', '')
            if '[ERROR]' not in message and 'Traceback' not in message: continue
            # This publisher contains no provider URLs/secrets; nevertheless keep
            # diagnostics bounded and strip URL query strings defensively.
            message = re.sub(r'(https?://[^\s?]+)\?[^\s]+', r'\1?[redacted]', message)
            errors.append(message[:12000])
        managed = [parse_runtime(row.get('message', ''), execution) for row in rows]
        managed = [item for item in managed if item]
        raw = producer.raw(s3, BUCKET, producer.CURRENT); current = source.strict(raw)
        ready = source.strict(producer.raw(s3, BUCKET, producer.READY))
        r.kv(commit=COMMIT, function=FUNCTION, request=state, dispatch=dispatch,
            managed_runtime_reports=managed, bounded_publisher_errors=errors,
            current_publication={'contract':current.get('contract'), 'generated_at':current.get('generated_at'),
                'sha256':hashlib.sha256(raw).hexdigest(), 'replay':current.get('replay')},
            ready={'contract':ready.get('contract'), 'status':ready.get('status'), 'replay':ready.get('replay')},
            producer_invocations=0, provider_requests=0, consumer_invocations=0, private_account_reads=0,
            notifications_sent=0, portfolio_writes=0)
        assert managed and errors, 'Retain exact runtime outcome before attempting any repair'


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
