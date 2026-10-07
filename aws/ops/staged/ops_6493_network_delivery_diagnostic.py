"""Read-only delivery bindings, aggregate metrics and publication metadata.

No object bodies, private-account inputs, raw application logs, credentials,
engine invocations, notifications, schedule changes or other cloud writes.
An authenticated HEAD proves storage metadata, never public HTTP access.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
from ops_6490_network_schedule_diagnostic import pages

BUCKET = 'justhodl-dashboard-live'
BUS = 'justhodl-system-events'
COORDINATOR = 'justhodl-event-coordinator'
FUNCTIONS = (COORDINATOR, 'justhodl-master-ranker', 'justhodl-signal-harvester',
             'justhodl-cross-asset-regime', 'justhodl-future-intelligence')
KEYS = ('data/research-network.json', 'data/prospective-research.json', 'data/master-ranker.json')
CONTEXT_PREFIX = 'data/research-forecasts/contexts/'
ROUTE_EVENTS = ('regime.changed', 'future.signal.high_conviction')


def error_code(exc):
    code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
    return code if code in ('404', '403', 'NoSuchKey', 'AccessDenied', 'ResourceNotFoundException') else type(exc).__name__


def clock(value):
    return value.isoformat() if isinstance(value, datetime) and value.tzinfo is not None else None


def pattern_projection(raw):
    """Preserve only source/type selectors; never payload filters or account IDs."""
    try:
        value = json.loads(raw)
        if not isinstance(value, dict): raise ValueError()
    except (TypeError, ValueError):
        return {'valid_json_object': False}
    result = {'valid_json_object': True, 'other_filter_fields_present': bool(set(value) - {'source', 'detail-type'})}
    for key in ('source', 'detail-type'):
        selected = value.get(key)
        result[key] = []
        if selected is None: continue
        if not isinstance(selected, list):
            result[key] = ['unsupported_selector']; continue
        for item in selected:
            if isinstance(item, str) and re.fullmatch(r'[a-zA-Z0-9_.:*\-]{1,100}', item):
                result[key].append(item)
            elif isinstance(item, dict) and set(item) == {'prefix'} and isinstance(item['prefix'], str) and re.fullmatch(r'[a-zA-Z0-9_.:\-]{1,100}', item['prefix']):
                result[key].append(item)
            else: result[key].append('unsupported_selector')
    return result


def collect(events, cloudwatch, logs, storage, lambdas, now):
    result = {'observed_at': now.isoformat(), 'native_invocations': 0, 'cloud_writes': 0,
              'object_body_reads': 0, 'private_packet_reads': 0, 'raw_logs_retained': False,
              'event_rules': [], 'functions': {}, 'publication_metadata': {}, 'log_samples': {}}
    for rule in pages(events.list_rules, 'Rules', EventBusName=BUS, Limit=100):
        targets = list(pages(events.list_targets_by_rule, 'Targets', EventBusName=BUS, Rule=rule['Name'], Limit=100))
        matching = [row for row in targets if row.get('Arn', '').split(':function:')[-1].split(':')[0] == COORDINATOR]
        if not matching: continue
        result['event_rules'].append({'name': rule['Name'], 'state': rule.get('State'),
            'event_pattern': pattern_projection(rule.get('EventPattern')), 'coordinator_targets': len(matching),
            'qualifiers': [row['Arn'].split(':function:', 1)[1][len(COORDINATOR):] or 'unqualified' for row in matching],
            'input_transform_present': [any(k in row for k in ('Input', 'InputPath', 'InputTransformer')) for row in matching],
            'dead_letter_configured': [bool(row.get('DeadLetterConfig')) for row in matching],
            'retry_policy': [{key: value for key, value in row.get('RetryPolicy', {}).items()
                              if key in ('MaximumEventAgeInSeconds', 'MaximumRetryAttempts') and type(value) is int}
                             for row in matching]})
    for fn in FUNCTIONS:
        item = {'metrics': {}}
        try:
            config = lambdas.get_function_configuration(FunctionName=fn)
            item['configuration'] = {k: config.get(k) for k in ('State', 'LastUpdateStatus', 'LastModified', 'CodeSha256', 'Runtime', 'Timeout', 'MemorySize')}
        except Exception as exc: item['configuration_error'] = error_code(exc)
        for metric, stat in (('Invocations', 'Sum'), ('Errors', 'Sum'), ('Duration', 'Maximum'), ('Throttles', 'Sum')):
            packet = cloudwatch.get_metric_statistics(Namespace='AWS/Lambda', MetricName=metric,
                Dimensions=[{'Name': 'FunctionName', 'Value': fn}], StartTime=now-timedelta(days=7),
                EndTime=now, Period=3600, Statistics=[stat])
            item['metrics'][metric] = [{'at': clock(row.get('Timestamp')), 'value': row.get(stat)}
                for row in sorted(packet.get('Datapoints', []), key=lambda row: row['Timestamp'])]
        result['functions'][fn] = item
    for key in KEYS:
        try:
            head = storage.head_object(Bucket=BUCKET, Key=key)
            result['publication_metadata'][key] = {'exists': True, 'bytes': head.get('ContentLength'),
                'last_modified': clock(head.get('LastModified')), 'content_type': head.get('ContentType')}
        except Exception as exc:
            result['publication_metadata'][key] = {'exists': False if error_code(exc) in ('404', 'NoSuchKey') else None, 'error': error_code(exc)}
    # One bounded LIST, metadata only. A full page does not establish total size.
    packet = storage.list_objects_v2(Bucket=BUCKET, Prefix=CONTEXT_PREFIX, MaxKeys=1000)
    context_rows = [row for row in packet.get('Contents', [])
                    if re.fullmatch(re.escape(CONTEXT_PREFIX)+r'[0-9a-f]{64}\.json', row.get('Key', ''))]
    stored_times = sorted(row['LastModified'] for row in context_rows
                          if isinstance(row.get('LastModified'), datetime) and row['LastModified'].tzinfo is not None)
    result['prospective_context_metadata'] = {'observed_objects': len(context_rows),
        'listing_complete': not packet.get('IsTruncated', False),
        'earliest_observed_storage_time': clock(stored_times[0]) if stored_times else None,
        'latest_observed_storage_time': clock(stored_times[-1]) if stored_times else None,
        'context_contents_verified': False}
    for fn, marker in ((COORDINATOR, '"[coordinator]"'), ('justhodl-signal-harvester', '"[harvester]"')):
        packet = logs.filter_log_events(logGroupName='/aws/lambda/'+fn,
            startTime=int((now-timedelta(days=7)).timestamp()*1000), endTime=int(now.timestamp()*1000),
            filterPattern=marker, limit=100)
        counts = Counter()
        completion_times = []
        for row in packet.get('events', []):
            message = row.get('message', '')
            if not isinstance(message, str): continue
            for event in ROUTE_EVENTS:
                if re.search(r'\[coordinator\] routed event='+re.escape(event)+r' invokes=\d+(?:\s|$)', message):
                    counts[event] += 1
            if re.search(r'\[harvester\] scanned=\d+ engines_with_picks=\d+ harvested=\d+ written=\d+ ', message):
                completion_times.append(row.get('timestamp'))
        result['log_samples'][fn] = {'observed_messages': len(packet.get('events', [])),
            'page_has_continuation': bool(packet.get('nextToken')), 'routed_event_counts': dict(counts),
            'harvester_completion_times_unix_ms': completion_times,
            'scope': 'At most 100 matching messages from seven days; not a complete count. Routed does not prove downstream acceptance.'}
    result['scope'] = 'Named bus target bindings; seven-day aggregate metrics (empty is unknown); three HEADs; one bounded context-prefix LIST; bounded log projections. Storage metadata does not establish public HTTP access or sidecar content validity.'
    return result


def main():
    import boto3
    from ops_report import report
    with report('ops_6493_network_delivery_diagnostic') as output:
        try:
            clients = [boto3.client(name, region_name='us-east-1') for name in ('events', 'cloudwatch', 'logs', 's3', 'lambda')]
            evidence = collect(*clients, datetime.now(timezone.utc))
        except Exception as exc:
            output.fail('Read-only delivery diagnostic failed: '+type(exc).__name__)
            raise RuntimeError('delivery_diagnostic_unavailable') from None
        output.log(json.dumps(evidence, sort_keys=True, separators=(',', ':')))


if __name__ == '__main__':
    main()
