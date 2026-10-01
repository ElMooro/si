"""DRAFT: exact-target, bounded read-only desk evidence; never invoke a producer.
Run only through reviewed run-ops-direct dispatch. Numeric REPORT projections,
not raw logs, environment values, request IDs, or provider/account payloads.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import hashlib
import json
import math
import os
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
FUNCTION = 'justhodl-etf-global-desk'
REGION = 'us-east-1'
FUNCTION_ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
BUCKET = 'justhodl-dashboard-live'
RECEIPT = 'data/ops/releases/' + FUNCTION + '.json'
OUTPUT = 'data/etf-desk-research.json'
EXPECTED_COMMIT = 'd00f52a256815d6798988f23abec68afd0e15d4d'
EXPECTED_SOURCES = {
    'lambda_function.py': 'a7d82a77e0562679c98598ebad2bb32d3555eaabf993b763764866cc4fd5f6ca',
    'etf_desk_model.py': '56938722dec09268dfa8b1042b68308a9e6f593d5ec85cd54f4465c0e63d53e9',
    'etf_desk_store.py': '3fba26e742d01f587d3e67caab15f170beb93e4a515a0d7897c85a09a7818aa1',
}
MAX_CALLS = 12
MAX_SECONDS = 90
MAX_RECEIPT_BYTES = 64 * 1024
MAX_LOG_PAGES = 3
LOG_PAGE_SIZE = 20
MAX_REPORT_TEXT = 4096
FILTER = '"REPORT RequestId:"'
METRICS = {
    'Duration': ('Milliseconds', ['Average', 'Maximum', 'SampleCount']),
    'Invocations': ('Count', ['Sum']),
    'Errors': ('Count', ['Sum']),
    'Throttles': ('Count', ['Sum']),
}
ALLOWED = frozenset({('lambda', 'get_function_configuration'), ('s3', 'head_object'),
                     ('s3', 'get_object'), ('cloudwatch', 'get_metric_statistics'),
                     ('logs', 'filter_log_events')})
STOP_REASONS = frozenset(('invalid_read_contract', 'read_deadline_reached', 'read_call_bound_reached',
                          'read_denied', 'read_missing', 'read_changed', 'read_failed'))
HEX = re.compile(r'[a-f0-9]{64}')
# No request-id capture. Only complete standard platform REPORT-shaped lines pass.
REPORT = re.compile(
    r'REPORT RequestId: [0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}'
    r'\s+Duration: (?P<duration_ms>[0-9]+(?:\.[0-9]+)?) ms'
    r'\s+Billed Duration: (?P<billed_duration_ms>[0-9]+(?:\.[0-9]+)?) ms'
    r'\s+Memory Size: (?P<memory_mb>[0-9]+) MB'
    r'\s+Max Memory Used: (?P<max_memory_mb>[0-9]+) MB'
    r'(?:\s+Init Duration: (?P<init_duration_ms>[0-9]+(?:\.[0-9]+)?) ms)?'
    r'(?:\s+Status: (?:success|error|timeout)(?:\s+Error Type: [A-Za-z0-9_.-]{1,80})?)?\s*')


class Stop(Exception):
    """Only locally defined reason tokens are passed to the reporter."""


def require(condition):
    if not condition:
        raise Stop('invalid_read_contract')


def number(value, maximum=10**12):
    require(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= maximum)
    return value


def instant(value):
    try:
        if isinstance(value, str):
            require(len(value) <= 40 and 'T' in value)
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(isinstance(value, datetime) and value.tzinfo is not None)
        return value.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        raise Stop('invalid_read_contract') from None


def digest(value):
    require(isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9+/]{43}=', value) is not None)
    require(len(base64.b64decode(value, validate=True)) == 32)
    return value


class Reads:
    """Validate operation AND exact parameters before ever touching an SDK method."""
    def __init__(self, clients, at, timer=time.monotonic):
        self.clients = clients
        self.at = instant(at)
        # Full hourly boundaries plus the current partial hour: <=48h, <=48 points.
        self.start = self.at.replace(minute=0, second=0, microsecond=0) - timedelta(hours=47)
        self.timer = timer
        self.deadline = timer() + MAX_SECONDS
        self.calls = 0
        self.phase = 'not_started'

    def budget(self):
        if self.timer() >= self.deadline:
            raise Stop('read_deadline_reached')

    def call(self, service, operation, **params):
        require((service, operation) in ALLOWED)
        if service == 'lambda':
            require(params == {'FunctionName': FUNCTION_ARN})
        elif service == 's3':
            required = {'Bucket', 'Key'} | ({'IfMatch'} if operation == 'get_object' else set())
            require(set(params) == required and params['Bucket'] == BUCKET)
            require(params['Key'] in ((RECEIPT,) if operation == 'get_object' else (RECEIPT, OUTPUT)))
            if operation == 'get_object':
                require(isinstance(params['IfMatch'], str) and re.fullmatch(r'"[a-f0-9]{32}(?:-[0-9]{1,5})?"', params['IfMatch']) is not None)
        elif service == 'cloudwatch':
            name = params.get('MetricName')
            require(name in METRICS)
            unit, stats = METRICS[name]
            require(params == {'Namespace': 'AWS/Lambda', 'MetricName': name,
                'Dimensions': [{'Name': 'FunctionName', 'Value': FUNCTION}],
                'StartTime': self.start, 'EndTime': self.at, 'Period': 3600,
                'Unit': unit, 'Statistics': stats})
        else:
            token = params.get('nextToken')
            require(token is None or isinstance(token, str) and 0 < len(token) <= 8192)
            expected = {'logGroupName': '/aws/lambda/' + FUNCTION, 'filterPattern': FILTER,
                'startTime': int(self.start.timestamp() * 1000), 'endTime': int(self.at.timestamp() * 1000), 'limit': LOG_PAGE_SIZE}
            if token is not None:
                expected['nextToken'] = token
            require(params == expected)
        self.budget()
        if self.calls >= MAX_CALLS:
            raise Stop('read_call_bound_reached')
        self.calls += 1
        self.phase = service + '.' + operation
        try:
            result = getattr(self.clients[service], operation)(**params)
        except Exception as exc:
            code = getattr(exc, 'response', {}).get('Error', {}).get('Code')
            if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', '403'):
                raise Stop('read_denied') from None
            if code in ('NoSuchKey', 'NotFound', 'ResourceNotFoundException', '404'):
                raise Stop('read_missing') from None
            if code in ('PreconditionFailed', '412'):
                raise Stop('read_changed') from None
            raise Stop('read_failed') from None
        self.budget()
        require(isinstance(result, dict))
        return result


def configuration(api):
    raw = api.call('lambda', 'get_function_configuration', FunctionName=FUNCTION_ARN)
    require(raw.get('FunctionName') == FUNCTION and raw.get('FunctionArn') == FUNCTION_ARN)
    require(raw.get('State') in ('Pending', 'Active', 'Inactive', 'Failed'))
    require(raw.get('LastUpdateStatus') in ('Successful', 'Failed', 'InProgress'))
    require(type(raw.get('Timeout')) is int and 1 <= raw['Timeout'] <= 900)
    require(type(raw.get('MemorySize')) is int and 128 <= raw['MemorySize'] <= 10240)
    stamp = instant(raw.get('LastModified'))
    require(stamp <= api.at)
    return {'State': raw['State'], 'LastUpdateStatus': raw['LastUpdateStatus'],
        'CodeSha256': digest(raw.get('CodeSha256')), 'MemorySize': raw['MemorySize'],
        'Timeout': raw['Timeout'], 'LastModified': stamp.isoformat()}


def headers(api, key, limit):
    raw = api.call('s3', 'head_object', Bucket=BUCKET, Key=key)
    size = raw.get('ContentLength')
    require(type(size) is int and 0 < size <= limit)
    etag = raw.get('ETag')
    require(isinstance(etag, str) and re.fullmatch(r'"[a-f0-9]{32}(?:-[0-9]{1,5})?"', etag) is not None)
    stamp = instant(raw.get('LastModified'))
    require(stamp <= api.at)
    return {'bytes': size, 'etag': etag, 'last_modified': stamp.isoformat()}


def receipt(api, runtime):
    meta = headers(api, RECEIPT, MAX_RECEIPT_BYTES)
    raw = api.call('s3', 'get_object', Bucket=BUCKET, Key=RECEIPT, IfMatch=meta['etag'])
    body = raw['Body']
    try:
        require(raw.get('ContentLength') == meta['bytes'] and raw.get('ETag') == meta['etag'])
        content = body.read(meta['bytes'] + 1)
    finally:
        body.close()
    api.budget()
    require(isinstance(content, bytes) and len(content) == meta['bytes'])
    def unique(pairs):
        out = {}
        for key, value in pairs:
            require(key not in out)
            out[key] = value
        return out
    def reject_constant(_):
        raise Stop('invalid_read_contract')
    doc = json.loads(content, object_pairs_hook=unique, parse_constant=reject_constant)
    require(isinstance(doc, dict) and doc.get('schema') == 'release-receipt.v1' and doc.get('function') == FUNCTION)
    require(isinstance(doc.get('commit'), str) and re.fullmatch(r'[a-f0-9]{40}', doc['commit']) is not None)
    require(type(doc.get('verified')) is bool)
    code = digest(doc.get('code_sha256'))
    require(isinstance(doc.get('zip_sha256_hex'), str) and HEX.fullmatch(doc['zip_sha256_hex']) is not None)
    require(type(doc.get('zip_bytes')) is int and 0 < doc['zip_bytes'] <= 300 * 1024 * 1024)
    stamp = instant(doc.get('deployed_at')); require(stamp <= api.at)
    sources = doc.get('source'); require(isinstance(sources, dict))
    selected = {}; matches = {}
    for name, expected in EXPECTED_SOURCES.items():
        entry = sources.get(name)
        if entry is None:
            selected[name] = None; matches[name] = None
            continue
        require(isinstance(entry, dict) and isinstance(entry.get('sha256'), str) and HEX.fullmatch(entry['sha256']) is not None)
        require(type(entry.get('bytes')) is int and 0 < entry['bytes'] <= 16 * 1024 * 1024)
        selected[name] = {key: entry[key] for key in ('sha256', 'bytes')}
        matches[name] = entry['sha256'] == expected
    zip_matches = base64.b64decode(code).hex() == doc['zip_sha256_hex']
    return {'metadata': meta, 'commit': doc['commit'], 'expected_commit': EXPECTED_COMMIT,
        'code_sha256': code, 'zip_bytes': doc['zip_bytes'], 'deployed_at': stamp.isoformat(),
        'receipt_verified_flag': doc['verified'], 'zip_digest_consistent': zip_matches,
        'commit_matches': doc['commit'] == EXPECTED_COMMIT, 'live_code_matches': code == runtime['CodeSha256'],
        'expected_source_entries': selected, 'expected_source_matches': matches,
        'all_expected_source_hashes_present_and_matching': all(v is True for v in matches.values())}


def metrics(api):
    result = {}
    for name, (unit, stats) in METRICS.items():
        raw = api.call('cloudwatch', 'get_metric_statistics', Namespace='AWS/Lambda', MetricName=name,
            Dimensions=[{'Name': 'FunctionName', 'Value': FUNCTION}], StartTime=api.start,
            EndTime=api.at, Period=3600, Unit=unit, Statistics=stats)
        points = raw.get('Datapoints'); require(isinstance(points, list) and len(points) <= 48)
        safe = []; seen = set()
        for point in points:
            stamp = instant(point.get('Timestamp'))
            require(api.start <= stamp <= api.at and stamp not in seen and point.get('Unit') == unit)
            seen.add(stamp)
            values = {field: number(point.get(field)) for field in stats}
            if name == 'Duration':
                require(values['Average'] <= values['Maximum'])
            safe.append({'timestamp': stamp.isoformat(), **values})
        result[name] = {'status': 'returned_datapoints' if safe else 'no_returned_datapoints',
            'unit': unit, 'points': sorted(safe, key=lambda p: p['timestamp'])}
    return result


def report_fields(message):
    if not isinstance(message, str) or len(message) > MAX_REPORT_TEXT:
        return None
    match = REPORT.fullmatch(message)
    if not match:
        return None
    try:
        out = {key: number(float(value)) for key, value in match.groupdict().items() if value is not None}
        require(128 <= out['memory_mb'] <= 10240 and out['max_memory_mb'] <= out['memory_mb'])
        return out
    except (Stop, ValueError, OverflowError):
        return None


def reports(api, runtime):
    token = None; seen = set(); records = []; ignored = 0; complete = False
    for page in range(MAX_LOG_PAGES):
        raw = api.call('logs', 'filter_log_events', logGroupName='/aws/lambda/' + FUNCTION,
            filterPattern=FILTER, startTime=int(api.start.timestamp() * 1000),
            endTime=int(api.at.timestamp() * 1000), limit=LOG_PAGE_SIZE,
            **({'nextToken': token} if token is not None else {}))
        events = raw.get('events'); require(isinstance(events, list) and len(events) <= LOG_PAGE_SIZE)
        for event in events:
            fields = report_fields(event.get('message'))
            stamp = event.get('timestamp')
            if fields is None or type(stamp) is not int or not api.start.timestamp()*1000 <= stamp <= api.at.timestamp()*1000:
                ignored += 1
                continue
            records.append({'timestamp_ms': stamp, **fields})
        token = raw.get('nextToken')
        if token is None:
            complete = True
            break
        require(isinstance(token, str) and 0 < len(token) <= 8192)
        if token in seen:
            break
        seen.add(token)
    boundary = instant(runtime['LastModified']).timestamp() * 1000
    return {'status': 'returned_numeric_reports' if records else 'no_accepted_reports',
        'pages_read': page + 1, 'pagination_complete': complete, 'ignored_records': ignored,
        'numeric_records': sorted(records, key=lambda r: r['timestamp_ms']),
        'reports_ending_after_last_modified': sum(r['timestamp_ms'] >= boundary for r in records),
        'code_version_attribution_verified': False}


def observe(api):
    before = configuration(api)
    released = receipt(api, before)
    public = headers(api, OUTPUT, 16 * 1024 * 1024)
    measured = metrics(api)
    runtime_reports = reports(api, before)
    after = configuration(api)
    stable = before == after
    durations = [p['Maximum'] for p in measured['Duration']['points'] if p['SampleCount'] > 0]
    memories = [p['max_memory_mb'] for p in runtime_reports['numeric_records']]
    # Missing data stays null; sampled maxima are not current capacity guarantees.
    peak_duration = max(durations) if durations else None
    peak_memory = max(memories) if memories else None
    matched = (stable and after['State'] == 'Active' and after['LastUpdateStatus'] == 'Successful'
        and all(released[k] for k in ('commit_matches', 'live_code_matches', 'receipt_verified_flag', 'zip_digest_consistent'))
        and released['expected_source_matches']['lambda_function.py'] is True
        and not any(v is False for v in released['expected_source_matches'].values()))
    return {'completed': True, 'function': FUNCTION, 'observed_at': api.at.isoformat(), 'window_start': api.start.isoformat(),
        'window_end': api.at.isoformat(), 'runtime_before': before, 'runtime_after': after,
        'runtime_stable': stable, 'receipt': released, 'receipt_and_live_code_verified': matched,
        'public_output_metadata': public, 'public_output_body_read': False,
        'public_output_semantic_freshness_verified': False, 'metrics': measured, 'reports': runtime_reports,
        'observed_max_duration_ms': peak_duration, 'observed_max_memory_mb': peak_memory,
        'timeout_minus_observed_duration_seconds': after['Timeout'] - peak_duration / 1000 if stable and peak_duration is not None else None,
        'memory_minus_observed_max_memory_mb': after['MemorySize'] - peak_memory if stable and peak_memory is not None else None,
        'enabled_supplement_capacity_established': False, 'new_supplement_network_cost_measured': False,
        'api_calls': api.calls, 'api_call_limit': MAX_CALLS, 'aws_mutations': 0, 'native_invocations': 0,
        'vendor_calls': 0, 'private_account_reads': 0, 'raw_log_messages_reported': 0, 'request_ids_reported': 0,
        'environment_values_reported': 0,
        'limitation': 'Window metrics and REPORT-shaped samples may describe older code; timestamps do not prove code-version attribution. Missing is not zero. Observed arithmetic headroom is not an enabled end-to-end guarantee. No new supplement write/readback latency measured.'}


def main():
    if not (os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REPOSITORY') == 'ElMooro/si'
            and os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
            and os.environ.get('GITHUB_WORKFLOW') == 'Run ops script (direct)'
            and re.fullmatch(r'[a-f0-9]{40}', os.environ.get('GITHUB_SHA', ''))):
        raise SystemExit('reviewed_direct_runner_only')
    import boto3
    from botocore.config import Config
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    api = None
    with report(Path(__file__).stem) as r:
        r.kv(probe_commit=os.environ['GITHUB_SHA'], probe_source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        try:
            config = Config(connect_timeout=3, read_timeout=5, retries={'total_max_attempts': 1})
            clients = {name: boto3.client(name, region_name=REGION, config=config) for name in ('lambda', 's3', 'cloudwatch', 'logs')}
            api = Reads(clients, datetime.now(timezone.utc))
            result = observe(api)
            r.kv(**result)
            if not result['receipt_and_live_code_verified']:
                r.fail('Expected receipt/live-code identity or stable runtime check did not pass')
                raise SystemExit(1)
        except Stop as exc:
            # Every Stop reason is locally generated; never include SDK exceptions.
            r.kv(completed=False, stop_reason=exc.args[0] if exc.args and isinstance(exc.args[0], str) and exc.args[0] in STOP_REASONS else 'unexpected_read_failure_details_withheld', last_operation=api.phase if api else 'client_setup', api_calls=api.calls if api else 0)
            r._failed = True
            raise SystemExit(1) from None
        except Exception:
            r.kv(completed=False, stop_reason='unexpected_read_failure_details_withheld', last_operation=api.phase if api else 'client_setup', api_calls=api.calls if api else 0)
            r._failed = True
            raise SystemExit(1) from None


if __name__ == '__main__':
    main()
