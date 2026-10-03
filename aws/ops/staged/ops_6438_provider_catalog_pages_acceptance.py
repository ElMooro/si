"""Reviewed runner-only acceptance: one function/rule and three public outputs.

At most nine AWS reads, one signed-package HTTPS GET, 100 target rows, 120s.
No invokes, writes, LIST of S3 objects, metrics, logs, billing or private bodies.
Only allowlisted technical facts reach the public report. Raw SDK errors,
environment, target payloads, signed URLs and public documents are withheld.
"""
import base64
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
FUNCTION = 'justhodl-provider-catalog'
BUCKET = 'justhodl-dashboard-live'
RULE = 'justhodl-provider-catalog-hourly'
OLD_HASH = '8188ccc4a062d01ffa64d88e24c87440e35f45c0f609b1ff74ec49b8310144c3'
MAX_CALLS = 9
PACKAGE_BOUND = 64 * 1024 * 1024
DOCUMENT_BOUND = 2 * 1024 * 1024


class Stop(Exception):
    pass


def require(condition, reason):
    if not condition:
        raise Stop(reason)


def bounded(stream, limit):
    try:
        raw = stream.read(limit + 1)
    finally:
        stream.close()
    require(len(raw) <= limit, 'byte_bound_reached')
    return raw


class Reader:
    def __init__(self):
        self.calls = 0

    def read(self, method, **kwargs):
        require(self.calls < MAX_CALLS, 'api_bound_reached')
        self.calls += 1
        try:
            return method(**kwargs)
        except Exception as exc:
            code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
            raise Stop('access_denied_stop' if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation', '403')
                       else 'aws_read_failed_details_withheld') from None


def clock(value):
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None, 'aware_timestamp_required')
    return result


def document(reader, s3, key):
    response = reader.read(s3.get_object, Bucket=BUCKET, Key=key)
    return json.loads(bounded(response['Body'], DOCUMENT_BOUND))


def inspect(lam, s3, events, reader, opener=urllib.request.urlopen):
    sys.path.insert(0, str(ROOT / 'aws/ops/checks'))
    from release_package_evidence import shared_imports
    source = ROOT / 'aws/lambdas' / FUNCTION / 'source'
    config = json.loads((source.parent / 'config.json').read_bytes())
    candidate = (source / 'lambda_function.py').read_bytes()
    needle = b'kw = {"Bucket": BUCKET, "Prefix": pref,\n                      "MaxKeys": 1000}'
    require(candidate.count(needle) == 1, 'candidate_literal_changed')
    predecessor = candidate.replace(needle, needle.replace(b'1000', b'400'))
    require(hashlib.sha256(predecessor).hexdigest() == OLD_HASH, 'predecessor_source_changed')
    state = reader.read(lam.get_function, FunctionName=FUNCTION)
    cfg = state['Configuration']
    require(cfg.get('FunctionName') == FUNCTION and cfg.get('State') == 'Active'
            and cfg.get('LastUpdateStatus') == 'Successful', 'function_not_ready')
    raw = bounded(opener(state['Code']['Location'], timeout=30), PACKAGE_BOUND)
    require(base64.b64encode(hashlib.sha256(raw).digest()).decode() == cfg['CodeSha256'], 'zip_hash_mismatch')
    tracked = subprocess.check_output(['git', 'ls-files', str(source.relative_to(ROOT))], cwd=ROOT, text=True).splitlines()
    paths = [ROOT / name for name in tracked]
    members = {path.relative_to(source).as_posix(): path for path in paths}
    members.update({p.name: p for p in shared_imports(ROOT, paths) if not (source / p.name).exists()})
    require(bool(members), 'tracked_source_required')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(archive.getinfo('lambda_function.py').file_size <= 8 * 1024 * 1024, 'source_member_byte_bound')
        actual = archive.read('lambda_function.py')
        require(actual in (predecessor, candidate), 'live_handler_not_reviewed')
        phase = 'predecessor' if actual == predecessor else 'candidate'
        for name, path in members.items():
            require(archive.getinfo(name).file_size <= 8 * 1024 * 1024, 'source_member_byte_bound')
            if name != 'lambda_function.py':
                require(archive.read(name) == path.read_bytes(), 'reachable_source_mismatch')
    env = cfg.get('Environment') or {}
    controls = {
        'runtime_matches': cfg.get('Runtime') == config['runtime'],
        'handler_matches': cfg.get('Handler') == config['handler'],
        'memory_matches': cfg.get('MemorySize') == config['memory'],
        'timeout_matches': cfg.get('Timeout') == config['timeout'],
        'ephemeral_storage_matches': cfg.get('EphemeralStorage', {}).get('Size') == config['ephemeral_storage'],
        'architecture_matches': cfg.get('Architectures') == config.get('architectures', ['x86_64']),
        'role_matches': cfg.get('Role') == config['role'],
        'declared_environment_matches': not env.get('Error') and all(env.get('Variables', {}).get(k) == v for k, v in config['env'].items()),
        'tracing_already_active': cfg.get('TracingConfig', {}).get('Mode') == 'Active',
        'dlq_already_standard': cfg.get('DeadLetterConfig', {}).get('TargetArn') == 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default',
    }
    require(all(controls.values()), 'release_control_mismatch')
    rule = reader.read(events.describe_rule, Name=RULE)
    targets = reader.read(events.list_targets_by_rule, Rule=RULE, Limit=100)
    require(not targets.get('NextToken'), 'target_row_bound_reached')
    require(rule.get('Name') == RULE and rule.get('State') == 'ENABLED'
            and rule.get('ScheduleExpression') == 'rate(1 hour)', 'hourly_rule_mismatch')
    native = [t for t in targets.get('Targets', []) if t.get('Arn') == cfg['FunctionArn']]
    require(len(native) == 1, 'native_target_mismatch')
    # No target Input/InputPath/InputTransformer contents or hashes are published.
    projection = {k: rule.get(k) for k in ('State', 'ScheduleExpression', 'EventBusName')}
    projection['targets'] = [{k: t.get(k) for k in ('Id', 'RetryPolicy', 'DeadLetterConfig')}
                             | {'native': t.get('Arn') == cfg['FunctionArn'],
                                'payload_fields_present': [k for k in ('Input', 'InputPath', 'InputTransformer') if k in t],
                                'role_present': bool(t.get('RoleArn'))}
                             for t in targets.get('Targets', [])]
    schedule_fingerprint = hashlib.sha256(json.dumps(projection, sort_keys=True).encode()).hexdigest()
    receipt = document(reader, s3, 'data/ops/releases/' + FUNCTION + '.json')
    require(receipt.get('function') == FUNCTION and receipt.get('verified') is True
            and receipt.get('code_sha256') == cfg['CodeSha256'], 'receipt_mismatch')
    published = clock(receipt['deployed_at'])
    heads = {key: reader.read(s3.head_object, Bucket=BUCKET, Key=key)
             for key in ('data/provider-catalog.json', 'data/search/provider-shards.json')}
    hub = document(reader, s3, 'data/provider-catalog.json')
    manifest = document(reader, s3, 'data/search/provider-shards.json')
    require(isinstance(hub.get('providers'), list) and bool(hub['providers']), 'catalog_population_missing')
    require(manifest.get('schema_version') == 1 and manifest.get('providers') == len(manifest.get('shards', []))
            and manifest.get('documents') == sum(s['count'] for s in manifest['shards']), 'search_manifest_invalid')
    require(hub.get('search', {}).get('providers') == manifest['providers']
            and hub['search'].get('documents') == manifest['documents']
            and hub['search'].get('coverage') == manifest.get('coverage'), 'catalog_search_join_mismatch')
    index = manifest['index']
    require(isinstance(index.get('key'), str) and re.fullmatch(r'data/search/index/provider-search-\d{8}T\d{6}Z-[a-f0-9]{12}\.sqlite\.gz', index['key']), 'index_key_invalid')
    require(index.get('format') == 'sqlite-fts5+gzip' and re.fullmatch('[a-f0-9]{64}', index.get('sha256', ''))
            and type(index.get('bytes')) is int and index['bytes'] > 0
            and type(index.get('uncompressed_bytes')) is int and 0 < index['uncompressed_bytes'] <= 1500*1024*1024,
            'index_identity_invalid')
    index_head = reader.read(s3.head_object, Bucket=BUCKET, Key=index['key'])
    require(index_head.get('ContentLength') == index['bytes'], 'index_size_mismatch')
    generated = clock(manifest['generated_at'])
    same_generation = hub.get('as_of') == manifest['generated_at']
    fresh = same_generation and published < generated <= datetime.now(timezone.utc)
    fresh = fresh and all(clock(h['LastModified']) > published for h in list(heads.values()) + [index_head])
    return {'source_phase': phase, 'handler_sha256': hashlib.sha256(actual).hexdigest(),
            'source_files_checked': len(members), 'code_sha256': cfg['CodeSha256'], 'receipt_commit': receipt['commit'],
            'deployed_at': receipt['deployed_at'], 'release_controls': controls,
            'hourly_rule_enabled': True, 'hourly_native_targets': len(native),
            'schedule_control_fingerprint': schedule_fingerprint,
            'output_last_modified': {k: str(v['LastModified']) for k, v in heads.items()},
            'generation_matches': same_generation, 'output_generation': manifest['generated_at'],
            'catalog_providers': len(hub['providers']), 'search_providers': manifest['providers'],
            'search_documents': manifest['documents'], 'index_bytes': index['bytes'],
            'natural_publication_after_candidate_release': phase == 'candidate' and fresh,
            'aws_read_calls': reader.calls, 'signed_package_gets': 1, 'aws_writes': 0, 'producer_invokes': 0,
            'scope': 'Source and control acceptance plus natural publication/schema joins; no full live inventory comparison or measured billing savings.'}


def main():
    require(os.environ.get('GITHUB_ACTIONS') == 'true', 'runner_only')
    import boto3
    from botocore.config import Config
    sys.path.insert(0, str(ROOT / 'aws/ops'))
    from ops_report import report
    reader = Reader()
    def deadline(*_):
        raise Stop('time_bound_reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(120)
    with report(Path(__file__).stem) as out:
        try:
            cfg = Config(connect_timeout=3, read_timeout=8, retries={'total_max_attempts': 1})
            clients = [boto3.client(service, region_name='us-east-1', config=cfg) for service in ('lambda', 's3', 'events')]
            out.kv(**inspect(*clients, reader))
        except Stop as exc:
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason=str(exc))
            raise SystemExit(1) from None
        except Exception:
            out.kv(completed=False, aws_read_calls=reader.calls, stop_reason='unexpected_failure_details_withheld')
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)


if __name__ == '__main__':
    main()
