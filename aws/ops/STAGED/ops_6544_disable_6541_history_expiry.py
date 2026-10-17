"""Exact authorized retention containment; AWS clients exist only in Actions main().

S3 has no conditional lifecycle PUT. A fresh double-read detects stale input but
is not CAS: the owner must serialize lifecycle writers before this bounded run.
No retry, rollback, object access, purge, or other configuration mutation exists.
"""
import contextlib
import copy
from datetime import datetime, timezone
import io
import json
import logging
import os
from pathlib import Path
import re
import signal
import sys
import time

BUCKET = 'justhodl-dashboard-live'
OWNER = '857687956942'
RULE_ID = 'ops6541-bound-noncurrent-versions'
REPORT = 'ops_6544_disable_6541_history_expiry'
RULE_BODY = {'ID': RULE_ID, 'Filter': {'Prefix': ''},
             'NoncurrentVersionExpiration': {'NoncurrentDays': 1, 'NewerNoncurrentVersions': 10}}
MAX_READS = 12


class Stop(Exception):
    def __init__(self, code):
        self.code = code


def canonical(value):
    def encode(v):
        if type(v) is datetime:
            return v.isoformat()
        raise Stop('UNREPRESENTABLE_CONFIGURATION')
    raw = json.dumps(value, default=encode, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if len(raw.encode('utf-8')) > 2_000_000:
        raise Stop('CONFIGURATION_LIMIT')
    return raw


def snapshot(raw):
    if type(raw) is not dict or set(raw) - {'Rules', 'TransitionDefaultMinimumObjectSize', 'ResponseMetadata'}:
        raise Stop('UNKNOWN_CONFIGURATION_FIELD')
    value = copy.deepcopy({k: v for k, v in raw.items() if k != 'ResponseMetadata'})
    rules = value.get('Rules')
    if type(rules) is not list or not 1 <= len(rules) <= 1000 or any(type(r) is not dict for r in rules):
        raise Stop('MALFORMED_CONFIGURATION')
    canonical(value)
    matches = [r for r in rules if r.get('ID') == RULE_ID]
    if len(matches) != 1:
        raise Stop('TARGET_NOT_UNIQUE_OR_MISSING')
    rule = matches[0]
    if {k: v for k, v in rule.items() if k != 'Status'} != RULE_BODY or rule.get('Status') not in {'Enabled', 'Disabled'}:
        raise Stop('TARGET_SHAPE_CHANGED')
    return value


def target(value):
    return next(r for r in value['Rules'] if r.get('ID') == RULE_ID)


def safe_text(value):
    if type(value) is str and len(value) <= 256 and re.fullmatch(r'[A-Za-z0-9_./* -]*', value):
        return value
    return 'WITHHELD'


def scope_rows(value):
    """Only requested retention configuration, never object inventories or inputs."""
    rows = []
    for rule in value['Rules']:
        if rule.get('ID') == RULE_ID or rule.get('Status') != 'Enabled':
            continue
        actions = {k: rule[k] for k in ('Expiration', 'NoncurrentVersionExpiration') if k in rule}
        if not actions:
            continue
        filt = rule.get('Filter', {})
        if not isinstance(filt, dict):
            raise Stop('MALFORMED_FILTER')
        prefix = rule.get('Prefix', filt.get('Prefix', (filt.get('And') or {}).get('Prefix', '')))
        row = {'id': safe_text(rule.get('ID')), 'prefix': safe_text(prefix),
               'tag_or_size_qualified': bool(set(filt) - {'Prefix'}), 'expiration': {}}
        for name, action in actions.items():
            row['expiration'][name] = {k: v.isoformat() if type(v) is datetime else v
                for k, v in action.items() if k in {'Days', 'Date', 'ExpiredObjectDeleteMarker',
                                                   'NoncurrentDays', 'NewerNoncurrentVersions'}
                and type(v) in (int, bool, datetime)}
        rows.append(row)
    return rows


def contain(s3, *, verify_only=False, validate=lambda _: None, sleep=time.sleep, clock=time.monotonic):
    result = {'schema': 'retention-containment.v1', 'op': 6544, 'verified': False,
              'verify_only': verify_only, 'put_attempts': 0, 'reads': 0,
              'status': 'INCOMPLETE', 'failure_code': None}
    deadline = clock() + 150

    def read():
        if result['reads'] >= MAX_READS or clock() > deadline:
            raise Stop('READ_LIMIT_OR_DEADLINE')
        result['reads'] += 1
        return snapshot(s3.get_bucket_lifecycle_configuration(Bucket=BUCKET, ExpectedBucketOwner=OWNER))

    try:
        before = read()
        result['target_before'] = copy.deepcopy(target(before))
        desired = copy.deepcopy(before)
        target(desired)['Status'] = 'Disabled'
        if verify_only and before != desired:
            raise Stop('VERIFY_TARGET_STILL_ENABLED')
        args = {'Bucket': BUCKET, 'ExpectedBucketOwner': OWNER,
                'LifecycleConfiguration': {'Rules': desired['Rules']}}
        if 'TransitionDefaultMinimumObjectSize' in desired:
            args['TransitionDefaultMinimumObjectSize'] = desired['TransitionDefaultMinimumObjectSize']
        elif before != desired:
            raise Stop('TRANSITION_DEFAULT_UNVERIFIED')
        validate(args)
        sleep(1)
        if read() != before:
            raise Stop('CONCURRENT_CONFIGURATION_CHANGE')
        if before != desired:
            result['put_attempts'] = 1
            try:
                s3.put_bucket_lifecycle_configuration(**copy.deepcopy(args))
            except BaseException:
                # May have applied despite a lost response. Never retry a PUT.
                result['put_outcome'] = 'AMBIGUOUS_RESPONSE'
        consecutive = 0
        while result['reads'] < MAX_READS:
            after = read()
            if after == desired:
                consecutive += 1
                if consecutive == 2:
                    result.update(verified=True, status='VERIFIED_DISABLED',
                                  target_after=copy.deepcopy(target(after)),
                                  other_rules_and_fields_unchanged=True,
                                  transition_default_before=before.get('TransitionDefaultMinimumObjectSize'),
                                  transition_default_after=after.get('TransitionDefaultMinimumObjectSize'),
                                  remaining_enabled_expirations=scope_rows(after))
                    return result
            else:
                consecutive = 0
                if after != before:
                    raise Stop('UNEXPECTED_POSTWRITE_CONFIGURATION')
            sleep(3)
        raise Stop('READBACK_NOT_CONFIRMED')
    except Stop as exc:
        result['failure_code'] = exc.code
    except BaseException:
        result['failure_code'] = 'REQUEST_OR_CONFIGURATION_FAILED'
    return result


class Discard(io.TextIOBase):
    def write(self, value):
        return len(value)


def main(verify_only=False):
    result = {'schema': 'retention-containment.v1', 'op': 6544,
              'status': 'INCOMPLETE', 'verified': False, 'failure_code': 'RUNNER_REQUIRED'}
    previous = logging.root.manager.disable
    try:
        if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('GITHUB_REPOSITORY') != 'ElMooro/si':
            raise Stop('RUNNER_REQUIRED')
        logging.disable(sys.maxsize)
        with contextlib.redirect_stdout(Discard()), contextlib.redirect_stderr(Discard()):
            import boto3
            from botocore.config import Config
            from botocore.validate import validate_parameters
            def expired(signum, frame):
                raise Stop('HARD_DEADLINE')
            signal.signal(signal.SIGALRM, expired)
            signal.alarm(180)
            s3 = boto3.client('s3', region_name='us-east-1',
                config=Config(connect_timeout=3, read_timeout=7, retries={'total_max_attempts': 1}))
            shape = s3.meta.service_model.operation_model('PutBucketLifecycleConfiguration').input_shape
            result = contain(s3, verify_only=verify_only,
                             validate=lambda args: validate_parameters(args, shape))
    except Stop as exc:
        result['failure_code'] = exc.code
    except BaseException:
        result['failure_code'] = 'RUNNER_REQUEST_FAILED'
    finally:
        signal.alarm(0)
        logging.disable(previous)
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    sha = os.environ.get('GITHUB_SHA', '')
    result['source_sha'] = sha if re.fullmatch('[0-9a-f]{40}', sha) else 'UNVERIFIED'
    public = json.dumps(result, sort_keys=True, indent=2)
    print(public)
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        name = REPORT + ('_readback' if verify_only else '')
        path = Path(__file__).resolve().parents[1] / 'reports' / 'latest' / (name + '.md')
        path.write_text('# Exact history-expiry containment\n\n```json\n' + public + '\n```\n', encoding='utf-8')
    return 0 if result['verified'] else 1


if __name__ == '__main__':
    sys.exit(main())
