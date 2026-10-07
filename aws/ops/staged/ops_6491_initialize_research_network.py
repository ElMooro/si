"""One bounded public-research initialization and redundant-trigger repair.

Pins the verified code to an immutable version; invokes it asynchronously once.
A durable conditional claim prevents retries after any ambiguous outcome. Only
the duplicate classic rule is disabled; the identical Scheduler is untouched.
No orders, downstream invokes, private-account reads or configuration updates.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import hashlib
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
FUNCTION = 'justhodl-ticker-360'
ARN = 'arn:aws:lambda:us-east-1:857687956942:function:' + FUNCTION
NAME = FUNCTION + '-schedule'
BUCKET = 'justhodl-dashboard-live'
RELEASE = '6c3998b0eccfff33d334672bb7cfa2df91de440a'
SOURCE_SHA = 'ec31bd20e70375683ba76e664bb32033b29b8e2018217d2b3642bb57fadb885f'
CADENCE = 'cron(20 6,18 * * ? *)'
CLAIM_KEY = 'data/ops/research-network-initialization/' + RELEASE + '.json'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str, allow_nan=False).encode()


def fingerprint(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def controls(events, scheduler, expected_state):
    rule = events.describe_rule(Name=NAME)
    response = events.list_targets_by_rule(Rule=NAME, Limit=100)
    targets = response.get('Targets')
    schedule = scheduler.get_schedule(Name=NAME, GroupName='default')
    if (rule.get('State') != expected_state or rule.get('ScheduleExpression') != CADENCE
            or rule.get('EventPattern') or rule.get('ManagedBy')
            or response.get('NextToken') or not isinstance(targets, list) or len(targets) != 1
            or targets[0].get('Arn') != ARN):
        raise ValueError('classic_control_guard_failed')
    if (schedule.get('State') != 'ENABLED' or schedule.get('ScheduleExpression') != CADENCE
            or schedule.get('ScheduleExpressionTimezone') != 'UTC'
            or schedule.get('FlexibleTimeWindow', {}).get('Mode') != 'OFF'
            or schedule.get('Target', {}).get('Arn') != ARN):
        raise ValueError('scheduler_control_guard_failed')
    # Retain hashes, never target inputs, environment variables or role metadata.
    stable_rule = {key: value for key, value in rule.items() if key not in ('State', 'ResponseMetadata')}
    stable_schedule = {key: value for key, value in schedule.items() if key != 'ResponseMetadata'}
    return {'classic_state': expected_state, 'classic_fingerprint': fingerprint(stable_rule),
            'targets_fingerprint': fingerprint(targets), 'scheduler_fingerprint': fingerprint(stable_schedule),
            'cadence': CADENCE, 'timezone': 'UTC', 'rollback': 'enable only the same guarded classic rule'}


def unchanged(before, after):
    return all(before[key] == after[key] for key in ('classic_fingerprint', 'targets_fingerprint', 'scheduler_fingerprint'))


def release_code(lam, s3, now):
    response = s3.get_object(Bucket=BUCKET, Key='data/ops/releases/' + FUNCTION + '.json')
    stream = response['Body']
    try:
        raw = stream.read(131073)
    finally:
        stream.close()
    if len(raw) > 131072 or response.get('ContentLength') != len(raw):
        raise ValueError('release_receipt_bound_failed')
    receipt = json.loads(raw)
    configuration = lam.get_function_configuration(FunctionName=FUNCTION)
    sha = receipt.get('code_sha256', '')
    modified = datetime.fromisoformat(configuration['LastModified'].replace('Z', '+00:00'))
    if (receipt.get('commit') != RELEASE or receipt.get('function') != FUNCTION
            or receipt.get('verified') is not True or receipt.get('schema') != 'release-receipt.v1'
            or receipt.get('source', {}).get('lambda_function.py', {}).get('sha256') != SOURCE_SHA
            or base64.b64decode(sha, validate=True).hex() != receipt.get('zip_sha256_hex')
            or configuration.get('CodeSha256') != sha
            or configuration.get('FunctionArn') != ARN or configuration.get('Version') != '$LATEST'
            or configuration.get('LastUpdateStatus') != 'Successful' or configuration.get('State') != 'Active'
            or configuration.get('Runtime') != 'python3.12' or configuration.get('MemorySize') != 1024
            or configuration.get('Timeout') != 600 or not configuration.get('RevisionId')
            or modified.tzinfo is None or (now - modified).total_seconds() < 90):
        raise ValueError('deployed_release_guard_failed')
    return sha, configuration['RevisionId']


def execute(lam, s3, events, scheduler, now):
    code_sha, revision = release_code(lam, s3, now)
    before = controls(events, scheduler, 'ENABLED')
    record = {'release': RELEASE, 'function': FUNCTION, 'status': 'claimed',
              'created_at': now.isoformat(), 'before': before, 'code_sha256': code_sha}
    try:
        claimed = s3.put_object(Bucket=BUCKET, Key=CLAIM_KEY, Body=canonical(record),
                               ContentType='application/json', IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in ('PreconditionFailed', '412', 'ConditionalRequestConflict'):
            return {'status': 'already_claimed_no_repeat', 'queued_this_run': False}
        raise RuntimeError('initialization_claim_failed') from None
    etag = claimed.get('ETag')
    if not etag:
        raise RuntimeError('claim_etag_missing_no_mutation_after_claim')

    def save(status, **extra):
        nonlocal etag
        record.update(status=status, **extra)
        result = s3.put_object(Bucket=BUCKET, Key=CLAIM_KEY, Body=canonical(record),
                               ContentType='application/json', IfMatch=etag)
        etag = result['ETag']

    disabled = False
    invoke_attempted = False
    try:
        # AWS atomically rejects a code or configuration race when pinning.
        version = lam.publish_version(FunctionName=FUNCTION, CodeSha256=code_sha, RevisionId=revision,
                                      Description='Bounded research initialization for ' + RELEASE)
        number = version.get('Version', '')
        if not re.fullmatch(r'[1-9][0-9]*', number) or version.get('CodeSha256') != code_sha:
            raise ValueError('immutable_version_guard_failed')
        save('version_pinned', version=number)
        if not unchanged(before, controls(events, scheduler, 'ENABLED')):
            raise ValueError('controls_changed_before_disable')
        disabled = True
        events.disable_rule(Name=NAME)
        after = controls(events, scheduler, 'DISABLED')
        if not unchanged(before, after):
            raise ValueError('controls_changed_after_disable')
        save('invocation_reserved', after=after)
        invoke_attempted = True
        result = lam.invoke(FunctionName=FUNCTION, Qualifier=number, InvocationType='Event',
                            Payload=canonical({'reason': 'initialize_verified_research_network', 'release': RELEASE}))
        if result.get('StatusCode') != 202:
            raise RuntimeError('initialization_acceptance_unknown')
        save('queued', accepted_at=datetime.now(timezone.utc).isoformat())
        return {'status': 'queued', 'queued_this_run': True, 'function': FUNCTION, 'release': RELEASE,
                'version': number, 'classic_state': 'DISABLED', 'scheduler_state': 'ENABLED',
                'cadence': CADENCE, 'claim_key': CLAIM_KEY,
                'scope': 'Explicit initialization; this is not natural-schedule acceptance.'}
    except Exception:
        rollback = 'not_needed'
        if disabled and not invoke_attempted:
            try:
                state = events.describe_rule(Name=NAME).get('State')
                if state not in ('ENABLED', 'DISABLED'):
                    raise ValueError('rollback_state_unknown')
                after = controls(events, scheduler, state)
                if not unchanged(before, after):
                    raise ValueError('rollback_controls_changed')
                if state == 'DISABLED':
                    events.enable_rule(Name=NAME)
                if not unchanged(before, controls(events, scheduler, 'ENABLED')):
                    raise ValueError('rollback_verification_failed')
                rollback = 'restored_prior_classic_state'
            except Exception:
                rollback = 'manual_review_required_controls_changed'
        try:
            save('invocation_outcome_unknown' if invoke_attempted else 'failed_before_invoke', rollback=rollback)
        except Exception:
            pass  # The original durable claim still forbids another invocation.
        raise RuntimeError('initialization_failed_no_retry; ' + rollback) from None


def main():
    import boto3
    from ops_report import report
    with report('ops_6491_initialize_research_network') as output:
        try:
            clients = [boto3.client(name, region_name='us-east-1') for name in ('lambda', 's3', 'events', 'scheduler')]
            result = execute(*clients, datetime.now(timezone.utc))
        except Exception as exc:
            output.fail('Initialization stopped: ' + str(exc) if isinstance(exc, (ValueError, RuntimeError)) else 'Initialization stopped: ' + type(exc).__name__)
            raise RuntimeError('bounded_initialization_stopped') from None
        output.log(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
