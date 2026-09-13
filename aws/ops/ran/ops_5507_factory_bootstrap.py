#!/usr/bin/env python3
"""Owner-authorized Gear A roles and frozen controls. No endpoints or training."""
import hashlib
import json
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

ACCOUNT = '857687956942'
PUBLIC = 'justhodl-dashboard-live'
PRIVATE = 'justhodl-ai-' + ACCOUNT
REGION = 'us-east-1'
CFG = Config(connect_timeout=5, read_timeout=20, retries={'max_attempts': 2})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def code(exc):
    return getattr(exc, 'response', {}).get('Error', {}).get('Code', type(exc).__name__)


def allow(actions, resources, condition=None):
    stmt = {'Effect': 'Allow', 'Action': actions, 'Resource': resources}
    if condition:
        stmt['Condition'] = condition
    return stmt


def policy(kind):
    pub = 'arn:aws:s3:::' + PUBLIC
    pri = 'arn:aws:s3:::' + PRIVATE
    logs = 'arn:aws:logs:us-east-1:' + ACCOUNT + ':log-group:/aws/lambda/justhodl-' + ('student-rsi' if kind == 'student' else 'factory-grader') + ':*'
    statements = [allow(['logs:CreateLogStream', 'logs:PutLogEvents'], logs),
        {'Effect': 'Deny', 'Action': ['iam:*', 'events:*', 'scheduler:*', 'sagemaker:*', 'bedrock:*', 'ssm:*', 'secretsmanager:*',
          'lambda:Create*', 'lambda:Update*', 'lambda:Delete*', 'lambda:AddPermission', 's3:DeleteObject', 's3:DeleteObjectVersion'], 'Resource': '*'}]
    if kind == 'student':
        read_private = ['factory/control/*', 'factory/runtime/*', 'factory/events/*', 'factory/sources/*', 'factory/experiments/*',
                        'factory/verdicts/*', 'factory/salon/accepted/*', 'factory/salon/results/*', 'factory/quarantine/*',
                        'factory/queue/*', 'factory/traces/*', 'factory/skillbook/*']
        statements += [allow('lambda:InvokeFunction', 'arn:aws:lambda:us-east-1:' + ACCOUNT + ':function:justhodl-factory-grader')]
        immutable = ['factory/events/*', 'factory/sources/*', 'factory/experiments/*', 'factory/queue/*', 'factory/traces/*',
                     'factory/runtime/snapshots/*', 'factory/skillbook/*']
        mutable = ['factory/runtime/current.json', 'factory/runtime/lease.json']
        public_read = ['student-state.json', 'data/student-state.json', 'factory/*', 'data/ofr-funding.json',
                       'data/warm/nyfed/sofr.json.gz', 'data/warm/tv-bars/universe/*']
        public_immutable = ['factory/salon/events/*', 'factory/traces/code/*', 'factory/teachers/*', 'factory/exams/*']
        public_mutable = ['student-state.json', 'data/student-state.json', 'factory/salon/board.json', 'factory/salon/wall.jsonl',
                          'factory/salon/events.jsonl', 'factory/scoreboard.json', 'factory/champions/current.json', 'factory/invites.json']
    else:
        read_private = ['factory/control/*', 'factory/exams/*', 'factory/experiments/*', 'factory/verdicts/*', 'factory/quarantine/*',
                        'factory/salon/accepted/*', 'factory/salon/results/*', 'factory/official-prints/*', 'factory/grader/*']
        immutable = ['factory/verdicts/*', 'factory/salon/results/*']
        mutable = ['factory/grader/daily/*']
        public_read = ['factory/salon/season.json']
        public_immutable, public_mutable = [], []
    statements += [allow('s3:GetObject', [pri + '/' + p for p in read_private]),
                   allow('s3:GetObject', [pub + '/' + p for p in public_read]),
                   allow('s3:ListBucket', pri, {'StringLike': {'s3:prefix': read_private}}),
                   allow('s3:ListBucket', pub, {'StringLike': {'s3:prefix': ['factory/*', 'data/warm/tv-bars/universe/*']}})]
    for bucket, paths in ((pri, immutable), (pub, public_immutable)):
        if paths:
            statements.append(allow('s3:PutObject', [bucket + '/' + p for p in paths], {'StringEquals': {'s3:if-none-match': '*'}}))
    for bucket, paths in ((pri, mutable), (pub, public_mutable)):
        if paths:
            arns = [bucket + '/' + p for p in paths]
            statements += [allow('s3:PutObject', arns, {'StringEquals': {'s3:if-none-match': '*'}}),
                           allow('s3:PutObject', arns, {'Null': {'s3:if-match': 'false'}})]
    return {'Version': '2012-10-17', 'Statement': statements}


def run(rep):
    iam = boto3.client('iam', config=CFG)
    s3 = boto3.client('s3', region_name=REGION, config=CFG)
    logs = boto3.client('logs', region_name=REGION, config=CFG)
    roles = {}
    for kind, fn in [('student', 'justhodl-student-rsi'), ('grader', 'justhodl-factory-grader')]:
        name = fn + '-role'
        try:
            found = iam.get_role(RoleName=name)['Role']
            tags = {t['Key']: t['Value'] for t in found.get('Tags', [])}
            if tags.get('JustHodlComponent') != 'factory-gear-a':
                raise RuntimeError('refusing_to_modify_unmanaged_role:' + name)
        except Exception as exc:
            if code(exc) != 'NoSuchEntity':
                raise
            iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps({'Version': '2012-10-17', 'Statement': [
                {'Effect': 'Allow', 'Principal': {'Service': 'lambda.amazonaws.com'}, 'Action': 'sts:AssumeRole'}]}),
                Description='Bounded factory ' + kind + '; no IAM, scheduling, training, or resource acquisition',
                Tags=[{'Key': 'JustHodlComponent', 'Value': 'factory-gear-a'}, {'Key': 'Owner', 'Value': 'Khalid'}])
        doc = policy(kind)
        iam.put_role_policy(RoleName=name, PolicyName='factory-gear-a', PolicyDocument=json.dumps(doc))
        roles[kind] = {'arn': 'arn:aws:iam::' + ACCOUNT + ':role/' + name, 'policy_sha256': hashlib.sha256(canonical(doc)).hexdigest()}
        try:
            logs.create_log_group(logGroupName='/aws/lambda/' + fn)
        except Exception as exc:
            if code(exc) != 'ResourceAlreadyExistsException':
                raise
        logs.put_retention_policy(logGroupName='/aws/lambda/' + fn, retentionInDays=14)
    created = []
    def seed(bucket, key, doc):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=canonical(doc), ContentType='application/json; charset=utf-8',
                ServerSideEncryption='AES256', CacheControl='max-age=60, must-revalidate' if bucket == PUBLIC else 'private, no-store', IfNoneMatch='*')
            created.append(key)
        except Exception as exc:
            if code(exc) not in ('PreconditionFailed', '412'):
                raise
    now = datetime.now(timezone.utc).isoformat(timespec='seconds')
    seed(PRIVATE, 'factory/control/policy.json', {
        'schema_version': 'factory-control.v1', 'enabled': True, 'gear_b_enabled': False, 'owner': 'Khalid',
        'created_at': now, 'max_experiments_per_day': 1, 'max_guest_traces_per_day': 10, 'max_grades_per_day': 50,
        'sense_interval_seconds': 900, 'max_tick_seconds': 40, 'model_adapter': None,
        'generative_status': 'blocked_no_verified_generative_model', 'gpu_jobs_per_day': 0,
        'automatic_release': False, 'official_print_status': 'awaiting_verified_official_price_adapter',
        'objective': 'Improve independently checked usefulness, reliability and efficiency within owner-set limits'})
    seed(PRIVATE, 'factory/control/invites.json', {'schema_version': 'factory-invites.v1', 'allowlist': [], 'capacity': 10,
         'worldwide_after_completed_seasons': 3, 'note': 'Owner adds verified user IDs; no invitations have been sent'})
    seed(PRIVATE, 'factory/exams/code-identity-v1.json', {'schema_version': 'factory-exam.v1', 'id': 'code-identity-v1',
         'created_at': now, 'seed': secrets.token_hex(32), 'cases': 64, 'holdout': True,
         'purpose': 'Configured role and handler resolution; one candidate per experiment family', 'frozen': True})
    # The first season ends before the Christmas early-close period. Calendar checked against NYSE on 2026-09-13.
    season = {'schema_version': 'factory-season.v1', 'id': 'season-2026-09-14', 'starts_on': '2026-09-14', 'weeks': 13,
      'frozen_at': now, 'timezone': 'America/New_York', 'symbols': ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC'],
      'flat_thresholds': {s: .015 if s == 'BTC' else .003 for s in ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC']},
      'crisis_drawdown_thresholds': {s: .12 if s == 'BTC' else .03 if s == 'TLT' else .05 for s in ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC']},
      'crisis_definition': 'weekly_daily_close_drawdown_proxy; not a systemic crisis diagnosis',
      'regime_definition': 'TREND: efficiency >= 0.6 and return outside flat band; RANGE: efficiency < 0.3 or flat; otherwise TRANSITION',
      'weights': {'direction': .5, 'regime': .3, 'crisis': .2}, 'minimum_independent_weeks_for_promotion': 26,
      'price_definition': 'first session open through last session close; official prints required; unadjusted OHLC; corporate action weeks void',
      'price_sources': {s: 'official-consolidated:' + s if s != 'BTC' else 'coinbase:BTC-USD' for s in ['SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'BTC']},
      'btc_venue': 'COINBASE:BTCUSD', 'btc_window': 'same New York session boundaries; hourly boundary prints required',
      'missing_data': 'pending; never substitute delayed unofficial bars for official grading',
      'extra_closed': [], 'early_closes': {'2026-11-27': '13:00'}, 'calendar_review_required': False,
      'calendar_source': 'https://www.nyse.com/markets/hours-calendars', 'calendar_verified_at': now,
      'elo_k': 16, 'elo_use': 'display_only'}
    season['policy_hash'] = hashlib.sha256(canonical(season)).hexdigest()
    seed(PRIVATE, 'factory/control/season.json', season)
    seed(PUBLIC, 'factory/salon/season.json', season)
    result = {'schema_version': 'factory-bootstrap.v1', 'created_at': now, 'roles': roles, 'seeded_keys': created,
              'student_cannot': ['iam:*', 'events:*', 'scheduler:*', 'sagemaker:*', 'bedrock:*', 'delete', 'read_hidden_exams', 'write_controls'],
              'endpoints_created': 0, 'training_jobs_started': 0, 'schedule_created': False}
    target = Path('aws/ops/reports/5507.json')
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2) + '\n')
    rep.kv(**result)
    rep.ok('Restricted factory roles and frozen controls seeded; all existing Brain assets retained.')


if __name__ == '__main__':
    try:
        with report('5507_factory_bootstrap') as rep:
            run(rep)
    except Exception as exc:
        print('Factory bootstrap failed:', code(exc), str(exc)[:160])
        sys.exit(1)
