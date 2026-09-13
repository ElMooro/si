#!/usr/bin/env python3
"""Finish only this factory's exact admission and public-read permissions."""
import json
import sys
from pathlib import Path
import boto3
from ops_report import report


def main(rep):
    iam = boto3.client('iam')
    name = 'justhodl-student-rsi-role'
    role = iam.get_role(RoleName=name)['Role']
    if {t['Key']: t['Value'] for t in role.get('Tags', [])}.get('JustHodlComponent') != 'factory-gear-a':
        raise RuntimeError('unmanaged_role')
    doc = iam.get_role_policy(RoleName=name, PolicyName='factory-gear-a')['PolicyDocument']
    if isinstance(doc, str):
        from urllib.parse import unquote
        doc = json.loads(unquote(doc))
    pub = 'arn:aws:s3:::justhodl-dashboard-live'
    pri = 'arn:aws:s3:::justhodl-ai-857687956942'
    for statement in doc['Statement']:
        if statement['Effect'] == 'Allow' and statement['Action'] == 's3:PutObject':
            statement['Resource'] = [r for r in statement['Resource'] if r != pub + '/factory/champions/current.json']
        if statement['Effect'] == 'Allow' and statement['Action'] == 's3:ListBucket' and statement['Resource'] == pub:
            values = statement['Condition']['StringLike']['s3:prefix']
            values.extend(p for p in ['student-state.json', 'data/student-state.json'] if p not in values)
    permission = {'Effect': 'Allow', 'Action': 's3:PutObject',
        'Resource': [pri + '/factory/salon/accepted/*-student-*.json'],
        'Condition': {'StringEquals': {'s3:if-none-match': '*'}}}
    if permission not in doc['Statement']:
        doc['Statement'].append(permission)
    iam.put_role_policy(RoleName=name, PolicyName='factory-gear-a', PolicyDocument=json.dumps(doc))
    s3 = boto3.client('s3', region_name='us-east-1')
    for key, value in {
        'factory/champions/current.json': {'schema_version': 'factory-champion.v1', 'model': None, 'status': 'no_model_promoted',
             'release_authority': 'owner', 'student_can_write': False},
        'factory/exams/index.json': {'schema_version': 'factory-exam-index.v1', 'code': {'id': 'code-identity-v1',
             'scope': 'bounded deployment identity repair', 'cases': 64, 'answers': 'private_grader_only'},
             'markets': {'status': 'collecting_prospective_weeks', 'minimum_independent_weeks': 26}}
    }.items():
        try:
            s3.put_object(Bucket='justhodl-dashboard-live', Key=key, Body=json.dumps(value).encode(),
                ContentType='application/json', IfNoneMatch='*', ServerSideEncryption='AES256')
        except Exception as exc:
            if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', '412'):
                raise
    rep.ok('Student can append only its own forecasts; cannot write the champion. Public read prefixes completed.')
    Path('aws/ops/reports/5508.json').write_text(json.dumps({'ok': True, 'role': name,
        'student_forecasts': 'append_only_own_agent', 'student_champion_write': False}) + '\n')


if __name__ == '__main__':
    try:
        with report('5508_factory_admission_permissions') as rep:
            main(rep)
    except Exception as exc:
        print('Factory admission permissions failed:', type(exc).__name__)
        sys.exit(1)
