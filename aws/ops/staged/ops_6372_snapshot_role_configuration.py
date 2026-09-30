"""Capture the accepted snapshot execution role's complete current IAM policies.

Configuration reads only. No object access, credentials, role assumption,
producer invocation, permission changes or schedule changes. Role association
comes from the prior accepted native configuration, not a new runtime invoke.
"""
from pathlib import Path
from datetime import datetime, timezone
import base64
import hashlib
import json
import re
import sys
import urllib.parse

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/staged'), str(ROOT/'scripts')]
from ops_6370_snapshot_storage_configuration import encoded

ROLE = 'lambda-execution-role'
ARN = 'arn:aws:iam::857687956942:role/'+ROLE
MAX_PAGES = 100
MAX_POLICIES = 1000
METHODS = {'get_role', 'list_role_policies', 'get_role_policy',
           'list_attached_role_policies', 'get_policy', 'get_policy_version'}


class RoleUnavailable(RuntimeError):
    pass


def call(client, method, **request):
    if method not in METHODS:
        raise RoleUnavailable('Unreviewed IAM configuration method')
    try:
        result = getattr(client, method)(**request)
    except Exception:
        raise RoleUnavailable('Required IAM configuration unavailable: '+method) from None
    if (type(result) is not dict or type(result.get('ResponseMetadata')) is not dict or
            type(result['ResponseMetadata'].get('HTTPStatusCode')) is not int or result['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise RoleUnavailable('Complete successful IAM response required')
    encoded(result)
    return result


def pages(client, method, field):
    retained, items, markers = [], [], set()
    marker = None
    for _ in range(MAX_PAGES):
        request = {'RoleName':ROLE, 'MaxItems':100}
        if marker is not None:request['Marker'] = marker
        row = call(client, method, **request)
        retained.append(row)
        if type(row.get(field)) is not list or type(row.get('IsTruncated')) is not bool:
            raise RoleUnavailable('Complete IAM policy page required')
        items.extend(row[field])
        if len(items) > MAX_POLICIES:
            raise RoleUnavailable('Complete IAM policy inventory exceeds bound')
        if not row['IsTruncated']:
            return retained, items
        marker = row.get('Marker')
        if type(marker) is not str or not marker or marker in markers or len(marker) > 1024:
            raise RoleUnavailable('IAM pagination marker missing or repeated')
        markers.add(marker)
    raise RoleUnavailable('Complete IAM pagination exceeds bound')


def policy_document(value):
    if type(value) is str:
        def pairs(rows):
            out = {}
            for key, child in rows:
                if key in out:raise RoleUnavailable('Duplicate IAM policy field')
                out[key] = child
            return out
        def constant(_):raise RoleUnavailable('Nonfinite IAM policy field')
        try:
            # SDKs may already decode the wire document. Do not URL-decode a
            # raw JSON string again and change literal policy percent sequences.
            text = value if value.lstrip().startswith('{') else urllib.parse.unquote(value, errors='strict')
            value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
        except (ValueError, UnicodeError):raise RoleUnavailable('Complete IAM policy document unavailable') from None
    if type(value) is not dict or type(value.get('Statement')) not in (dict, list):
        raise RoleUnavailable('Complete IAM policy statement inventory required')
    encoded(value)
    return value


def capture(client):
    role = call(client, 'get_role', RoleName=ROLE)
    detail = role.get('Role')
    if type(detail) is not dict or detail.get('Arn') != ARN or detail.get('RoleName') != ROLE:
        raise RoleUnavailable('Exact accepted execution role required')
    policy_document(detail.get('AssumeRolePolicyDocument'))
    inline_pages, names = pages(client, 'list_role_policies', 'PolicyNames')
    if any(type(name) is not str or not re.fullmatch(r'[A-Za-z0-9_+=,.@-]{1,128}', name) for name in names) or len(set(names)) != len(names):
        raise RoleUnavailable('Unique complete inline policy names required')
    inline = {}
    for name in sorted(names):
        row = call(client, 'get_role_policy', RoleName=ROLE, PolicyName=name)
        if row.get('RoleName') != ROLE or row.get('PolicyName') != name:
            raise RoleUnavailable('Inline policy identity differs')
        policy_document(row.get('PolicyDocument'))
        inline[name] = row
    attached_pages, attachments = pages(client, 'list_attached_role_policies', 'AttachedPolicies')
    arns = []
    for item in attachments:
        if type(item) is not dict or type(item.get('PolicyArn')) is not str or type(item.get('PolicyName')) is not str:
            raise RoleUnavailable('Complete managed policy attachment required')
        arns.append(item['PolicyArn'])
    if len(set(arns)) != len(arns):
        raise RoleUnavailable('Duplicate managed policy attachment')
    boundary = detail.get('PermissionsBoundary')
    if boundary is not None:
        if type(boundary) is not dict or boundary.get('PermissionsBoundaryType') != 'Policy' or type(boundary.get('PermissionsBoundaryArn')) is not str:
            raise RoleUnavailable('Complete permissions boundary identity required')
        arns.append(boundary['PermissionsBoundaryArn'])
    managed = {}
    for arn in sorted(set(arns)):
        if not re.fullmatch(r'arn:aws:iam::(?:aws|857687956942):policy/[^\s]{1,1024}', arn):
            raise RoleUnavailable('Reviewed attached policy ARN required')
        policy = call(client, 'get_policy', PolicyArn=arn)
        info = policy.get('Policy')
        if type(info) is not dict or info.get('Arn') != arn or type(info.get('DefaultVersionId')) is not str or not re.fullmatch(r'v[1-9][0-9]*', info['DefaultVersionId']):
            raise RoleUnavailable('Complete managed policy version identity required')
        version = call(client, 'get_policy_version', PolicyArn=arn, VersionId=info['DefaultVersionId'])
        body = version.get('PolicyVersion')
        if type(body) is not dict or body.get('VersionId') != info['DefaultVersionId'] or body.get('IsDefaultVersion') is not True:
            raise RoleUnavailable('Current managed policy version differs')
        policy_document(body.get('Document'))
        managed[arn] = {'policy':policy, 'version':version}
    out = {'role':role, 'inline_pages':inline_pages, 'inline':inline,
           'attached_pages':attached_pages, 'managed':managed}
    encoded(out)
    return out


def permission_view(value):
    """Compare all retained permission records, allowing only transport/last-use changes."""
    def response(row):return {key:child for key,child in row.items() if key != 'ResponseMetadata'}
    role = response(value['role'])
    role['Role'] = {key:child for key,child in role['Role'].items() if key != 'RoleLastUsed'}
    result = {'role':role, 'inline_pages':[response(row) for row in value['inline_pages']],
              'inline':{name:response(row) for name,row in value['inline'].items()},
              'attached_pages':[response(row) for row in value['attached_pages']],
              'managed':{arn:{kind:response(row) for kind,row in records.items()} for arn,records in value['managed'].items()}}
    def clean(item):
        if type(item) is dict:
            return {key:clean(item[key]) for key in sorted(item)}
        if type(item) is list:return [clean(child) for child in item]
        return item
    return clean(result)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    from check_secrets import findings
    with report('ops_6372_snapshot_role_configuration') as result:
        client = boto3.client('iam', config=Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts':2}))
        first, second = capture(client), capture(client)
        record = {'contract':'snapshot-role-configuration.v1', 'captured_at':datetime.now(timezone.utc).isoformat(),
                  'accepted_role_arn':ARN, 'observations':[first, second]}
        raw = encoded(record)
        retired = set(json.loads((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())['sha256'])
        if findings(raw.decode('utf-8'), retired):
            raise RoleUnavailable('Credential pattern prevents role configuration retention')
        result.kv(complete_role_configuration_base64=base64.b64encode(raw).decode('ascii'),
                  complete_role_configuration_bytes=len(raw), complete_role_configuration_sha256=hashlib.sha256(raw).hexdigest())
        if encoded(permission_view(first)) != encoded(permission_view(second)):
            raise RoleUnavailable('Role permission configuration changed during capture')
        result.kv(role_arn=ARN, inline_policies=len(first['inline']), managed_and_boundary_policies=len(first['managed']),
                  configuration_stable_across_two_reads=True, effective_permissions_verified=False,
                  object_reads=0, private_reads=0, native_invocations=0, role_assumptions=0,
                  permission_changes=0, schedule_changes=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
