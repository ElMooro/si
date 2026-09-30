"""Whole invented IAM role/policy responses; complete pagination and no data calls."""
from pathlib import Path
from datetime import datetime, timezone
import copy
import json
import runpy
import urllib.parse
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
S = runpy.run_path(str(ROOT/'aws/ops/staged/ops_6372_snapshot_role_configuration.py'))
A = 'arn:aws:iam::aws:policy/InventedReadPolicy'
B = 'arn:aws:iam::857687956942:policy/InventedSecondPolicy'
C = 'arn:aws:iam::857687956942:policy/InventedBoundary'


def policy():
    return {'Version':'2012-10-17','Statement':[{'Effect':'Allow','Action':['s3:GetBucketPolicy'],
        'Resource':'arn:aws:s3:::invented-bucket', 'Condition':{'StringEquals':{'ResponseMetadata':'retained-policy-context'}}}]}


class Client:
    def __init__(self):self.calls=[];self.change=None
    def __getattr__(self, method):
        assert method in S['METHODS'], 'Unreviewed IAM/data method: '+method
        def call(**request):
            self.calls.append((method,request))
            if method in ('get_role','list_role_policies','get_role_policy','list_attached_role_policies'):
                assert request['RoleName'] == S['ROLE']
            if method == 'get_role':
                value = {'Role':{'RoleName':S['ROLE'],'Arn':S['ARN'],'RoleId':'INVENTEDROLE',
                    'CreateDate':datetime(2020,1,1,tzinfo=timezone.utc),'Path':'/','MaxSessionDuration':3600,
                    'AssumeRolePolicyDocument':{'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':{'Service':'lambda.amazonaws.com'},'Action':'sts:AssumeRole'}]},
                    'PermissionsBoundary':{'PermissionsBoundaryType':'Policy','PermissionsBoundaryArn':C},
                    'RoleLastUsed':{'LastUsedDate':datetime(2026,9,30,12,tzinfo=timezone.utc),'Region':'us-east-1'}}}
            elif method == 'list_role_policies':
                assert request['MaxItems'] == 100
                value = {'PolicyNames':[],'IsTruncated':True,'Marker':'inline-next'} if 'Marker' not in request else {'PolicyNames':['inline-A','inline-B'],'IsTruncated':False}
            elif method == 'get_role_policy':
                assert request['PolicyName'] in ('inline-A','inline-B')
                document = policy()
                if request['PolicyName'] == 'inline-B':document = urllib.parse.quote(json.dumps(document),safe='')
                value = {'RoleName':S['ROLE'],'PolicyName':request['PolicyName'],'PolicyDocument':document}
            elif method == 'list_attached_role_policies':
                assert request['MaxItems'] == 100
                value = {'AttachedPolicies':[{'PolicyArn':A,'PolicyName':'InventedReadPolicy'}], 'IsTruncated':True,'Marker':'managed-next'} if 'Marker' not in request else {'AttachedPolicies':[{'PolicyArn':B,'PolicyName':'InventedSecondPolicy'}],'IsTruncated':False}
            elif method == 'get_policy':
                assert request['PolicyArn'] in (A,B,C)
                value = {'Policy':{'Arn':request['PolicyArn'],'PolicyName':request['PolicyArn'].split('/')[-1],
                    'DefaultVersionId':'v2','IsAttachable':True,'AttachmentCount':1,'PermissionsBoundaryUsageCount':1 if request['PolicyArn']==C else 0,
                    'CreateDate':datetime(2020,1,1,tzinfo=timezone.utc),'UpdateDate':datetime(2026,1,1,tzinfo=timezone.utc)}}
            elif method == 'get_policy_version':
                assert request['PolicyArn'] in (A,B,C) and request['VersionId'] == 'v2'
                value = {'PolicyVersion':{'Document':policy(),'VersionId':'v2','IsDefaultVersion':True,'CreateDate':datetime(2026,1,1,tzinfo=timezone.utc)}}
            value['ResponseMetadata'] = {'HTTPStatusCode':200,'RequestId':'invented-'+str(len(self.calls))}
            if self.change:self.change(method,request,value)
            return copy.deepcopy(value)
        return call


def refuses(fn):
    try:fn()
    except RuntimeError:pass
    else:raise AssertionError('Incomplete or changed IAM configuration accepted')


def test_snapshot_role_captures_every_inline_managed_boundary_and_empty_truncated_page():
    client = Client();out = S['capture'](client)
    assert len(out['inline_pages']) == len(out['attached_pages']) == 2
    assert set(out['inline']) == {'inline-A','inline-B'} and set(out['managed']) == {A,B,C}
    assert out['inline_pages'][0]['PolicyNames'] == [] and out['inline_pages'][0]['IsTruncated'] is True
    assert type(out['inline']['inline-B']['PolicyDocument']) is str
    assert out['managed'][C]['version']['PolicyVersion']['Document'] == policy()
    assert len(client.calls) == 13
    assert all(method in S['METHODS'] for method,_ in client.calls)


def test_snapshot_role_compares_permission_fields_without_discarding_nested_metadata_names():
    client = Client();first = S['capture'](client);second = S['capture'](client)
    second['role']['Role']['RoleLastUsed']['LastUsedDate'] = datetime(2026,9,30,13,tzinfo=timezone.utc)
    assert S['encoded'](first) != S['encoded'](second)
    assert S['encoded'](S['permission_view'](first)) == S['encoded'](S['permission_view'](second))
    second['inline']['inline-A']['PolicyDocument']['Statement'][0]['Condition']['StringEquals']['ResponseMetadata'] = 'changed'
    assert S['encoded'](S['permission_view'](first)) != S['encoded'](S['permission_view'](second))
    assert first['inline']['inline-A']['PolicyDocument']['Statement'][0]['Condition']['StringEquals']['ResponseMetadata'] == 'retained-policy-context'


def test_snapshot_role_refuses_missing_repeated_and_unbounded_pagination():
    for kind in ('missing','repeated','wrong-type'):
        client = Client()
        def change(method,request,value):
            if method != 'list_role_policies':return
            if kind == 'missing':value.pop('Marker',None)
            if kind == 'repeated':value.update(IsTruncated=True,Marker='inline-next')
            if kind == 'wrong-type':value['IsTruncated'] = 0
        client.change = change;refuses(lambda:S['capture'](client))
    with patch.dict(S['pages'].__globals__,{'MAX_PAGES':1}):
        refuses(lambda:S['capture'](Client()))


def test_snapshot_role_refuses_duplicates_wrong_role_and_unbound_policy_versions():
    for kind in ('duplicate-inline','duplicate-managed','role','inline-identity','version','not-default','boundary','foreign-arn'):
        client = Client()
        def change(method,request,value):
            if kind == 'duplicate-inline' and method == 'list_role_policies' and not value['IsTruncated']:value['PolicyNames'] *= 2
            if kind == 'duplicate-managed' and method == 'list_attached_role_policies' and not value['IsTruncated']:value['AttachedPolicies'][0]['PolicyArn'] = A
            if kind == 'role' and method == 'get_role':value['Role']['Arn'] = 'wrong'
            if kind == 'boundary' and method == 'get_role':value['Role']['PermissionsBoundary']['PermissionsBoundaryType'] = 'Unknown'
            if kind == 'inline-identity' and method == 'get_role_policy':value['PolicyName'] = 'other'
            if kind == 'version' and method == 'get_policy_version':value['PolicyVersion']['VersionId'] = 'v1'
            if kind == 'not-default' and method == 'get_policy_version':value['PolicyVersion']['IsDefaultVersion'] = False
            if kind == 'foreign-arn' and method == 'list_attached_role_policies':value['AttachedPolicies'][0]['PolicyArn'] = 'arn:aws:iam::999999999999:policy/Other'
        client.change = change;refuses(lambda:S['capture'](client))


def test_snapshot_role_accepts_decoded_and_urlencoded_documents_and_refuses_ambiguous_json():
    doc = policy();assert S['policy_document'](doc) == S['policy_document'](urllib.parse.quote(json.dumps(doc),safe=''))
    doc['Id'] = 'Keep literal %2F and %20 in already decoded JSON'
    assert S['policy_document'](json.dumps(doc)) == S['policy_document'](urllib.parse.quote(json.dumps(doc),safe='')) == doc
    for value in ('%7B', '%7B%22Statement%22%3A%5B%5D%2C%22Id%22%3A%22%FF%22%7D', '{"Statement":[],"Statement":[]}', '{"Statement":[],"x":NaN}', [], None):
        refuses(lambda:S['policy_document'](value))


def test_snapshot_role_does_not_mistake_capture_for_effective_authorization():
    source = (ROOT/'aws/ops/staged/ops_6372_snapshot_role_configuration.py').read_text(encoding='utf-8')
    assert "boto3.client('iam'" in source and 'effective_permissions_verified=False' in source
    assert 'role_assumptions=0' in source and 'private_reads=0' in source and 'sys.exit(1)' in source
    assert S['METHODS'] == {'get_role','list_role_policies','get_role_policy','list_attached_role_policies','get_policy','get_policy_version'}


if __name__ == '__main__':
    tests = [value for key,value in globals().copy().items() if key.startswith('test_') and callable(value)]
    for test in tests:test()
    print('Snapshot role configuration tests:',len(tests),'passed')
