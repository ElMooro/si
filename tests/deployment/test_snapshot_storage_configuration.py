"""Whole invented SDK settings; configuration-only read boundaries and retention."""
from pathlib import Path
from datetime import date, datetime, timezone
import ast
import copy
import hashlib
import json
import math
import runpy
import sys

ROOT = Path(__file__).resolve().parents[2]
PATH = 'aws/ops/staged/ops_6370_snapshot_storage_configuration.py'
S = runpy.run_path(str(ROOT/PATH))


class Missing(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code, 'Message': 'Invented error text must not be retained'}}


def fixture():
    def wrap(**value):return {**value, 'ResponseMetadata': {'HTTPStatusCode': 200, 'RequestId': 'invented-1', 'HTTPHeaders': {'date': 'Wed, 30 Sep 2026 12:00:00 GMT'}, 'RetryAttempts': 0}}
    policy = '{\n "Version": "2012-10-17", "Statement": [{"Sid":"CompleteInventedPolicy", "Effect":"Deny", "Principal":"*", "Action":["s3:GetObject"], "Resource":"arn:aws:s3:::invented-bucket/private/*"}]\n}\n'
    return {
        'get_bucket_policy': wrap(Policy=policy),
        'get_bucket_versioning': wrap(Status='Enabled', MFADelete='Disabled'),
        'get_public_access_block': wrap(PublicAccessBlockConfiguration={'BlockPublicAcls': True, 'IgnorePublicAcls': True, 'BlockPublicPolicy': False, 'RestrictPublicBuckets': False}),
        'get_bucket_replication': wrap(ReplicationConfiguration={'Role': 'arn:aws:iam::111122223333:role/invented', 'Rules': [{'ID':'complete-invented-rule','Status':'Disabled','Priority':1,'Filter':{'Prefix':'invented/'},'Destination':{'Bucket':'arn:aws:s3:::invented-target'}}]}),
        'get_bucket_ownership_controls': wrap(OwnershipControls={'Rules':[{'ObjectOwnership':'BucketOwnerEnforced'}]}),
        'get_bucket_lifecycle_configuration': wrap(Rules=[{'ID':'complete-invented-expiry','Status':'Disabled','Filter':{'Prefix':'invented/'},'Expiration':{'Date':datetime(2030,1,1,tzinfo=timezone.utc)}}]),
        'get_bucket_encryption': wrap(ServerSideEncryptionConfiguration={'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'},'BucketKeyEnabled':False}]}),
    }


class Client:
    def __init__(self, values=None):self.values = fixture() if values is None else values;self.calls = []
    def __getattr__(self, name):
        assert name in S['METHODS'], 'Unreviewed data or mutation call: '+name
        def call(**request):
            assert request == {'Bucket': S['BUCKET'], 'ExpectedBucketOwner': S['ACCOUNT']}
            self.calls.append((name, request))
            value = self.values[name]
            if isinstance(value, Exception):raise value
            return copy.deepcopy(value)
        return call


def refuses(fn, message=None):
    try:fn()
    except S['ConfigurationUnavailable'] as exc:
        if message:assert message in str(exc)
        assert 'Invented error text' not in str(exc)
    else:raise AssertionError('Unsupported configuration was accepted')


def decode(value):
    kind = value[0]
    if kind == 'object':return {key:decode(child) for key,child in value[1]}
    if kind == 'array':return [decode(child) for child in value[1]]
    if kind == 'datetime':return datetime.fromisoformat(value[1])
    if kind == 'date':return date.fromisoformat(value[1])
    if kind == 'integer':return int(value[1])
    if kind == 'float':return float.fromhex(value[1])
    if kind == 'null':return None
    if kind in ('string', 'boolean'):return value[1]
    raise AssertionError('Unknown retained SDK value kind')


def test_snapshot_storage_reads_only_all_seven_complete_configuration_responses_twice():
    client = Client();first = S['collect'](client);second = S['collect'](client)
    result = S['assess'](first, second)
    assert [name for name,_ in client.calls] == list(S['METHODS'])*2
    assert result['configuration_stable_across_two_reads'] is True
    assert result['policy_statement_count'] == 1
    assert result['conditional_write_enforcement_verified'] is False
    assert result['object_reads'] == result['object_head_requests'] == result['object_lists'] == result['private_reads'] == result['native_invocations'] == result['policy_changes'] == result['schedule_changes'] == 0
    for method, original in client.values.items():
        assert first[method]['response'] == original
    policy = client.values['get_bucket_policy']['Policy'].encode()
    assert result['policy_bytes'] == len(policy) and result['policy_sha256'] == hashlib.sha256(policy).hexdigest()


def test_snapshot_storage_retains_whole_typed_sdk_values_and_unmodified_policy_text():
    client = Client();captured = S['collect'](client)
    captured['invented-extra'] = {'whole': [None, True, 1, 2**70, -0.0, 0.123456789123, 'é\n', date(2030,1,1), ['object', []]]}
    raw = S['encoded'](captured);decoded = decode(json.loads(raw))
    assert decoded == captured
    assert type(decoded['invented-extra']['whole'][1]) is bool
    assert type(decoded['invented-extra']['whole'][2]) is int
    assert math.copysign(1, decoded['invented-extra']['whole'][4]) == -1
    assert decoded['get_bucket_policy']['response']['Policy'] == client.values['get_bucket_policy']['Policy']
    assert 'ResponseMetadata' in decoded['get_bucket_policy']['response']


def test_snapshot_storage_transport_changes_are_retained_and_only_metadata_ignored_for_stability():
    first = S['collect'](Client());second = copy.deepcopy(first)
    for row in second.values():
        row['response']['ResponseMetadata']['RequestId'] = 'invented-2'
        row['response'] = dict(reversed(list(row['response'].items())))
    assert S['encoded'](first) != S['encoded'](second)
    assert S['assess'](first,second)['configuration_stable_across_two_reads']
    second['get_bucket_versioning']['response']['Status'] = 'Suspended'
    refuses(lambda:S['assess'](first,second), 'changed during capture')


def test_snapshot_storage_optional_absence_is_explicit_but_access_denial_is_not_absence():
    for method, absent in S['METHODS'].items():
        if absent is None:continue
        values = fixture();values[method] = Missing(absent)
        captured = S['collect'](Client(values))
        assert captured[method] == {'status':'absent','error_code':absent}
        assert S['assess'](captured,captured)['configuration_stable_across_two_reads']
        values[method] = Missing('AccessDenied')
        refuses(lambda:S['collect'](Client(values)), 'unavailable')
    values = fixture();values['get_bucket_policy'] = Missing('NoSuchBucketPolicy')
    refuses(lambda:S['collect'](Client(values)), 'unavailable')


def test_snapshot_storage_rejects_partial_inventory_invalid_sdk_types_and_policy_json():
    first = S['collect'](Client());second = copy.deepcopy(first);second.pop('get_bucket_encryption')
    refuses(lambda:S['assess'](first,second), 'inventory')
    for policy in ('', '{"Statement": [', '{"Statement":[],"Statement":[]}', '{"Statement":[true]}', '{"Statement":[],"x":1e999}', ' '*20481):
        values = fixture();values['get_bucket_policy']['Policy'] = policy
        captured = S['collect'](Client(values));refuses(lambda:S['assess'](captured,captured))
    for bad in (b'unsupported bytes', float('nan'), float('inf'), datetime(2030,1,1), {1:'nonstring key'}):
        refuses(lambda:S['encoded'](bad))
    for status in (206, True, '200', None):
        values = fixture();values['get_bucket_policy']['ResponseMetadata']['HTTPStatusCode'] = status
        refuses(lambda:S['collect'](Client(values)), 'Successful complete')
    refuses(lambda:S['encoded']({'whole':'x'*S['LIMIT']}), 'retention bound')


def test_snapshot_storage_current_operation_has_no_object_access_or_mutation_code():
    text = (ROOT/PATH).read_text(encoding='utf-8');tree = ast.parse(text)
    forbidden = {'get_object','head_object','list_objects','list_objects_v2','list_object_versions','put_object','delete_object','put_bucket_policy','delete_bucket_policy','invoke','put_rule','update_schedule'}
    assert not {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)} & forbidden
    assert 'sys.exit(1)' in text and "boto3.client('s3'" in text
    assert 'result.kv(complete_sdk_configuration_base64=' in text
    assert "findings(raw.decode('utf-8'),retired)" in text


def test_snapshot_storage_runner_report_retains_delimiters_as_verified_whole_bytes():
    import base64,contextlib,io,os,tempfile,types
    from unittest.mock import patch
    values=fixture();values['get_bucket_policy']['ResponseMetadata']['HTTPHeaders']['invented-delimiters']='a|b\nsecond line'
    client=Client(values)
    with tempfile.TemporaryDirectory() as folder:
        root=Path(folder);retired=root/'tests/security/retired-secret-sha256.json'
        retired.parent.mkdir(parents=True);retired.write_text('{"sha256":[]}',encoding='utf-8')
        fake=types.SimpleNamespace(client=lambda *args,**kwargs:client)
        with patch.dict(sys.modules,{'boto3':fake,'botocore.config':types.SimpleNamespace(Config=lambda **kwargs:kwargs)}),patch.dict(S['main'].__globals__,{'ROOT':root}),patch.dict(os.environ,{'GITHUB_WORKSPACE':str(root),'GITHUB_STEP_SUMMARY':''}),contextlib.redirect_stdout(io.StringIO()):
            S['main']()
        report=(root/'aws/ops/reports/latest/ops_6370_snapshot_storage_configuration.md').read_text(encoding='utf-8')
        rows=[line.strip('|').split('|') for line in report.splitlines() if line.startswith('|')]
        headers=[cell.strip() for cell in rows[0]]
        assert all(len(row)==len(headers) for row in rows)
        values={key:cell.strip() for row in rows[2:] for key,cell in zip(headers,row) if cell.strip()}
        raw=base64.b64decode(values['complete_sdk_configuration_base64'],validate=True)
        assert len(raw)==int(values['complete_sdk_configuration_bytes'])
        assert hashlib.sha256(raw).hexdigest()==values['complete_sdk_configuration_sha256']
        captured=decode(json.loads(raw))
        assert captured['contract']=='snapshot-storage-configuration.v1' and len(captured['observations'])==2
        assert captured['observations'][0]['get_bucket_policy']['response']['ResponseMetadata']['HTTPHeaders']['invented-delimiters']=='a|b\nsecond line'
        assert len(client.calls)==14 and '**Status:** success' in report


if __name__ == '__main__':
    tests = [v for k,v in globals().copy().items() if k.startswith('test_') and callable(v)]
    for test in tests:test()
    print('Snapshot storage configuration tests:', len(tests), 'passed')
