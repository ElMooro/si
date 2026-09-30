"""Additive bucket-policy repair on whole invented settings; no object access."""
from pathlib import Path
import ast
import copy
import hashlib
import json
import runpy
import tempfile

ROOT = Path(__file__).resolve().parents[2]
PATH = 'aws/ops/staged/ops_6371_private_constitution_policy.py'
S = runpy.run_path(str(ROOT/PATH))


def fixture():
    private = S['anonymous_deny_statement'](S['BUCKET'], S['OWNER'])
    missing = {'arn:aws:s3:::'+S['BUCKET']+'/'+name for name in S['PATHS']}
    private['Resource'] = [name for name in private['Resource'] if name not in missing]
    statements = [{'Sid':'CompleteInventedRule'+str(i),'Effect':'Allow','Principal':'*','Action':['s3:GetObject'],
                   'Resource':['arn:aws:s3:::invented-'+str(i)+'/public/*']} for i in range(30)]
    statements.insert(12, private)
    return {'Id':'Complete invented bucket fixture', 'Version':'2012-10-17', 'Statement':statements}


def refuses(fn, message=None):
    try:fn()
    except S['PolicyUnavailable'] as exc:
        if message:assert message in str(exc),str(exc)
    else:raise AssertionError('Unsafe policy repair accepted')


class Client:
    def __init__(self, value=None, drift_at=None, fail=None):
        self.value = copy.deepcopy(fixture() if value is None else value)
        self.calls = [];self.reads = 0;self.drift_at = drift_at;self.fail = fail
    def get_bucket_policy(self, **request):
        assert request == {'Bucket':S['BUCKET'],'ExpectedBucketOwner':S['OWNER']}
        self.calls.append('get');self.reads += 1
        if self.reads == self.drift_at:self.value['Statement'][0]['Resource'].append('arn:aws:s3:::invented-concurrent/public/*')
        return {'Policy':S['encoded'](self.value).decode(), 'ResponseMetadata':{'HTTPStatusCode':200,'RequestId':'invented-'+str(self.reads),'HTTPHeaders':{'date':'Wed, 30 Sep 2026 14:00:00 GMT'}}}
    def put_bucket_policy(self, **request):
        assert set(request) == {'Bucket','ExpectedBucketOwner','Policy'}
        assert request['Bucket'] == S['BUCKET'] and request['ExpectedBucketOwner'] == S['OWNER']
        self.calls.append('put')
        if self.fail != 'before':self.value = S['document'](request['Policy'])
        if self.fail:raise RuntimeError('Invented transport detail must not escape')
    def __getattr__(self, name):raise AssertionError('Unreviewed data/mutation method: '+name)


def test_constitution_policy_adds_exactly_four_arns_preserving_every_original_field():
    before = fixture();saved = copy.deepcopy(before);target = S['prepare'](before)
    assert before == saved and len(target['Statement']) == 31
    for old,new in zip(before['Statement'],target['Statement']):
        if old['Sid'] != S['SID']:assert old == new
        else:
            assert {k:v for k,v in old.items() if k != 'Resource'} == {k:v for k,v in new.items() if k != 'Resource'}
            assert new['Resource'][:len(old['Resource'])] == old['Resource']
            assert set(new['Resource'][len(old['Resource']):]) == {'arn:aws:s3:::'+S['BUCKET']+'/'+name for name in S['PATHS']}
    assert target['Id'] == before['Id'] and target['Version'] == before['Version']


def test_constitution_policy_complete_apply_is_idempotent_and_retains_whole_readbacks():
    baseline = fixture();client = Client(baseline);retained = []
    result = S['apply'](client, baseline, lambda *row:retained.append(row))
    assert result['policy_changed'] is True and client.calls == ['get','get','put','get','get']
    assert [phase for phase,_ in retained] == ['before','after'] and all(len(rows) == 2 for _,rows in retained)
    assert all('ResponseMetadata' in response for _,rows in retained for response in rows)
    assert result['private_reads'] == result['object_reads'] == result['native_invocations'] == 0
    assert result['actual_object_exposure_tested'] is False and result['bucket_policy_update_atomic_with_other_writers'] is False
    client.calls.clear();again = S['apply'](client, baseline, lambda *row:None)
    assert again['policy_changed'] is False and client.calls == ['get']*4


def test_constitution_policy_resolves_lost_ack_without_blind_retry_or_rollback():
    baseline = fixture();client = Client(baseline, fail='after')
    result = S['apply'](client, baseline, lambda *row:None)
    assert result['private_definition_matches'] and client.calls.count('put') == 1
    client = Client(baseline, fail='before');retained = []
    refuses(lambda:S['apply'](client, baseline, lambda *row:retained.append(row)), 'not confirmed')
    assert client.value == baseline and client.calls.count('put') == 1 and len(retained) == 2


def test_constitution_policy_refuses_unknown_or_concurrent_changes_without_replacing_them():
    baseline = fixture()
    for at in (1, 2):
        client = Client(baseline, drift_at=at)
        refuses(lambda:S['apply'](client, baseline, lambda *row:None))
        assert 'put' not in client.calls
        assert client.value['Statement'][0]['Resource'][-1] == 'arn:aws:s3:::invented-concurrent/public/*'
    client = Client(baseline, drift_at=3)
    refuses(lambda:S['apply'](client, baseline, lambda *row:None), 'not confirmed')
    assert client.calls.count('put') == 1


def test_constitution_policy_refuses_scope_changes_duplicate_fields_and_oversized_policy():
    for change in ('condition','extra_missing','extra_resource','duplicate','count','size'):
        baseline = fixture();private = next(row for row in baseline['Statement'] if row['Sid'] == S['SID'])
        if change == 'condition':private['Condition'] = {}
        if change == 'extra_missing':private['Resource'].pop()
        if change == 'extra_resource':private['Resource'].append('arn:aws:s3:::invented-extra/*')
        if change == 'duplicate':private['Resource'].append(private['Resource'][0])
        if change == 'count':baseline['Statement'].pop()
        if change == 'size':baseline['Id'] = 'x'*20480
        refuses(lambda:S['prepare'](baseline))
    for text in ('{"Statement":[],"Statement":[]}', '{"Version":"2012-10-17","Statement":[],"extra":1e999}', '{}', ' '*20481):
        refuses(lambda:S['document'](text))


def test_constitution_policy_binds_the_complete_control_plane_report_and_fits_real_limit():
    # Approved original control-plane configuration only; never an account packet.
    original = S['original_policy']();target = S['prepare'](original)
    assert len(original['Statement']) == len(target['Statement']) == 31
    assert 19275 < len(S['encoded'](target)) <= 20480
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory);path = root/S['REPORT'];path.parent.mkdir(parents=True)
        path.write_bytes((ROOT/S['REPORT']).read_bytes()+b'\n')
        refuses(lambda:S['original_policy'](root), 'Exact retained')
    assert hashlib.sha256((ROOT/S['REPORT']).read_bytes()).hexdigest() == S['REPORT_SHA']


def test_constitution_policy_has_only_reviewed_bucket_configuration_calls():
    text = (ROOT/PATH).read_text(encoding='utf-8');tree = ast.parse(text)
    methods = {node.func.attr for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name) and node.func.value.id == 'client'}
    assert methods == {'get_bucket_policy','put_bucket_policy'}
    assert 'sys.exit(1)' in text and 'whole_policy_responses_base64' in text


if __name__ == '__main__':
    tests = [value for key,value in globals().copy().items() if key.startswith('test_') and callable(value)]
    for test in tests:test()
    print('Private constitution policy tests:',len(tests),'passed')
