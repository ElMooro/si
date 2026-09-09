"""Execute real caller functions without importing boto3 or contacting AWS."""
import ast
import copy
import io
import json
import re
import runpy
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
HELPER = runpy.run_path(str(ROOT / 'aws/shared/governed_targets.py'))
governed_target = HELPER['governed_target']
function_identity = HELPER['function_identity']
EXPECTED = {
    'justhodl-backtest-engine', 'justhodl-calibration-snapshotter',
    'justhodl-engine-fusion', 'justhodl-katlin', 'justhodl-khalid', 'justhodl-khalid-risk',
    'justhodl-fleet-freshness-monitor',
    'justhodl-portfolio-snapshot', 'justhodl-research-backtest',
    'justhodl-public-archive-index',
    'justhodl-risk-gate', 'justhodl-risk-sizer',
}
ARN = 'arn:aws:lambda:us-east-1:123456789012:function:'


def extracted(engine, names, extra=None):
    path = ROOT / 'aws/lambdas' / ('justhodl-' + engine) / 'source/lambda_function.py'
    tree = ast.parse(path.read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in nodes} == set(names), (engine, names)
    scope = {'governed_target': governed_target, 'function_identity': function_identity,
             'json': json, 'datetime': datetime, 'timezone': timezone, 'time': time, 're': re}
    scope.update(extra or {})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), scope)
    return scope


class LambdaRecorder:
    class exceptions:
        class ResourceConflictException(Exception): pass
    def __init__(self): self.calls, self.permissions = [], []
    def invoke(self, **kwargs): self.calls.append(kwargs); return {'StatusCode': 202}
    def add_permission(self, **kwargs): self.permissions.append(kwargs)


class EventRecorder:
    def __init__(self, targets): self.targets, self.puts = copy.deepcopy(targets), []
    def list_targets_by_rule(self, Rule): return {'Targets': copy.deepcopy(self.targets)}
    def put_targets(self, Rule, Targets):
        self.puts.append({'Rule': Rule, 'Targets': copy.deepcopy(Targets)})
        # EventBridge upserts matching IDs; unrelated targets remain bound.
        replacements = {row['Id']: row for row in Targets}
        self.targets = [copy.deepcopy(replacements.pop(row['Id'], row)) for row in self.targets]
        self.targets.extend(copy.deepcopy(list(replacements.values())))
        return {'FailedEntryCount': 0}
    def get_paginator(self, name):
        assert name == 'list_rules'
        return SimpleNamespace(paginate=lambda: iter([{'Rules': [
            {'Name':'hourly','State':'ENABLED','ScheduleExpression':'rate(1 hour)'}]}]))


def test_governed_allowlist_has_exact_twelve_release_functions():
    assert HELPER['GOVERNED_FUNCTIONS'] == EXPECTED


def test_all_governed_bare_names_and_arns_route_live():
    for name in EXPECTED:
        for target in (name, ARN + name):
            assert governed_target(target) == target + ':live'
            assert governed_target(target + ':$LATEST') == target + ':live'


def test_explicit_aliases_and_versions_preserved_and_ungoverned_unchanged():
    for name in EXPECTED:
        for target in (name, ARN + name):
            for qualifier in ('live', 'candidate', 'production', '42'):
                assert governed_target(target + ':' + qualifier) == target + ':' + qualifier
    for name in ('justhodl-other', 'other', 'justhodl-risk-gate-copy'):
        for target in (name, ARN + name, name + ':$LATEST', ARN + name + ':9'):
            assert governed_target(target) == target


def test_scheduler_actual_invoke_function_routes_every_governed_function():
    lam = LambdaRecorder(); scope = extracted('scheduler', ['_invoke_one'], {'lam':lam})
    for name in EXPECTED:
        result = scope['_invoke_one'](name)
        assert result == (name, 202, None)
        assert lam.calls[-1]['FunctionName'] == name + ':live'
        assert lam.calls[-1]['InvocationType'] == 'Event'


def test_event_coordinator_actual_invoke_preserves_event_and_routes_arn():
    lam=LambdaRecorder(); scope=extracted('event-coordinator', ['invoke_target'], {'lam':lam})
    for name in EXPECTED:
        result=scope['invoke_target'](ARN+name, 'regime.changed', {'from':'prior','to':'new'})
        assert result == {'ok':True,'status':202}
        assert lam.calls[-1]['FunctionName'] == ARN+name+':live'
        payload=json.loads(lam.calls[-1]['Payload'])
        assert payload['trigger_event']=='regime.changed'
        assert payload['trigger_detail']=={'from':'prior','to':'new'}


def test_backend_actual_restart_routes_governed_names_and_keeps_inventory_guard():
    lam=LambdaRecorder()
    scope=extracted('backend-agent', ['_cap_restart_engine'],
                    {'lam':lam,'_get':lambda key:{'functions':dict.fromkeys(EXPECTED,{})}})
    for name in EXPECTED:
        assert scope['_cap_restart_engine'](name)['ok']
        assert lam.calls[-1]['FunctionName']==name+':live'
    count=len(lam.calls)
    assert not scope['_cap_restart_engine']('justhodl-nonexistent')['ok']
    assert len(lam.calls)==count


def test_feed_identity_strips_all_qualifiers_and_arn_before_override_lookup():
    scope=extracted('schedule-liveness', ['feed_for'],
                    {'FEED_OVERRIDE':{'justhodl-stock-screener':'screener/data.json'}})
    for prefix in ('',ARN):
        for suffix in ('',':live',':7',':$LATEST'):
            assert scope['feed_for'](prefix+'justhodl-khalid-risk'+suffix)=='data/khalid-risk.json'
            assert scope['feed_for'](prefix+'justhodl-stock-screener'+suffix)=='screener/data.json'


def test_liveness_rebuild_preserves_unrelated_targets_payload_retries_dlq():
    original=[{'Id':'risk','Arn':ARN+'justhodl-khalid-risk','Input':'{"mode":"refresh"}',
               'RetryPolicy':{'MaximumRetryAttempts':4},'DeadLetterConfig':{'Arn':'arn:aws:sqs:us-east-1:123:dlq'},
               'RoleArn':'role','InputTransformer':{'InputPathsMap':{'id':'$.id'},'InputTemplate':'<id>'}},
              {'Id':'pinned','Arn':ARN+'justhodl-khalid-risk:17','Input':'{"pinned":true}'},
              {'Id':'review','Arn':ARN+'justhodl-khalid-risk:candidate','Input':'{"review":true}'},
              {'Id':'unrelated','Arn':'arn:aws:sqs:us-east-1:123:queue','Input':'{"keep":true}'}]
    events=EventRecorder(original);lam=LambdaRecorder()
    scope=extracted('schedule-liveness',['rebuild_binding'],
                    {'EVENTS':events,'LAM':lam,'REGION':'us-east-1','ACCT':'123456789012'})
    assert scope['rebuild_binding']('justhodl-khalid-risk','hourly','rate(1 hour)')
    expected=copy.deepcopy(original);expected[0]['Arn']+=':live'
    assert events.targets==expected
    assert events.puts[0]['Targets']==expected[:3]
    assert lam.permissions[0]['FunctionName']=='justhodl-khalid-risk:live'
    assert original[0]['Arn']==ARN+'justhodl-khalid-risk'


def test_liveness_rebuild_keeps_explicit_pinned_alias():
    events=EventRecorder([{'Id':'risk','Arn':ARN+'justhodl-risk-gate:42','Input':'{}'}]);lam=LambdaRecorder()
    scope=extracted('schedule-liveness',['rebuild_binding'],
                    {'EVENTS':events,'LAM':lam,'REGION':'us-east-1','ACCT':'123456789012'})
    assert scope['rebuild_binding']('justhodl-risk-gate:42','hourly','rate(1 hour)')
    assert events.targets[0]['Arn']==ARN+'justhodl-risk-gate:42'
    assert lam.permissions[0]['FunctionName']=='justhodl-risk-gate:42'


def test_actual_liveness_handler_repair_and_invoke_route_live():
    events=EventRecorder([{'Id':'risk','Arn':ARN+'justhodl-risk-gate:$LATEST','Input':'{}'}]);lam=LambdaRecorder()
    writes=[]
    s3=SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(b'{}')},
                       put_object=lambda **kw:writes.append(kw))
    scope=extracted('schedule-liveness',['lambda_handler','rebuild_binding','feed_for','cadence_h'],
        {'EVENTS':events,'LAM':lam,'S3':s3,'REGION':'us-east-1','ACCT':'123456789012',
         'BUCKET':'fixture','STATE_KEY':'data/schedule-liveness.json','SKIP_FN_SUBSTR':(),
         'STALE_FLOOR_H':8,'STALE_MULT':2.5,'FEED_OVERRIDE':{},'feed_age_h':lambda key:100,
         'telegram':lambda message:None})
    scope['lambda_handler']({},None)
    assert lam.calls[0]['FunctionName']=='justhodl-risk-gate:live'
    assert events.targets[0]['Arn']==ARN+'justhodl-risk-gate:live'
    assert writes and json.loads(writes[0]['Body'])['n_revived']==1
