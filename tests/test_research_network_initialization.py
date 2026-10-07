"""Mutation guards for the single explicitly initialized research publisher."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import copy
import importlib.util
import io
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('initialization', ROOT / 'aws/ops/staged/ops_6491_initialize_research_network.py')
op = importlib.util.module_from_spec(spec)
spec.loader.exec_module(op)
NOW = datetime(2026, 10, 7, 20, tzinfo=timezone.utc)
SHA = base64.b64encode(b'x' * 32).decode()


class Collision(Exception):
    response = {'Error': {'Code': 'PreconditionFailed'}}


class Clients:
    def __init__(self):
        self.config = dict(FunctionArn=op.ARN, Version='$LATEST', CodeSha256=SHA,
                           LastModified='2026-10-07T19:00:00+00:00', State='Active', LastUpdateStatus='Successful',
                           Runtime='python3.12', MemorySize=1024, Timeout=600, RevisionId='revision',
                           Environment={'Variables': {'PRIVATE_CANARY': 'secret'}})
        self.receipt = dict(schema='release-receipt.v1', function=op.FUNCTION, verified=True, commit=op.RELEASE,
                            code_sha256=SHA, zip_sha256_hex=(b'x' * 32).hex(), source={'lambda_function.py': {'sha256': op.SOURCE_SHA}})
        self.rule = dict(Name=op.NAME, State='ENABLED', ScheduleExpression=op.CADENCE)
        self.targets = {'Targets': [{'Id': 'original', 'Arn': op.ARN, 'Input': '{"PRIVATE_CANARY":"secret"}'}]}
        self.schedule = dict(Name=op.NAME, State='ENABLED', ScheduleExpression=op.CADENCE,
                             ScheduleExpressionTimezone='UTC', FlexibleTimeWindow={'Mode': 'OFF'},
                             Target={'Arn': op.ARN, 'RoleArn': 'PRIVATE_CANARY', 'Input': 'PRIVATE_CANARY'})
        self.marker = None
        self.calls = []
        self.fail_save = None
        self.invoke_error = False
        self.pin_race = False
        self.change_after_disable = False
        self.fail_after_disable = False
        self.etag = 0

    def get_function_configuration(self, **kwargs): return copy.deepcopy(self.config)
    def describe_rule(self, **kwargs): return copy.deepcopy(self.rule)
    def list_targets_by_rule(self, **kwargs): return copy.deepcopy(self.targets)
    def get_schedule(self, **kwargs): return copy.deepcopy(self.schedule)
    def get_object(self, **kwargs):
        assert kwargs['Key'] == 'data/ops/releases/' + op.FUNCTION + '.json'
        raw = json.dumps(self.receipt).encode()
        return {'Body': io.BytesIO(raw), 'ContentLength': len(raw)}
    def put_object(self, **kwargs):
        if kwargs.get('IfNoneMatch') and self.marker is not None: raise Collision()
        if kwargs.get('IfMatch') and kwargs['IfMatch'] != str(self.etag): raise AssertionError('stale marker write')
        record = json.loads(kwargs['Body'])
        if record['status'] == self.fail_save: raise OSError('PRIVATE_CANARY')
        self.calls.append(('marker', record['status']))
        self.marker = record
        self.etag += 1
        return {'ETag': str(self.etag)}
    def publish_version(self, **kwargs):
        assert kwargs['CodeSha256'] == SHA and kwargs['RevisionId'] == 'revision'
        self.calls.append(('publish_version', kwargs))
        if self.pin_race: raise OSError('PRIVATE_CANARY')
        return {'Version': '17', 'CodeSha256': SHA}
    def disable_rule(self, **kwargs):
        self.calls.append(('disable_rule', kwargs)); self.rule['State'] = 'DISABLED'
        if self.change_after_disable: self.schedule['State'] = 'DISABLED'
        if self.fail_after_disable: raise TimeoutError('PRIVATE_CANARY')
    def enable_rule(self, **kwargs):
        self.calls.append(('enable_rule', kwargs)); self.rule['State'] = 'ENABLED'
    def invoke(self, **kwargs):
        self.calls.append(('invoke', kwargs))
        if self.invoke_error: raise TimeoutError('PRIVATE_CANARY')
        return {'StatusCode': 202}


class Initialization(unittest.TestCase):
    def run_op(self, clients): return op.execute(clients, clients, clients, clients, NOW)

    def test_one_qualified_async_invoke_preserves_scheduler_and_retains_rollback(self):
        c = Clients(); before = copy.deepcopy(c.schedule)
        result = self.run_op(c)
        self.assertEqual(result['status'], 'queued')
        invokes = [x[1] for x in c.calls if x[0] == 'invoke']
        self.assertEqual(len(invokes), 1)
        self.assertEqual(invokes[0]['Qualifier'], '17')
        self.assertEqual(invokes[0]['InvocationType'], 'Event')
        self.assertEqual(c.schedule, before)
        self.assertEqual(c.rule['State'], 'DISABLED')
        self.assertEqual(c.marker['before']['classic_state'], 'ENABLED')
        self.assertNotIn('PRIVATE_CANARY', json.dumps(c.marker) + json.dumps(result))

    def test_wrong_code_state_resources_or_recent_update_perform_no_writes(self):
        cases = [('CodeSha256', 'wrong'), ('State', 'Pending'), ('LastUpdateStatus', 'InProgress'),
                 ('MemorySize', 2048), ('Timeout', 900), ('Runtime', 'python3.13'), ('Version', '1'),
                 ('LastModified', '2026-10-07T19:59:59Z'), ('FunctionArn', op.ARN + ':live')]
        for key, value in cases:
            with self.subTest(key=key):
                c = Clients(); c.config[key] = value
                with self.assertRaises(ValueError): self.run_op(c)
                self.assertEqual(c.calls, [])

    def test_wrong_receipt_performs_no_writes(self):
        for key, value in [('commit', 'wrong'), ('verified', False), ('function', 'wrong'), ('zip_sha256_hex', 'wrong')]:
            c = Clients(); c.receipt[key] = value
            with self.assertRaises(ValueError): self.run_op(c)
            self.assertEqual(c.calls, [])

    def test_schedule_drift_or_ambiguous_target_performs_no_writes(self):
        for kind in ('cadence', 'scheduler_disabled', 'timezone', 'flex', 'extra_target', 'pagination', 'alias', 'event_pattern'):
            c = Clients()
            if kind == 'cadence': c.rule['ScheduleExpression'] = 'rate(5 minutes)'
            if kind == 'scheduler_disabled': c.schedule['State'] = 'DISABLED'
            if kind == 'timezone': c.schedule['ScheduleExpressionTimezone'] = 'America/New_York'
            if kind == 'flex': c.schedule['FlexibleTimeWindow']['Mode'] = 'FLEXIBLE'
            if kind == 'extra_target': c.targets['Targets'].append({'Arn': op.ARN})
            if kind == 'pagination': c.targets['NextToken'] = 'more'
            if kind == 'alias': c.targets['Targets'][0]['Arn'] += ':live'
            if kind == 'event_pattern': c.rule['EventPattern'] = '{}'
            with self.subTest(kind=kind), self.assertRaises(ValueError): self.run_op(c)
            self.assertEqual(c.calls, [])

    def test_existing_claim_never_repeats_mutations(self):
        c = Clients(); c.marker = {'status': 'unknown'}
        self.assertEqual(self.run_op(c)['status'], 'already_claimed_no_repeat')
        self.assertEqual(c.calls, [])

    def test_immutable_code_race_stops_before_schedule_change(self):
        c = Clients(); c.pin_race = True
        with self.assertRaises(RuntimeError): self.run_op(c)
        self.assertFalse(any(k in ('invoke', 'disable_rule') for k, _ in c.calls))
        self.assertEqual(c.marker['status'], 'failed_before_invoke')

    def test_failure_after_disable_restores_unchanged_prior_rule(self):
        c = Clients(); c.fail_save = 'invocation_reserved'
        with self.assertRaises(RuntimeError): self.run_op(c)
        self.assertEqual(c.rule['State'], 'ENABLED')
        self.assertEqual(c.marker['rollback'], 'restored_prior_classic_state')
        self.assertFalse(any(k == 'invoke' for k, _ in c.calls))

    def test_concurrent_control_change_is_not_blindly_overwritten(self):
        c = Clients(); c.change_after_disable = True
        with self.assertRaises(RuntimeError): self.run_op(c)
        self.assertEqual(c.marker['rollback'], 'manual_review_required_controls_changed')
        self.assertFalse(any(k in ('enable_rule', 'invoke') for k, _ in c.calls))

    def test_ambiguous_disable_checks_state_and_restores_prior_rule(self):
        c = Clients(); c.fail_after_disable = True
        with self.assertRaises(RuntimeError): self.run_op(c)
        self.assertEqual(c.rule['State'], 'ENABLED')
        self.assertEqual(c.marker['rollback'], 'restored_prior_classic_state')
        self.assertFalse(any(k == 'invoke' for k, _ in c.calls))

    def test_ambiguous_invoke_or_final_marker_failure_cannot_repeat(self):
        for mode in ('invoke', 'marker'):
            c = Clients()
            if mode == 'invoke': c.invoke_error = True
            else: c.fail_save = 'queued'
            with self.assertRaises(RuntimeError): self.run_op(c)
            self.assertEqual(c.marker['status'], 'invocation_outcome_unknown')
            with self.assertRaises(ValueError): self.run_op(c)
            self.assertEqual(sum(k == 'invoke' for k, _ in c.calls), 1)
            self.assertEqual(c.rule['State'], 'DISABLED')


if __name__ == '__main__': unittest.main(verbosity=2)
