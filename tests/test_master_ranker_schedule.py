"""Actual migration with fake AWS, plus coordinator and calendar boundaries."""
import ast
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/ops/staged'))
spec = importlib.util.spec_from_file_location('ranker_schedule', ROOT / 'aws/ops/staged/ops_6494_master_ranker_weekday_close.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Missing(Exception):
    response = {'Error': {'Code': 'ResourceNotFoundException'}}


class FakeAWS:
    def __init__(self):
        self.journal = None
        self.schedule = None
        self.actions = []
        self.other = False
        self.fail_enable = False
        self.ambiguous_create = False
        self.changed = False
    def get_object(self, **kw):
        if self.journal is None:
            raise Missing()
        return {'Body': io.BytesIO(self.journal)}
    def put_object(self, **kw):
        if kw.get('IfNoneMatch') == '*' and self.journal is not None:
            raise ValueError('conditional_conflict')
        self.journal = kw['Body']
        self.actions.append('journal')
    def list_event_buses(self, **kw):
        return {'EventBuses': [{'Name': 'default'}, {'Name': 'custom'}]}
    def list_rules(self, **kw):
        return {'Rules': [{'Name': 'unrelated', 'State': 'ENABLED'}]}
    def list_targets_by_rule(self, **kw):
        return {'Targets': [{'Arn': m.ARN + ':42' if self.other else 'unrelated'}]}
    def list_schedules(self, **kw):
        return {'Schedules': [copy.deepcopy(self.schedule)] if self.schedule else []}
    def get_schedule(self, **kw):
        if kw['Name'] == 'justhodl-ticker-360-schedule':
            return {'State': 'ENABLED', 'Target': {'Arn': m.ARN.replace(m.FUNCTION, 'justhodl-ticker-360'), 'RoleArn': m.SPEC['role_arn']}}
        if self.schedule is None:
            raise Missing()
        return copy.deepcopy(self.schedule)
    def create_schedule(self, **kw):
        self.actions.append('create')
        self.schedule = {k:v for k,v in kw.items() if k != 'ClientToken'}
        self.schedule['CreationDate'] = datetime(2026, 10, 7, tzinfo=timezone.utc)
        if self.ambiguous_create:
            raise TimeoutError()
        return {'ScheduleArn': 'created'}
    def update_schedule(self, **kw):
        self.actions.append('enable')
        if self.fail_enable:
            if self.changed:
                self.schedule['Description'] = 'external change'
            raise RuntimeError('update unavailable')
        self.schedule.update({k:v for k,v in kw.items() if k != 'ClientToken'})
    def delete_schedule(self, **kw):
        self.actions.append('delete')
        self.schedule = None


class ScheduleTests(unittest.TestCase):
    def setUp(self):
        self.aws = FakeAWS()
        self.guard = patch.object(m, 'release_guard', return_value={'verified': True})
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.fanout = patch.object(m, 'fanout_guard', return_value={'memberships': 0})
        self.fanout.start()
        self.addCleanup(self.fanout.stop)
    def run_op(self):
        return m.execute(self.aws, self.aws, self.aws, self.aws, datetime(2026, 10, 7, 22, tzinfo=timezone.utc))
    def test_create_disabled_then_enable_and_verify(self):
        result = self.run_op()
        self.assertEqual(result['status'], 'verified')
        self.assertEqual([x for x in self.aws.actions if x != 'journal'], ['create', 'enable'])
        self.assertEqual(m.shape(self.aws.schedule), m.desired())
        self.assertEqual(json.loads(self.aws.journal)['before']['matching_bindings'], [])
        self.assertEqual(result['native_invocations'], 0)
    def test_successful_repeat_only_reads(self):
        self.run_op()
        before = list(self.aws.actions)
        self.assertEqual(self.run_op()['status'], 'verified_existing')
        self.assertEqual(before, self.aws.actions)
    def test_existing_numbered_classic_target_blocks_all_writes(self):
        self.aws.other = True
        with self.assertRaisesRegex(ValueError, 'existing_binding'):
            self.run_op()
        self.assertEqual(self.aws.actions, [])
    def test_ambiguous_create_does_not_blindly_retry_or_delete(self):
        self.aws.ambiguous_create = True
        with self.assertRaisesRegex(RuntimeError, 'outcome_unknown'):
            self.run_op()
        self.assertNotIn('delete', self.aws.actions)
        before = list(self.aws.actions)
        with self.assertRaisesRegex(ValueError, 'prior_attempt'):
            self.run_op()
        self.assertEqual(before, self.aws.actions)
    def test_failed_enable_rolls_back_only_new_unchanged_schedule(self):
        self.aws.fail_enable = True
        with self.assertRaisesRegex(RuntimeError, 'removed_only_created_schedule'):
            self.run_op()
        self.assertIsNone(self.aws.schedule)
    def test_external_edit_withholds_rollback(self):
        self.aws.fail_enable = self.aws.changed = True
        with self.assertRaisesRegex(RuntimeError, 'withheld_changed_control'):
            self.run_op()
        self.assertNotIn('delete', self.aws.actions)
    def test_source_guard_failure_has_no_writes(self):
        with patch.object(m, 'release_guard', side_effect=ValueError('source_mismatch')):
            with self.assertRaisesRegex(ValueError, 'source_mismatch'):
                self.run_op()
        self.assertEqual(self.aws.actions, [])
    def test_second_binding_after_success_is_not_acknowledged(self):
        self.run_op()
        self.aws.other = True
        with self.assertRaisesRegex(ValueError, 'additional_binding'):
            self.run_op()
    def test_universal_lambda_target_and_arn_prefix_boundaries(self):
        self.assertTrue(m.targets_ranker({'Arn':'arn:aws:scheduler:::aws-sdk:lambda:invoke', 'Input':json.dumps({'FunctionName':m.FUNCTION+':live'})}))
        self.assertFalse(m.targets_ranker({'Arn':m.ARN+'-other'}))
    def test_timezone_and_weekday_contract(self):
        minute, hour, dom, month, weekdays, year = m.SPEC['cron'][5:-1].split()
        self.assertEqual((minute, hour, dom, month, weekdays, year), ('15', '16', '?', '*', 'MON-FRI', '*'))
        zone = ZoneInfo(m.SPEC['timezone'])
        # Fall and spring transitions retain 16:15 local, while UTC shifts.
        for day, utc_hour in [('2026-10-30',20), ('2026-11-02',21), ('2027-03-12',21), ('2027-03-15',20)]:
            local = datetime.fromisoformat(day).replace(hour=int(hour), minute=int(minute), tzinfo=zone)
            self.assertLess(local.weekday(), 5)
            self.assertEqual(local.astimezone(timezone.utc).hour, utc_hour)
        days = [datetime(2026,10,5,tzinfo=zone)+timedelta(days=i) for i in range(7)]
        self.assertEqual([d.strftime('%a') for d in days if d.weekday()<5], ['Mon','Tue','Wed','Thu','Fri'])
    def test_actual_routes_preserve_other_targets_without_ranker(self):
        tree = ast.parse((ROOT/'aws/lambdas/justhodl-event-coordinator/source/lambda_function.py').read_text(encoding='utf-8'))
        routes = ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ROUTES' for t in n.targets)))
        self.assertFalse(any(m.FUNCTION in row.get('invoke',[]) for row in routes.values()))
        self.assertEqual(routes['regime.changed'], {'invoke':['justhodl-alpha-compass','justhodl-signal-board'], 'notify':True, 'audit':True})
        self.assertEqual(routes['future.signal.high_conviction']['invoke'], ['justhodl-alpha-compass'])


class SourceGuardTests(unittest.TestCase):
    def test_indirect_fanout_membership_blocks_schedule(self):
        from types import SimpleNamespace
        packet = {'ticks':{'daily':[m.FUNCTION+':live']}}
        aws = SimpleNamespace(get_function_configuration=lambda **kw:{},
            get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(packet).encode())})
        with self.assertRaisesRegex(ValueError, 'has_fanout_trigger'):
            m.fanout_guard(aws, aws)
        packet['disabled'] = [m.FUNCTION+':live']
        self.assertEqual(len(m.fanout_guard(aws, aws)), 2)

    def test_guard_checks_source_receipt_and_current_code(self):
        class SourceAWS:
            def get_object(self, **kw):
                fn = kw['Key'].split('/')[-1][:-5]
                sha = hashlib.sha256((ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes()).hexdigest()
                return {'Body':io.BytesIO(json.dumps({'function':fn,'verified':True,'source':{'lambda_function.py':{'sha256':sha}},'code_sha256':'expected','commit':'reviewed'}).encode())}
            def get_function_configuration(self, **kw):
                return {'CodeSha256':'expected','State':'Active','LastUpdateStatus':'Successful','RevisionId':'r1'}
        aws = SourceAWS()
        self.assertEqual(len(m.release_guard(aws, aws)), 2)
        with patch.object(aws, 'get_function_configuration', return_value={'CodeSha256':'other'}):
            with self.assertRaisesRegex(ValueError, 'required_source'):
                m.release_guard(aws, aws)


class DeclaredScheduleTests(unittest.TestCase):
    def test_engine_config_preserves_established_cadence_without_resource_overrides(self):
        config = json.loads((ROOT/'aws/lambdas/justhodl-master-ranker/config.json').read_text(encoding='utf-8'))
        self.assertEqual(config, {'eventbridge_scheduler': m.SPEC})

    def test_deploy_guard_accepts_established_schedule_and_rejects_drift(self):
        from types import SimpleNamespace
        from check_existing_schedule import check, ScheduleMismatch
        config = json.loads((ROOT/'aws/lambdas/justhodl-master-ranker/config.json').read_text(encoding='utf-8'))
        current = m.desired()
        aws = SimpleNamespace(get_schedule=lambda **kw:copy.deepcopy(current))
        self.assertEqual(check(config, m.ARN, None, aws)['bindings_checked'], ['scheduler_existing_binding'])
        current['ScheduleExpressionTimezone'] = 'UTC'
        with self.assertRaises(ScheduleMismatch):
            check(config, m.ARN, None, aws)


if __name__ == '__main__':
    unittest.main()
