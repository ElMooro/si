"""Exercise observable safety boundaries of duplicate-trigger cleanup."""
import copy
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'aws/ops/checks'))
import audit_20260909_schedules as audit


class Fleet:
    def __init__(self):
        self.rows = {}; self.writes = []; self.reads = 0; self.race = False; self.corrupt = False
        for name, expr in audit.CADENCES.items():
            self.rows[name] = (
                {'Name': name, 'State': 'ENABLED', 'ScheduleExpression': expr,
                 'FlexibleTimeWindow': {'Mode': 'OFF'},
                 'Target': {'Arn': audit.ARN, 'RoleArn': audit.ROLE, 'Input': '{}'}},
                {'Name': name, 'State': 'ENABLED', 'ScheduleExpression': expr},
                [{'Id': 'original', 'Arn': audit.ARN}])
    def get_schedule(self, Name, **kwargs):
        self.reads += 1
        result = copy.deepcopy(self.rows[Name][0])
        if self.race and self.reads > len(self.rows):
            result['Target']['Input'] = '{"owner":"SECRET_CANARY"}'
        return result
    def describe_rule(self, Name):
        return copy.deepcopy(self.rows[Name][1])
    def list_targets_by_rule(self, Rule, **kwargs):
        return {'Targets': copy.deepcopy(self.rows[Rule][2])}
    def disable_rule(self, Name):
        self.writes.append(Name)
        if not self.corrupt:
            self.rows[Name][1]['State'] = 'DISABLED'


def blocked(fleet):
    report = {}
    try:
        audit.consolidate(fleet, fleet, report)
    except ValueError:
        pass
    else:
        raise AssertionError('unsafe cleanup accepted')
    assert 'SECRET_CANARY' not in str(report)


def test_cleanup_preserves_both_cadences_and_is_idempotent():
    fleet = Fleet(); before = copy.deepcopy(fleet.rows)
    report = audit.consolidate(fleet, fleet, {})
    assert len(report['changes']) == len(fleet.writes) == 2
    for name in before:
        assert fleet.rows[name][0] == before[name][0]
        assert fleet.rows[name][2] == before[name][2]
        assert fleet.rows[name][1]['State'] == 'DISABLED'
    assert audit.consolidate(fleet, fleet, {})['changes'] == []


def test_all_triggers_are_validated_before_first_mutation():
    for change in (
        lambda s, r, t: s.update(ScheduleExpressionTimezone='America/New_York'),
        lambda s, r, t: s.update(FlexibleTimeWindow={'Mode': 'FLEXIBLE', 'MaximumWindowInMinutes': 10}),
        lambda s, r, t: s.update(EndDate='2026-10-01'),
        lambda s, r, t: s['Target'].update(Input='{"owner":"SECRET_CANARY"}'),
        lambda s, r, t: t.append({'Arn': 'unrelated'}),
        lambda s, r, t: t[0].update(InputPath='$.detail'),
        lambda s, r, t: t[0].update(RetryPolicy={'MaximumRetryAttempts': 1}),
        lambda s, r, t: t[0].update(DeadLetterConfig={'Arn': 'private-dlq'}),
        lambda s, r, t: r.update(EventPattern='{}'),
        lambda s, r, t: s['Target'].update(Arn=audit.ARN + ':live'),
    ):
        fleet = Fleet(); change(*fleet.rows[list(audit.CADENCES)[-1]])
        blocked(fleet); assert fleet.writes == []


def test_concurrent_change_blocks_and_readback_failure_cannot_pass():
    fleet = Fleet(); fleet.race = True; blocked(fleet); assert fleet.writes == []
    fleet = Fleet(); fleet.corrupt = True; blocked(fleet); assert len(fleet.writes) == 1


def test_explicit_default_retry_and_empty_input_are_equivalent():
    fleet = Fleet()
    for s, r, targets in fleet.rows.values():
        s['Target']['RetryPolicy'] = {'MaximumRetryAttempts': 185, 'MaximumEventAgeInSeconds': 86400}
        targets[0]['Input'] = '{}'
    assert len(audit.consolidate(fleet, fleet, {})['changes']) == 2
