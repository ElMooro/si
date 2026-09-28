"""Stale configuration cannot add an unintended second trigger during a code ship."""
from pathlib import Path
from unittest.mock import Mock
from datetime import datetime, timezone
import copy
import importlib.util
import json
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'scripts'))
spec = importlib.util.spec_from_file_location('existing_schedule_guard', ROOT/'scripts/check_existing_schedule.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
ARN = 'arn:aws:lambda:us-east-1:123456789012:function:fixture'
CONFIG = {'schedule': {'rule_name': 'fixture-daily', 'cron': 'cron(0 14 * * ? *)'}}


class Missing(Exception):
    response = {'Error': {'Code': 'ResourceNotFoundException'}}


def failure(config, events, scheduler):
    try:
        m.check(config, ARN, events, scheduler)
    except m.ScheduleMismatch:
        return
    raise AssertionError('Mismatched existing schedule must fail before mutation')


def classic():
    events = Mock()
    events.describe_rule.return_value = {'Name': 'fixture-daily', 'ScheduleExpression': CONFIG['schedule']['cron'], 'State': 'DISABLED'}
    events.list_targets_by_rule.return_value = {'Targets': [{'Id': 'old', 'Arn': ARN, 'Input': 'SYNTHETIC_PRIVATE_PAYLOAD'}]}
    return events


def test_missing_classic_rule_cannot_be_created_for_existing_function():
    events = Mock(); events.describe_rule.side_effect = Missing()
    failure(CONFIG, events, Mock())
    assert [c[0] for c in events.mock_calls] == ['describe_rule']


def test_empty_or_foreign_rule_cannot_gain_an_extra_target():
    for targets in ([], [{'Id': 'foreign', 'Arn': ARN + '-other'}], [{'Id': 'numbered', 'Arn': ARN + ':17'}]):
        events = classic(); events.list_targets_by_rule.return_value = {'Targets': targets}
        failure(CONFIG, events, Mock())


def test_existing_disabled_rule_input_and_qualified_binding_are_read_only():
    for arn in (ARN, ARN + ':$LATEST', ARN + ':live'):
        events = classic(); events.list_targets_by_rule.return_value['Targets'][0]['Arn'] = arn
        result = m.check(CONFIG, ARN, events, Mock())
        assert result['schedule_writes'] == result['native_invocations'] == 0
        assert 'SYNTHETIC_PRIVATE_PAYLOAD' not in json.dumps(result)
        assert [c[0] for c in events.mock_calls] == ['describe_rule', 'list_targets_by_rule']


def test_classic_cadence_change_rejected_and_target_pagination_is_complete():
    events = classic(); events.describe_rule.return_value['ScheduleExpression'] = 'rate(1 minute)'
    failure(CONFIG, events, Mock())
    events = classic(); events.list_targets_by_rule.side_effect = [{'Targets': [], 'NextToken': 'page-two'}, {'Targets': [{'Id': 'existing', 'Arn': ARN}]}]
    assert m.check(CONFIG, ARN, events, Mock())['status'] == 'VERIFIED'
    assert events.list_targets_by_rule.call_args.kwargs['NextToken'] == 'page-two'
    events = classic(); events.list_targets_by_rule.return_value = {'Targets': [], 'NextToken': 'loop'}
    failure(CONFIG, events, Mock())


def scheduler_fixture():
    config = {'eventbridge_scheduler': {'schedule_name': 'original', 'cron': 'cron(0 14 * * ? *)', 'role_arn': 'synthetic-role'}}
    current = {'Name': 'original', 'GroupName': 'default', 'ScheduleExpression': 'cron(0 14 * * ? *)',
               'State': 'DISABLED', 'Target': {'Arn': ARN, 'RoleArn': 'synthetic-role', 'Input': 'SYNTHETIC_PRIVATE_PAYLOAD'}}
    client = Mock(); client.get_schedule.return_value = current
    return config, client


def test_missing_scheduler_or_different_cadence_cannot_replace_original():
    config, client = scheduler_fixture(); client.get_schedule.side_effect = Missing()
    failure(config, Mock(), client)
    for mutation in ({'ScheduleExpression': 'rate(1 minute)'}, {'Target': {'Arn': ARN + '-other'}}, {'GroupName': 'different'}):
        config, client = scheduler_fixture(); client.get_schedule.return_value.update(mutation)
        failure(config, Mock(), client)


def test_scheduler_settings_and_private_payload_preserved_without_log_disclosure():
    config, client = scheduler_fixture()
    assert 'SYNTHETIC_PRIVATE_PAYLOAD' not in json.dumps(m.check(config, ARN, Mock(), client))
    assert [c[0] for c in client.mock_calls] == ['get_schedule']
    for key, value in [('role_arn', 'changed'), ('input', {}), ('state', 'ENABLED'), ('timezone', 'America/New_York'), ('retry_policy', {'MaximumRetryAttempts': 100})]:
        changed = copy.deepcopy(config); changed['eventbridge_scheduler'][key] = value
        failure(changed, Mock(), client)


def test_reference_only_config_does_not_read_or_mutate_any_schedule():
    events, scheduler = Mock(), Mock()
    result = m.check({'release_schedule_note': {'binding_action': 'PRESERVE_EXISTING'}}, ARN, events, scheduler)
    assert result['bindings_checked'] == [] and not events.mock_calls and not scheduler.mock_calls


def test_aware_scheduler_dates_compare_instants_without_inventing_naive_timezones():
    config, client = scheduler_fixture()
    config['eventbridge_scheduler']['start_date'] = '2026-09-28T01:00:00-04:00'
    client.get_schedule.return_value['StartDate'] = datetime(2026, 9, 28, 5, tzinfo=timezone.utc)
    assert m.check(config, ARN, Mock(), client)['status'] == 'VERIFIED'
    config['eventbridge_scheduler']['start_date'] = '2026-09-28T05:00:00'
    failure(config, Mock(), client)


def test_both_real_duplicate_regression_configs_fail_but_preserve_references_pass():
    from normalize_lambda_config import normalize_config
    for name in ('opportunity', 'scarcity'):
        old = json.loads((ROOT/'tests/fixtures'/('pre-' + name + '-scheduler-reference-config.json.txt')).read_bytes())
        events = Mock(); events.describe_rule.side_effect = Missing()
        failure(normalize_config(old), events, Mock())
    for fn in ('justhodl-opportunity-screener', 'justhodl-scarcity-radar'):
        fixed = normalize_config(json.loads((ROOT/'aws/lambdas'/fn/'config.json').read_bytes()))
        events, scheduler = Mock(), Mock()
        assert m.check(fixed, ARN, events, scheduler)['bindings_checked'] == []
        assert not events.mock_calls and not scheduler.mock_calls


def test_deploy_guard_precedes_existing_code_update_and_rechecks_before_scheduling():
    text = (ROOT/'scripts/deploy_lambdas.sh').read_text(encoding='utf-8')
    assert text.index('python3 scripts/check_existing_schedule.py') < text.index('aws lambda update-function-code')
    assert text.count('python3 scripts/check_existing_schedule.py') == 2
    assert text.index('# Recheck schedule intent') < text.index('aws events put-rule')
