"""Legacy schedules normalize without inventing or replacing live bindings."""
import json
import runpy
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
normalize = runpy.run_path(str(ROOT / "scripts/normalize_lambda_config.py"))["normalize_config"]
validate = runpy.run_path(str(ROOT / "scripts/validate_lambda_configs.py"))["validate_configs"]


def test_both_named_classic_contracts_resolve_identical_rule_and_expression():
    a=normalize({"schedule":{"rule_name":"fixture-rule","cron":"cron(0 20 * * ? *)"}})
    b=normalize({"schedule":{"name":"fixture-rule","expression":"cron(0 20 * * ? *)"}})
    for result in (a,b):
        assert result["schedule"]["rule_name"]=="fixture-rule"
        assert result["schedule"]["cron"]=="cron(0 20 * * ? *)"
        assert result["release_schedule_note"]["status"]=="MANAGED_CLASSIC_RULE"


def test_actual_liveness_config_never_emits_null_rule_or_cadence():
    config=json.loads((ROOT/"aws/lambdas/justhodl-schedule-liveness/config.json").read_text())
    result=normalize(config)
    output=subprocess.check_output(["jq","-r",".schedule.rule_name, .schedule.cron"],input=json.dumps(result),text=True)
    assert output.splitlines()==["justhodl-schedule-liveness-daily","cron(30 13 * * ? *)"]
    assert config["schedule"].get("rule_name") is None, "Pure normalization must not mutate source"


def test_expression_only_configs_preserve_bindings_including_legacy_named_rule_hints():
    for name in ("crypto-funding","liquidity-profile","retail-sentiment"):
        config=json.loads((ROOT/f"aws/lambdas/justhodl-{name}/config.json").read_text())
        result=normalize(config)
        assert "schedule" not in result
        assert result["release_schedule_note"]=={"status":"CONFIG_CADENCE_ONLY",
            "configured_expression":config["schedule"],"binding_action":"PRESERVE_EXISTING"}


def test_scheduler_reference_is_never_misrouted_to_classic_eventbridge():
    result=normalize({"schedule":{"scheduler_name":"existing-scheduler","cron":"rate(1 hour)"}})
    assert "schedule" not in result and "eventbridge_scheduler" not in result
    assert result["release_schedule_note"]["status"]=="EXISTING_SCHEDULER_REFERENCE"
    assert result["release_schedule_note"]["binding_action"]=="PRESERVE_EXISTING"


def test_absent_null_and_disabled_legacy_schedule_have_no_mutation_intent():
    for config in ({},{"schedule":None},{"schedule":False}):
        assert "schedule" not in normalize(config)


def test_ambiguous_or_incomplete_schedule_is_rejected_by_selected_preflight():
    invalid=[{}, True, {"name":"one","rule_name":"two","cron":"rate(1 hour)"},
             {"name":"one","cron":"rate(1 hour)","expression":"rate(2 hours)"},
             {"name":"one","cron":"null"}, {"name":"bad name","cron":"rate(1 hour)"},
             {"name":"one","scheduler_name":"two","cron":"rate(1 hour)"}]
    with tempfile.TemporaryDirectory() as temp:
        folder=Path(temp)/"aws/lambdas/fixture";folder.mkdir(parents=True)
        for schedule in invalid:
            (folder/"config.json").write_text(json.dumps({"schedule":schedule}))
            errors=validate(temp,["fixture"])
            assert len(errors)==1 and errors[0]["field"]=="schedule"


def test_normalization_preserves_complete_other_config_and_existing_target_payload_logic():
    config={"env":{"TOKEN":"synthetic,value=="},"release_validation":{"schema_version":"v1"},
            "schedule":{"name":"daily","expression":"cron(0 20 * * ? *)"},
            "eventbridge_scheduler":{"schedule_name":"explicit","cron":"rate(1 hour)","role_arn":"arn:aws:iam::123456789012:role/scheduler","input":{"mode":"research"}}}
    result=normalize(config)
    for key in ("env","release_validation","eventbridge_scheduler"):
        assert result[key]==config[key]
    shell=(ROOT/"scripts/deploy_lambdas.sh").read_text()
    assert shell.index("scripts/normalize_lambda_config.py") < shell.index("aws lambda update-function-code")
    assert 'rule_name=$(jq -r \'.schedule.rule_name\' "$config_file")' in shell
    assert 'State:(.State // "ENABLED")' in shell
    assert '.Targets[] | select(.Arn == $base' in shell and '| .Arn=$arn]' in shell
    assert 'phase:"schedule_configuration"' in shell


def test_scheduler_roles_are_checked_before_code_staging():
    for role in (None, "null", "not-an-arn"):
        try:
            normalize({"eventbridge_scheduler":{"schedule_name":"daily","cron":"rate(1 hour)","role_arn":role}})
        except ValueError as exc: assert str(exc)=="scheduler_role_required"
        else: raise AssertionError("Missing execution role was accepted")
