"""Static release-safety checks with no third-party test dependency."""
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/deploy-lambdas.yml"
CANDIDATE_SCRIPT = ROOT / "scripts/deploy_validated_candidate.sh"


def test_required_service_tests_run_before_aws_mutation():
    workflow = WORKFLOW.read_text()
    preflight = workflow.index("- name: Run deployment preflight tests")
    deploy = workflow.index("- name: Deploy each changed Lambda")
    first_code_mutation = workflow.index("aws lambda update-function-code")
    assert preflight < deploy < first_code_mutation
    for function_name in (
        "justhodl-settlement-fails",
        "justhodl-engine-fusion",
        "justhodl-khalid-risk",
    ):
        assert function_name in workflow[preflight:deploy]
    assert 'python3 "$dir/tests/run_tests.py"' in workflow[preflight:deploy]


def test_governed_engines_use_numbered_candidate_path():
    workflow = WORKFLOW.read_text()
    candidate_call = workflow.index("bash scripts/deploy_validated_candidate.sh")
    scheduler_block = workflow.index(
        "# ── EventBridge Scheduler (if config.json has .eventbridge_scheduler)"
    )
    assert candidate_call < scheduler_block
    assert (
        'if [ "$fn" = "justhodl-engine-fusion" ] '
        '|| [ "$fn" = "justhodl-khalid-risk" ]'
    ) in workflow
    assert (
        'if [ "$fn" != "justhodl-engine-fusion" ] '
        '&& [ "$fn" != "justhodl-khalid-risk" ] '
        '&& [ -f "$dir/config.json" ]'
    ) in workflow
    assert (
        '[ "$fn" = "justhodl-khalid" ] '
        '|| [ "$fn" = "justhodl-khalid-risk" ]'
    ) in workflow


def test_candidate_script_pins_validates_promotes_then_schedules():
    script = CANDIDATE_SCRIPT.read_text()
    snapshot = script.index("candidate_info=$(aws lambda get-function-configuration")
    publish = script.index("candidate_version=$(aws lambda publish-version")
    invoke = script.index("aws lambda invoke")
    promote = script.index("promoted_alias_revision=$(aws lambda update-alias")
    schedule = script.index("aws scheduler update-schedule")
    assert snapshot < publish < invoke < promote < schedule
    assert '--revision-id "$candidate_revision"' in script
    assert '--code-sha256 "$candidate_sha"' in script
    assert '--qualifier "$candidate_version"' in script
    assert 'case "$candidate_version" in' in script
    assert '$body.validation_only == true' in script
    assert '$body.schema_version == $schema' in script


def test_candidate_script_targets_live_and_has_conditional_rollback():
    script = CANDIDATE_SCRIPT.read_text()
    assert '--arg arn "${function_arn}:${alias_name}"' in script
    assert "rollback_alias()" in script
    assert '--function-version "$previous_version"' in script
    assert '--revision-id "$promoted_alias_revision"' in script
    assert "aws lambda delete-alias" in script
    assert script.index("trap rollback_alias ERR") < script.index(
        "promoted_alias_revision=$(aws lambda update-alias"
    )
    assert script.index("aws lambda invoke") < script.index(
        "aws scheduler update-schedule"
    )
