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
    assert 'if [ "$candidate_managed" -eq 0 ] && [ -f "$dir/config.json" ]' in workflow
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
    assert '"${function_arn}:${alias_name}"' in script
    assert "rollback_alias()" in script
    assert '--function-version "$previous_version"' in script
    assert '--revision-id "$promoted_alias_revision"' in script
    assert "aws lambda delete-alias" not in script
    assert script.index("trap rollback_alias ERR") < script.index(
        "promoted_alias_revision=$(aws lambda update-alias"
    )
    assert script.index("aws lambda invoke") < script.index(
        "aws scheduler update-schedule"
    )


def test_risk_validation_configs_and_minimal_config_preserve_runtime():
    import json
    for engine, schema in [("katlin", "1.1"), ("risk-gate", "risk-gate.v2.5"), ("risk-sizer", "3.0")]:
        config = json.loads((ROOT / f"aws/lambdas/justhodl-{engine}/config.json").read_text())
        assert config["release_validation"]["schema_version"] == schema
    workflow = WORKFLOW.read_text()
    assert "has(\"timeout\")" in workflow
    assert "has(\"memory\")" in workflow
    assert '"${code_revision_args[@]}"' in workflow
    for name in ["deploy-lambdas.yml", "deploy-workers.yml", "pages.yml"]:
        assert "python3 tests/test_brain_public_boundaries.py" in (ROOT / ".github/workflows" / name).read_text()
    assert "justhodl-risk-gate|justhodl-tradingview" in workflow


def test_failed_per_engine_command_cannot_continue_to_production_scheduling():
    import subprocess
    workflow = WORKFLOW.read_text()
    start = workflow.index("            set +e\n            (", workflow.index("- name: Deploy each changed Lambda"))
    prefix_end = workflow.index("              dir=", start)
    suffix_start = workflow.index("            )\n            deploy_status=$?", prefix_end)
    suffix_end = workflow.index("          done", suffix_start)
    prefix, suffix = workflow[start:prefix_end], workflow[suffix_start:suffix_end]
    script = 'set -e\nfn=test\nfailed_lambdas=()\n' + prefix + 'false\nprintf forbidden-production-mutation\n' + suffix
    result = subprocess.run(["bash"], input=script, text=True, capture_output=True, check=True)
    assert "forbidden-production-mutation" not in result.stdout
    assert "Deploy failed for test" in result.stdout


def test_classic_target_update_preserves_input_retry_and_unrelated_targets():
    import json, re, subprocess
    workflow = WORKFLOW.read_text()
    start = workflow.index('              jq --arg arn "$target_fn_arn"')
    snippet = workflow[start:workflow.index('> "$tmp/classic-target-update.json"', start)]
    expression = re.search(r"'([\s\S]+)'", snippet).group(1)
    base = "arn:aws:lambda:us-east-1:123:function:test"
    row = {"Id":"target1","Arn":base,"Input":"private-input","RetryPolicy":{"MaximumRetryAttempts":8},"DeadLetterConfig":{"Arn":"dlq"}}
    unrelated = {"Id":"other", "Arn":"another-function"}
    output = subprocess.run(["jq", "--arg", "arn", base+":live", "--arg", "base", base, "--arg", "id", "audit-test", expression],
                            input=json.dumps({"Targets":[row,unrelated]}), text=True, capture_output=True, check=True)
    assert json.loads(output.stdout) == [{**row,"Arn":base+":live"}]
