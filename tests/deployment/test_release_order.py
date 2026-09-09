"""Caller migration completes before any governed candidate can reach $LATEST."""
import base64
import hashlib
import runpy
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SCOPE = runpy.run_path(str(ROOT / "scripts/release_order.py"))
SHELL = ROOT / "scripts/deploy_lambdas.sh"


def test_every_selected_router_and_admin_precedes_producer_staging():
    callers = sorted(SCOPE["ROUTING_CALLERS"])
    producers = ["justhodl-engine-fusion", "justhodl-risk-gate", "justhodl-risk-sizer"]
    targets = [producers[0], callers[0], producers[1], *callers[1:], producers[2]]
    ordered = SCOPE["ordered_targets"](targets)
    assert ordered[:len(callers)] == callers
    assert ordered[len(callers):] == producers


def fake_transaction(fail_caller=False):
    # Execute the actual phase selection, errexit boundary and failure handler;
    # substitute the AWS mutation body with observable local stage markers.
    source = SHELL.read_text()
    prefix = source[:source.index('    dir="aws/lambdas/$fn"')]
    suffix = source[source.index('  )\n  deploy_status=$?'):]
    body = 'printf "stage:%s\\n" "$fn"\n'
    if fail_caller:
        body += 'if [ "$caller_phase" -eq 1 ]; then false; fi\n'
    body += 'printf "ready:%s\\n" "$fn"\n'
    return subprocess.run(["bash"], input=prefix+body+suffix, text=True, cwd=ROOT,
        env={"PATH":__import__("os").environ["PATH"], "DEPLOY_AWS_REGION":"us-east-1",
             "DEPLOY_TARGETS":"justhodl-risk-gate justhodl-scheduler justhodl-engine-fusion justhodl-portfolio-admin"},
        capture_output=True)


def test_successful_caller_phase_runs_before_producers_in_actual_shell():
    result = fake_transaction()
    assert result.returncode == 0, result.stderr
    stages = [line for line in result.stdout.splitlines() if line.startswith("stage:")]
    assert stages == ["stage:justhodl-scheduler", "stage:justhodl-portfolio-admin",
                      "stage:justhodl-risk-gate", "stage:justhodl-engine-fusion"]


def test_failed_required_caller_aborts_actual_shell_before_any_producer():
    result = fake_transaction(True)
    assert result.returncode != 0
    assert "producer staging blocked" in result.stdout
    assert "stage:justhodl-risk-gate" not in result.stdout
    assert "stage:justhodl-engine-fusion" not in result.stdout
    assert "ready:justhodl-scheduler" not in result.stdout


def test_caller_receipt_requires_exact_zip_hash_and_stable_function():
    with tempfile.TemporaryDirectory() as temp:
        archive = Path(temp) / "deploy.zip"
        archive.write_bytes(b"exact deployed archive")
        sha = base64.b64encode(hashlib.sha256(archive.read_bytes()).digest()).decode()
        config = {"CodeSha256":sha, "State":"Active", "LastUpdateStatus":"Successful"}
        lam = SimpleNamespace(get_function_configuration=lambda **kw:dict(config))
        assert SCOPE["verify_caller"](lam, "justhodl-scheduler", archive)["code_sha256"] == sha
        for key, value in [("CodeSha256","different"), ("State","Pending"), ("LastUpdateStatus","Failed")]:
            original = config[key];config[key] = value
            try:SCOPE["verify_caller"](lam, "justhodl-scheduler", archive)
            except RuntimeError as error:assert "producers must remain unstaged" in str(error)
            else:raise AssertionError("Unverified caller admitted producer staging")
            config[key] = original
    source = SHELL.read_text()
    assert source.index('scripts/release_order.py verify-caller') < source.index('echo "✅ $fn deployed"')
