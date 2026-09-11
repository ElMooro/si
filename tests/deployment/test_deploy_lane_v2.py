"""Deploy lane v2 contract — the things that silently broke on 2026-09-11 stay fixed."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WF = ROOT / ".github/workflows"


def test_apply_lane_dispatches_a_pinned_deploy_after_its_token_push():
    # A push made with GITHUB_TOKEN never fires push-triggered workflows; the lane must dispatch.
    text = (WF / "apply-staged-large-files.yml").read_text()
    assert "actions: write" in text
    assert "gh workflow run deploy-lambdas.yml" in text and "-f expected_sha=" in text
    assert text.index("Commit and push the applied result") < text.index("Dispatch a pinned Lambda deploy")
    assert "scripts/assemble_parts.py" in text and "scripts/guard_stub_lambdas.py" in text
    assert "git reset -q --hard origin/main" in text          # patch the freshest main, not a stale checkout
    assert "aws/ops/patchers/**" in text and "aws/ops/staged/grok_*.py" in text
    assert "python3 -m py_compile" in text
    assert "scripts/push_evidence.py" in text and "git push -q origin HEAD:main" in text
    assert 'if [ -e "$tree" ]; then git add -A "$tree"; fi' in text   # js/ and css/ do not exist; git add must not fatal


def test_apply_lane_never_executes_the_staged_ops_scripts():
    # aws/ops/staged/ holds ops scripts that must NOT run (e.g. the key-rotation fan-out); only grok_* patchers.
    text = (WF / "apply-staged-large-files.yml").read_text()
    assert "aws/ops/staged/*.py" not in text
    assert "aws/ops/pending" not in text


def test_deploy_transaction_proves_code_sha_and_publishes_receipts():
    script = (ROOT / "scripts/deploy_lambdas.sh").read_text()
    first_update = script.index("aws lambda update-function-code")
    proof = script.index('if [ "$local_code_sha" != "$live_code_sha" ]')
    receipt = script.index("python3 scripts/release_receipt.py")
    config_update = script.index("# Apply config overrides if present")
    assert first_update < proof < receipt < config_update
    assert script.count("CodeSha256 verified") == 2          # update path AND create path
    assert 'openssl dgst -sha256 -binary "$tmp/deploy.zip" | base64 -w0' in script
    subprocess.run(["bash", "-n"], input=script, text=True, check=True)
    wf = (WF / "deploy-lambdas.yml").read_text()
    for var in ("DEPLOY_RUN_ID", "DEPLOY_WORKFLOW", "DEPLOY_ACTOR"):
        assert var in wf


def test_receipt_and_verifier_share_the_public_key_layout():
    receipt = (ROOT / "scripts/release_receipt.py").read_text()
    verify = (ROOT / "scripts/verify_release.py").read_text()
    assert 'PREFIX = "data/ops/releases"' in receipt
    assert "data/ops/releases/{args.function}.json" in verify
    assert "boto3" not in verify                              # verifiable without AWS credentials


def test_deploy_commit_back_and_evidence_never_lose_to_concurrent_lanes():
    wf = (WF / "deploy-lambdas.yml").read_text()
    block = wf[wf.index("Commit Function URL patches back"):wf.index("- name: Summary")]
    assert "git rebase -q origin/main && git push -q origin HEAD:main" in block
    assert 'git push origin main || echo "push failed' not in block


def test_stub_guard_workflow_compares_against_the_push_base():
    text = (WF / "guard-lambda-stubs.yml").read_text()
    assert "GUARD_BASE_SHA" in text and "github.event.before" in text and "fetch-depth: 50" in text


def test_ledger_baseline_exists_and_freezes_legacy_pending_scripts():
    import json
    ledger = json.loads((ROOT / "aws/ops/reports/_ops_ledger.json").read_text())
    assert ledger["schema"] == "ops-ledger.v1" and ledger["baseline_at"]
    frozen = [k for k, v in ledger["entries"].items() if v["status"] == "legacy-frozen"]
    assert len(frozen) >= 400 and all(k.startswith("aws/ops/pending/") for k in frozen)


def test_all_workflows_parse_and_no_workflow_pushes_audit_receipts_to_main():
    try:
        import yaml
    except ImportError:  # runner system python may lack PyYAML; the YAML parse is covered locally + by GitHub itself
        yaml = None
    for path in WF.glob("*.yml"):
        text = path.read_text()
        assert text.lstrip().startswith(("name:", "#")), path
        if yaml:
            yaml.safe_load(text)
    for name in ("audit-release-progress.yml", "audit-release-observation.yml"):
        text = (WF / name).read_text()
        assert not re.search(r"git push .*main", text), name
