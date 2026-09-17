"""A failed deploy or absent proof must not become a successful release run."""
import importlib.util
import subprocess
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def test_workflow_preserves_deploy_exit_through_tee():
    workflow = (ROOT / '.github/workflows/deploy-lambdas.yml').read_text()
    block = workflow.split('- name: Deploy each changed Lambda', 1)[1].split('\n      - name:', 1)[0]
    run = block.split('        run: |\n', 1)[1]
    command = '\n'.join(line[10:] for line in run.splitlines())
    assert 'set -euo pipefail' in command
    # Execute the actual workflow pipeline with a failing stand-in deploy.
    command = command.replace('bash scripts/deploy_lambdas.sh', "bash -c 'exit 7'")
    command = command.replace('"$RUNNER_TEMP/deploy.log"', '/dev/null')
    assert subprocess.run(['bash', '-c', command], capture_output=True).returncode == 7


def test_receipt_publish_failure_is_a_release_failure():
    spec = importlib.util.spec_from_file_location('receipt_failure_test', ROOT / 'scripts/release_receipt.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    receipt = dict(function='test', commit='a' * 40, code_sha256='hash', zip_bytes=10, deployed_at='now')
    with patch.object(module, 'build', return_value=receipt), patch.object(module, 'publish', side_effect=RuntimeError('write failed')):
        assert module.main(['test', 'unused.zip', 'source', 'hash']) == 1
    with patch.object(module, 'build', return_value=receipt), patch.object(module, 'publish', return_value=['key']):
        assert module.main(['test', 'unused.zip', 'source', 'hash']) == 0


def test_both_deploy_paths_require_the_receipt():
    script = (ROOT / 'scripts/deploy_lambdas.sh').read_text()
    lines = script.splitlines()
    calls = [i for i, line in enumerate(lines) if 'python3 scripts/release_receipt.py' in line]
    assert len(calls) == 2
    for i in calls:
        assert not lines[i].rstrip().endswith('\\')
        assert '||' not in lines[i] + lines[i + 1]
