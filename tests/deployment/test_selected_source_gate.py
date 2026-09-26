"""A rejected source must stop the actual deployment shell before AWS access."""
import ast
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'scripts'))
import validate_lambda_sources as gate


def fixture(root, source, name='fixture', config=None):
    folder = root/'aws/lambdas'/name
    (folder/'source').mkdir(parents=True, exist_ok=True)
    (folder/'source/lambda_function.py').write_text(source, encoding='utf-8')
    (folder/'config.json').write_text(json.dumps(config or {}))
    return name


PADDING = '# complete synthetic fixture\n'*25
GOOD = PADDING+'def lambda_handler(event, context):\n    return {"statusCode": 200}\n'


def test_placeholder_and_padded_missing_handler_fail_without_execution():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        for source in ('PLACEHOLDER', PADDING+'PLACEHOLDER\n', PADDING+'raise RuntimeError("PRIVATE_CANARY")\n'):
            errors = gate.validate_sources(root, [fixture(root, source)])
            assert len(errors) == 1
            assert errors[0]['error_code'] in ('stub_body', 'module_level_callable_handler_required')
            assert 'PRIVATE_CANARY' not in json.dumps(errors)


def test_real_handlers_and_existing_aliases_work_but_noncallable_overwrites_fail():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        for source in (GOOD, PADDING+'def handler(e,c):\n    return {}\nlambda_handler = handler\n',
                       PADDING+'from reviewed import handler as lambda_handler\n'):
            assert not gate.validate_sources(root, [fixture(root, source)])
        for ending in ('lambda_handler = None', 'lambda_handler = missing', 'del lambda_handler',
                       'class lambda_handler: pass', 'async def lambda_handler(e,c): pass'):
            assert gate.validate_sources(root, [fixture(root, GOOD+ending+'\n')])[0]['error_code'] == 'module_level_callable_handler_required'


def test_source_syntax_missing_entry_and_target_traversal_fail_closed():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        for source in (PADDING+'return 1\n', PADDING+'def lambda_handler(\n'):
            error = gate.validate_sources(root, [fixture(root, source)])[0]
            assert error['error_code'] == 'invalid_source_or_configuration'
        target = fixture(root, GOOD, config={'handler':'missing.lambda_handler'})
        assert gate.validate_sources(root, [target])[0]['error_code'] == 'handler_source_missing'
        assert gate.validate_sources(root, ['../../outside'])[0]['error_code'] == 'invalid_target'


def test_shrink_override_never_bypasses_floor_or_handler_requirement():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        with patch.object(gate, 'previous_size', return_value=4000), patch.object(gate, 'shrink_override', return_value=False):
            assert gate.validate_sources(root, [fixture(root, GOOD)])[0]['error_code'] == 'unreviewed_source_shrink'
        with patch.object(gate, 'previous_size', return_value=4000), patch.object(gate, 'shrink_override', return_value=True):
            assert not gate.validate_sources(root, [fixture(root, GOOD)])
            assert gate.validate_sources(root, [fixture(root, 'PLACEHOLDER')])[0]['error_code'] == 'stub_body'
            assert gate.validate_sources(root, [fixture(root, PADDING+'PLACEHOLDER')])[0]['error_code'] == 'module_level_callable_handler_required'


def test_selected_batch_checks_every_target_without_blocking_unselected_source():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        good = fixture(root, GOOD, 'complete')
        bad = fixture(root, 'PLACEHOLDER', 'incomplete')
        assert not gate.validate_sources(root, [good])
        errors = gate.validate_sources(root, [good, bad])
        assert len(errors) == 1 and errors[0]['function'] == bad


def test_real_deploy_shell_rejects_entire_batch_before_any_aws_command():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp); (root/'scripts').mkdir(); (root/'bin').mkdir()
        for name in ('deploy_lambdas.sh','validate_lambda_sources.py','guard_stub_lambdas.py','lambda_identity.py'):
            shutil.copyfile(ROOT/'scripts'/name, root/'scripts'/name)
        # Reaching the next metadata validation is itself a failure. It precedes
        # configuration/environment and all Lambda APIs in the real shell.
        (root/'scripts/validate_lambda_configs.py').write_text('from pathlib import Path\nPath("gate-bypassed").write_text("bad")\nraise SystemExit(99)\n')
        (root/'bin/aws').write_text('#!/usr/bin/env bash\nprintf invoked >> aws-called\nexit 98\n', newline='\n')
        (root/'bin/aws').chmod(0o755)
        fixture(root, GOOD, 'complete'); fixture(root, 'PLACEHOLDER', 'incomplete')
        env = {**os.environ, 'DEPLOY_TARGETS':'complete incomplete', 'DEPLOY_AWS_REGION':'us-east-1',
               'PATH':str(root/'bin')+os.pathsep+os.environ['PATH']}
        result = subprocess.run(['bash', 'scripts/deploy_lambdas.sh'], cwd=root, env=env, capture_output=True, text=True)
        assert result.returncode == 1, (result.returncode, result.stdout, result.stderr)
        assert 'stub_body' in result.stdout
        assert not (root/'aws-called').exists() and not (root/'gate-bypassed').exists()


def test_source_gate_is_in_preflight_and_transaction_before_configuration():
    shell = (ROOT/'scripts/deploy_lambdas.sh').read_text()
    workflow = (ROOT/'.github/workflows/deploy-lambdas.yml').read_text()
    assert shell.index('scripts/validate_lambda_sources.py') < shell.index('scripts/validate_lambda_configs.py') < shell.index('lambda_config_environment.py')
    assert 'python3 scripts/validate_lambda_sources.py ${{ steps.detect.outputs.targets }}' in workflow
    assert "- 'scripts/validate_lambda_sources.py'" in workflow
