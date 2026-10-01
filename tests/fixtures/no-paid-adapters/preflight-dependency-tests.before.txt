"""Custom engine runners need the same dependencies as the pytest fallback."""
from pathlib import Path
import re

def test_custom_and_fallback_runners_have_declared_dependencies_before_dispatch():
    root=Path(__file__).resolve().parents[2]
    text=(root/'.github/workflows/deploy-lambdas.yml').read_text(encoding='utf-8')
    body=text.split('- name: Run deployment preflight tests',1)[1].split('- name:',1)[0]
    before=body.split('for fn in',1)[0]
    installed=set()
    for line in before.splitlines():
        if 'python3 -m pip install ' in line:
            installed.update(re.findall(r'\b[a-z][a-z0-9_-]*\b',line.split('pip install ',1)[1]))
    assert {'boto3','pytest','jsonschema'}<=installed,'Both custom and fallback runners need installed dependencies'
    assert 'python3 "$dir/tests/run_tests.py"' in body
    assert 'python3 -m pytest -q "$dir/tests" -x' in body
