"""A changed caller never becomes live before its governed dependencies exist."""
import json
import runpy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCOPE = runpy.run_path(str(ROOT / 'scripts/prime_governed_aliases.py'))
prime = SCOPE['targets_to_prime']
GOVERNED = SCOPE['GOVERNED_FUNCTIONS']


def test_all_selected_governed_aliases_are_primed_as_one_batch():
    assert set(prime(list(GOVERNED), {})) == GOVERNED


def test_generic_router_change_also_primes_unchanged_dependencies():
    for caller in SCOPE['CALLERS']:
        assert set(prime([caller], {})) == GOVERNED
    assert prime(['justhodl-portfolio-admin'], {}) == ['justhodl-portfolio-snapshot']


def test_configured_engine_names_and_new_validation_opt_ins_are_resolved():
    assert prime(['folder'], {'folder': {'function_name':'new-engine', 'release_validation':{'schema_version':'v1'}}}) == ['new-engine']
    assert prime(['other'], {}) == []


def test_routing_allowlist_covers_every_candidate_config():
    configured = {'justhodl-engine-fusion', 'justhodl-khalid-risk'}
    for path in (ROOT / 'aws/lambdas').glob('*/config.json'):
        config = json.loads(path.read_text())
        if not isinstance(config, dict):
            continue
        validation = config.get('release_validation')
        if isinstance(validation, dict) and validation.get('schema_version'):
            configured.add(config.get('function_name', path.parent.name))
    assert configured <= GOVERNED, 'Add newly governed engines to automated-caller routing before deployment'


def test_batch_priming_runs_after_tests_before_any_code_updates():
    workflow = (ROOT / '.github/workflows/deploy-lambdas.yml').read_text()
    assert workflow.index('- name: Run deployment preflight tests') < workflow.index('- name: Prime selected governed aliases')
    assert workflow.index('python3 scripts/prime_governed_aliases.py') < workflow.index('- name: Deploy each changed Lambda')
    assert workflow.index('- name: Deploy each changed Lambda') < workflow.index('run: bash scripts/deploy_lambdas.sh')
    assert 'aws lambda update-function-code' in (ROOT / 'scripts/deploy_lambdas.sh').read_text()
