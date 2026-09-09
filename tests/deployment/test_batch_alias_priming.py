"""A changed caller never becomes live before its governed dependencies exist."""
import json
import runpy
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
SCOPE = runpy.run_path(str(ROOT / 'scripts/prime_governed_aliases.py'))
prime = SCOPE['targets_to_prime']
GOVERNED = SCOPE['GOVERNED_FUNCTIONS']
NEW = 'justhodl-public-archive-index'


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


def approved_config():
    return json.loads((ROOT/'aws/lambdas'/NEW/'config.json').read_text())


class Inventory:
    def __init__(self, missing=(), error='ResourceNotFoundException'):
        self.missing=set(missing);self.error=error;self.reads=[]
    def get_function_configuration(self, FunctionName):
        self.reads.append(FunctionName)
        if FunctionName in self.missing:
            exc=RuntimeError('SDK detail must not escape')
            exc.response={'Error':{'Code':self.error}}
            raise exc
        return {'State':'Active','LastUpdateStatus':'Successful'}


def test_only_selected_approved_source_with_explicit_opt_in_can_bootstrap_after_absence_proof():
    lam=Inventory([NEW]);protected=[]
    def protect(*args):
        protected.append(args[-1]);return {'function':args[-1],'protected_version':'1'}
    rows=SCOPE['prime_batch']([NEW],{NEW:approved_config()},[lam,None,None],protect)
    assert lam.reads==[NEW] and not protected
    assert rows==[{'function':NEW,'phase':'validated_bootstrap_pending','absence_verified':True,
                  'schema_version':'public-engine-archive-index.v1',
                  'requires':'create_then_pinned_validation_before_live_alias_and_schedule'}]


def test_missing_established_dependencies_still_fail_before_any_alias_mutation():
    for missing in SCOPE['ESTABLISHED_DEPENDENCIES']:
        lam=Inventory([missing]);calls=[]
        config={NEW:approved_config(),missing:{'function_name':missing,'handler':'lambda_function.lambda_handler',
                    'release_validation':{'schema_version':'public-engine-archive-index.v1','bootstrap_if_absent':True}}}
        try:SCOPE['prime_batch'](['justhodl-scheduler',NEW],config,[lam,None,None],lambda *args:calls.append(args))
        except RuntimeError as exc:assert 'dependency is absent' in str(exc)
        else:raise AssertionError('Missing established dependency bypassed priming')
        assert calls==[]


def test_absence_permission_error_and_unselected_or_unapproved_services_never_bootstrap():
    scenarios=[([NEW],{NEW:{**approved_config(),'release_validation':{'schema_version':'public-engine-archive-index.v1'}}},NEW,'ResourceNotFoundException'),
               (['justhodl-scheduler'],{},NEW,'ResourceNotFoundException'),
               (['unapproved'],{'unapproved':{'release_validation':{'schema_version':'v1','bootstrap_if_absent':True}}},'unapproved','ResourceNotFoundException'),
               ([NEW],{NEW:approved_config()},NEW,'AccessDeniedException')]
    for selected,configs,missing,error in scenarios:
        calls=[]
        try:SCOPE['prime_batch'](selected,configs,[Inventory([missing],error),None,None],lambda *args:calls.append(args))
        except RuntimeError as exc:assert 'SDK detail' not in str(exc)
        else:raise AssertionError('Unproved or unapproved bootstrap allowed')
        assert calls==[]


def test_missing_source_wrong_schema_or_remapped_folder_cannot_claim_approved_bootstrap():
    with tempfile.TemporaryDirectory() as temp:
        assert not SCOPE['bootstrap_eligible'](NEW,[NEW],{NEW:approved_config()},Path(temp))
    config=approved_config();config['release_validation']['schema_version']='unreviewed.v1'
    assert not SCOPE['bootstrap_eligible'](NEW,[NEW],{NEW:config},ROOT)
    assert not SCOPE['bootstrap_eligible'](NEW,['different-folder'],{'different-folder':approved_config()},ROOT)


def test_existing_new_service_uses_normal_protection_and_generic_callers_route_it_live():
    calls=[]
    rows=SCOPE['prime_batch']([NEW],{NEW:approved_config()},[Inventory(),None,None],
                             lambda *args:calls.append(args[-1]) or {'function':args[-1],'protected_version':'7'})
    assert calls==[NEW] and rows[0]['protected_version']=='7'
    assert NEW in GOVERNED and NEW not in SCOPE['ESTABLISHED_DEPENDENCIES']
    assert NEW in prime(['justhodl-scheduler'],{})


def test_new_index_actual_validation_envelope_satisfies_pinned_candidate_protocol():
    scope=runpy.run_path(str(ROOT/'aws/lambdas'/NEW/'tests/run_tests.py'))
    fixture=scope['HandlerTests']('test_validate_only_runs_real_listings_but_zero_publication')
    fixture.setUp()
    result=fixture.mod.lambda_handler({'mode':'validate_only'},None)
    assert result['ok'] is True and result['validation_only'] is True
    assert result['schema_version']==approved_config()['release_validation']['schema_version']
    assert isinstance(result['status'],str) and result['status']
    assert isinstance(result['artifact_size_bytes'],int) and result['artifact_size_bytes']>=0
    assert fixture.s3.writes=={}
    shell=(ROOT/'scripts/deploy_lambdas.sh').read_text()
    assert shell.index('scripts/secret_lambda_config.py create-function') < shell.index('bash scripts/deploy_validated_candidate.sh')
    candidate=(ROOT/'scripts/deploy_validated_candidate.sh').read_text()
    assert candidate.index('aws lambda invoke') < candidate.index('promoted_alias_revision=$(aws lambda create-alias') < candidate.index('aws scheduler create-schedule')
