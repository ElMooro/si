from pathlib import Path
import runpy

ROOT=Path(__file__).resolve().parents[2]
ARCH=runpy.run_path(str(ROOT/'scripts/lambda_architecture.py'))['architecture']


def test_architecture_updates_require_opt_in_and_leave_legacy_metadata_alone():
    assert ARCH({'architectures':['arm64']},'create')=='arm64'
    assert ARCH({'architectures':['arm64']},'update')==''
    assert ARCH({'architectures':['x86_64'],'update_architecture':True},'update')=='x86_64'
    assert ARCH({},'create')==ARCH({},'update')==''


def test_architecture_preflight_rejects_unknown_ambiguous_or_incomplete_settings():
    for config in ({'architectures':[]},{'architectures':['x86_64','arm64']},
                   {'architectures':['other']},{'architectures':'arm64'},
                   {'update_architecture':True},{'update_architecture':'true'}):
        try:ARCH(config,'update')
        except ValueError:pass
        else:raise AssertionError('invalid architecture must block deployment')


def test_architecture_is_passed_to_code_update_and_create_only():
    shell=(ROOT/'scripts/deploy_lambdas.sh').read_text()
    code=shell.split('aws lambda update-function-code',1)[1].split('aws lambda wait',1)[0]
    create=shell.split('scripts/secret_lambda_config.py create-function',1)[1].split('aws lambda wait',1)[0]
    assert '"${architecture_update_args[@]}"' in code
    assert '"${architecture_create_args[@]}"' in create
