"""Offline snapshot promotion binds the complete ZIP and forbids private I/O."""
from pathlib import Path
import copy,importlib.util,json,subprocess,tempfile,zipfile
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('snapshot_offline_candidate',ROOT/'scripts/validate_snapshot_candidate.py');validator=importlib.util.module_from_spec(spec);spec.loader.exec_module(validator)


def package(path, changed=None, extra=None):
    with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED) as archive:
        for name,source in validator.package_sources(ROOT).items():
            archive.writestr(name,b'changed reviewed source' if name==changed else source.read_bytes())
        if extra:archive.writestr(extra,b'unreviewed')


def test_snapshot_offline_zip_rejects_changed_missing_duplicate_and_extra_members():
    with tempfile.TemporaryDirectory() as directory:
        path=Path(directory)/'candidate.zip';package(path);assert validator.verify_package(ROOT,path)['source_files_checked']>3
        for changed,extra in [('lambda_function.py',None),(None,'unreviewed.py'),(None,'lambda_function.py')]:
            package(path,changed,extra)
            try:validator.verify_package(ROOT,path)
            except ValueError:pass
            else:raise AssertionError('Unreviewed package accepted')
        with zipfile.ZipFile(path,'w') as archive:archive.writestr('lambda_function.py',b'missing complete closure')
        try:validator.verify_package(ROOT,path)
        except ValueError:pass
        else:raise AssertionError('Incomplete package accepted')


def test_snapshot_offline_mode_is_fixed_to_function_schema_and_exact_configuration():
    config=json.loads((ROOT/'aws/lambdas/justhodl-portfolio-snapshot/config.json').read_bytes())
    for function,schema,changed in [('justhodl-khalid-risk',validator.SCHEMA,config),(validator.FUNCTION,'wrong',config),(validator.FUNCTION,validator.SCHEMA,{}),(validator.FUNCTION,validator.SCHEMA,{**config,'release_validation':{'schema_version':validator.SCHEMA,'mode':'offline_snapshot_v1','skip_tests':True}})]:
        try:validator.validate(function,schema,changed,Path('missing.zip'))
        except ValueError as error:assert 'configuration' in str(error)
        else:raise AssertionError('Unreviewed offline scope accepted')


def test_snapshot_offline_child_has_no_credentials_network_or_subprocess_permissions():
    source=(ROOT/'scripts/validate_snapshot_candidate.py').read_text(encoding='utf-8')
    assert 'socket.connect' in source and 'socket.getaddrinfo' in source and 'subprocess.Popen' in source and 'os.system' in source
    assert 'AWS_ACCESS_KEY_ID' not in source and 'AWS_SESSION_TOKEN' not in source
    assert 'normal_private_publication_verified' in source and 'verify_package(root, package) != proof' in source
    shell=(ROOT/'scripts/deploy_validated_candidate.sh').read_text(encoding='utf-8')
    offline=shell.split('  offline_snapshot_v1)',1)[1].split('  native_validate_only)',1)[0]
    assert 'validate_snapshot_candidate.py' in offline and 'aws lambda invoke' not in offline
    assert 'validation_mode:$validation_mode' in shell


def test_snapshot_offline_failure_cannot_claim_full_regression_success():
    from unittest.mock import patch
    config=json.loads((ROOT/'aws/lambdas/justhodl-portfolio-snapshot/config.json').read_bytes())
    with tempfile.TemporaryDirectory() as directory:
        path=Path(directory)/'candidate.zip';package(path)
        original_run=subprocess.run
        for result in [subprocess.CompletedProcess([],1,'','failure'),subprocess.CompletedProcess([],0,'partial suite','')]:
            def run(args,**kwargs):
                return result if args[0]==validator.sys.executable else original_run(args,**kwargs)
            with patch.object(validator.subprocess,'run',side_effect=run):
                try:validator.validate(validator.FUNCTION,validator.SCHEMA,config,path)
                except ValueError:pass
                else:raise AssertionError('Incomplete regression run accepted')
