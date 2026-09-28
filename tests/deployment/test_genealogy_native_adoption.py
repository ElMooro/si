from copy import deepcopy
from pathlib import Path
import ast,importlib.util,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_packaged_genealogy_replays_synthetic_originals_and_all_recovery_boundaries():
    result=subprocess.run([sys.executable,str(ROOT/'aws/lambdas/justhodl-signal-genealogy/tests/run_tests.py')],
        cwd=ROOT,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr


def test_native_runtime_acceptance_is_exact_and_cannot_invoke_or_read_old_outputs():
    path=ROOT/'aws/ops/staged/ops_6321_genealogy_native_runtime_acceptance.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','scan','query','get_object','put_object','get_secret_value','update_schedule','put_rule','update_function_configuration'}
    assert 'sys.exit(1)' in source
    spec=importlib.util.spec_from_file_location('genealogy_runtime_acceptance',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    baseline=json.loads((ROOT/'docs/audit/2026-09-28/genealogy-calendar-audit.json').read_bytes())['runtime_baseline']['actual_runtime']
    actual={**baseline,'source_files_checked':17,'receipt':{'status':'matched','commit':'a'*40},'memory_mb':2048,'timeout':600}
    module.validate(actual,baseline,'a'*40)
    for field,value in (('source_files_checked',16),('memory_mb',512),('timeout',180),('schedules',[]),
        ('receipt',{'status':'matched','commit':'b'*40})):
        bad=deepcopy(actual);bad[field]=value
        try:module.validate(bad,baseline,'a'*40)
        except ValueError:pass
        else:raise AssertionError('Native acceptance allowed changed '+field)
