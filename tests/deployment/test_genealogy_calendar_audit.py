from pathlib import Path
import ast,runpy,subprocess,sys,tempfile
ROOT=Path(__file__).resolve().parents[2]

def test_whole_genealogy_predecessor_calendar_scan_and_inference_counterexamples():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_calendar_audit.py')],cwd=ROOT,capture_output=True,text=True,timeout=30)
    assert result.returncode==0,result.stdout+result.stderr

def test_genealogy_runtime_baseline_has_no_ledger_provider_or_mutation_path():
    path=ROOT/'aws/ops/staged/ops_6302_genealogy_runtime_baseline.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6302_genealogy_runtime_baseline.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls.intersection({'invoke','scan','query','get_object','put_object','put_item','update_function_configuration','update_schedule','put_rule','urlopen'})
    assert 'learning_ledger_reads=0' in source and 'downstream_output_reads=0' in source and 'sys.exit(1)' in source
    assert "('lambda','s3','events','scheduler')" in source
    module=runpy.run_path(str(path));main=module['main'];state=main.__globals__;original=state['ROOT']
    try:
        with tempfile.TemporaryDirectory() as name:
            root=Path(name);file=root/'aws/lambdas/justhodl-signal-genealogy/source/lambda_function.py'
            file.parent.mkdir(parents=True);file.write_bytes(b'changed baseline')
            state['ROOT']=root
            try:main()
            except ValueError as error:assert 'audited native source changed' in str(error)
            else:raise AssertionError('A changed source reached the AWS client boundary')
    finally:state['ROOT']=original
