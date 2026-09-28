from pathlib import Path
import ast
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_whole_native_orchestration_and_publication_reconciliation():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_native_runtime.py')],cwd=ROOT,
        capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr


def test_native_orchestration_probe_only_writes_isolated_storage():
    source=(ROOT/'aws/ops/staged/ops_6319_genealogy_native_orchestration.py').read_text(encoding='utf-8')
    tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','get_secret_value','get_parameter','update_schedule','put_rule','urlopen'}
    for n in ast.walk(tree):
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name):
            if n.func.value.id=='native' and n.func.attr=='run':
                assert isinstance(n.args[1],ast.Name) and n.args[1].id=='store'
    assert 'store=DiskStore(' in source and 'sys.exit(1)' in source
