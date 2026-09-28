from pathlib import Path
import ast
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]


def test_whole_original_publication_and_tamper_boundaries():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_native_publication.py')],
        cwd=ROOT,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr


def test_retained_probe_only_writes_isolated_artifacts():
    path=ROOT/'aws/ops/staged/ops_6317_genealogy_retained_publication.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6317_genealogy_retained_publication.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','scan','query','get_secret_value','get_parameter','update_schedule','put_rule','urlopen'}
    assert 'sys.exit(1)' in source and 'store=DiskStore(' in source
    for node in ast.walk(tree):
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name):
            if node.func.value.id in ('checkpoint','publication') and node.func.attr in ('snapshot','retain','publish'):
                assert isinstance(node.args[0],ast.Name) and node.args[0].id=='store'
