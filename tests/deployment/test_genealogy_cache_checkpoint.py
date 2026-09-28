from pathlib import Path
import ast
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_checkpoint_restart_corruption_revision_and_atomicity():
    result = subprocess.run([sys.executable, str(ROOT/'tests/test_genealogy_cache_checkpoint.py')],
                            cwd=ROOT, capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stdout+result.stderr


def test_restart_probe_writes_only_isolated_checkpoint_transport():
    path = ROOT/'aws/ops/staged/ops_6314_genealogy_checkpoint_restart.py'
    if not path.exists():
        path = ROOT/'aws/ops/STAGED/ops_6314_genealogy_checkpoint_restart.py'
    source = path.read_text(encoding='utf-8'); tree = ast.parse(source)
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','scan','query','get_secret_value','get_parameter','update_schedule','put_rule','urlopen'}
    assert 'sys.exit(1)' in source and 'DiskStore(root/' in source
    assert "'--isolated-restart'" in source and 'client.gets != 0' in source
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Name):
            if node.func.value.id == 'checkpoint' and node.func.attr in ('snapshot','publish'):
                assert isinstance(node.args[0],ast.Name) and node.args[0].id == 'store'
