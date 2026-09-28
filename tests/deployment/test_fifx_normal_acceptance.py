from pathlib import Path
import ast
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_fifx_acceptance_read_scope_and_runtime_guard():
    result = subprocess.run([sys.executable,str(ROOT/'tests/test_fifx_normal_acceptance.py')],
                            cwd=ROOT,capture_output=True,text=True,timeout=30)
    assert result.returncode == 0, result.stdout+result.stderr


def test_fifx_normal_probe_cannot_invoke_write_or_acquire():
    path = ROOT/'aws/ops/staged/ops_6315_fifx_normal_publication.py'
    if not path.exists():path = ROOT/'aws/ops/STAGED/ops_6315_fifx_normal_publication.py'
    source = path.read_text(encoding='utf-8'); tree = ast.parse(source)
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','get_parameter','get_secret_value','acquire','update_schedule','put_rule'}
    assert 'sys.exit(1)' in source and 'store.replay(packet, read)' in source
