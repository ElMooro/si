from pathlib import Path
import ast
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_complete_public_genealogy_archive_reconciliation_and_corruption_cases():
    result = subprocess.run([sys.executable, str(ROOT/'tests/test_genealogy_public_archive.py')],
                            cwd=ROOT, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout+result.stderr


def test_genealogy_archive_candidate_has_no_private_native_or_mutation_path():
    operation = ROOT/'aws/ops/staged/ops_6304_genealogy_public_archive_reconcile.py'
    if not operation.exists(): operation = ROOT/'aws/ops/STAGED/ops_6304_genealogy_public_archive_reconcile.py'
    for path in (operation, ROOT/'aws/ops/checks/genealogy_public_archive.py'):
        source = path.read_text(encoding='utf-8')
        calls = {n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        assert not calls.intersection({'scan', 'query', 'invoke', 'put_object', 'put_item', 'update_schedule', 'put_rule', 'urlopen', 'get_secret_value', 'get_parameter'})
    assert 'sys.exit(1)' in operation.read_text(encoding='utf-8')
