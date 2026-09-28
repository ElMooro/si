from pathlib import Path
import ast
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def test_original_revision_cache_complete_replay_and_failure_boundaries():
    result = subprocess.run([sys.executable, str(ROOT/'tests/test_genealogy_revision_cache.py')],
                            cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout+result.stderr


def test_revision_probe_only_reads_the_approved_original_population():
    path = ROOT/'aws/ops/staged/ops_6312_genealogy_revision_cache.py'
    if not path.exists():
        path = ROOT/'aws/ops/STAGED/ops_6312_genealogy_revision_cache.py'
    source = path.read_text(encoding='utf-8')
    tree = ast.parse(source)
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not calls & {'invoke', 'put_object', 'scan', 'query', 'get_secret_value', 'get_parameter', 'update_schedule', 'put_rule', 'urlopen'}
    assert 'sys.exit(1)' in source
    assert 'expected_input' in source and 'expected_output' in source
    assert "result['aws_body_reads'] != 0" in source
