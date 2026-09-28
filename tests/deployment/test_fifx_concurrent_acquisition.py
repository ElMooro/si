from pathlib import Path
import ast
import hashlib
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]


def test_native_fifx_collection_and_full_original_replay():
    result=subprocess.run([sys.executable,str(ROOT/'aws/lambdas/justhodl-fifx-vol-migration/tests/run_tests.py')],
        cwd=ROOT,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr


def test_exact_whole_sequential_predecessors_are_preserved():
    # Whole bytes compared with bdfd0c10bc locally before pinning. The deployment
    # checkout is shallow; a regression must not depend on fetching old commits.
    expected={'acquire':(1959,'d4b235db54e6a8dca1c43010865b5550efe06aeb9ce17735e41c91b9891256af'),
              'store':(17465,'e67c1d4fc51a5b64c4a76dc24e69828e1cdc68a808684525ae002e07181afa4d'),
              'tests':(14829,'f5676e23a9185a559f8ed2f547791a68779c4f5ad30b6ef85f48db4276469a99')}
    for name,proof in expected.items():
        raw=(ROOT/'tests/fixtures'/('pre-fifx-concurrent-'+name+'.py.txt')).read_bytes()
        assert (len(raw),hashlib.sha256(raw).hexdigest())==proof


def test_repaired_runtime_probe_cannot_force_new_publication():
    path=ROOT/'aws/ops/staged/ops_6316_fifx_concurrent_runtime.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6316_fifx_concurrent_runtime.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','get_parameter','get_secret_value','acquire','update_schedule','put_rule'}
    assert 'sys.exit(1)' in source and 'repaired_normal_publication_verified=False' in source
