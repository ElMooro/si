from pathlib import Path
import ast
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]


def test_complete_streamed_pipeline_parity_and_interruption_boundaries():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_streamed_pipeline.py')],
                          cwd=ROOT,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr


def test_streamed_pipeline_probe_cannot_write_or_invoke_aws():
    path=ROOT/'aws/ops/staged/ops_6313_genealogy_streamed_pipeline.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6313_genealogy_streamed_pipeline.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','scan','query','get_secret_value','get_parameter','update_schedule','put_rule','urlopen'}
    assert 'sys.exit(1)' in source
    assert "client.gets-before_gets != 0" in source
