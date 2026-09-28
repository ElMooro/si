from pathlib import Path
import ast,subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_genealogy_spool_whole_output_parity_coverage_and_memory_bound():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_source_fold.py')],cwd=ROOT,capture_output=True,text=True,timeout=90)
    assert result.returncode==0,result.stdout+result.stderr


def test_spooled_probe_never_mutates_aws_or_reads_outside_the_reviewed_archive():
    path=ROOT/'aws/ops/staged/ops_6311_genealogy_spooled_timing.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6311_genealogy_spooled_timing.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','scan','query','get_secret_value','get_parameter','update_schedule','put_rule','urlopen'}
    assert "range(0,len(inventory['objects']),8)" in source
    assert 'sys.exit(1)' in source
