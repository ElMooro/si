from pathlib import Path
import ast,subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_capture_timing_preserves_censoring_ambiguity_and_partial_identity_coverage():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_capture_timing.py')],cwd=ROOT,capture_output=True,text=True,timeout=45)
    assert result.returncode==0,result.stdout+result.stderr


def test_capture_interval_probe_is_fixed_population_and_read_only():
    path=ROOT/'aws/ops/staged/ops_6307_genealogy_capture_intervals.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6307_genealogy_capture_intervals.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls.intersection({'scan','query','invoke','put_object','put_item','update_schedule','put_rule','urlopen','get_secret_value','get_parameter'})
    assert 'CAPTURE_HASH' in source and 'sys.exit(1)' in source
