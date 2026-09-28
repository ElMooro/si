from pathlib import Path
import ast,runpy,subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_complete_registration_chronology_has_no_arbitrary_pair_cut_or_implicit_clock_coercion():
    result=subprocess.run([sys.executable,str(ROOT/'tests/test_genealogy_registration_model.py')],cwd=ROOT,capture_output=True,text=True,timeout=45)
    assert result.returncode==0,result.stdout+result.stderr


def test_registration_chronology_acceptance_is_read_only_and_binds_the_prior_complete_population():
    path=ROOT/'aws/ops/staged/ops_6306_genealogy_chronology_candidate.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6306_genealogy_chronology_candidate.py'
    source=path.read_text(encoding='utf-8');tree=ast.parse(source)
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls.intersection({'scan','query','invoke','put_object','put_item','update_schedule','put_rule','urlopen','get_secret_value','get_parameter'})
    assert 'sys.exit(1)' in source and 'EXPECTED_PROJECTION' in source
    saved=list(sys.path)
    try:
        sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
        op=runpy.run_path(str(path))
        # An unapproved source population must fail before any body request.
        class NoRead:
            def get_object(self,**kw):raise AssertionError('Unexpected source read')
        try:op['reconcile'](NoRead(),{})
        except ValueError:pass
        else:raise AssertionError('Missing inventory entered the reader')
    finally:sys.path[:]=saved
