from pathlib import Path
import ast,runpy
ROOT=Path(__file__).resolve().parents[2]


def staged(name):
    path=ROOT/'aws/ops/staged'/name
    return path if path.exists() else ROOT/'aws/ops/STAGED'/name


def test_normal_credit_acceptance_cannot_accept_an_old_or_naive_publication():
    scope=runpy.run_path(str(staged('ops_6309_credit_normal_cadence_acceptance.py')))
    check=scope['validate_new_publication']
    check({'version':'2.1.0','generated_at':'2026-09-28T20:01:00+00:00'})
    check({'version':'2.1.0','generated_at':'2026-09-28T16:01:00-04:00'})
    for packet in ({'version':'2.0.0','generated_at':'2026-09-28T20:01:00Z'},
                   {'version':'2.1.0','generated_at':'2026-09-28T19:59:59Z'},
                   {'version':'2.1.0','generated_at':'2026-09-28T20:01:00'},{}):
        try:check(packet)
        except ValueError:pass
        else:raise AssertionError('Old/invalid publication accepted')


def test_normal_capture_requires_new_policy_and_whole_head_stability():
    source=staged('ops_6310_research_normal_capture_acceptance.py').read_text(encoding='utf-8')
    assert 'if not new:raise ValueError' in source and 'if not unchanged:raise ValueError' in source
    assert "validate_capture(capture,evidence)" in source and "validate_registered_record(doc,receipt)" in source
    assert "receipt['sha256']!=ref['sha256']" in source


def test_normal_publication_observers_have_no_mutation_or_provider_acquisition():
    for name in ('ops_6309_credit_normal_cadence_acceptance.py','ops_6310_research_normal_capture_acceptance.py'):
        source=staged(name).read_text(encoding='utf-8');tree=ast.parse(source)
        calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        assert not calls&{'invoke','put_object','update_function_code','put_rule','update_schedule','start_execution','collect','acquire','get_secret_value','get_parameter'}
        assert 'sys.exit(1)' in source
