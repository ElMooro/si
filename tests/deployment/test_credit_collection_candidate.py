from pathlib import Path
from datetime import datetime,timedelta,timezone
import ast,copy,importlib.util,runpy

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('credit_clock_candidate_test',ROOT/'aws/ops/checks/credit_collection_clock.py')
C=importlib.util.module_from_spec(spec);spec.loader.exec_module(C)


def test_verified_weekday_slots_cross_weekends_years_leap_days_and_utc_offsets():
    for generated,next_at in [
        ('2026-09-25T20:01:00Z','2026-09-25T22:10:00+00:00'),
        ('2026-09-25T22:11:08.786018+00:00','2026-09-28T20:00:00+00:00'),
        ('2026-09-28T19:59:59Z','2026-09-28T20:00:00+00:00'),
        ('2026-09-28T20:00:00Z','2026-09-28T22:10:00+00:00'),
        ('2026-09-25T18:11:00-04:00','2026-09-28T20:00:00+00:00'),
        ('2027-12-31T22:11:00Z','2028-01-03T20:00:00+00:00'),
        ('2028-02-28T22:11:00Z','2028-02-29T20:00:00+00:00'),
        ('2026-03-06T22:11:00Z','2026-03-09T20:00:00+00:00')]:
        result=C.policy(generated);assert result['next_collection_at']==next_at
        assert C.stamp(result['pipeline_check_due_at'])-C.stamp(next_at)==timedelta(seconds=300)
        assert result['provider_release_calendar_verified'] is False and result['successful_collection_asserted'] is False


def test_next_slot_agrees_with_independent_whole_calendar_enumeration():
    # Enumerate a fixed calendar independently instead of reproducing the helper's search.
    base=datetime(2027,12,20,tzinfo=timezone.utc)
    slots=sorted(base+timedelta(days=day,minutes=minute) for day in range(90)
                 if (base+timedelta(days=day)).isoweekday()<=5 for minute in (1200,1330))
    for hour in range(0,80*24,7):
        now=base+timedelta(hours=hour,seconds=17)
        expected=min(slot for slot in slots if slot>now)
        actual=C.stamp(C.policy(now.isoformat())['next_collection_at'])
        assert actual==expected and actual>now and actual-now<timedelta(hours=70)


def sample():
    return {'contract':'credit-native-research.v1','version':'2.0.0','generated_at':'2026-09-25T22:11:00Z',
        'freshness':{'pipeline_check_due_at':'2026-09-27T10:11:00Z','valid_until':'2026-09-27T10:11:00Z'},
        'measurements':{'hy':{'observation_date':'2026-09-24','source_valid_until':'2026-09-30T00:00:00Z','value_bps':0}},
        'comparisons':{'same_date_zero':0},'replay':{'manifest_key':'old identity'},
        'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}


def test_candidate_preserves_every_measurement_but_cannot_borrow_old_replay_identity():
    original=sample();snapshot=copy.deepcopy(original);out=C.candidate(original)
    assert original==snapshot and 'replay' not in out
    assert out['measurements']==original['measurements'] and out['comparisons']==original['comparisons']
    assert out['freshness']['pipeline_check_due_at']=='2026-09-28T20:05:00+00:00'
    assert out['freshness']['valid_until']=='2026-09-28T20:05:00+00:00'
    assert C.collection_current(out,'2026-09-28T20:04:59.999999Z') is True
    assert C.collection_current(out,'2026-09-28T20:05:00Z') is False
    assert C.collection_current(original,'2026-09-28T17:00:00Z') is False
    assert all(out[k] is False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible'))
    original['measurements']['hy']['source_valid_until']='2026-09-26T00:00:00Z'
    assert C.candidate(original)['freshness']['valid_until']=='2026-09-26T00:00:00+00:00'


def test_missed_slots_and_unknown_extended_deadlines_fail_closed():
    out=C.candidate(sample())
    for mutate in [lambda p:p['freshness']['collection_policy'].update(completion_allowance_seconds=3600),
                   lambda p:p['freshness']['collection_policy'].update(weekdays=[0,1,2,3,4,5,6]),
                   lambda p:p['freshness']['collection_policy'].update(weekdays=[False,True,2,3,4]),
                   lambda p:p['freshness'].update(pipeline_check_due_at='2026-09-29T20:05:00Z'),
                   lambda p:p.update(version='2.0.0'),lambda p:p['freshness'].update(collection_policy=None),
                   lambda p:p['freshness'].pop('collection_policy')]:
        changed=copy.deepcopy(out);mutate(changed)
        try:C.collection_current(changed,'2026-09-28T17:00:00Z')
        except ValueError:pass
        else:raise AssertionError('Unreviewed extended clock accepted')
    out=C.candidate({**sample(),'generated_at':'2026-09-25T20:01:00Z'})
    assert C.collection_current(out,'2026-09-25T22:15:00Z') is False
    assert C.collection_current(out,'2026-09-28T17:00:00Z') is False,'Missing the Friday late collection must not be hidden by a weekend allowance'
    for value in (None,True,123,'2026-02-30T12:00:00Z','2026-09-25T22:11:00','20260925T221100Z'):
        try:C.policy(value)
        except (ValueError,TypeError):pass
        else:raise AssertionError('Invalid timestamp accepted')


def test_runner_candidate_is_bounded_to_clock_changes_and_read_only_evidence():
    path=ROOT/'aws/ops/staged/ops_6300_credit_clock_candidate.py'
    if not path.exists():path=ROOT/'aws/ops/STAGED/ops_6300_credit_clock_candidate.py'
    operation=runpy.run_path(str(path));p=sample();candidate=C.candidate(p)
    result=operation['check_candidate'](p,candidate,C,'2026-09-28T17:00:00Z')
    assert result['old_collection_current'] is False and result['candidate_collection_current'] is True
    assert result['candidate_published'] is False and result['production_policy_changed'] is False
    for mutate in (lambda v:v.update(replay={'manifest_key':'old'}),lambda v:v['measurements']['hy'].update(value_bps=1),
                   lambda v:v.update(calls_eligible=True),lambda v:v['freshness'].update(valid_until='2026-09-30T00:00:00Z')):
        bad=copy.deepcopy(candidate);mutate(bad)
        try:operation['check_candidate'](p,bad,C,'2026-09-28T17:00:00Z')
        except ValueError:pass
        else:raise AssertionError('Candidate changed measurements, authority or expiry without refusal')
    source=path.read_text(encoding='utf-8')
    calls={n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','update_function_code','put_rule','update_schedule','start_execution','run','collect','acquire'}
    assert 'sys.exit(1)' in source
