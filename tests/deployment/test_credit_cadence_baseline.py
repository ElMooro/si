from pathlib import Path
from datetime import datetime,timezone
import ast,copy,runpy

ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6297_credit_cadence_baseline.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6297_credit_cadence_baseline.py'
NS=runpy.run_path(str(PATH))


def test_weekend_deadline_diagnosis_does_not_extend_currentness():
    packet={'generated_at':'2026-09-25T22:11:00Z','as_of':'2026-09-24',
            'freshness':{'pipeline_check_due_at':'2026-09-27T10:11:00Z','valid_until':'2026-09-27T10:11:00Z'},
            'measurements':{'recent':{'observation_date':'2026-09-24'},'old':{'observation_date':'2026-09-01'},'absent':{}},'quality':{'status':'fresh'}}
    result=NS['diagnosis'](packet,datetime(2026,9,28,16,tzinfo=timezone.utc))
    assert result['pipeline_deadline_on_unscheduled_day'] is True and result['pipeline_expired_at_evaluation'] is True
    assert result['rows_within_existing_five_calendar_day_ceiling']==1 and result['dated_rows']==2
    assert result['quality_status_at_publication']=='fresh' and result['currentness_not_extended'] is True
    assert result['calls_eligible'] is False and result['sizing_eligible'] is False


def test_credit_baseline_rejects_changed_resources_duplicate_triggers_and_timezones():
    base={'receipt':{'status':'matched','commit':NS['EXPECTED']},'function_name':NS['FN'],'timeout':300,'memory_mb':512,
          'schedules':[{'state':'ENABLED','expression':NS['CRON'],'native_targets':1}]}
    NS['validate_runtime'](base)
    bad=[]
    for field,value in [('timeout',301),('memory_mb',1024),('receipt',{'status':'missing'})]:
        item=copy.deepcopy(base);item[field]=value;bad.append(item)
    item=copy.deepcopy(base);item['schedules']*=2;bad.append(item)
    item=copy.deepcopy(base);item['schedules'][0]['timezone']='America/New_York';bad.append(item)
    for item in bad:
        try:NS['validate_runtime'](item)
        except ValueError:pass
        else:raise AssertionError('Changed original cadence/runtime accepted')


def test_credit_observer_has_no_mutation_or_provider_collection_calls():
    tree=ast.parse(PATH.read_text(encoding='utf-8'))
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','update_function_code','put_rule','update_schedule','start_execution','run','collect','acquire'}
    assert 'sys.exit(1)' in PATH.read_text(encoding='utf-8')
