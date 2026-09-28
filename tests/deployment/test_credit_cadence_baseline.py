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


def test_schedule_diagnostic_records_mismatch_without_accepting_the_baseline():
    path=PATH.with_name('ops_6298_credit_schedule_diagnostic.py');diagnostic=runpy.run_path(str(path))
    actual={'receipt':{'status':'matched','commit':NS['EXPECTED']},'function_name':NS['FN'],'timeout':300,'memory_mb':512,
            'schedules':[{'state':'ENABLED','expression':'different actual cron','native_targets':1}]}
    result=diagnostic['summarize'](actual,NS)
    assert result['actual_runtime']==actual and result['strict_baseline_matches'] is False
    assert result['baseline_accepted'] is False and result['retained_originals_replayed'] is False
    assert result['strict_baseline_refusal']=='One original weekday 22:10 UTC schedule required'
    actual['receipt']['commit']='0'*40
    try:diagnostic['summarize'](actual,NS)
    except ValueError:pass
    else:raise AssertionError('Different deployed release accepted')
    calls={n.func.attr for n in ast.walk(ast.parse(path.read_text(encoding='utf-8'))) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','update_function_code','put_rule','update_schedule','start_execution','run','collect','acquire'}


def test_observed_credit_inventory_requires_both_exact_existing_bindings():
    path=PATH.with_name('ops_6299_credit_observed_baseline.py');observer=runpy.run_path(str(path))
    actual={'receipt':{'status':'matched','commit':NS['EXPECTED']},'function_name':NS['FN'],'timeout':300,'memory_mb':512,
        'schedules':[{'kind':kind,'name':name,'expression':cron,'timezone':tz,'group':group,'state':'ENABLED','native_targets':1}
                     for kind,name,cron,tz,group in observer['BINDINGS']]}
    observer['validate_observed_runtime'](actual)
    reversed_order=copy.deepcopy(actual);reversed_order['schedules'].reverse();observer['validate_observed_runtime'](reversed_order)
    bad=[]
    for field,value in [('name','unexpected'),('expression','cron(0 20 * * ? *)'),('timezone','America/New_York'),
                        ('group','different'),('state','DISABLED'),('native_targets',2)]:
        changed=copy.deepcopy(actual);changed['schedules'][1][field]=value;bad.append(changed)
    for schedules in [actual['schedules'][:1],actual['schedules']*2,[]]:
        changed=copy.deepcopy(actual);changed['schedules']=schedules;bad.append(changed)
    changed=copy.deepcopy(actual);changed['receipt']['commit']='0'*40;bad.append(changed)
    for changed in bad:
        try:observer['validate_observed_runtime'](changed)
        except ValueError:pass
        else:raise AssertionError('Changed observed credit schedule accepted')
    calls={n.func.attr for n in ast.walk(ast.parse(path.read_text(encoding='utf-8'))) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','update_function_code','put_rule','update_schedule','start_execution','run','collect','acquire'}
    assert 'sys.exit(1)' in path.read_text(encoding='utf-8')
