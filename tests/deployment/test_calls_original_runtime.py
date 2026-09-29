from copy import deepcopy
from pathlib import Path
import ast,importlib.util
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6325_calls_original_runtime_acceptance.py'


def loaded():
    spec=importlib.util.spec_from_file_location('calls_original_acceptance',PATH)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def test_calls_original_acceptance_has_only_exact_receipt_storage_access():
    probe=loaded()
    class Client:
        def __init__(self):self.reads=[]
        def get_object(self,**kw):self.reads.append(kw);return {}
    underlying=Client();client=probe.ReceiptsOnly(underlying)
    for fn in probe.SCHEDULES:client.get_object(Bucket=probe.BUCKET,Key='data/ops/releases/'+fn+'.json')
    for key in ('data/ai-brief-public.json','data/decisive-call-history.json','data/prospective-outcomes.json',
                'data/research-forecasts/records/a.json','private/account.json','data/ops/releases/other.json'):
        try:client.get_object(Bucket=probe.BUCKET,Key=key)
        except ValueError:pass
        else:raise AssertionError(key)
    assert len(underlying.reads)==6 and not hasattr(client,'put_object')
    tree=ast.parse(PATH.read_text(encoding='utf-8'))
    calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls&{'invoke','put_object','get_secret_value','get_parameter','update_schedule','put_rule'}
    assert 'sys.exit(1)' in PATH.read_text(encoding='utf-8')


def test_calls_original_acceptance_rejects_wrong_receipt_or_schedule():
    probe=loaded();expected='a'*40
    for fn,bindings in probe.SCHEDULES.items():
        value={'function_name':fn,'receipt':{'status':'matched','commit':expected},'source_files_checked':10,'memory_mb':512,'timeout':180,
               'schedules':[{'name':name,'expression':expression or 'rate(1 day)','state':'ENABLED','native_targets':1} for name,expression in bindings.items()]}
        probe.validate(value,fn,expected)
        for change in ('commit','schedule','state','target','duplicate','identity','closure'):
            bad=deepcopy(value)
            if change=='commit':bad['receipt']['commit']='b'*40
            elif change=='schedule':bad['schedules'][0]['expression']=''
            elif change=='state':bad['schedules'][0]['state']='DISABLED'
            elif change=='target':bad['schedules'][0]['native_targets']=0
            elif change=='duplicate':bad['schedules']+=deepcopy(bad['schedules'])
            elif change=='identity':bad['function_name']='other'
            else:bad['source_files_checked']=1
            try:probe.validate(bad,fn,expected)
            except ValueError:pass
            else:raise AssertionError((fn,change))
