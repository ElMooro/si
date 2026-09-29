from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]


def test_actual_frozen_calls_producer_to_morning_boundary():
    run=subprocess.run([sys.executable,str(ROOT/'tests/calls_consumer_boundary.py')],cwd=ROOT,
        capture_output=True,text=True,timeout=60)
    assert run.returncode==0,run.stderr


def test_morning_acceptance_reads_one_receipt_and_no_consumer_or_recipient():
    import ast,importlib.util
    path=ROOT/'aws/ops/staged/ops_6324_morning_brief_contract_acceptance.py'
    spec=importlib.util.spec_from_file_location('morning_contract_acceptance',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    class Storage:
        def __init__(self):self.reads=[]
        def get_object(self,**kw):self.reads.append(kw);return {}
    storage=Storage();client=module.ReceiptOnly(storage)
    client.get_object(Bucket=module.BUCKET,Key='data/ops/releases/'+module.FUNCTION+'.json')
    for key in ('data/ai-brief-public.json','data/morning-intelligence.json','private/account.json',
                'data/prospective-outcomes.json','data/decisive-call-history.json','data/ops/releases/other.json'):
        try:client.get_object(Bucket=module.BUCKET,Key=key)
        except ValueError:pass
        else:raise AssertionError(key)
    assert len(storage.reads)==1 and not hasattr(client,'put_object')
    methods={n.func.attr for n in ast.walk(ast.parse(path.read_text(encoding='utf-8')))
        if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not methods&{'invoke','put_object','get_secret_value','get_parameter','send_message','put_rule','update_schedule'}
    good={'function_name':module.FUNCTION,'source_files_checked':5,'receipt':{'status':'matched','commit':'a'*40},
          'schedules':[{'name':'justhodl-morning-brief-daily','state':'ENABLED','native_targets':1,'expression':'fixture-only'}]}
    module.validate(good,'a'*40)
    try:module.validate(good,'b'*40)
    except ValueError:pass
    else:raise AssertionError('Wrong receipt accepted')
    assert 'sys.exit(1)' in path.read_text(encoding='utf-8')
