"""Native admin acceptance must not use a private book as deployment proof."""
from pathlib import Path
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6361_portfolio_admin_runtime_acceptance.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6361_portfolio_admin_runtime_acceptance.py'


def evidence():
    return {'function_name':'justhodl-portfolio-admin','receipt':{'status':'matched','commit':'a'*40},'source_files_checked':1,
            'memory_mb':256,'timeout':30,'runtime':'python3.12','handler':'lambda_function.lambda_handler',
            'architectures':['x86_64'],'role':'invented-original-role','ephemeral_storage_mb':512,'schedules':[]}


def test_admin_native_acceptance_requires_exact_receipt_and_complete_source():
    scope=runpy.run_path(str(PATH));base=evidence();scope['validate'](base,'a'*40,base)
    for key,value in [('receipt',{'status':'missing_predecessor_receipt'}),('receipt',{'status':'matched','commit':'b'*40}),('source_files_checked',True),('source_files_checked',0),('function_name','other')]:
        bad=copy.deepcopy(base);bad[key]=value
        try:scope['validate'](bad,'a'*40,base)
        except ValueError:pass
        else:raise AssertionError('Unproven admin deployment accepted')


def test_admin_original_resources_and_absent_schedule_are_preserved():
    scope=runpy.run_path(str(PATH));base=evidence()
    for key,value in [('timeout',31),('memory_mb',512),('runtime','python3.13'),('handler','other.handler'),('role','other-role'),('architectures',['arm64']),('ephemeral_storage_mb',1024),('schedules',[{'name':'unexpected'}])]:
        bad=copy.deepcopy(base);bad[key]=value
        try:scope['validate'](bad,'a'*40,base)
        except ValueError:pass
        else:raise AssertionError('Native settings changed: '+key)


def test_admin_read_capability_is_exactly_one_receipt():
    scope=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**request):calls.append(request);return {'invented':True}
    reader=scope['ReceiptOnly'](Client());request={'Bucket':scope['BUCKET'],'Key':'data/ops/releases/justhodl-portfolio-admin.json'}
    assert reader.get_object(**request)=={'invented':True}
    for changed in [{**request,'Key':'portfolio/snapshot.json'},{**request,'VersionId':'unreviewed'},{**request,'Bucket':'other'}]:
        try:reader.get_object(**changed)
        except ValueError:pass
        else:raise AssertionError('Unreviewed read allowed')
    assert calls==[request]


def test_admin_acceptance_never_invokes_or_modifies_native_resources():
    source=PATH.read_text(encoding='utf-8');calls={node.func.attr for node in ast.walk(ast.parse(source)) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','transact_write_items','put_item','update_item','update_function_code','put_rule','update_schedule'}
    assert 'sys.exit(1)' in source and 'private_reads=0' in source and 'native_invocations=0' in source
