"""Native snapshot acceptance must not use a private book as deployment proof."""
from pathlib import Path
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6367_portfolio_collection_runtime_acceptance.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6367_portfolio_collection_runtime_acceptance.py'


def evidence():
    return {'function_name':'justhodl-portfolio-snapshot','receipt':{'status':'matched','commit':'a'*40},'source_files_checked':3,
            'memory_mb':512,'timeout':180,'runtime':'python3.12','handler':'lambda_function.lambda_handler',
            'architectures':['x86_64'],'role':'invented-original-role','ephemeral_storage_mb':512,
            'schedules':[{'kind':'EventBridge rule','name':'invented-rule','expression':'cron(40 * * * ? *)','state':'ENABLED','native_targets':1},
                         {'kind':'EventBridge Scheduler','name':'invented-schedule','expression':'cron(40 * * * ? *)','state':'ENABLED','native_targets':1}],
            'code_sha256':'invented-hash','active_alias':{'alias':'live','version':'7','code_sha256':'invented-hash'}}


def test_snapshot_native_acceptance_requires_exact_receipt_and_complete_source():
    scope=runpy.run_path(str(PATH));base=evidence();scope['validate'](base,'a'*40,base)
    for key,value in [('receipt',{'status':'missing_predecessor_receipt'}),('receipt',{'status':'matched','commit':'b'*40}),('source_files_checked',True),('source_files_checked',0),('function_name','other'),('active_alias',{}),('active_alias',{'alias':'live','version':'7','code_sha256':'other'})]:
        bad=copy.deepcopy(base);bad[key]=value
        try:scope['validate'](bad,'a'*40,base)
        except ValueError:pass
        else:raise AssertionError('Unproven snapshot deployment accepted')


def test_snapshot_original_resources_and_schedule_bindings_are_preserved():
    scope=runpy.run_path(str(PATH));base=evidence()
    reordered=copy.deepcopy(base);reordered['schedules'].reverse();scope['validate'](reordered,'a'*40,base)
    for key,value in [('timeout',181),('memory_mb',1024),('runtime','python3.13'),('handler','other.handler'),('role','other-role'),('architectures',['arm64']),('ephemeral_storage_mb',1024),('schedules',[{'name':'unexpected'}]),('schedules',None),('schedules',[]),('schedules',[{**base['schedules'][0],'expression':'cron(41 * * * ? *)'},base['schedules'][1]])]:
        bad=copy.deepcopy(base);bad[key]=value
        try:scope['validate'](bad,'a'*40,base)
        except ValueError:pass
        else:raise AssertionError('Native settings changed: '+key)


def test_snapshot_read_capability_is_exactly_one_receipt():
    scope=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**request):calls.append(request);return {'invented':True}
    reader=scope['ReceiptOnly'](Client());request={'Bucket':scope['BUCKET'],'Key':'data/ops/releases/justhodl-portfolio-snapshot.json'}
    assert reader.get_object(**request)=={'invented':True}
    for changed in [{**request,'Key':'portfolio/snapshot.json'},{**request,'VersionId':'unreviewed'},{**request,'Bucket':'other'}]:
        try:reader.get_object(**changed)
        except ValueError:pass
        else:raise AssertionError('Unreviewed read allowed')
    assert calls==[request]


def test_snapshot_acceptance_never_invokes_or_modifies_native_resources():
    source=PATH.read_text(encoding='utf-8');calls={node.func.attr for node in ast.walk(ast.parse(source)) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','transact_write_items','put_item','update_item','update_function_code','put_rule','update_schedule'}
    assert 'sys.exit(1)' in source and 'private_reads=0' in source and 'native_invocations=0' in source


def test_unchanged_admin_redeployment_requires_exact_receipt_source_and_resources():
    scope=runpy.run_path(str(PATH));base={**evidence(),'function_name':'justhodl-portfolio-admin','source_files_checked':1,'handler_bytes':29794,'memory_mb':256,'timeout':30,'schedules':[]}
    scope['validate_admin'](base,'a'*40,base)
    for key,value in [('receipt',{'status':'matched','commit':'b'*40}),('source_files_checked',True),('handler_bytes',29795),('memory_mb',512),('timeout',180),('schedules',[{'name':'invented-rule'}]),('role','other'),('function_name','justhodl-portfolio-snapshot')]:
        bad=copy.deepcopy(base);bad[key]=value
        try:scope['validate_admin'](bad,'a'*40,base)
        except ValueError:pass
        else:raise AssertionError('Unproven changed admin deployment accepted')


def test_selected_admin_receipt_cannot_read_snapshot_or_account_data():
    scope=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**request):calls.append(request);return {}
    reader=scope['ReceiptOnly'](Client(),'justhodl-portfolio-admin')
    request={'Bucket':scope['BUCKET'],'Key':'data/ops/releases/justhodl-portfolio-admin.json'};reader.get_object(**request)
    for key in ('portfolio/snapshot.json','data/ops/releases/justhodl-portfolio-snapshot.json'):
        try:reader.get_object(**{**request,'Key':key})
        except ValueError:pass
        else:raise AssertionError('Unselected receipt or private packet read')
    assert calls==[request]
    try:scope['ReceiptOnly'](Client(),'other')
    except ValueError:pass
    else:raise AssertionError('Unreviewed function admitted')
