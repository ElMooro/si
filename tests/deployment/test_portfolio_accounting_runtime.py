"""The accounting baseline accepts only two complete read-only native releases."""
from pathlib import Path
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6359_portfolio_accounting_runtime_acceptance.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6359_portfolio_accounting_runtime_acceptance.py'


def evidence(function):
    return {'function_name':function,'receipt':{'status':'matched','commit':'a'*40},'source_files_checked':3,
        'memory_mb':512 if function.endswith('snapshot') else 256,'timeout':180 if function.endswith('snapshot') else 30,
        'runtime':'python3.12','handler':'lambda_function.lambda_handler','architectures':['x86_64'],'role':'invented-role','ephemeral_storage_mb':512,
        'schedules':[{'kind':'EventBridge rule','name':'justhodl-portfolio-snapshot-hourly','state':'ENABLED','expression':'cron(40 * * * ? *)','native_targets':1}] if function.endswith('snapshot') else []}


def test_accounting_receipt_wrapper_refuses_private_or_extra_reads_before_client():
    scope=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**request):calls.append(request);return 'invented receipt'
    client=scope['ReceiptsOnly'](Client())
    for function in scope['FUNCTIONS']:client.get_object(Bucket=scope['BUCKET'],Key='data/ops/releases/'+function+'.json')
    for key in ['portfolio/snapshot.json','portfolio/risk.json','screener/alpha-score.json','data/ops/releases/justhodl-portfolio-risk.json']:
        try:client.get_object(Bucket=scope['BUCKET'],Key=key)
        except ValueError:pass
        else:raise AssertionError('Unreviewed read accepted')
    assert len(calls)==2


def test_accounting_runtime_rejects_source_receipt_resource_and_schedule_drift():
    scope=runpy.run_path(str(PATH));validate=scope['validate']
    for function in scope['FUNCTIONS']:
        original=evidence(function);validate(original);validate(original,'a'*40,original)
        for key,value in [('function_name','other'),('source_files_checked',True),('source_files_checked',4),('memory_mb',128),('timeout',60),('role','changed-role'),('receipt',{'status':'matched','commit':'b'*40}),('schedules',[{'kind':'EventBridge rule','name':'new-rule'}])]:
            actual=copy.deepcopy(original);actual[key]=value
            try:validate(actual,'a'*40,original)
            except ValueError:pass
            else:raise AssertionError('Drift accepted: '+key)


def test_accounting_operation_never_invokes_or_mutates_native_resources():
    source=PATH.read_text(encoding='utf-8');calls={node.func.attr for node in ast.walk(ast.parse(source)) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','transact_write_items','update_function_code','put_rule','update_schedule','start_execution'}
    assert 'sys.exit(1)' in source and 'native_invocations=0' in source and 'private_reads=0' in source
