"""Portfolio risk acceptance is restricted to one exact native release and runtime."""
from pathlib import Path
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6358_portfolio_snapshot_runtime_acceptance.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6358_portfolio_snapshot_runtime_acceptance.py'


def evidence():
    return {'function_name':'justhodl-portfolio-snapshot','receipt':{'status':'matched','commit':'a'*40},'source_files_checked':7,
        'memory_mb':512,'timeout':180,'runtime':'python3.12','handler':'lambda_function.lambda_handler',
        'architectures':['x86_64'],'role':'synthetic-role','ephemeral_storage_mb':512,
        'schedules':[{'name':'justhodl-portfolio-snapshot-hourly','kind':'EventBridge rule','expression':'cron(40 * * * ? *)',
        'state':'ENABLED','native_targets':1}]}


def test_portfolio_snapshot_acceptance_rejects_nonreceipt_reads_before_client_call():
    scope=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**kw):calls.append(kw);return 'synthetic-receipt'
    client=scope['ReceiptsOnly'](Client());key='data/ops/releases/justhodl-portfolio-snapshot.json'
    assert client.get_object(Bucket=scope['BUCKET'],Key=key)=='synthetic-receipt'
    for kw in [dict(Bucket=scope['BUCKET'],Key='portfolio/risk.json'),dict(Bucket='wrong',Key=key),dict(Bucket=scope['BUCKET'],Key=key,VersionId='v'),dict(Bucket=scope['BUCKET'],Key='data/ops/releases/justhodl-ciss-stress.json')]:
        try:client.get_object(**kw)
        except ValueError:pass
        else:raise AssertionError('Unreviewed read accepted')
    assert len(calls)==1


def test_portfolio_snapshot_runtime_rejects_commit_source_resource_and_schedule_drift():
    scope=runpy.run_path(str(PATH));validate=scope['validate'];original=evidence();baseline=copy.deepcopy(original);baseline['source_files_checked']=7;validate(original);validate(original,'a'*40,baseline)
    for field,value in [('function_name','other'),('source_files_checked',True),('source_files_checked',6),('source_files_checked',8),('memory_mb',256),('timeout',90),('role','other-role'),('schedules',[]),('receipt',{'status':'matched','commit':'b'*40})]:
        bad=copy.deepcopy(original);bad[field]=value
        try:validate(bad,'a'*40,baseline)
        except ValueError:pass
        else:raise AssertionError('Native drift accepted: '+field)
    for key,value in [('expression','cron(0 * * * ? *)'),('state','DISABLED'),('native_targets',2)]:
        bad=copy.deepcopy(original);bad['schedules'][0][key]=value
        try:validate(bad)
        except ValueError:pass
        else:raise AssertionError('Invalid primary schedule accepted: '+key)


def test_portfolio_snapshot_runtime_operation_has_no_invocation_or_storage_mutation():
    source=PATH.read_text(encoding='utf-8-sig');attrs={n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not attrs & {'invoke','put_object','delete_object','update_function_code','put_rule','update_schedule','start_execution'}
    assert 'sys.exit(1)' in source and 'current_packet_reads=0' in source and 'provider_requests=0' in source
