"""The research runtime verifier can only read exact releases and code/resources."""
from pathlib import Path
import ast,copy,runpy
ROOT=Path(__file__).resolve().parents[2]
PATH=ROOT/'aws/ops/staged/ops_6349_calls_byte_proof_runtime_acceptance.py'
if not PATH.exists():PATH=ROOT/'aws/ops/STAGED/ops_6349_calls_byte_proof_runtime_acceptance.py'


def evidence(fn):
    resources={'justhodl-ai-brief':(1536,300),'justhodl-calls-research-audit':(1536,300),'justhodl-liquidity-flow':(256,120),'justhodl-prospective-evaluator':(512,300),'justhodl-signal-harvester':(1024,900)}
    return {'function_name':fn,'receipt':{'status':'matched','commit':'a'*40},'source_files_checked':8,
        'memory_mb':resources[fn][0],'timeout':resources[fn][1],'runtime':'python3.12','handler':'lambda_function.lambda_handler',
        'architectures':['x86_64'],'role':'synthetic-role','ephemeral_storage_mb':512,
        'schedules':[{'name':fn+'-synthetic','kind':'EventBridge rule','expression':'cron(0 1 * * ? *)',
        'state':'ENABLED','native_targets':1}]}



def test_calls_byte_proof_acceptance_rejects_packet_reads_before_client_call():
    scope=runpy.run_path(str(PATH));calls=[]
    class Client:
        def get_object(self,**kw):calls.append(kw);return 'synthetic-receipt'
    client=scope['ReceiptsOnly'](Client())
    for fn in scope['FUNCTIONS']:assert client.get_object(Bucket=scope['BUCKET'],Key='data/ops/releases/'+fn+'.json')=='synthetic-receipt'
    for kw in [dict(Bucket=scope['BUCKET'],Key='data/prospective-outcomes.json'),dict(Bucket='wrong',Key='data/ops/releases/justhodl-liquidity-flow.json'),dict(Bucket=scope['BUCKET'],Key='data/ops/releases/justhodl-liquidity-flow.json',VersionId='v')]:
        try:client.get_object(**kw)
        except ValueError:pass
        else:raise AssertionError('Non-receipt read accepted')
    assert len(calls)==1


def test_calls_byte_proof_runtime_requires_exact_commit_source_closure_and_original_settings():
    scope=runpy.run_path(str(PATH));validate=scope['validate']
    for fn in scope['FUNCTIONS']:
        original=evidence(fn);validate(original,fn);validate(original,fn,'a'*40,original)
        for field,value in [('function_name','other'),('source_files_checked',True),('source_files_checked',7),('memory_mb',999),('timeout',90),('role','other-role'),('schedules',[]),('receipt',{'status':'matched','commit':'b'*40})]:
            bad=copy.deepcopy(original);bad[field]=value
            try:validate(bad,fn,'a'*40,original)
            except ValueError:pass
            else:raise AssertionError('Native drift accepted: '+field)


def test_calls_byte_proof_runtime_operation_has_no_invocation_or_storage_mutation():
    source=PATH.read_text(encoding='utf-8-sig');tree=ast.parse(source)
    attrs={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not attrs & {'invoke','put_object','delete_object','update_function_code','put_rule','update_schedule','start_execution'}
    assert 'sys.exit(1)' in source and 'current_packet_reads=0' in source and 'provider_requests=0' in source
