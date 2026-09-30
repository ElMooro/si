"""Complete native acceptance and receipt-only deployment scope."""
from pathlib import Path
import ast,copy,hashlib,json,runpy
ROOT=Path(__file__).resolve().parents[2]
OP=ROOT/'aws/ops/staged/ops_6369_portfolio_snapshot_bytes_runtime_acceptance.py'

def runtime(function):
    snapshot=function.endswith('snapshot')
    return {'function_name':function,'receipt':{'status':'matched','commit':'a'*40},'source_files_checked':5 if snapshot else 1,
        'memory_mb':512 if snapshot else 256,'timeout':180 if snapshot else 30,'runtime':'python3.12','handler':'lambda_function.lambda_handler',
        'architectures':['x86_64'],'role':'invented-original-role','ephemeral_storage_mb':512,'schedules':[],
        'code_sha256':'invented-whole-package','active_alias':{'alias':'live','version':'14','code_sha256':'invented-whole-package'}}

def rejected(action):
    try:action()
    except ValueError:pass
    else:raise AssertionError('Unproven native state was accepted')

def test_whole_new_source_closures_require_the_intended_receipts():
    s=runpy.run_path(str(OP))
    assert s['FUNCTIONS']=={'justhodl-portfolio-snapshot':(5,512,180),'justhodl-portfolio-admin':(1,256,30)}
    for fn in s['FUNCTIONS']:
        base=runtime(fn);s['validate'](base,'a'*40,base,fn)
        for key,value in [('receipt',{'status':'matched','commit':'b'*40}),('source_files_checked',True),('source_files_checked',0),('function_name','wrong')]:
            bad=copy.deepcopy(base);bad[key]=value;rejected(lambda:s['validate'](bad,'a'*40,base,fn))

def test_original_resources_schedules_and_snapshot_alias_cannot_be_changed():
    s=runpy.run_path(str(OP))
    for fn in s['FUNCTIONS']:
        base=runtime(fn)
        for key,value in [('memory_mb',2048),('timeout',900),('runtime','changed'),('handler','changed'),('role','changed'),('architectures',['arm64']),('ephemeral_storage_mb',1024),('schedules',None),('schedules',[{'kind':'new','name':'new'}])]:
            bad=copy.deepcopy(base);bad[key]=value;rejected(lambda:s['validate'](bad,'a'*40,base,fn))
    fn='justhodl-portfolio-snapshot';base=runtime(fn);bad=copy.deepcopy(base);bad['active_alias']['code_sha256']='other'
    rejected(lambda:s['validate'](bad,'a'*40,base,fn))

def test_acceptance_can_read_only_the_selected_release_receipts_and_never_invokes_producers():
    s=runpy.run_path(str(OP));calls=[]
    class Client:
        def get_object(self,**request):calls.append(request);return {}
    for fn in s['FUNCTIONS']:
        reader=s['ReceiptOnly'](Client(),fn);reader.get_object(Bucket=s['BUCKET'],Key='data/ops/releases/'+fn+'.json')
        rejected(lambda:reader.get_object(Bucket=s['BUCKET'],Key='portfolio/snapshot.json'))
    reader=s['WorkerReceiptOnly'](Client());reader.get_object(Bucket=s['BUCKET'],Key='data/ops/releases/worker-justhodl-data-proxy.json')
    rejected(lambda:reader.get_object(Bucket=s['BUCKET'],Key='portfolio/risk.json'))
    assert len(calls)==3
    source=OP.read_text(encoding='utf-8');calls={n.func.attr for n in ast.walk(ast.parse(source)) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
    assert not calls & {'invoke','put_object','put_item','transact_write_items','update_function_code','put_rule','update_schedule'}
    assert 'sys.exit(1)' in source and 'native_invocations=0' in source and 'required_worker_commit' in source

def test_whole_predecessors_bind_the_reproduced_ack_failures_and_unchanged_measurement_functions():
    audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-snapshot-byte-publication.json').read_bytes())
    for row in audit['fixtures'].values():
        raw=(ROOT/row['path']).read_bytes();assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']
    fixture=json.loads((ROOT/audit['fixtures']['complete_synthetic']['path']).read_bytes());assert len(fixture['cases'])==4
    assert all(row['accepted'] and row['read_sizes']==[-1] and row['closed'] for row in fixture['cases'])
    def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
    before=functions(ROOT/audit['fixtures']['aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py']['path']);after=functions(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py')
    assert before.keys()==after.keys() and {name for name in before if before[name]!=after[name]}=={'lambda_handler'}
    assert audit['required_worker_commit']=='47446e46037aaa66e7b6071c6792b11da2037cfd'
