from pathlib import Path
from types import SimpleNamespace
from copy import deepcopy
import importlib.util,sys
HERE=Path(__file__).resolve().parent
EXTERNAL=(HERE/'ops_6403_symdir_runtime_baseline.py').exists()
ROOT=HERE.parent/'si-batch-improvements' if EXTERNAL else HERE.parents[1]
PATH=HERE/'ops_6403_symdir_runtime_baseline.py' if EXTERNAL else ROOT/'aws/ops/staged/ops_6403_symdir_runtime_baseline.py'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
spec=importlib.util.spec_from_file_location('symdir_baseline',PATH);op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)

def value():
 return {'function_name':op.FN,'receipt':{'status':'missing_predecessor_receipt'},'source_files_checked':3,
  'handler_bytes':171303,'timeout':900,'memory_mb':6144,'ephemeral_storage_mb':2048,'runtime':'python3.12',
  'handler':'lambda_function.lambda_handler','code_sha256':'invented','role':'invented-role','architectures':['x86_64'],
  'schedules':[{'kind':'EventBridge rule','name':'invented-rule','state':'ENABLED','expression':'cron(0 5 * * ? *)','native_targets':1}]}

def reject(fn):
 try:fn()
 except ValueError:return
 raise AssertionError('Invalid baseline accepted')

def test_explicit_missing_receipt_is_not_relabelled_commit_verified():
 op.validate(value())
 op.validate({**value(),'receipt':{'status':'matched','commit':'a'*40}})
 for receipt in ({'status':'403'},{'status':'missing'},{'status':'matched','commit':None},{'status':'matched','commit':'not-a-sha'},{'status':'matched','commit':'a'*40,'extra':True}):
  reject(lambda:op.validate({**value(),'receipt':receipt}))

def test_runtime_baseline_requires_full_typed_native_identity():
 for key in ('source_files_checked','handler_bytes','timeout','memory_mb','ephemeral_storage_mb'):
  for bad in (True,0,-1,None):reject(lambda:op.validate({**value(),key:bad}))
 for key in ('runtime','handler','role','code_sha256'):
  reject(lambda:op.validate({**value(),key:''}))
 bad=deepcopy(value());bad['schedules'][0]['native_targets']=False;reject(lambda:op.validate(bad))

def test_receipt_reader_has_no_scope_to_read_current_report_or_inputs():
 calls=[];reader=op.ReceiptOnly(SimpleNamespace(get_object=lambda **kw:calls.append(kw)))
 reader.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')
 for key in ('data/audit/coverage-gap.json','data/symbology/master.json','data/audit/data-source-rollup.json','audit-private/anything'):
  reject(lambda:reader.get_object(Bucket=op.BUCKET,Key=key))
 assert len(calls)==1 and not hasattr(reader,'put_object')

if __name__=='__main__':
 tests=[fn for name,fn in globals().copy().items() if name.startswith('test_') and callable(fn)]
 for fn in tests:fn()
 print('Symbol directory native baseline tests passed:',len(tests))
