from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
import importlib.util,sys
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('equity_release_operation',ROOT/'aws/ops/staged/ops_6399_equity_identity_integrity_acceptance.py')
op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)

def value():
    return {**deepcopy(op.EXPECTED_CONTROL),'receipt':{'status':'matched','commit':'a'*40},'source_files_checked':5,'handler_bytes':12345,'code_sha256':'invented-code-hash'}

def reject(fn):
    try:fn()
    except ValueError:return
    raise AssertionError('Changed native release accepted')

def test_symbology_receipt_must_be_exact_and_sources_complete():
    op.validate(value(),'a'*40)
    for change in ({'receipt':{'status':'matched','commit':'b'*40}},{'source_files_checked':4},{'source_files_checked':True},{'handler_bytes':True}):
        reject(lambda:op.validate({**value(),**change},'a'*40))

def test_symbology_original_native_controls_cannot_drift():
    for change in ({'timeout':121},{'memory_mb':2048},{'schedules':[]},{'role':'invented-other'},{'architectures':['arm64']},{'ephemeral_storage_mb':1024}):
        reject(lambda:op.validate({**value(),**change},'a'*40))

def test_symbology_acceptance_refuses_every_other_object_or_write():
    calls=[];wrapper=op.ReceiptOnly(SimpleNamespace(get_object=lambda **kw:calls.append(kw)))
    wrapper.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')
    reject(lambda:wrapper.get_object(Bucket=op.BUCKET,Key='data/symbology/master.json'))
    assert len(calls)==1 and not hasattr(wrapper,'put_object')

if __name__=='__main__':
    tests=[v for k,v in globals().copy().items() if k.startswith('test_') and callable(v)]
    for fn in tests:fn()
    print('Symbology release acceptance tests passed:',len(tests))
