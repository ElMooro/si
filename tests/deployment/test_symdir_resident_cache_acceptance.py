from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
import importlib.util,sys
ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('symdir_release_operation',ROOT/'aws/ops/staged/ops_6409_symdir_resident_cache_acceptance.py')
op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)

def value():
    return {**deepcopy(op.EXPECTED_CONTROL),'receipt':{'status':'matched','commit':'a'*40},'source_files_checked':8,'handler_bytes':12345,'code_sha256':'invented-code-hash'}

def reject(fn):
    try:fn()
    except ValueError:return
    raise AssertionError('Changed native release accepted')

def test_symdir_resident_receipt_must_be_exact_and_sources_complete():
    op.validate(value(),'a'*40)
    for change in ({'receipt':{'status':'matched','commit':'b'*40}},{'source_files_checked':7},{'source_files_checked':True},{'handler_bytes':True}):
        reject(lambda:op.validate({**value(),**change},'a'*40))

def test_symdir_resident_original_native_controls_cannot_drift():
    for change in ({'timeout':121},{'memory_mb':2048},{'schedules':[]},{'role':'invented-other'},{'architectures':['arm64']},{'ephemeral_storage_mb':1024}):
        reject(lambda:op.validate({**value(),**change},'a'*40))

    for index in range(7):
        changed=value();changed['schedules'][index]['expression']='rate(2 minutes)'
        reject(lambda:op.validate(changed,'a'*40))


def test_symdir_resident_acceptance_refuses_every_other_object_or_write():
    calls=[];wrapper=op.ReceiptOnly(SimpleNamespace(get_object=lambda **kw:calls.append(kw)))
    wrapper.get_object(Bucket=op.BUCKET,Key='data/ops/releases/'+op.FN+'.json')
    reject(lambda:wrapper.get_object(Bucket=op.BUCKET,Key='data/symdir/master.json'))
    assert len(calls)==1 and not hasattr(wrapper,'put_object')

def test_symdir_resident_schedule_order_is_not_control_drift():
    original=value();reversed_value=deepcopy(original)
    reversed_value['schedules'].reverse()
    assert op.normalized(reversed_value,'a'*40)==op.normalized(original,'a'*40)
    assert reversed_value['schedules']==list(reversed(original['schedules']))
    # This is the exact predecessor failure: validation ran before sorting.
    reject(lambda:op.validate(reversed_value,'a'*40))


def test_symdir_resident_order_normalization_preserves_every_real_difference():
    for bad in (None,[None],[{'kind':True}],[] ):
        reject(lambda:op.normalized({**value(),'schedules':bad},'a'*40))
    for index in range(7):
        changed=value();changed['schedules'].reverse();changed['schedules'][index]['expression']='rate(2 minutes)'
        reject(lambda:op.normalized(changed,'a'*40))
    for mutate in (lambda rows:rows.append(deepcopy(rows[0])),lambda rows:rows.pop()):
        changed=value();mutate(changed['schedules']);reject(lambda:op.normalized(changed,'a'*40))


if __name__=='__main__':
    tests=[v for k,v in globals().copy().items() if k.startswith('test_') and callable(v)]
    for fn in tests:fn()
    print('Symdir release acceptance tests passed:',len(tests))
