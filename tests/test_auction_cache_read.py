"""Synthetic object-storage faults must not erase retained Treasury histories."""
from pathlib import Path
from contextlib import redirect_stdout
from copy import deepcopy
from unittest.mock import patch
import gzip,io,json,math,runpy,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
Store,StorageError=support['Store'],support['StorageError']


def loaded(store=None):
    store=store or Store();scope=support['load']('auction-desk',store)
    return store,scope,scope['lambda_handler'].__globals__


def rejects(fn,expected=Exception):
    try:fn()
    except expected as error:return error
    raise AssertionError('Invalid cache was silently treated as missing')


def test_only_explicit_missing_keys_can_initialize_from_the_callers_default():
    store,scope,env=loaded();marker={'missing_only':True}
    assert scope['_s3_json'](scope['HIST_KEY'],marker) is marker
    for code in ['AccessDenied','NoSuchBucket','SlowDown','InternalError']:
        error=StorageError(code)
        with patch.object(store,'get_object',side_effect=error):
            assert rejects(lambda:scope['_s3_json'](scope['HIST_KEY'],marker)) is error
    for response in [None,{}, {'Error':None}]:
        error=TimeoutError('synthetic');error.response=response
        with patch.object(store,'get_object',side_effect=error):
            assert rejects(lambda:scope['_s3_json'](scope['HIST_KEY'],marker)) is error
    assert not store.writes


def test_corrupt_ambiguous_and_wrong_shape_cache_bytes_never_become_defaults():
    store,scope,env=loaded();key=scope['BUY_KEY']
    for raw in [b'{',b'null',b'[]',b'{}',b'{"operations":[]}',b'{"operations":{"old":null}}',
                b'{"operations":{},"operations":{}}',b'{"operations":{},"superseded_operations":[]}',
                b'{"operations":{},"superseded_operations":{"old":false}}']:
        store.docs[key]=raw;assert rejects(lambda:scope['_s3_json'](key,{}));assert store.docs[key]==raw
    store.docs[scope['HIST_KEY']]=b'not a gzip document'
    rejects(lambda:scope['_s3_json'](scope['HIST_KEY'],{}))
    assert not store.writes


def test_valid_empty_mappings_and_complete_legacy_diagnostics_remain_readable():
    store,scope,env=loaded()
    for key,field in [(scope['HIST_KEY'],'records'),(scope['BUY_KEY'],'operations'),(scope['FULL_KEY'],'rows'),
                      (scope['ASSETS_KEY'],'series'),(scope['PAR_KEY'],'rows')]:
        store.docs[key]={field:{},'retained_extra':{'zero':0,'missing':None}}
        assert scope['_s3_json'](key)==store.docs[key]
    store.docs[scope['HIST_KEY']]={'records':{'legacy':{'btc':float('nan'),'source_note':'retained diagnostic'}}}
    result=scope['_s3_json'](scope['HIST_KEY'])
    assert math.isnan(result['records']['legacy']['btc']) and not store.writes


def test_short_reads_are_fully_consumed_and_streams_are_closed():
    store,scope,env=loaded()
    class Short(io.BytesIO):
        def read(self,n=-1):return super().read(min(n,3))
    for raw,valid in [(b'{"operations":{}}',True),(b'{"operations":{}}unread trailing bytes',False)]:
        stream=Short(raw)
        with patch.object(store,'get_object',return_value={'Body':stream}):
            if valid:assert scope['_s3_json'](scope['BUY_KEY'])=={'operations':{}}
            else:rejects(lambda:scope['_s3_json'](scope['BUY_KEY']))
        assert stream.closed


def test_raw_and_inflated_byte_bounds_reject_whole_objects_without_truncating():
    store,scope,env=loaded();env['MAX_CACHE_BYTES']=128
    for key,raw in [(scope['BUY_KEY'],b'x'*129),(scope['HIST_KEY'],gzip.compress(b'{"records":{},"padding":"'+b'x'*512+b'"}'))]:
        stream=io.BytesIO(raw)
        with patch.object(store,'get_object',return_value={'Body':stream}):
            assert 'byte limit' in str(rejects(lambda:scope['_s3_json'](key)))
        assert stream.closed and not store.writes


def test_a_stream_failure_after_get_object_is_not_a_missing_object():
    store,scope,env=loaded();failure=StorageError('NoSuchKey')
    class Broken(io.BytesIO):
        def read(self,n=-1):raise failure
    stream=Broken(b'partial')
    with patch.object(store,'get_object',return_value={'Body':stream}):
        assert rejects(lambda:scope['_s3_json'](scope['BUY_KEY'],{})) is failure
    assert stream.closed and not store.writes


def handler_fixture(target):
    class Denied(Store):
        def get_object(self,**request):
            if request['Key']==self.denied_key:raise StorageError('AccessDenied')
            return super().get_object(**request)
    store,scope,env=loaded(Denied());store.denied_key=scope[target]
    store.docs[scope['HIST_KEY']]={'records':{}}
    store.docs[scope['BUY_KEY']]={'operations':{'saved':{'operation_date':'2026-09-28','accepted':0,'raw_extra':'keep all supplied evidence'}}}
    calls=[]
    def provider(*a,**k):calls.append(a);return []
    env.update(fetch_fd=provider,fetch_td=provider,load_assets=lambda **k:{'series':{}},
               load_full_bank=lambda **k:{'rows':{}},fetch_fred_daily=lambda *a:{})
    return store,scope,calls


def test_actual_handler_denied_history_read_preserves_the_bank_and_makes_no_writes():
    store,scope,calls=handler_fixture('HIST_KEY');store.docs[scope['HIST_KEY']]['records']={'saved':scope['norm_td'](support['auction']())}
    original=deepcopy(store.docs)
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):
        error=rejects(lambda:scope['lambda_handler']({},None),StorageError)
    assert error.response['Error']['Code']=='AccessDenied'
    assert store.docs==original and not store.writes and not calls


def test_actual_handler_denied_buyback_read_preserves_retired_inputs_and_public_snapshot():
    store,scope,calls=handler_fixture('BUY_KEY');original=deepcopy(store.docs[scope['BUY_KEY']])
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):
        rejects(lambda:scope['lambda_handler']({},None),StorageError)
    assert store.docs[scope['BUY_KEY']]==original
    assert scope['BUY_KEY'] not in store.writes and scope['OUT_KEY'] not in store.writes
    assert 'data/auction-desk-view.json' not in store.writes


def test_secondary_cache_failure_preserves_bank_and_marks_dependent_research_unavailable():
    for target,loader,field in [('ASSETS_KEY','load_assets','series'),('FULL_KEY','load_full_bank','rows')]:
        store,scope,calls=handler_fixture(target)
        store.docs[scope['BUY_KEY']]={'operations':{}}
        store.docs[scope[target]]={field:{'retained':{'value':0,'note':'synthetic original'}}}
        original=deepcopy(store.docs[scope[target]])
        scope['lambda_handler'].__globals__[loader]=scope[loader]
        with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):
            assert scope['lambda_handler']({},None)['ok']
        assert store.docs[scope[target]]==original and scope[target] not in store.writes
        packet=store.docs[scope['OUT_KEY']]
        if target=='ASSETS_KEY':
            assert packet['reactions']['comparison_inputs'] is None and 'AccessDenied' in packet['reactions']['note']
        else:assert 'AccessDenied' in packet['composite_history']['error'] and scope['COMPOSITE_KEY'] not in store.writes
        assert packet['decision']['call'] is None and packet['decision']['sizing_eligible'] is False


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction cache read regressions passed:',len(tests))
