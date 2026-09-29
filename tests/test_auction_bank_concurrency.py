"""Observed S3 versions prevent stale auction-bank writers from dropping rows."""
from pathlib import Path
from contextlib import redirect_stdout
from copy import deepcopy
from unittest.mock import patch
import io,runpy,sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
Store,StorageError=support['Store'],support['StorageError']

def loaded(store=None):
    store=store or Store();scope=support['load']('auction-desk',store)
    return store,scope,scope['lambda_handler'].__globals__

def fails(fn):
    try:fn()
    except (ValueError,StorageError,TimeoutError) as error:return error
    raise AssertionError('Unversioned or stale bank write succeeded')

def field(scope,key):
    return {scope['HIST_KEY']:'records',scope['BUY_KEY']:'operations',scope['FULL_KEY']:'rows',scope['ASSETS_KEY']:'series'}[key]

def test_stale_existing_bank_writers_cannot_drop_a_concurrent_addition():
    for name in ('HIST_KEY','BUY_KEY','FULL_KEY','ASSETS_KEY'):
        store,a,env=loaded();_,b,_=loaded(store);key=a[name];mapping=field(a,key)
        store.docs[key]={mapping:{'original':{'zero':0,'unknown':None}}}
        first=a['_s3_json'](key);second=b['_s3_json'](key)
        second[mapping]['newer']={'value':2};b['_put_json'](key,second,gz=key.endswith('.gz'))
        retained=deepcopy(store.docs[key]);writes=len(store.writes)
        first[mapping]['stale']={'value':1}
        assert isinstance(fails(lambda:a['_put_json'](key,first,gz=key.endswith('.gz'))),StorageError)
        assert store.docs[key]==retained and len(store.writes)==writes

def test_racing_initializers_cannot_replace_an_already_created_bank():
    store,a,env=loaded();_,b,_=loaded(store);key=a['BUY_KEY']
    assert a['_s3_json'](key) is None and b['_s3_json'](key) is None
    b['_put_json'](key,{'operations':{'winner':{'zero':0}}});before=deepcopy(store.docs[key])
    fails(lambda:a['_put_json'](key,{'operations':{'late':{}}}))
    assert store.docs[key]==before

def test_bank_write_requires_a_read_and_each_read_condition_is_used_once():
    store,scope,env=loaded();key=scope['BUY_KEY'];doc={'operations':{}}
    fails(lambda:scope['_put_json'](key,doc));assert not store.writes
    scope['_s3_json'](key);scope['_put_json'](key,doc)
    fails(lambda:scope['_put_json'](key,doc));assert len(store.writes)==1
    scope['_s3_json'](key);scope['_put_json'](key,doc);assert len(store.writes)==2

def test_failed_reread_invalidates_prior_write_authority():
    store,scope,env=loaded();key=scope['BUY_KEY'];store.docs[key]={'operations':{}}
    old=scope['_s3_json'](key)
    with patch.object(store,'get_object',side_effect=StorageError('AccessDenied')):fails(lambda:scope['_s3_json'](key))
    fails(lambda:scope['_put_json'](key,old));assert not store.writes
    scope['_s3_json'](key);store.docs[key]=b'broken json'
    fails(lambda:scope['_s3_json'](key));fails(lambda:scope['_put_json'](key,old));assert store.docs[key]==b'broken json'

def test_complete_owned_bank_read_requires_a_nonempty_quoted_etag():
    for etag in (None,'',True,'opaque','""'):
        store,scope,env=loaded();key=scope['BUY_KEY'];response=io.BytesIO(b'{"operations":{}}')
        with patch.object(store,'get_object',return_value={'Body':response,'ETag':etag}):fails(lambda:scope['_s3_json'](key))
        assert response.closed;fails(lambda:scope['_put_json'](key,{'operations':{}}));assert not store.writes

def test_failed_or_ambiguous_put_cannot_be_blindly_retried():
    store,scope,env=loaded();key=scope['BUY_KEY'];scope['_s3_json'](key)
    with patch.object(store,'put_object',side_effect=TimeoutError('synthetic ambiguous write')):fails(lambda:scope['_put_json'](key,{'operations':{}}))
    fails(lambda:scope['_put_json'](key,{'operations':{}}));assert not store.writes
    scope['_s3_json'](key);scope['_put_json'](key,{'operations':{}});assert len(store.writes)==1

def test_handler_clears_warm_invocation_conditions_before_any_read():
    store,scope,env=loaded();scope['_s3_json'](scope['BUY_KEY']);assert env['_BANK_CONDITIONS']
    with patch.object(store,'get_object',side_effect=StorageError('AccessDenied')):fails(lambda:scope['lambda_handler']({},None))
    assert not env['_BANK_CONDITIONS'] and not store.writes

def test_handler_primary_bank_conflict_preserves_the_winner_and_public_snapshot():
    class Race(Store):
        def put_object(self,**kwargs):
            if kwargs['Key']==self.race_key:
                self.docs[self.race_key]={'records':{'winner':{'all_original_fields':'retained'}}}
            return super().put_object(**kwargs)
    store,scope,env=loaded(Race());store.race_key=scope['HIST_KEY']
    record=scope['norm_fd'](support['auction']());store.docs[scope['HIST_KEY']]={'records':{scope['rec_key'](record):record}}
    store.docs[scope['OUT_KEY']]={'previous_publication':'retain'}
    env.update(fetch_td=lambda *a,**k:[],fetch_fd=lambda *a,**k:[])
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):fails(lambda:scope['lambda_handler']({},None))
    assert store.docs[scope['HIST_KEY']]=={'records':{'winner':{'all_original_fields':'retained'}}}
    assert store.docs[scope['OUT_KEY']]=={'previous_publication':'retain'} and not store.writes

def test_secondary_loader_conflicts_preserve_newer_banks_and_expose_the_failure():
    class Race(Store):
        def put_object(self,**kwargs):
            if kwargs['Key']==self.race_key:self.docs[self.race_key]=deepcopy(self.winner)
            return super().put_object(**kwargs)
    for name,loader,mapping in [('ASSETS_KEY','load_assets','series'),('FULL_KEY','load_full_bank','rows')]:
        store,scope,env=loaded(Race());store.race_key=scope[name]
        store.docs[store.race_key]={mapping:{}};store.winner={mapping:{'concurrent':{'whole':'preserve'}}}
        def provider(*args,**kwargs):
            if kwargs.get('proof_out') is not None:kwargs['proof_out'].append({'complete':True,'rows':0,'pages':1})
            return []
        env.update(_get_json=lambda *a,**k:{'bars':[]},fetch_fd=provider)
        with patch('urllib.request.urlopen',side_effect=AssertionError('No network')):
            if name=='ASSETS_KEY':assert isinstance(fails(lambda:scope[loader](force=True)),StorageError)
            else:assert 'PreconditionFailed' in scope[loader](force=True).get('error','')
        assert store.docs[store.race_key]==store.winner and store.race_key not in store.writes


def test_readonly_source_keys_never_gain_owned_bank_write_conditions():
    store,scope,env=loaded();key=scope['PAR_KEY'];store.docs[key]={'rows':{}}
    assert scope['_s3_json'](key)=={'rows':{}} and key not in env['_BANK_CONDITIONS']
    assert set(env['BANK_KEYS'])=={scope[name] for name in ('HIST_KEY','BUY_KEY','FULL_KEY','ASSETS_KEY')}

if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction bank concurrency regressions passed:',len(tests))
