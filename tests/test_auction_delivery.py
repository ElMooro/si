"""Complete delivery, corruption, interrupted publication and race regressions."""
from copy import deepcopy
import gzip
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import auction_delivery as model
import auction_delivery_store as storage
support = runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))
Store, StorageError = support['Store'], support['StorageError']
CASES = json.loads((ROOT/'tests/fixtures/auction-buybacks.json').read_bytes())['cases']


def packet(name='measured_zero'):
    return deepcopy(CASES[name])


def rejects(fn):
    try:
        fn()
    except (ValueError, TypeError, KeyError, StorageError, OSError):
        return
    raise AssertionError('Invalid delivery was accepted')


def test_replay_preserves_whole_packet_unknown_fields_and_all_case_occurrences():
    for name in CASES:
        original = packet(name)
        original['future_unknown_fields'] = {'source_text': 'é 零', 'zero': 0, 'missing': None}
        locator, artifacts = model.build(original)
        saved, view = model.replay(locator, artifacts.__getitem__)
        assert saved == original
        assert len(view['buybacks']['operations']) == len(original['buybacks']['operations'])
        assert view['future_unknown_fields'] == original['future_unknown_fields']
        assert all(view['delivery'][key] is False for key in model.FLAGS)
        assert view['reactions']['supplied_input_replay_available'] is False


def test_view_reduces_initial_bytes_without_reducing_complete_inputs():
    original = packet('sample_45')
    locator, artifacts = model.build(original)
    saved, view = model.replay(locator, artifacts.__getitem__)
    assert locator['view']['bytes'] < len(model.encoded(original))/2
    for row, full in zip(view['buybacks']['operations'], saved['buybacks']['operations']):
        detail = row['delivery_detail']
        chunk = model.checked(detail['artifact'], artifacts.__getitem__, 'rows')
        assert chunk['rows'][detail['index']] == full
        assert 'measurement_inputs' not in row
        assert row['measurement_summary']['status'] == full['measurement_inputs']['status']


def test_missing_zero_duplicate_future_and_hostile_fields_do_not_change_in_projection():
    for name in ('measured_zero','missing','duplicates','future','hostile','legacy'):
        original = packet(name); locator, artifacts = model.build(original)
        _, view = model.replay(locator, artifacts.__getitem__)
        for before, after in zip(original['buybacks']['operations'],view['buybacks']['operations']):
            for field in ('accepted','operation_date','maturity_bucket','fill_pct','coverage','liquidity_signal'):
                assert before.get(field) == after.get(field)


def test_malformed_packet_and_portfolio_permission_fail_before_any_writes():
    for mutate in (lambda d:d.update(engine='other'),lambda d:d.update(generated_at='2026-09-29'),
                   lambda d:d['today'].update(auctions={}),lambda d:d['decision'].update(call='LONG'),
                   lambda d:d['decision'].update(sizing_eligible=True),lambda d:d.update(buybacks=None)):
        doc=packet();mutate(doc);store=Store()
        rejects(lambda:storage.publish(store,'fixture',doc));assert not store.writes


def test_corruption_missing_chunks_and_altered_projection_fail_replay():
    locator, artifacts = model.build(packet())
    for key in artifacts:
        damaged = dict(artifacts); damaged[key] = b'corrupt'
        rejects(lambda:model.replay(locator,damaged.__getitem__))
        del damaged[key];rejects(lambda:model.replay(locator,damaged.__getitem__))
    changed=deepcopy(locator);changed['generated_at']='2026-09-28T12:00:00Z'
    rejects(lambda:model.replay(changed,artifacts.__getitem__))
    changed=deepcopy(locator);changed['sizing_eligible']=True
    rejects(lambda:model.replay(changed,artifacts.__getitem__))


def test_reference_namespace_size_and_storage_encoding_are_strict():
    locator, _=model.build(packet());good=locator['view']
    for change in ({'key':'data/private/portfolio.json'}, {'key':'data/auction-desk-delivery/../private.json'},
                   {'bytes':True},{'bytes':0},{'sha256':'f'*64},{'encoding':'gzip'},{'extra':1}):
        rejects(lambda:model.validate_reference({**good,**change},'views'))
    rejects(lambda:model.strict(b'{"same":1,"same":2}'))
    rejects(lambda:model.strict(b'{"value":NaN}'))
    with patch.object(model,'MAX_VIEW',10):rejects(lambda:model.build(packet()))
    with patch.object(model,'MAX_ROWS',10):rejects(lambda:model.build(packet()))


def test_gzip_container_variants_preserve_exact_uncompressed_identity():
    locator, artifacts=model.build(packet())
    manifest=model.checked(locator['manifest'],artifacts.__getitem__,'manifests')
    ref=manifest['source_packet'];raw=gzip.decompress(artifacts[ref['key']])
    artifacts[ref['key']]=gzip.compress(raw,compresslevel=1,mtime=123)
    assert model.replay(locator,artifacts.__getitem__)[0]==packet()
    artifacts[ref['key']]=gzip.compress(raw+b' ',mtime=0)
    rejects(lambda:model.replay(locator,artifacts.__getitem__))


def test_publish_retains_verifies_every_artifact_before_conditional_pointer():
    store=Store();doc=packet();result=storage.publish(store,'fixture',doc)
    assert store.writes[-1]==model.CURRENT and result['source_bytes']>result['view_bytes']
    assert set(store.writes[:-1])<=set(store.reads)
    assert all(k==model.CURRENT or k.startswith(model.PREFIX) for k in store.reads+store.writes)
    locator=json.loads(store.docs[model.CURRENT]);assert model.replay(locator,store.docs.__getitem__)[0]==doc
    previous=dict(store.docs);storage.publish(store,'fixture',doc)
    assert store.docs==previous


def test_interrupted_artifact_put_or_readback_never_replaces_previous_pointer():
    for failure in ('put','read'):
        store=Store();storage.publish(store,'fixture',packet());before=store.docs[model.CURRENT]
        newer=packet('sample_45');newer['generated_at']='2026-09-29T12:01:00Z'
        original=store.put_object if failure=='put' else store.get_object
        def fail(**kw):
            if '/rows/' in kw['Key']:raise StorageError('AccessDenied')
            return original(**kw)
        with patch.object(store,'put_object' if failure=='put' else 'get_object',fail):
            rejects(lambda:storage.publish(store,'fixture',newer))
        assert store.docs[model.CURRENT]==before


def test_existing_corrupt_immutable_bytes_are_never_overwritten():
    store=Store();locator,artifacts=model.build(packet())
    key=next(k for k in artifacts if '/rows/' in k);store.docs[key]=b'corrupt'
    rejects(lambda:storage.publish(store,'fixture',packet()))
    assert store.docs[key]==b'corrupt' and model.CURRENT not in store.docs


def test_access_denied_and_invalid_previous_pointer_are_not_treated_as_missing():
    for failure in ('AccessDenied','InternalError'):
        store=Store()
        with patch.object(store,'get_object',side_effect=StorageError(failure)):
            rejects(lambda:storage.publish(store,'fixture',packet()))
        assert not store.writes
    store=Store({model.CURRENT:b'{"error":"corrupt"}'})
    rejects(lambda:storage.publish(store,'fixture',packet()));assert not store.writes


def test_older_and_same_clock_conflicting_publications_preserve_pointer():
    store=Store();storage.publish(store,'fixture',packet());before=store.docs[model.CURRENT]
    for date,name in (('2026-09-28T12:00:00Z','measured_zero'),(packet()['generated_at'],'missing')):
        doc=packet(name);doc['generated_at']=date
        rejects(lambda:storage.publish(store,'fixture',doc));assert store.docs[model.CURRENT]==before


def test_competing_writers_and_identical_retry_cannot_overwrite_newer_pointer():
    for identical in (False,True):
        store=Store();storage.publish(store,'fixture',packet());original=store.put_object
        newer=packet();newer['generated_at']='2026-09-29T12:02:00Z'
        winner,_=model.build(newer);winner_bytes=model.encoded(winner)
        def compete(**kw):
            if kw['Key']==model.CURRENT:store.docs[model.CURRENT]=winner_bytes
            return original(**kw)
        candidate=packet()
        if not identical:candidate['generated_at']='2026-09-29T12:01:00Z'
        with patch.object(store,'put_object',compete):rejects(lambda:storage.publish(store,'fixture',candidate))
        assert store.docs[model.CURRENT]==winner_bytes


def test_offline_cli_rebuilds_current_compiler_without_network_or_archive_execution():
    locator,artifacts=model.build(packet())
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);path=root/'locator.json';path.write_bytes(model.encoded(locator))
        for key,raw in artifacts.items():
            target=root/key;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
        args=[sys.executable,str(ROOT/'scripts/replay_auction_delivery.py'),str(path),'--root',str(root)]
        result=subprocess.run(args,capture_output=True,text=True)
        assert result.returncode==0,result.stderr
        assert json.loads(result.stdout)['sizing_eligible'] is False
        (root/locator['view']['key']).write_bytes(b'{}')
        assert subprocess.run(args,capture_output=True).returncode==1


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction delivery regressions passed:',len(tests))
