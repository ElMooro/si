"""Public research run reproducibility, privacy and conditional publication."""
import copy
import io
import json
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'shared'))
from calls_research_replay import prepare, replay, persist, publish_current, canonical, digest, FIELDS
from calls_contract import make_snapshot

AT = '2026-09-18T17:00:00+00:00'
DOCS = {'data/settlement-fails.json': {
    'treasury': {'as_of': '2026-09-09', 'ftd_bn': 0, 'ftr_bn': 2, 'gross_bn': 2, 'quality': {'status': 'fresh'}},
    'headline': {'as_of': '2026-09-09', 'ftd_bn': 1, 'ftr_bn': 1, 'combined_bn': 2, 'quality': {'status': 'fresh'}}},
    'data/ciss-stress.json': {'ea_composite': 0, 'ea_composite_date': '2026-09-17', 'quality': {'status': 'fresh'}}}


class StorageError(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Store:
    def __init__(self): self.objects = {}; self.version = 0; self.denied = False; self.conflict = False
    def get_object(self, Bucket, Key):
        if self.denied: raise StorageError('AccessDenied')
        if Key not in self.objects: raise StorageError('NoSuchKey')
        return {'Body': io.BytesIO(self.objects[Key]), 'ETag': str(self.version)}
    def put_object(self, Bucket, Key, Body, **kw):
        if kw.get('IfNoneMatch') == '*' and Key in self.objects: raise StorageError('PreconditionFailed')
        if self.conflict or ('IfMatch' in kw and kw['IfMatch'] != str(self.version)): raise StorageError('PreconditionFailed')
        self.objects[Key] = Body; self.version += 1


def test_frozen_replay_does_not_read_revised_sources():
    docs = copy.deepcopy(DOCS)
    run = prepare(lambda key: docs.get(key, {}), AT)
    docs['data/ciss-stress.json']['ea_composite'] = .9
    receipt = replay(run)
    assert receipt['status'] == 'reproduced' and receipt['inputs'] == len(FIELDS)
    new = prepare(lambda key: docs.get(key, {}), AT)
    assert new['run_id'] != run['run_id'] and new['payload_sha256'] != run['payload_sha256']
    assert run['payload']['output']['evidence'][7]['value'] == 0


def test_private_fields_and_untyped_strings_never_enter_public_archive():
    def load(key):
        doc = copy.deepcopy(DOCS.get(key, {}))
        doc.update(snapshot={'positions': ['PRIVATE-CANARY']}, notes='PRIVATE-CANARY', narrative='PRIVATE-CANARY')
        if key == 'data/liquidity-flow.json': doc['as_of'] = 'PRIVATE-CANARY'
        return doc
    run = prepare(load, AT)
    assert 'PRIVATE-CANARY' not in canonical(run).decode()
    assert replay(run)['status'] == 'reproduced'


def test_wrong_settlement_scope_is_preserved_as_invalid():
    docs = copy.deepcopy(DOCS)
    docs['data/settlement-fails.json']['treasury']['scope_id'] = 'PRIVATE-CANARY'
    run = prepare(lambda key: docs.get(key, {}), AT)
    assert 'PRIVATE-CANARY' not in canonical(run).decode()
    rows = run['payload']['output']['evidence']
    assert next(row for row in rows if row['series_id'].endswith('#treasury.ftd_bn'))['quality_status'] == 'unavailable'
    assert replay(run)['status'] == 'reproduced'


def test_tampered_input_or_output_fails_replay_even_after_outer_rehash():
    run = prepare(lambda key: DOCS.get(key, {}), AT)
    for kind in ('input', 'output', 'compiler'):
        bad = copy.deepcopy(run)
        if kind == 'input': bad['payload']['inputs']['data/ciss-stress.json']['projection']['ea_composite'] = .7
        if kind == 'output': bad['payload']['output']['brief_md'] += 'a changed conclusion'
        if kind == 'compiler': bad['payload']['compiler']['method'] = 'unreviewed'
        bad['payload_sha256'] = digest(bad['payload']); bad['run_id'] = 'calls-research-'+bad['payload_sha256']
        try: replay(bad)
        except ValueError: pass
        else: raise AssertionError('tampering accepted: '+kind)


def test_input_root_overlap_cannot_become_six_independent_votes():
    run = prepare(lambda key: DOCS.get(key, {}), AT)
    inventory = run['payload']['output']['evidence_inventory']
    group = next(g for g in inventory['root_groups'] if g['root_id'] == 'FR2004')
    assert len(group['fields']) == 6 and inventory['independent_evidence_count'] is None
    assert inventory['eligible_votes'] == 0 and all(p['may_size'] is False for p in inventory['permission_mask'])
    assert len(inventory['unmapped_fields']) == 2


def test_immutable_run_idempotency_and_conflict_detection():
    run = prepare(lambda key: DOCS.get(key, {}), AT); store = Store()
    ref = persist(store, 'fixture', run)
    assert persist(store, 'fixture', run) == ref and len(store.objects) == 1
    store.objects[ref['bundle_key']] = b'corrupt'
    try: persist(store, 'fixture', run)
    except ValueError: pass
    else: raise AssertionError('immutable conflict ignored')


def test_slow_old_run_cannot_replace_new_current_and_denied_read_never_resets():
    store = Store(); key = 'data/ai-brief-public.json'
    assert publish_current(store, 'fixture', key, {'generated_at': AT})['published']
    before = store.objects[key]
    assert not publish_current(store, 'fixture', key, {'generated_at': '2026-09-18T16:00:00Z'})['published']
    assert store.objects[key] == before
    store.denied = True
    try: publish_current(store, 'fixture', key, {'generated_at': '2026-09-18T18:00:00Z'})
    except StorageError: pass
    else: raise AssertionError('read denial ignored')
    assert store.objects[key] == before


def test_decision_identity_binds_brief_and_frozen_evidence():
    run = prepare(lambda key: DOCS.get(key, {}), AT); store = Store()
    output = copy.deepcopy(run['payload']['output'])
    output['research_replay'] = persist(store, 'fixture', run)
    first = make_snapshot({'as_of': AT}, output)
    output['brief_md'] += '\nAdditional observation.'
    second = make_snapshot({'as_of': AT}, output)
    assert first['snapshot_id'] != second['snapshot_id'] and first['brief_sha256'] != second['brief_sha256']
    assert len(first['evidence_ids']) == 12 and first['call_verb'] == 'WAIT'
    assert first['research_replay']['run_id'] == run['run_id'] and not first['sizing_eligible']


if __name__ == '__main__':
    tests = [v for k, v in sorted(globals().items()) if k.startswith('test_')]
    for test in tests: test()
    print('Calls public research replay tests passed:', len(tests))
