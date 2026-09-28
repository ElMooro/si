from pathlib import Path
import hashlib,json,sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from build_page_data_contracts import dependency_group


def test_shared_self_and_unresolved_references_do_not_assert_runtime_or_independence():
    group=dependency_group('consumer',{'reads':['data/common.json','data/common.json','data/self.json','data/unknown.json']},
                           {'data/common.json':{'a','b'},'data/self.json':{'consumer'}},set())
    assert len(group['public_references'])==3
    rows={r['key']:r for r in group['public_references']}
    assert rows['data/common.json']['possible_producers']==['a','b']
    assert rows['data/common.json']['producer_status']=='multiple_source_writers'
    assert rows['data/self.json']['own_output_reference'] is True
    assert rows['data/unknown.json']['producer_status']=='unresolved'
    assert all(r['runtime_read_verified'] is False for r in rows.values())
    assert group['independent_evidence_count'] is None and group['calls_eligible'] is False


def test_private_internal_and_dynamic_keys_are_withheld_without_access_changes():
    hidden=['data/retail-alert-state.json','portfolio/snapshot.json','data/private-user.json','data/dynamic/*.json','data/internal-public-shape.json']
    group=dependency_group('consumer',{'reads':hidden+['data/visible.json']},{}, {'data/internal-public-shape.json'})
    assert group['withheld_or_dynamic_count']==5
    assert [r['key'] for r in group['public_references']]==['data/visible.json']
    assert all(key not in json.dumps(group) for key in hidden)


def test_missing_inventory_is_unavailable_not_an_independent_root():
    for reads in (None,False,'data/one.json',[False]):
        group=dependency_group('consumer',{'reads':reads},{},set())
        assert group['inventory_available'] is False and group['public_references']==[]
        assert group['independent_evidence_count'] is None
    group=dependency_group('consumer',{'reads':[]},{},set())
    assert group['inventory_available'] is True and group['independent_evidence_count'] is None


def test_full_inspector_and_builder_predecessors_are_preserved():
    original={'build_page_data_contracts.py':'091c59d30feca664a19992c99804cdbfd14e9ec9a542e3c0f523bb63ff8af4e3',
              'jh-data-inspector.js':'7d7fbd7fcc6af0312df8b559a9f13071fabacc154cb75789f17ef3df8eecbfca'}
    for name,digest in original.items():
        assert hashlib.sha256((ROOT/'tests/fixtures'/('pre-inspector-dependencies-'+name+'.txt')).read_bytes()).hexdigest()==digest
