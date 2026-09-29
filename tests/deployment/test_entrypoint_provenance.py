"""Candidate source inventory cannot certify a configured-handler producer."""
import hashlib,json,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from gen_engine_manifest import build
from build_page_data_contracts import contract


def fixture(root,handler='lambda_function.lambda_handler',body='def lambda_handler(event,context):\n return {}',extra=None):
    source=root/'aws/lambdas/example/source';source.mkdir(parents=True)
    (source.parent/'config.json').write_text(json.dumps({'handler':handler} if handler else {}),encoding='utf-8')
    (source/'lambda_function.py').write_text(body,encoding='utf-8')
    for name,code in (extra or {}).items():
        path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(code,encoding='utf-8')
    return source


def test_unimported_legacy_handler_is_retained_only_as_candidate():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,extra={'legacy.py':"def lambda_handler(event,context):\n s3.put_object(Key='data/never-called.json')"})
        row=build(root)['engines'][0];key='data/never-called.json'
        assert row['keys']==[key] and row['ownership_status']=='incomplete'
        assert row['output_reachability'][key]=={'status':'unproven','evidence_indexes':[],'runtime_verified':False}
        assert row['write_evidence'][key][0]['file']=='legacy.py'
        assert any(x.get('key')==key and x['operation']=='entrypoint_ownership' for x in row['unresolved_writes'])


def test_direct_local_and_imported_writes_have_exact_callable_paths():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,body="from helper import save\ndef lambda_handler(event,context):\n s3.put_object(Key='data/direct.json')\n save('data/helper.json')",extra={'helper.py':"def save(key):\n s3.put_object(Key=key)\ndef unused():\n s3.put_object(Key='data/unused.json')"})
        row=build(root)['engines'][0]
        for key,module in [('data/direct.json','lambda_function.py'),('data/helper.json','helper.py')]:
            out=row['output_reachability'][key];assert out['status']=='reachable_in_source'
            assert row['write_evidence'][key][out['evidence_indexes'][0]]['repository_path']=='aws/lambdas/example/source/'+module
            assert row['write_evidence'][key][out['evidence_indexes'][0]]['via'][0]['function'].endswith(':lambda_handler')
            assert out['runtime_verified'] is False
        assert row['output_reachability']['data/unused.json']['status']=='unproven'


def test_dynamic_and_concrete_calls_to_same_site_remain_distinct():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,body="def save(key):\n s3.put_object(Key=key)\ndef lambda_handler(event,context):\n save('data/current.json')\n save(event['key'])")
        row=build(root)['engines'][0]
        assert row['output_reachability']['data/current.json']['status']=='reachable_in_source'
        assert row['unresolved_writes'] and row['ownership_status']=='incomplete'


def test_conventional_source_is_not_runtime_configuration_proof():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,handler=None,body="def lambda_handler(event,context):\n s3.put_object(Key='data/current.json')")
        row=build(root)['engines'][0];proof=row['write_evidence']['data/current.json'][row['output_reachability']['data/current.json']['evidence_indexes'][0]]
        assert proof['entrypoint_basis']=='conventional_source_handler_runtime_unverified'
        assert row['entrypoint_verified'] is False and row['deployment_overrides_verified'] is False


def test_missing_or_shadowed_configured_callable_cannot_claim_ownership():
    for handler,tail in [('missing.handler',''),('lambda_function.no_such_handler',''),('lambda_function.lambda_handler','\nlambda_handler=None')]:
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);fixture(root,handler,body="def lambda_handler(event,context):\n s3.put_object(Key='data/current.json')"+tail)
            row=build(root)['engines'][0]
            assert row['output_reachability']['data/current.json']['status']=='unproven'
            assert not row['shared_writer_analysis']['callable_resolved']
            assert row['ownership_status']=='incomplete'


def test_import_time_or_uncalled_writes_remain_unproven_not_assumed_dead():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,body="s3.put_object(Key='data/import-time.json')\ndef lambda_handler(event,context):\n return event")
        row=build(root)['engines'][0]
        assert row['keys']==['data/import-time.json']
        assert row['output_reachability'][row['keys'][0]]['status']=='unproven'
        assert 'import-time' in row['unresolved_writes'][-1]['reason']


def test_scoped_page_cannot_hide_unproven_legacy_candidate():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,extra={'legacy.py':"OUTPUT='data/legacy.json'\ndef lambda_handler(event,context):\n s3.put_object(Key=OUTPUT)"})
        (root/'engine-manifest.json').write_text(json.dumps(build(root)),encoding='utf-8')
        (root/'desk.html').write_text('<meta name="jh-primary-engine" content="example">',encoding='utf-8')
        (root/'config').mkdir()
        (root/'config/page-role-overrides.json').write_text(json.dumps({'pages':{'desk.html':{'primary_output_keys':{'example':['data/legacy.json']},'primary_scope_evidence':{'example':{'source':'aws/lambdas/example/source/legacy.py','constants':['OUTPUT'],'purpose':'Inspect retained candidate'}}}}}),encoding='utf-8')
        doc=contract(root);page=doc['pages']['desk.html']
        assert page['coverage_class']=='PRIMARY_PARTIAL' and page['primary_unresolved_write_count']==1
        assert page['outputs'][0]['entrypoint_reachability']['status']=='unproven'
        assert 'unproven' in page['outputs'][0]['ownership_evidence'][0]['basis']


def test_whole_predecessors_are_preserved_without_execution():
    doc=json.loads((ROOT/'docs/audit/2026-09-29/entrypoint-provenance-predecessor.json').read_bytes())
    for source,row in doc['sources'].items():
        raw=(ROOT/row['fixture']).read_bytes()
        assert len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256']


def test_statements_after_unconditional_return_are_candidates_only():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,body="def lambda_handler(event,context):\n return {}\n s3.put_object(Key='data/after-return.json')")
        row=build(root)['engines'][0]
        assert row['output_reachability']['data/after-return.json']['status']=='unproven'


def test_dependency_lineage_retains_candidate_edges_without_promoting_them():
    from build_dependency_map import lineage
    engines={'legacy':{'reads':[],'output_reachability':{'data/legacy.json':{'status':'unproven'}}},
             'active':{'reads':['data/legacy.json'],'output_reachability':{'data/current.json':{'status':'reachable_in_source'}}}}
    result=lineage(engines,{'desk.html':{'keys':['data/current.json']}},{'data/current.json':['active'],'data/legacy.json':['legacy']})
    row=result['page_lineage']['desk.html']
    assert row['upstream_engines']==['active','legacy']
    assert row['unproven_writer_keys']==['data/legacy.json']
    assert row['independent_evidence_count'] is None and row['sizing_eligible'] is False


def test_reachability_indexes_reference_complete_unique_write_chains():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);fixture(root,body="def lambda_handler(event,context):\n s3.put_object(Key='data/current.json')")
        row=build(root)['engines'][0];key='data/current.json'
        assert row['output_reachability'][key]['evidence_indexes']==[0]
        assert len(row['write_evidence'][key])==1
        proof=row['write_evidence'][key][0]
        assert proof['repository_path']=='aws/lambdas/example/source/lambda_function.py'
        assert proof['line']==2 and len(proof['via'])==1
