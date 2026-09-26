"""Shared ownership must follow exact invoked functions without executing them."""
import json
import sys
import tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from gen_engine_manifest import Scan, imported_symbols, build
from source_write_graph import SharedWriteGraph


def scan_files(handler, modules):
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);source=root/'aws/lambdas/test/source';shared=root/'aws/shared'
        source.mkdir(parents=True);shared.mkdir(parents=True)
        entry=source/'lambda_function.py';entry.write_text(handler,encoding='utf-8')
        for name,body in modules.items():
            path=shared/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(body,encoding='utf-8')
        graph=SharedWriteGraph(root,source,{},Scan,imported_symbols)
        return graph.analyze(entry,'lambda_handler')


WRITER='''
raise RuntimeError('application must never execute during analysis')
DEFAULT='data/default.json'
def save(client, key=DEFAULT, *, suffix=''):
 client.put_object(Key=key+suffix,Body=b'{}')
def unrelated(client):
 client.put_object(Key='data/not-owned.json',Body=b'{}')
'''


def test_local_import_arguments_and_uncalled_sibling_are_separate():
    out=scan_files('def lambda_handler(event,context):\n from writer import save as publish\n publish(client,key="data/real.json")',{'writer.py':WRITER})
    assert out['keys']=={'data/real.json'} and not out['unresolved'],out
    proof=out['proofs']['data/real.json'][0]
    assert proof['repository_path']=='aws/shared/writer.py' and proof['line']==5
    assert proof['via'][-1]['function']=='aws/shared/writer.py:save'


def test_module_alias_defaults_and_reexport_remain_source_bound():
    out=scan_files('import facade as w\ndef lambda_handler(event,context):\n w.write(client)',
        {'writer.py':WRITER,'facade.py':'from writer import save as write\n'})
    assert out['keys']=={'data/default.json'} and not out['unresolved'],out


def test_concrete_call_does_not_hide_same_site_dynamic_call():
    out=scan_files('from writer import save\ndef lambda_handler(event,context):\n save(client,"data/real.json")\n save(client,event["key"])',{'writer.py':WRITER})
    assert out['keys']=={'data/real.json'} and out['unresolved'],out
    assert all(row['file']=='aws/shared/writer.py' for row in out['unresolved'])


def test_uninvoked_import_is_not_ownership():
    out=scan_files('from writer import save\ndef lambda_handler(event,context):\n return {}',{'writer.py':WRITER})
    assert not out['keys'] and not out['proofs'],out


def test_unknown_argument_unpack_cannot_certify_a_default_output_key():
    for invocation in ('save(*event["args"])','save(client,**event["kwargs"])'):
        out=scan_files('from writer import save\ndef lambda_handler(event,context):\n '+invocation,{'writer.py':WRITER})
        assert not out['keys'] and out['unresolved'],out


def test_shared_default_uses_definition_scope_not_callers_same_named_value():
    code='from writer import save\ndef lambda_handler(event,context):\n DEFAULT="data/wrong.json"\n save(client)'
    out=scan_files(code,{'writer.py':WRITER})
    assert out['keys']=={'data/default.json'},out


def test_shared_siblings_and_bound_local_wrappers_keep_call_chain():
    code='from writer import save\ndef helper(key):\n save(client,key)\ndef lambda_handler(event,context):\n helper("data/chained.json")'
    out=scan_files(code,{'writer.py':WRITER})
    assert out['keys']=={'data/chained.json'}
    assert [x['function'].split(':')[-1] for x in out['proofs']['data/chained.json'][0]['via']]==['lambda_handler','helper','save']


def test_recursive_shared_calls_terminate_with_explicit_uncertainty():
    code='def a(client):\n client.put_object(Key="data/a.json")\n b(client)\ndef b(client):\n a(client)'
    out=scan_files('from writer import a\ndef lambda_handler(event,context):\n a(client)',{'writer.py':code})
    assert out['keys']=={'data/a.json'} and any('recursion' in x['reason'] for x in out['unresolved']),out


def test_decorated_callable_is_not_assumed_to_invoke_original_writer():
    code='@replace_function\ndef save(client):\n client.put_object(Key="data/wrong.json")'
    out=scan_files('from writer import save\ndef lambda_handler(event,context):\n save(client)',{'writer.py':code})
    assert not out['keys'] and any('decorated' in x['reason'] for x in out['unresolved']),out


def test_shadowed_import_or_parameter_cannot_retain_old_callable():
    for body in ('save=event["callback"]\n save(client)', 'pass'):
        out=scan_files('from writer import save\ndef lambda_handler(event,context):\n '+body,{'writer.py':WRITER})
        assert not out['keys'],out
    out=scan_files('from writer import save\ndef wrapper(save):\n save(client)\ndef lambda_handler(event,context):\n wrapper(event["callback"])',{'writer.py':WRITER})
    assert not out['keys'],out


def test_top_level_shadow_and_later_function_definition_obey_source_order():
    for code in ('def save(client):\n client.put_object(Key="data/wrong.json")\nsave=None',
                 'from writer import save\nsave=None'):
        out=scan_files('import facade\ndef lambda_handler(event,context):\n facade.save(client)',{'writer.py':WRITER,'facade.py':code})
        assert not out['keys'],out
    out=scan_files('from facade import save\ndef lambda_handler(event,context):\n save(client)',
        {'facade.py':'save=None\ndef save(client):\n client.put_object(Key="data/right.json")'})
    assert out['keys']=={'data/right.json'},out


def test_real_ciss_engines_never_own_each_others_current_outputs():
    for engine,key,other in [('justhodl-ciss-stress','data/ciss-stress.json','data/ciss-ai.json'),('justhodl-ciss-ai','data/ciss-ai.json','data/ciss-stress.json')]:
        source=ROOT/'aws/lambdas'/engine/'source'
        out=SharedWriteGraph(ROOT,source,{},Scan,imported_symbols).analyze(source/'lambda_function.py','lambda_handler')
        assert key in out['keys'] and other not in out['keys'],out


def test_real_calls_public_writer_and_history_have_exact_shared_proofs():
    source=ROOT/'aws/lambdas/justhodl-ai-brief/source'
    out=SharedWriteGraph(ROOT,source,{},Scan,imported_symbols).analyze(source/'lambda_function.py','lambda_handler')
    for key in ('data/ai-brief-public.json','data/decisive-call-history.json'):
        assert key in out['keys'] and out['proofs'][key]
        assert all(row['repository_path'].startswith('aws/shared/') and row['via'] for row in out['proofs'][key])


def test_build_includes_repository_relative_evidence_with_configured_handler():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);source=root/'aws/lambdas/test/source';shared=root/'aws/shared'
        source.mkdir(parents=True);shared.mkdir(parents=True)
        (source.parent/'config.json').write_text(json.dumps({'handler':'lambda_function.lambda_handler'}))
        (source/'lambda_function.py').write_text('from writer import save\ndef lambda_handler(event,context):\n save(client,"data/current.json")')
        (shared/'writer.py').write_text(WRITER)
        row=build(root)['engines'][0]
        assert row['keys']==['data/current.json'] and row['write_evidence'][row['keys'][0]][0]['repository_path']=='aws/shared/writer.py'
        (source.parent/'config.json').write_text('{}')
        row=build(root)['engines'][0]
        assert row['keys']==['data/current.json'] and row['configured_handler'] is None and row['entrypoint_verified'] is False
        assert row['shared_writer_analysis']['basis']=='conventional_source_handler_runtime_unverified'
        (source.parent/'config.json').write_text(json.dumps({'handler':'missing.handler'}))
        row=build(root)['engines'][0]
        assert not row['keys'] and row['entrypoint_status']=='CONFIGURED_SOURCE_MISSING'
