"""Pure key-return proofs stay conservative and never execute source modules."""
import ast,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts'))
from gen_engine_manifest import Scan,imported_symbols,scan_code
from source_write_graph import SharedWriteGraph
from page_sources import HTML
from test_shared_write_graph import scan_files

def inspect(helper,call='"market-extremes"',extra=''):
    writer='raise RuntimeError("Do not execute application code")\n'+extra+helper+'\ndef save(client,engine):\n client.put_object(Key=current(engine),Body=b"{}")\n'
    return scan_files('import writer\ndef lambda_handler(event,context):\n writer.save(client,'+call+')',{'writer.py':writer})

def test_literal_guarded_key_is_bound_through_actual_shared_call_path():
    out=inspect('def current(engine):\n if engine not in ("capitulation","market-extremes"):raise ValueError()\n return "data/"+engine+".json"')
    assert out['keys']=={'data/market-extremes.json'} and not out['unresolved'],out
    proof=out['proofs']['data/market-extremes.json'][0]
    assert proof['repository_path']=='aws/shared/writer.py' and proof['via'][-1]['function']=='aws/shared/writer.py:save'

def test_dictionary_membership_guards_use_known_imported_constants():
    out=scan_files('from writer import save\ndef lambda_handler(event,context):\n save(client,"market-extremes")',{
      'catalog.py':'INPUTS={"market-extremes":None,"capitulation":None}',
      'writer.py':'import catalog\ndef current(engine):\n if engine not in catalog.INPUTS:raise ValueError()\n return f"data/{engine}.json"\ndef save(client,engine):\n client.put_object(Key=current(engine))'})
    assert out['keys']=={'data/market-extremes.json'},out

def test_guard_that_provably_raises_cannot_lend_a_key():
    for guard in ('engine not in ("capitulation",)','engine in ("market-extremes",)','engine == "market-extremes"','engine != "capitulation"'):
        out=inspect('def current(engine):\n if '+guard+':raise ValueError()\n return "data/"+engine+".json"')
        assert not out['keys'] and out['unresolved'],(guard,out)

def test_unknown_mutating_or_conditional_guard_is_not_guessed():
    for helper in ('def current(engine):\n if engine not in unknown:raise ValueError()\n return "data/"+engine+".json"',
      'def current(engine):\n if mutate(engine):raise ValueError()\n return "data/"+engine+".json"',
      'def current(engine):\n if engine == "x":return "data/other.json"\n return "data/"+engine+".json"',
      'def current(engine):\n if engine == "x":raise ValueError()\n else:engine="changed"\n return "data/"+engine+".json"'):
        out=inspect(helper);assert not out['keys'] and out['unresolved'],out

def test_mutation_arbitrary_calls_decorators_async_and_recursion_remain_unresolved():
    for helper in ('def current(engine):\n engine="changed"\n return "data/"+engine+".json"',
      'def current(engine):\n log(engine)\n return "data/"+engine+".json"',
      'def current(engine):\n return transform(engine)',
      '@decorate\ndef current(engine):\n return "data/"+engine+".json"',
      'async def current(engine):\n return "data/"+engine+".json"',
      'def current(engine):\n return current(engine)'):
        out=inspect(helper);assert not out['keys'],out

def test_unknown_key_values_and_starred_expansions_are_not_concrete_keys():
    out=inspect('def current(engine):\n return "data/"+engine+".json"',call='event["engine"]')
    assert not out['keys'] and out['unresolved'],out
    for call in ('*args','**kwargs'):
        out=scan_files('import writer\ndef lambda_handler(event,context):\n client.put_object(Key=writer.current('+call+'))',
          {'writer.py':'def current(engine="default"):\n return "data/"+engine+".json"'})
        assert not out['entrypoint_proofs'],out

def test_defaults_keyword_only_and_definition_scope_are_bound_correctly():
    out=scan_files('import writer\nPREFIX="data/wrong/"\ndef lambda_handler(event,context):\n writer.save(client)',{
      'writer.py':'PREFIX="data/actual/"\ndef current(engine="market-extremes",*,prefix=PREFIX):\n return prefix+engine+".json"\ndef save(client):\n client.put_object(Key=current())'})
    assert out['keys']=={'data/actual/market-extremes.json'},out

def test_invalid_python_bindings_cannot_claim_literal_return_keys():
    helper='def current(engine,/,*,suffix=".json"):\n return "data/"+engine+suffix'
    for call in ('','engine="x"','"x","y"','"x",suffix=".json",extra="no"','"x",suffix="a",suffix="b"'):
        out=scan_files('import writer\ndef lambda_handler(event,context):\n client.put_object(Key=writer.current('+call+'))',{'writer.py':helper})
        assert not out['entrypoint_proofs'],(call,out)

def test_shadowed_helper_and_unknown_format_conversion_are_not_key_evidence():
    for helper in ('def current(engine):\n return "data/"+engine+".json"\ncurrent=None',
                   'def current(engine):\n return f"data/{engine!r}.json"',
                   'def current(engine):\n return f"data/{engine:10}.json"'):
        out=inspect(helper);assert not out['keys'],out

def test_local_key_helper_uses_same_pure_return_rules():
    code='def current(engine):\n if engine != "market-extremes":raise ValueError()\n return "data/"+engine+".json"\ndef lambda_handler(event,context):\n client.put_object(Key=current("market-extremes"))'
    out=scan_code(code);assert out.writes=={'data/market-extremes.json'},out.unresolved

def test_real_extremes_current_writers_have_exact_native_source_paths():
    for name in ('capitulation','market-extremes'):
        source=ROOT/f'aws/lambdas/justhodl-{name}/source'
        out=SharedWriteGraph(ROOT,source,{},Scan,imported_symbols).analyze(source/'lambda_function.py','lambda_handler')
        key='data/'+name+'.json';assert key in out['keys'],out['unresolved']
        assert all(p['repository_path']=='aws/shared/extremes_native_store.py' and p['via'] for p in out['proofs'][key])
        assert not any('legacy' in p['repository_path'] for p in out['entrypoint_proofs'][key])

def test_primary_declarations_match_the_actual_extremes_renderer_engine():
    for name in ('capitulation','market-extremes'):
        raw=(ROOT/(name+'.html')).read_text(encoding='utf-8');doc=HTML();doc.feed(raw)
        assert doc.primary_engines==['justhodl-'+name]
        assert 'data-extremes-engine="'+name+'"' in raw
        assert any(url.startswith('/jh-extremes-research.js?') for url in doc.scripts)
