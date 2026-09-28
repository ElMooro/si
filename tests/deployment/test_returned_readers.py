"""Returned source readers must keep their lexical key binding without execution."""
from pathlib import Path
import hashlib, runpy, sys, tempfile

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from gen_engine_manifest import Scan, imported_symbols, ast_keys
from source_write_graph import SharedWriteGraph

FACTORY='''
def reader(client, prefix='data/default/'):
    def read(key='head.json'):
        return client.get_object(Key=prefix+key)
    return read
'''


def analyze(handler, helper, shared=False):
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);source=root/'aws/lambdas/test/source';source.mkdir(parents=True)
        target=root/'aws/shared' if shared else source;target.mkdir(exist_ok=True,parents=True)
        (source/'lambda_function.py').write_text(handler,encoding='utf-8')
        (target/'reader.py').write_text(helper,encoding='utf-8')
        return SharedWriteGraph(root,source,{},Scan,imported_symbols).analyze(source/'lambda_function.py','lambda_handler')


def test_local_returned_reader_preserves_default_and_captured_prefix():
    code=FACTORY+'''
def lambda_handler(event,context):
    read=reader(client,'data/retained/')
    prefix='data/wrong/'
    read()
    read('other.json')
'''
    assert ast_keys(code)==([],['data/retained/head.json','data/retained/other.json'],True)
    assert ast_keys(code.replace("'data/retained/'","'data/default/'"))[1]==['data/default/head.json','data/default/other.json']
    assert ast_keys(FACTORY+"def lambda_handler(event,context):\n reader(client,'data/direct/')('head.json')")[1]==['data/direct/head.json']


def test_distinct_imported_closures_and_callback_arguments_do_not_collapse():
    code='''from reader import reader
def consume(read):
    return read('same.json')
def lambda_handler(event,context):
    first=reader(client,'data/first/')
    second=reader(client,'data/second/')
    consume(first)
    consume(second)
    reader(client,'data/direct/')('same.json')
'''
    for shared in (False,True):
        out=analyze(code,"raise RuntimeError('must not execute application')\n"+FACTORY,shared)
        assert out['reads']=={'data/first/same.json','data/second/same.json','data/direct/same.json'},out
        assert not out['keys'] and not out['proofs']


def test_delegated_local_reads_are_retained_without_shared_write_claims():
    code="from reader import collect\ndef lambda_handler(event,context):\n collect('data/exact.json')"
    out=analyze(code,"def collect(key):\n client.get_object(Key=key)\n client.put_object(Key='data/local-output.json')")
    assert out['reads']=={'data/exact.json'} and out['keys']==set() and not out['proofs'],out


def test_deep_captured_configurations_remain_distinct_at_fingerprint_bound():
    helper="def reader(config):\n def read(key):\n  return client.get_object(Key=config['a']['b']['c']['prefix']+key)\n return read\n"
    code="from reader import reader\ndef consume(read):\n read('same.json')\ndef lambda_handler(event,context):\n first=reader({'a':{'b':{'c':{'prefix':'data/first/'}}}})\n second=reader({'a':{'b':{'c':{'prefix':'data/second/'}}}})\n consume(first)\n consume(second)\n"
    assert analyze(code,helper)['reads']=={'data/first/same.json','data/second/same.json'}


def test_shadowed_returned_reader_and_uncalled_factory_do_not_add_reads():
    for body in ("read=reader(client)\n read=event['reader']\n read('wrong.json')", "reader(client)"):
        code='def lambda_handler(event,context):\n    '+body.replace('\n ','\n    ')
        assert ast_keys(FACTORY+code)[1]==[]
        assert analyze('from reader import reader\n'+code,FACTORY)['reads']==set()


def test_uncertain_factory_paths_and_argument_shapes_are_not_guessed():
    code="from reader import reader\ndef lambda_handler(event,context):\n read=reader(client)\n read('wrong.json')"
    helpers=[FACTORY.replace('def reader','@decorate\ndef reader',1),
             FACTORY.replace('    def read','    @decorate\n    def read',1),
             FACTORY.replace('    return read','    if flag:\n        return read\n    return other'),
             FACTORY.replace('    def read','    prefix=dynamic()\n    def read',1),
             FACTORY.replace('def reader','async def reader',1),
             FACTORY.replace('        return client','        nonlocal prefix\n        return client',1),
             FACTORY.replace('    return read','    return reader(client)')]
    for helper in helpers:
        assert analyze(code,helper)['reads']==set()
        assert ast_keys(helper+code.replace('from reader import reader\n',''))[1]==[]
    for invoke in ('reader()', 'reader(client, **event)', 'reader(*event)', "reader(client,'data/a/',prefix='data/b/')", "reader(client,unknown='data/b/')"):
        assert analyze(code.replace('reader(client)',invoke),FACTORY)['reads']==set(),invoke


def test_definition_defaults_do_not_borrow_callers_local_values():
    helper="DEFAULT='data/definition/'\n"+FACTORY.replace("prefix='data/default/'","prefix=DEFAULT")
    code="def lambda_handler(event,context):\n DEFAULT='data/wrong/'\n read=reader(client)\n read()"
    assert ast_keys(helper+code)[1]==['data/definition/head.json']
    assert analyze('from reader import reader\n'+code,helper)['reads']=={'data/definition/head.json'}


def test_real_bond_desk_current_flow_and_credit_are_source_references():
    source=ROOT/'aws/lambdas/justhodl-bond-desk/source'
    out=SharedWriteGraph(ROOT,source,{},Scan,imported_symbols).analyze(source/'lambda_function.py','lambda_handler')
    assert {'data/etf-true-flows.json','data/credit-stress.json'}<=out['reads'],sorted(out['reads'])


def test_whole_predecessor_scanners_remain_byte_identical():
    expected={'gen_engine_manifest.py':'11eef39a7709d54adda44c21e5a01396017efbbb0313e1da4fe936a67830eb0b',
              'build_dependency_map.py':'e704724c73c95f7638464ea5b8c3a80c6cbeace1d5b47776e77b9d34f92d3f08',
              'source_write_graph.py':'60d9b8eeb96edf11d30ca37b1e3ed95cc31089979b0fc4119c3d080c9db280e6'}
    for name,digest in expected.items():
        assert hashlib.sha256((ROOT/'tests/fixtures'/('pre-returned-reader-'+name+'.txt')).read_bytes()).hexdigest()==digest


def test_preserved_predecessor_reproduces_the_missing_input():
    before=runpy.run_path(str(ROOT/'tests/fixtures/pre-returned-reader-gen_engine_manifest.py.txt'))
    code=FACTORY+"def lambda_handler(event,context):\n read=reader(client)\n read('head.json')"
    assert before['ast_keys'](code)==([],[],True)
    assert ast_keys(code)==([],['data/default/head.json'],True)


def test_source_text_is_utf8_and_invalid_input_does_not_get_replacement_characters():
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);p=root/'helper.py';entry=root/'entry.py'
        entry.write_text('from helper import LABEL\n',encoding='utf-8')
        p.write_bytes("LABEL='债券'\n".encode('utf-8'))
        assert imported_symbols(entry,[root])['LABEL']=='债券'
        p.write_bytes(b"LABEL='bad \xff'\n")
        try:imported_symbols(entry,[root])
        except UnicodeDecodeError:pass
        else:raise AssertionError('invalid source was silently decoded')
