"""Invalid Python invocations cannot certify source ownership or input lineage."""
from pathlib import Path
import ast,inspect,itertools,sys,textwrap
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts'),str(ROOT/'tests/deployment')]
from gen_engine_manifest import scan_code
from test_shared_write_graph import scan_files
from source_call_binding import bind_call


def check(body,writer):
    local=scan_code(writer+'\ndef lambda_handler(event,context):\n '+body)
    shared=scan_files('from writer import save\ndef lambda_handler(event,context):\n '+body,{'writer.py':writer})
    return (local.writes,local.unresolved),(shared['keys'],shared['unresolved'])


def test_duplicate_missing_excess_and_unexpected_arguments_are_not_writes():
    writer="def save(client,key='data/default.json',*,required):\n client.put_object(Key=key)"
    calls=("save(client,'data/a.json',key='data/b.json',required='ok')",
           "save(client,'data/a.json','extra',required='ok')",
           "save(client,'data/a.json')",
           "save(key='data/a.json',required='ok')",
           "save(client,key='data/a.json',required='ok',extra='bad')",
           "save(client,key='data/a.json',required='ok',**{'key':'data/b.json'})",
           "save(client,required='ok',**{'key':'data/a.json'},**{'key':'data/b.json'})")
    for call in calls:
        # These are valid parsed Python whose invocation raises TypeError.
        ast.parse(writer+'\n'+call)
        for keys,issues in check(call,writer):
            assert keys==set(),(call,keys)
            assert any('invalid Python call binding' in row['reason'] for row in issues),(call,issues)


def test_valid_keyword_only_defaults_and_known_unpack_keep_lexical_values():
    writer="KEY='data/default.json'\ndef save(client,key=KEY,*,suffix='.json'):\n client.put_object(Key=key)"
    for call,expected in (("save(client)",'data/default.json'),("save(client,**{'key':'data/known.json'})",'data/known.json')):
        for keys,issues in check(call,writer):assert keys=={expected},(keys,issues)


def test_positional_only_keyword_capture_cannot_replace_its_actual_parameter():
    writer="def save(key,/,**kwargs):\n client.put_object(Key=key)\n client.put_object(Key=kwargs['key'])"
    for keys,issues in check("save('data/position.json',key='data/captured.json')",writer):
        assert keys=={'data/position.json','data/captured.json'},(keys,issues)
    for keys,issues in check("save(key='data/wrong.json')",writer):
        assert not keys and any('missing required' in r['reason'] for r in issues)
    writer="def save(key,/):\n client.put_object(Key=key)"
    for keys,issues in check("save(key='data/wrong.json')",writer):assert not keys and issues


def test_extra_keywords_never_shadow_unrelated_globals_and_distinct_kwargs_calls_remain_distinct():
    writer="KEY='data/global.json'\ndef save(client,**kwargs):\n client.put_object(Key=KEY)\n client.put_object(Key=kwargs['key'])"
    body="save(client,KEY='data/false-global.json',key='data/one.json')\n save(client,key='data/two.json')"
    for keys,issues in check(body,writer):
        assert keys=={'data/global.json','data/one.json','data/two.json'},(keys,issues)


def test_unknown_expansions_cannot_certify_defaults_or_unrelated_caller_globals():
    writer="key='data/global-shadow.json'\ndef save(client,key='data/default.json',**kwargs):\n client.put_object(Key=key)\n client.put_object(Key=kwargs['key'])"
    for body in ("save(client,**event['kwargs'])","save(*event['args'])"):
        for keys,issues in check(body,writer):assert not keys and issues,(keys,issues)


def test_invalid_call_does_not_erase_valid_call_to_the_same_writer():
    writer="def save(client,key):\n client.put_object(Key=key)"
    body="save(client,'data/valid.json')\n save(client,'data/other.json',key='data/impossible.json')"
    for keys,issues in check(body,writer):
        assert keys=={'data/valid.json'} and any('multiple values' in r['reason'] for r in issues),(keys,issues)


def test_duplicate_keywords_in_direct_storage_calls_cannot_be_reads_or_writes():
    for method in ('put_object','get_object'):
        for arguments in ("Key='data/a.json',**{'Key':'data/b.json'}", "Key='data/a.json',Bucket='bucket',**{'Bucket':'other'}", "**{'Key':'data/a.json'},**{'Key':'data/b.json'}"):
            result=scan_code('def lambda_handler(event,context):\n client.'+method+'('+arguments+')')
            assert not result.writes and not result.reads
            assert any('duplicate keyword' in r['reason'] for r in result.unresolved),result.unresolved
    result=scan_code("def lambda_handler(event,context):\n client.put_object(Key='data/current.json',**{'IfMatch':etag})")
    assert result.writes=={'data/current.json'} and not result.unresolved


def test_concrete_binding_matches_python_signature_rules_without_running_any_function():
    def ordinary(a,b='B',*,c='C'):raise AssertionError('Never execute synthetic body')
    def required(a,b,*,c):raise AssertionError('Never execute synthetic body')
    def positional(a,/,b='B',**kwargs):raise AssertionError('Never execute synthetic body')
    def variadic(a,*rest,b='B',**kwargs):raise AssertionError('Never execute synthetic body')
    def keyword(*,a,b='B'):raise AssertionError('Never execute synthetic body')
    def empty():raise AssertionError('Never execute synthetic body')
    for function in (ordinary,required,positional,variadic,keyword,empty):
        fn=ast.parse(textwrap.dedent(inspect.getsource(function))).body[0];signature=inspect.signature(function)
        for n in range(5):
            values=['position-'+str(i) for i in range(n)]
            for switches in itertools.product((False,True),repeat=4):
                keywords={key:'keyword-'+key for key,on in zip(('a','b','c','outer'),switches) if on}
                call=ast.Call(func=ast.Name(id=function.__name__),args=[ast.Constant(v) for v in values],keywords=[ast.keyword(arg=k,value=ast.Constant(v)) for k,v in keywords.items()])
                bound,invalid=bind_call(fn,call,ast.literal_eval,ast.literal_eval,{'outer':'lexical','rest':'stale','kwargs':'stale'})
                try:expected=signature.bind(*values,**keywords);expected.apply_defaults()
                except TypeError:assert invalid is not None,(function.__name__,values,keywords,bound);continue
                assert invalid is None,(function.__name__,values,keywords,invalid)
                for name,value in expected.arguments.items():
                    if signature.parameters[name].kind is inspect.Parameter.VAR_POSITIONAL:assert bound[name] is None
                    else:assert bound[name]==value,(function.__name__,name,bound[name],value)
                assert bound['outer']=='lexical'


def test_executor_unknown_iterable_values_keep_the_real_callback_argument_count():
    prefix='from concurrent.futures import ThreadPoolExecutor\n'
    writer="def save(key):\n client.put_object(Key=key)\n client.put_object(Key='data/constant.json')"
    body="with ThreadPoolExecutor() as pool:\n  list(pool.map(save,event['keys']))"
    local=scan_code(prefix+writer+'\ndef lambda_handler(event,context):\n '+body)
    shared=scan_files(prefix+'from writer import save\ndef lambda_handler(event,context):\n '+body,{'writer.py':writer})
    for keys,issues in ((local.writes,local.unresolved),(shared['keys'],shared['unresolved'])):
        assert keys=={'data/constant.json'} and issues
        assert not any('invalid Python call binding' in row['reason'] for row in issues),issues
