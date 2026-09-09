"""Behavioral regressions for page ownership, complete traversal and scanner call binding."""
import importlib.util,json,os,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from gen_engine_manifest import ast_keys
from page_sources import page_graph,scan_pages
import gen_engine_wiring as wiring
import bake_engine_directory as baker
from build_page_data_contracts import public_key

def test_ast_binds_environment_tuples_upload_and_wrappers_without_false_outputs():
    fixtures=[
      ('BUCKET,OUT="b","data/real.json"\ns3.put_object(Key=OUT)', ['data/real.json']),
      ('OUT=os.environ.get("OUT","data/real.json")\ns3.put_object(Key=OUT)', ['data/real.json']),
      ('def save(source):\n s3.put_object(Key="data/real.json",Body=source)\nsave("data/not-an-output.json")', ['data/real.json']),
      ('s3.upload_file("data/local.json","b","data/real.json")', ['data/real.json']),
      ('def put_json(key,obj):\n s3.put_object(Key=key,Body=obj)\nput_json(key="data/real.json",obj={})', ['data/real.json']),
      ('s3.put_object(Key="data/day/{}.json".format(day))', ['data/day/*.json']),
      ('OUT="data/wrong.json"\ndef put_json(OUT):\n s3.put_object(Key=OUT)\nput_json("data/real.json")', ['data/real.json']),
      ('MODULES=[("one","data/one.json"),("two","data/two.json")]\nfor name,key in MODULES:\n s3.put_object(Key=key)', ['data/one.json','data/two.json']),
    ]
    for code,expected in fixtures:
        w,_,ok=ast_keys(code);assert ok and w==expected,(code,w)

def test_recursive_routes_relative_imports_exact_keys_and_comments():
    with tempfile.TemporaryDirectory() as td:
        r=Path(td);(r/'desk/deep').mkdir(parents=True)
        (r/'desk/deep/index.html').write_text('<script type="module" src="app.js"></script>')
        (r/'desk/deep/app.js').write_text('import "../common.js";fetch("/data/crypto-cycle-risk.json"); // fetch("data/fake.json")')
        (r/'desk/common.js').write_text('export {x} from "./leaf.js";')
        (r/'desk/leaf.js').write_text('fetch("/portfolio/risk.json");')
        graph=page_graph(r,r/'desk/deep/index.html')
        assert graph['keys']==['data/crypto-cycle-risk.json','portfolio/risk.json'],graph
        assert len(graph['scripts'])==3 and not graph['missing_scripts']
        assert 'desk/deep/index.html' in scan_pages(r)
        (r/'only.html').write_text('<script>fetch("data/crypto-cycle-risk.json")</script>')
        assert 'portfolio/risk.json' not in page_graph(r,r/'only.html')['keys']

def test_wiring_check_rejects_wrong_title_schema_and_duplicate_records():
    with tempfile.TemporaryDirectory() as td:
        old=os.getcwd();os.chdir(td)
        try:
            Path('data').mkdir();Path('page.html').write_text('<script src="/jh-wire.js" data-feeds="data/real.json|engine|TITLE"></script>')
            Path('engine-manifest.json').write_text(json.dumps({'engines':[{'engine':'engine','keys':['data/real.json']}]}))
            wiring.main(['--reconcile']);doc=json.loads(Path('data/engine-wiring.json').read_text())
            for field,value in [('title','WRONG'),('schema_version','wrong.v9'),('engine','wrong')]:
                altered=json.loads(json.dumps(doc));altered['wired'][0][field]=value;Path('data/engine-wiring.json').write_text(json.dumps(altered))
                try:wiring.main(['--check'])
                except SystemExit as exc:assert exc.code!=0
                else:raise AssertionError(field+' drift passed')
            altered=json.loads(json.dumps(doc));altered['wired']*=2;Path('data/engine-wiring.json').write_text(json.dumps(altered))
            try:wiring.main(['--check'])
            except SystemExit as exc:assert exc.code!=0
            else:raise AssertionError('duplicate passed')
        finally:os.chdir(old)

def test_wiring_transaction_rolls_back_partial_rename_failure():
    with tempfile.TemporaryDirectory() as td:
        a,b=Path(td)/'a.html',Path(td)/'b.html';a.write_text('old-a');b.write_text('old-b')
        replace=wiring.os.replace;count=0
        def failing(src,dst):
            nonlocal count
            count+=1
            if count==2:raise OSError('simulated disk failure')
            return replace(src,dst)
        wiring.os.replace=failing
        try:
            try:wiring._apply_transaction([(str(a),'new-a'),(str(b),'new-b')])
            except OSError:pass
            else:raise AssertionError('failure missing')
        finally:wiring.os.replace=replace
        assert a.read_text()=='old-a' and b.read_text()=='old-b'

def test_internal_private_and_pattern_outputs_are_not_published_by_inspector():
    for key in ['portfolio/state.json','data/brain.json','data/users/x.json','data/_council/log.json','data/secrets.json','data/history/*.json','../data/public.json']:
        assert not public_key(key),key
    assert public_key('data/risk-gate.json') and public_key('etf-flows/daily.json')

def test_directory_never_fetches_families_or_labels_partial_as_complete():
    with tempfile.TemporaryDirectory() as td:
        r=Path(td);(r/'engines.html').write_text('__JH_ENGINE_DATA__');(r/'p.html').write_text('<script>fetch("data/one.json");fetch("data/two.json")</script>')
        old_load,old_get=baker.load_entries,baker.get;requests=[]
        baker.load_entries=lambda _:({'engine':{'outs':['data/one.json','data/two.json'],'key_patterns':['data/history/*.json']}},None)
        def get(url,to=12):
            requests.append(url)
            if 'one.json' in url:
                return 200,json.dumps({'generated_at':'2099-01-01T00:00:00Z','rows':[1]}).encode(),{}
            return 404,b'',{}
        baker.get=get
        try:baker.main(td)
        finally:baker.load_entries,baker.get=old_load,old_get
        row=json.loads((r/'engines.html').read_text())['rows'][0]
        assert row['status']!='wired' and not any('*' in u for u in requests)
        assert row['outputs'][0]['state']=='future_timestamp'
        assert row['outputs'][-1]['state']=='pattern_requires_index'

def test_dictionary_kwargs_preserve_key_across_unrelated_conditional_metadata():
    fixtures=[
        ('def put(key,body,gz=False):\n kw={"Bucket":"b","Key":key,"ContentType":"application/json"}\n if gz:\n  kw["ContentEncoding"]="gzip"\n s3.put_object(Body=body,**kw)\nput("data/real.json",{"source":"data/read.json"})', ['data/real.json']),
        ('kw=dict(Bucket="b",Key="data/real.json")\ns3.put_object(**kw)', ['data/real.json']),
        ('kw={"Key":"data/maybe.json"}\nif other:\n kw["Key"]=dynamic\ns3.put_object(**kw)', []),
        ('def put(key):\n s3.put_object(Key=key)\nargs={"key":"data/real.json"}\nput(**args)', ['data/real.json']),
    ]
    for code,expected in fixtures:
        writes,_,ok=ast_keys(code);assert ok and writes==expected,(code,writes)

def test_manifest_includes_alternate_entrypoints_and_explicit_unsupported_runtime():
    from gen_engine_manifest import build
    with tempfile.TemporaryDirectory() as td:
        r=Path(td)
        for name,module,runtime,code in [('alternate','agent.py','python3.12','s3.put_object(Key="data/alternate.json")'),('api','index.js','nodejs18.x','exports.handler=async()=>({ok:true});')]:
            d=r/'aws/lambdas'/name;(d/'source').mkdir(parents=True)
            (d/'source'/module).write_text(code)
            (d/'config.json').write_text(json.dumps({'handler':module.split('.')[0]+'.handler','runtime':runtime}))
        archive=r/'aws/lambdas/_archived/source';archive.mkdir(parents=True);(archive/'lambda_function.py').write_text('s3.put_object(Key="data/wrong.json")')
        doc=build(r);rows={x['engine']:x for x in doc['engines']}
        assert doc['n_engines']==2 and rows['alternate']['keys']==['data/alternate.json']
        assert rows['api']['entrypoint_verified'] and rows['api']['unresolved_writes'] and not rows['api']['keys']

def test_directory_transport_and_denied_responses_never_claim_absence():
    for code in [None,403,500,404]:
        with tempfile.TemporaryDirectory() as td:
            r=Path(td);(r/'engines.html').write_text('__JH_ENGINE_DATA__');(r/'p.html').write_text('<script>fetch("data/one.json")</script>')
            old_load,old_get=baker.load_entries,baker.get
            baker.load_entries=lambda _:({'engine':{'outs':['data/one.json']}},None)
            baker.get=lambda *a,**kw:(code,b'',{})
            try:baker.main(td)
            finally:baker.load_entries,baker.get=old_load,old_get
            row=json.loads((r/'engines.html').read_text())['rows'][0]
            assert row['status']==('wired-missing-feed' if code==404 else 'wired-unverified-feed'),row
            assert row['outputs'][0]['present'] is (False if code==404 else None)

def test_backtest_primary_output_and_owner_mirror_survive_access_classification():
    from build_page_data_contracts import contract
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)
        rows=[]
        for engine,keys in [('backtest-engine',['backtest/results.json','backtest/summary.json']),('context',['data/market-tape.json']),('owner',['portfolio/snapshot.json'])]:
            rows.append({'engine':engine,'keys':keys,'write_evidence':{k:[{'file':'producer.py','line':1}] for k in keys},'key_patterns':[],'unresolved_writes':[]})
        (root/'engine-manifest.json').write_text(json.dumps({'schema_version':'engine-manifest.v3','engines':rows}))
        (root/'backtest.html').write_text('<script>fetch("/backtest/results.json")</script><script src="context.js"></script><script src="private-artifacts.js"></script>')
        (root/'second.html').write_text('<script src="context.js"></script>')
        (root/'private.html').write_text('<script>fetch("/portfolio/snapshot.json")</script>')
        (root/'context.js').write_text('fetch("/data/market-tape.json")')
        (root/'private-artifacts.js').write_text('const mapping={"portfolio/snapshot.json":"portfolio-snapshot"};')
        doc=contract(root);page=doc['pages']['backtest.html']
        assert page['primary_producers']==['backtest-engine'] and page['supplemental_producers']==['context'],page
        assert {o['key'] for o in page['outputs']}=={'backtest/results.json','backtest/summary.json','data/market-tape.json'}
        owner=doc['pages']['private.html']['outputs'][0]
        assert owner['access']=='owner_authenticated' and owner['private_kind']=='portfolio-snapshot'
        assert not doc['pages']['second.html']['primary_producers']

def test_absolute_root_json_reference_is_exact_and_does_not_expand_basenames():
    from page_sources import literal_keys
    keys=literal_keys('fetch("/repo-data.json");fetch("https://justhodl.ai/ecb_data.json?t=1");fetch("/data/crypto-cycle-risk.json");')
    assert keys=={'repo-data.json','ecb_data.json','data/crypto-cycle-risk.json'}
    assert 'risk.json' not in keys

def test_js_ast_handles_regex_templates_wrappers_and_rejects_fragment_ownership():
    from page_sources import literal_keys
    code=r'''const quote=/["']/g;const nested=`<b>${`nested ${value}`}</b>`;
const S3='https://justhodl.ai/data/';const J=key=>fetch(S3+key+'?t='+Date.now());
J('conviction.json');fetch('/data/asset-compass.json');fetch('https://external.example/data/not-owned.json');
fetch('/data/providers/'+slug+'/flows.json.gz');
function mapped(key){const u=S3+key+'?t='+Date.now();return fetch(u);}
function nestedMapped(key){return mapped(key);}
nestedMapped('real.json');
'''
    keys=literal_keys(code)
    assert keys=={'data/conviction.json','data/asset-compass.json','data/real.json'},keys
    bad=literal_keys("const OUT='data/wrong.json';function load(OUT){return fetch(OUT)}load('data/real.json');")
    # A referenced literal can remain source evidence, but a basename is never aliased into a namespace.
    assert 'data/real.json' in bad

def test_inspector_bootstrap_precedes_first_app_request_and_is_idempotent():
    from build_page_data_contracts import install_html
    source='<html><head><script>fetch("https://api.justhodl.ai/first")</script></head><body></body></html>'
    apis=[{'engine':'engine','origin':'https://api.justhodl.ai','pathname':'/first','methods':['GET']}]
    result=install_html(source,apis)
    assert result.index('jh-api-data-contract')<result.index('src="/jh-data-inspector.js?v=')<result.index('fetch(')
    assert 'defer data-contract' not in result
    assert install_html(result,apis)==result
    refreshed=install_html(result,apis,asset_version='0123456789abcdef')
    assert refreshed.count('src="/jh-data-inspector.js?')==1 and 'v=0123456789abcdef' in refreshed

def test_manifest_accepts_only_source_verified_compare_and_swap_augmentation():
    from gen_engine_manifest import build
    with tempfile.TemporaryDirectory() as td:
        r=Path(td);source=r/'aws/lambdas/augment/source';source.mkdir(parents=True)
        code='KEY="data/report.json"\nOUTPUT_OWNERSHIP={"key":KEY,"role":"augmentation","base_producer":"base","compare_and_swap":True}\ns3.put_object(Key=KEY,Body=body,IfMatch=etag)'
        (source/'lambda_function.py').write_text(code)
        entry=build(r)['engines'][0];assert entry['output_roles'][0]['cas_write_verified'] is True
        (source/'lambda_function.py').write_text(code.replace(',IfMatch=etag',''))
        try:build(r)
        except ValueError:pass
        else:raise AssertionError('Unsafe shared writer accepted as augmentation')

def test_entrypoint_call_graph_does_not_fabricate_unknown_helper_invocations():
    from gen_engine_manifest import scan_code
    code='def save(key):\n s3.put_object(Key=key)\ndef lambda_handler(event,context):\n save("data/real.json")'
    scan=scan_code(code);assert scan.writes=={'data/real.json'} and not scan.unresolved,scan.unresolved
    scan=scan_code(code+'\n save(event["dynamic_key"])');assert scan.writes=={'data/real.json'} and scan.unresolved
    scan=scan_code('def lambda_handler(event,context):\n s3.put_object(Key="reports/readme.md",Body="text")')
    assert not scan.unresolved and scan.other_writes[0]['classification']=='non_json_output'

def test_indirect_executor_and_dispatch_targets_retain_ownership_or_explicit_uncertainty():
    from gen_engine_manifest import scan_code
    code='from concurrent.futures import ThreadPoolExecutor\ndef save(key):\n s3.put_object(Key=key)\ndef lambda_handler(event,context):\n with ThreadPoolExecutor() as pool:\n  pool.submit(save,"data/submitted.json")\n  list(pool.map(save,["data/mapped.json"]))'
    scan=scan_code(code);assert scan.writes=={'data/submitted.json','data/mapped.json'} and not scan.unresolved,scan.unresolved
    code='def operation(event):\n s3.put_object(Key=event["key"])\nROUTES={"save":operation}\ndef lambda_handler(event,context):\n return ROUTES[event["action"]](event)'
    scan=scan_code(code);assert scan.unresolved and not scan.writes
    code='class DeadWriter:\n def unused(self,key):\n  s3.put_object(Key=key)\ndef lambda_handler(event,context):\n return {"ok":True}'
    scan=scan_code(code);assert scan.unresolved and not scan.writes

def test_wrapped_handler_aliases_and_known_format_prefixes_preserve_real_output_binding():
    from gen_engine_manifest import scan_code
    code='def lambda_handler(event,context):\n s3.put_object(Key="data/base.json")\n_base_handler=lambda_handler\ndef lambda_handler(event,context):\n return _base_handler(event,context)'
    scan=scan_code(code);assert scan.writes=={'data/base.json'} and not scan.unresolved,scan.unresolved
    code='PREFIX="data/ledger/"\ns3.put_object(Key="%s%s/%s.json.gz" % (PREFIX,day,run))\ns3.put_object(Key="{prefix}{name}.json".format(prefix=PREFIX,name="latest"))'
    scan=scan_code(code);assert scan.writes=={'data/ledger/*/*.json.gz','data/ledger/latest.json'} and not scan.unresolved,scan.writes


def test_imported_constants_are_source_bound_without_executing_modules():
    from gen_engine_manifest import imported_symbols,scan_code
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);shared=root/'shared';shared.mkdir();source=root/'source';source.mkdir()
        (shared/'state_keys.py').write_text('REGISTRY_KEY="data/registry.json"\nraise RuntimeError("must never execute")\n')
        (source/'local_keys.py').write_text('from state_keys import REGISTRY_KEY\nOUT=REGISTRY_KEY\n')
        entry=source/'lambda_function.py';entry.write_text('from local_keys import OUT\nimport state_keys as state\ndef lambda_handler(event,context):\n s3.put_json(OUT,{})\n s3.put_object(Key=state.REGISTRY_KEY)\n')
        symbols=imported_symbols(entry,[source,shared]);scan=scan_code(entry.read_text(),initial_symbols=symbols)
        assert scan.writes=={'data/registry.json'} and not scan.unresolved


def test_separate_archive_index_publisher_keeps_source_engine_and_exact_family_proof():
    from build_page_data_contracts import add_archive_index_relationships
    with tempfile.TemporaryDirectory() as td:
        root=Path(td);publisher='justhodl-public-archive-index';source=root/'aws/lambdas'/publisher/'source';source.mkdir(parents=True)
        (source/'lambda_function.py').write_text('REGISTRY=(("source-engine","data/archive/reviewed/*.json"),)')
        key='data/archive-indexes/source-engine.json';proof=[{'file':'lambda_function.py','line':1}]
        engines={'source-engine':{'key_patterns':['data/archive/reviewed/*.json'],'write_evidence':{'data/archive/reviewed/*.json':proof}},publisher:{'key_patterns':[]}}
        emap={'source-engine':{'outputs':[]},publisher:{'outputs':[{'engine':publisher,'key':key,'ownership_evidence':proof}]}}
        add_archive_index_relationships(emap,engines,root)
        output=emap['source-engine']['outputs'][0]
        assert output['engine']==publisher and output['source_engine']=='source-engine' and output['archive_index']['engine']=='source-engine'
        assert output['archive_family_evidence']==proof
        engines['other-writer']={'key_patterns':['data/archive/reviewed/*.json']}
        try:add_archive_index_relationships(emap,engines,root)
        except ValueError:pass
        else:raise AssertionError('shared ownership was silently accepted')
