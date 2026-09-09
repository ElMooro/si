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
