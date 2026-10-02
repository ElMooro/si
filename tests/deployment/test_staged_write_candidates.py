"""Returned-bundle candidate inventory never grants handler or runtime proof."""
import importlib.util,json,sys,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from gen_engine_manifest import scan_code,build
from source_staged_writes import candidates
SOURCE='''
def collect():
    pending=[]
    for key, document in (("data/a.json", None),("data/b.json", None)):
        if document is not None:
            pending.append(dict(Key=key,Body=document))
    pending.append(dict(Key="data/c.json",Body=b"{}"))
    return {"writes":pending}
def lambda_handler(event,context):
    result=collect()
    for item in result["writes"]:
        s3.put_object(**item)
'''

def test_complete_staged_candidates_keep_literal_scope_and_locations():
 rows=candidates(scan_code(SOURCE));assert {r['key'] for r in rows}=={'data/a.json','data/b.json','data/c.json'}
 assert all(r['line']==12 and r['runtime_verified'] is False for r in rows)
 assert {r['builder_line'] for r in rows}=={6,7}

def test_unused_bundle_or_different_field_is_not_a_write_candidate():
 for source in (SOURCE.replace('s3.put_object(**item)','inspect(item)'),SOURCE.replace('result["writes"]','result["other"]'),SOURCE.replace('result=collect()','result=unknown()')):
  assert candidates(scan_code(source))==[]

def test_rebound_collection_or_result_cannot_retain_previous_candidates():
 for source in (SOURCE.replace('return {"writes":pending}','pending=[]\n    return {"writes":pending}'),SOURCE.replace('for item in result','result={}\n    for item in result'),SOURCE.replace('s3.put_object(**item)','item={}\n        s3.put_object(**item)'),SOURCE.replace('return {"writes":pending}','mutate(pending)\n    return {"writes":pending}')):
  assert candidates(scan_code(source))==[]

def test_arbitrary_return_factory_parameters_and_shadowed_builder_stay_unresolved():
 for source in (SOURCE.replace('def collect():','def collect(value):'),SOURCE.replace('return {"writes":pending}','return opaque(pending)'),SOURCE+'\ncollect = other\n',SOURCE.replace('def collect():','@decorator\ndef collect():')):
  assert candidates(scan_code(source))==[]

def test_read_keys_and_unappended_specs_are_not_candidates():
 source=SOURCE.replace('pending.append(dict(Key="data/c.json",Body=b"{}"))','unused=dict(Key="data/not-output.json",Body=b"{}")\n    s3.get_object(Key="data/read.json")\n    pending.append(dict(Key="data/c.json",Body=b"{}"))')
 assert {r['key'] for r in candidates(scan_code(source))}=={'data/a.json','data/b.json','data/c.json'}

def test_complete_manifest_keeps_candidates_but_reachability_and_ownership_unproven():
 with tempfile.TemporaryDirectory() as directory:
  root=Path(directory);p=root/'aws/lambdas/invented/source';p.mkdir(parents=True);(p/'lambda_function.py').write_text(SOURCE,encoding='utf-8');(p.parent/'config.json').write_text(json.dumps({'handler':'lambda_function.lambda_handler'}),encoding='utf-8')
  row=build(root)['engines'][0];assert row['keys']==['data/a.json','data/b.json','data/c.json'];assert row['ownership_status']=='incomplete';assert row['unresolved_writes']
  for key in row['keys']:
   assert row['output_reachability'][key]['status']=='unproven';assert all(e['entrypoint_reachability']=='unproven' and e['runtime_verified'] is False for e in row['write_evidence'][key])

def test_actual_compound_three_pending_outputs_remain_declared_candidates():
 source=(ROOT/'aws/lambdas/justhodl-compound-aggregator/source/lambda_function.py').read_text(encoding='utf-8')
 rows=candidates(scan_code(source));assert {r['key'] for r in rows}=={'data/compound-firstseen.json','data/compound-history.json','data/prime-convergence.json'}


def test_imported_or_local_shadowed_builder_and_dict_are_not_assumed_builtin():
 for source in (SOURCE+'\nfrom unknown import collect\n',SOURCE+'\nimport unknown as collect\n',SOURCE.replace('result=collect()','from unknown import collect\n    result=collect()'),SOURCE+'\ndict=opaque\n',SOURCE.replace('pending=[]','pending=[]\n    dict=opaque')):
  assert candidates(scan_code(source))==[]


def test_changed_loop_key_does_not_inherit_literal_candidates():
 source=SOURCE.replace('if document is not None:','key=unknown()\n        if document is not None:')
 assert {r['key'] for r in candidates(scan_code(source))}=={'data/c.json'}


def test_nested_function_shadow_cannot_borrow_the_module_builder():
 source=SOURCE.replace("result=collect()","def collect(): return unknown()\n    result=collect()")
 assert candidates(scan_code(source))==[]
