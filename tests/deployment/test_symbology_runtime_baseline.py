from pathlib import Path
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,importlib.util,json,sys,tempfile
R=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(R/'aws/ops'),str(R/'aws/ops/checks')]
spec=importlib.util.spec_from_file_location('symbology_baseline_candidate',R/'aws/ops/staged/ops_6397_symbology_runtime_baseline.py');op=importlib.util.module_from_spec(spec);spec.loader.exec_module(op)
def actual():
 return {'function_name':op.FN,'receipt':{'status':'matched','commit':op.SOURCE_COMMIT},'source_files_checked':2,'handler_bytes':20,'timeout':120,'memory_mb':1024,'ephemeral_storage_mb':512,
  'code_sha256':'invented-code-hash','runtime':'python3.12','handler':'lambda_function.lambda_handler','role':'invented-role','architectures':['x86_64'],
  'schedules':[{'kind':'EventBridge rule','name':'invented-native-schedule','state':'ENABLED','expression':'cron(0 2 * * ? *)','native_targets':1}]}
def refuses(fn):
 try:fn()
 except (ValueError,KeyError,TypeError):return
 raise AssertionError('Invalid baseline accepted')
def test_exact_function_receipt_and_counts_required():
 d=actual();op.validate(d)
 for update in ({'function_name':'other'},{'receipt':{'status':'matched','commit':'b'*40}},{'source_files_checked':True},{'timeout':120.0},{'memory_mb':0},{'architectures':[]},{'schedules':None}):refuses(lambda:op.validate({**d,**update}))
def test_schedule_order_normalized_without_dropping_any_binding():
 d=actual();d['schedules']+=[{**d['schedules'][0],'name':'another'}];other=deepcopy(d);other['schedules'].reverse()
 assert op.normalized(d)==op.normalized(other) and len(op.normalized(d)['schedules'])==2
 for update in ({'native_targets':0},{'native_targets':True},{'expression':None},{'state':''}):
  bad=deepcopy(d);bad['schedules'][0].update(update);refuses(lambda:op.validate(bad))
def test_receipt_wrapper_refuses_every_other_path_or_extra_argument():
 calls=[];client=SimpleNamespace(get_object=lambda **kw:calls.append(kw) or {})
 wrapped=op.ReceiptOnly(client);good={'Bucket':op.BUCKET,'Key':'data/ops/releases/'+op.FN+'.json'};wrapped.get_object(**good)
 for kw in ({**good,'Key':'data/symbology/master.json'},{**good,'Key':'data/_state/bond-cusip-queue.json'},{**good,'Bucket':'other'},{**good,'Range':'bytes=0-99'}):refuses(lambda:wrapped.get_object(**kw))
 assert calls==[good] and not hasattr(wrapped,'put_object')
def exercise(before,after):
 records=[];services=[];observations=iter([before,after])
 @contextmanager
 def report(name):
  assert name=='ops_6397_symbology_runtime_baseline'
  yield SimpleNamespace(kv=lambda **kw:records.append(kw))
 def client(name,**kw):services.append(name);return SimpleNamespace()
 with tempfile.TemporaryDirectory() as directory:
  root=Path(directory);folder=root/'aws/lambdas'/op.FN;folder.mkdir(parents=True)
  (folder/'config.json').write_text(json.dumps({'function_name':op.FN,'runtime':'python3.12','handler':'lambda_function.lambda_handler','timeout':120,'memory':1024,'role':'invented-role'}),encoding='utf-8')
  (root/'whole-invented-source.py').write_bytes(b'VALUE = "invented complete fixture"\n')
  hashes={'whole-invented-source.py':hashlib.sha256((root/'whole-invented-source.py').read_bytes()).hexdigest()}
  with patch.object(op,'ROOT',root),patch.object(op,'SOURCE_HASHES',hashes),patch.object(op,'runtime',side_effect=lambda *a:next(observations)),patch.dict(sys.modules,{'boto3':SimpleNamespace(client=client),'ops_report':SimpleNamespace(report=report)}):op.main()
 assert services==['lambda','s3','events','scheduler']
 return records
def test_main_records_controls_only_and_no_publication_authority():
 d=actual();records=exercise(d,deepcopy(d));e=records[0]['evidence'];assert e['status']=='baseline_observed'
 assert e['normal_publication_verified'] is e['source_replay_verified'] is e['investment_authority'] is False
 for key in ('native_invocations','provider_requests','current_packet_reads','private_reads','account_reads','consumer_reads','native_writes','schedule_changes','application_log_queries'):assert e[key]==0
def test_changed_native_snapshot_fails_before_success_evidence():
 d=actual();other=deepcopy(d);other['timeout']=121;refuses(lambda:exercise(d,other))
def test_declared_runtime_mismatch_or_missing_schedule_is_review_required():
 for change in ({'timeout':119},{'schedules':[]}):
  d={**actual(),**change};e=exercise(d,deepcopy(d))[0]['evidence'];assert e['status']=='baseline_observed_review_required'
if __name__=='__main__':
 tests=[v for k,v in globals().copy().items() if k.startswith('test_') and callable(v)]
 for test in tests:test()
 print('Baseline tests passed:',len(tests))
