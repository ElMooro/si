"""Actual personal-journal HTTP/CRUD paths; all network/cloud I/O replaced."""
import importlib.util
import io
import json
import sys
import types
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[2]/'shared'))


class Missing(Exception):
 def __init__(self):self.response={'Error':{'Code':'NoSuchKey'}}


class Store:
 def __init__(self):self.objects={};self.reads=[];self.writes=[]
 def get_object(self,**kw):
  self.reads.append(kw['Key'])
  if kw['Key'] not in self.objects:raise Missing()
  return {'Body':io.BytesIO(self.objects[kw['Key']])}
 def put_object(self,**kw):
  self.writes.append(kw);self.objects[kw['Key']]=kw['Body']


def load():
 boto=types.ModuleType('boto3');boto.client=lambda *a,**k:types.SimpleNamespace()
 sys.modules['boto3']=boto
 exc=types.ModuleType('botocore.exceptions');exc.ClientError=Missing
 sys.modules['botocore.exceptions']=exc
 secret=types.ModuleType('managed_secret');secret.managed_secret=lambda *args,**kw:''
 sys.modules['managed_secret']=secret
 spec=importlib.util.spec_from_file_location('personal_journal',HERE.parent/'source/lambda_function.py')
 mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
 store=Store();mod.S3=store;mod.get_admin_token=lambda:'legacy-owner-token'
 import private_artifact
 private_artifact.service_headers=lambda:{'X-JH-Service-Token':'owner-service-token'}
 publications=[]
 mod.publish_private=lambda kind,doc:publications.append((kind,json.loads(json.dumps(doc))))
 return mod,store,publications


def http(method='GET',path='/',body=None,service=True):
 return {'requestContext':{'http':{'method':method}},'rawPath':path,
         'headers':{'X-JH-Service-Token':'owner-service-token'} if service else {},
         'body':json.dumps(body or {})}


def response(mod,event):
 out=mod.lambda_handler(event,None)
 return out,json.loads(out['body'])


def test_anonymous_get_and_scheduled_spoof_denied_before_any_account_access():
 mod,store,pub=load()
 mod.op_mtm=lambda _:(_ for _ in ()).throw(AssertionError('HTTP cannot bypass auth with scheduled flags'))
 for event in (http(service=False),{**http(service=False),'source':'aws.events','scheduled':True},http('POST','/add',{'scheduled':True},False),{**http(service=False),'body':'{invalid json'}):
  result,_=response(mod,event)
  assert result['statusCode']==401
  assert not store.reads and not store.writes and not pub


def test_owner_service_get_returns_full_private_ledger_with_no_store():
 mod,store,pub=load()
 store.objects[mod.S3_KEY_TRADES]=json.dumps({'version':2,'trades':[{'id':'personal','ticker':'AAA','status':'OPEN','thesis':'private fixture'}]}).encode()
 result,body=response(mod,http())
 assert result['statusCode']==200 and result['headers']['Cache-Control']=='private, no-store'
 assert body['trades']['trades'][0]['thesis']=='private fixture'
 assert body['stats']['n_open']==1 and not store.writes and not pub


def test_authorized_service_crud_preserves_behavior_and_private_publication():
 mod,store,pub=load()
 result,body=response(mod,http('POST','/add',{'ticker':'AAA','entry_price':100,'size_usd':1000,'entry_date':'2026-09-01','thesis':'private fixture'}))
 assert result['statusCode']==200,body
 tid=body['trades']['trades'][0]['id']
 assert len(store.writes)==2 and {k for k,d in pub}=={'personal-trades','personal-trades-stats'}
 assert all(w['CacheControl']=='private, no-store' for w in store.writes)
 result,body=response(mod,http('POST','/update',{'id':tid,'stop':90}))
 assert result['statusCode']==200 and body['trades']['trades'][0]['stop']==90
 result,body=response(mod,http('POST','/close',{'id':tid,'exit_price':110,'exit_date':'2026-09-03'}))
 assert result['statusCode']==200 and body['trades']['trades'][0]['status']=='CLOSED'
 result,body=response(mod,http('POST','/delete',{'id':tid}))
 assert result['statusCode']==200 and body['trades']['trades']==[]


def test_existing_valid_admin_crud_and_trusted_schedule_still_work():
 mod,store,pub=load();event=http('POST','/add',{'ticker':'AAA','entry_price':100,'size_usd':1000},False)
 event['headers']={'x-justhodl-token':'legacy-owner-token'}
 assert response(mod,event)[0]['statusCode']==200
 mod.op_mtm=lambda doc:0
 out=mod.lambda_handler({'source':'aws.events'},None)
 assert out['statusCode']==200
 assert pub[-1][0]=='personal-trades-stats'


def test_private_publish_failure_prevents_canonical_trade_write():
 mod,store,pub=load()
 mod.publish_private=lambda *args:(_ for _ in ()).throw(RuntimeError('private publisher unavailable'))
 try:
  mod.lambda_handler(http('POST','/add',{'ticker':'AAA','entry_price':100,'size_usd':1000}),None)
  raise AssertionError('private publication failure must propagate')
 except RuntimeError:pass
 assert not store.writes


def test_empty_seed_uses_real_stats_without_inventing_performance():
 mod,store,pub=load();seed=mod.empty_private_artifacts()
 assert seed['personal-trades']=={'version':0,'trades':[]}
 assert set(seed['personal-trades-stats'])=={'as_of','n_total','n_open','n_closed'}
 assert seed['personal-trades-stats']['n_total']==0 and not store.reads and not pub

if __name__=='__main__':
 for name,fn in sorted(globals().copy().items()):
  if name.startswith('test_') and callable(fn):fn();print('ok',name)
 print('personal journal privacy/CRUD tests passed')
