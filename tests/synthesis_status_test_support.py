from pathlib import Path
from unittest.mock import patch
import copy,hashlib,importlib.util,io,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];C=R/'aws/lambdas/justhodl-ai-website-synthesis/source'
sys.path.insert(0,str(R/'aws/shared'))
import context_evidence_store as store

class Error(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}};super().__init__('Invented storage result')

class Memory:
 def __init__(self):self.objects={};self.reads=[];self.writes=[];self.fail=None;self.race=None;self.clients=[];self.streams=[]
 def get_object(self,**kw):
  key=kw['Key'];assert kw['Bucket']=='justhodl-dashboard-live';self.reads.append(key)
  assert key in self.inputs or key==self.head or key.startswith(self.prefix),key
  if self.fail==('get',key):raise Error('AccessDenied')
  if key not in self.objects:raise Error('NoSuchKey')
  raw=self.objects[key];body=io.BytesIO(raw);self.streams.append(body)
  return {'Body':body,'ContentLength':len(raw),'ETag':'"'+hashlib.sha256(raw).hexdigest()+'"'}
 def put_object(self,**kw):
  key=kw['Key'];raw=kw['Body'];assert isinstance(raw,bytes);assert key==self.head or key.startswith(self.prefix),key
  self.writes.append(copy.deepcopy(kw))
  if self.fail==('put',key) or self.fail==('put_kind','sources') and '/sources/' in key:raise Error('AccessDenied')
  if key==self.head and self.race is not None:self.objects[key]=self.race
  if kw.get('IfNoneMatch')=='*' and key in self.objects:raise Error('PreconditionFailed')
  if 'IfMatch' in kw and kw['IfMatch']!='"'+hashlib.sha256(self.objects.get(key,b'')).hexdigest()+'"':raise Error('PreconditionFailed')
  self.objects[key]=raw
  if self.fail==('ack',key):raise TimeoutError('Invented lost acknowledgement')
  return {'ETag':'"'+hashlib.sha256(raw).hexdigest()+'"'}

def load(memory):
 fake=types.ModuleType('boto3')
 def client(service,**kw):assert service=='s3';memory.clients.append(kw);return memory
 fake.client=client
 cfg=types.ModuleType('botocore.config');cfg.Config=lambda **kw:kw
 spec=importlib.util.spec_from_file_location('research_status',C/'research_status.py');kernel=importlib.util.module_from_spec(spec);spec.loader.exec_module(kernel)
 spec=importlib.util.spec_from_file_location('candidate_synthesis',C/'lambda_function.py');mod=importlib.util.module_from_spec(spec)
 with patch.dict(sys.modules,{'boto3':fake,'botocore.config':cfg,'research_status':kernel}),patch('urllib.request.urlopen',side_effect=AssertionError('No HTTP')),patch('socket.create_connection',side_effect=AssertionError('No sockets')):
  spec.loader.exec_module(mod)
 assert memory.clients==[],'Import must not initialize credentials or services'
 memory.inputs={s['key'] for s in mod.ENGINE_INPUTS.values()};memory.head=mod.OUTPUT_KEY;memory.prefix=mod.PRIVATE
 return mod,kernel

