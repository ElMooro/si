"""Actual source replay, arithmetic, publication and consumer boundaries; no AWS."""
from pathlib import Path
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_UP
from unittest.mock import patch
import ast,gzip,io,json,sys,textwrap,unittest,statistics
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import fedwatch_research_model as model
import fedwatch_research_store as store
import report_observations as compiler
STAMP='2026-09-20T22:55:00+00:00';AT=datetime.fromisoformat(STAMP)

class Failure(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
 def __init__(self,objects=None):self.objects=dict(objects or {});self.reads=[];self.writes=[]
 def get_object(self,**kw):
  k=kw['Key'];self.reads.append(k)
  if k not in self.objects:raise Failure('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[k]),'ETag':model.sha(self.objects[k])}
 def put_object(self,**kw):
  k=kw['Key'];old=self.objects.get(k)
  if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
  if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
  self.objects[k]=kw['Body'];self.writes.append(kw)

def fixture():
 objects={};entries={};measurements={};originals={}
 for sid,(label,freq) in model.SPECS.items():
  unit='Percent';adj='NSA'
  if freq=='D':days=[date(2025,1,1)+timedelta(days=i) for i in range(626) if (date(2025,1,1)+timedelta(days=i)).weekday()<5]
  elif freq=='W':days=[date(2024,9,18)+timedelta(days=7*i) for i in range(105)]
  elif freq=='M':days=[date(2020+i//12,i%12+1,1) for i in range(80)]
  else:days=[date(2014+i//4,1+3*(i%4),1) for i in range(51)]
  definition={'seriess':[{'id':sid,'title':label,'units':unit,'frequency_short':freq,'seasonal_adjustment_short':adj,'seasonal_adjustment':adj}]}
  base=Decimal('0.5' if sid=='RECPROUSM156N' else '5.5' if sid=='DFEDTARU' else '5.25' if sid=='DFEDTARL' else '5.3' if sid=='DFF' else '15')
  rows=[{'date':str(d),'value':str(base+Decimal(i)/1000+Decimal(i%7)/10000)} for i,d in enumerate(days)]
  rows[10]['value']='.'
  obs={'observations':rows[::-1],'units':'lin','output_type':1,'count':len(rows),'limit':4000,'offset':0}
  evidence={}
  for kind,doc in (('definition',definition),('observations',obs)):
   raw=model.encoded(doc);digest=model.sha(raw);url='https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?series_id='+sid
   if kind=='observations':url+='&units=lin&limit=4000&sort_order=desc'
   key='data/evidence/fred/'+model.sha(url.encode())+'/'+digest+'.bin.gz'
   evidence[kind]={'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':STAMP}
   objects[key]=gzip.compress(raw,mtime=0)
  entries[sid]={'evidence':evidence,'acquired_at':STAMP};originals[sid]={**entries[sid],'definition':definition,'observations':obs}
  measurements[sid]=compiler.measurement(sid,definition,obs,evidence,STAMP,STAMP)
 packet={'contract':compiler.CONTRACT,'generated_at':STAMP,'measurements':measurements}
 raw=Path(compiler.__file__).read_bytes();digest=model.sha(raw);ck='data/report-research/compilers/'+digest+'.py';objects[ck]=raw
 md={'contract':'report-research-replay.v1','generated_at':STAMP,'inputs':entries,'compiler':{'key':ck,'sha256':digest},'output_sha256':model.sha(model.encoded(packet))}
 raw=model.encoded(md);key='data/report-research/runs/'+model.sha(raw)+'.json';objects[key]=raw
 packet['replay']={'manifest_key':key,'output_sha256':md['output_sha256']};objects[store.SOURCES[0]]=model.encoded(packet)
 for key in store.SOURCES[1:]:objects[key]=b'{"call":"LONG","system_tail_gauge":99,"indices":[{"ticker":"SPY","p_drop_10":0.99}],"all_original_fields":"preserved"}'
 client=Storage(objects)
 from test_fedwatch_sources import collection,GENERATED
 capture,bodies=collection()
 for raw in bodies.values():store.original(client,'b',raw)
 inputs={'contract':'fedwatch-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),
         'legacy':{k:store.snapshot(client,'b',k) for k in store.SOURCES[1:]},'collection':capture}
 inputs['generated_at']=GENERATED
 return client,inputs,packet,originals

def packet():
 s,i,_,_=fixture();o=store.compile_output(i,store.reader(s,'b'));return s,i,{**o,'replay':store.retain(s,'b',i,o)}

