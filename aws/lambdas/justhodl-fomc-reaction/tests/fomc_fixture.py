"""Actual source replay, arithmetic, publication and consumer boundaries; no AWS."""
from pathlib import Path
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_UP
from unittest.mock import patch
import ast,gzip,io,json,sys,textwrap,unittest,statistics
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import fomc_research_model as model
import fomc_research_store as store
import report_observations as compiler
STAMP='2026-09-20T23:40:00+00:00';AT=datetime.fromisoformat(STAMP)

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

def calendar_raw():
 rows=[('January','27-28'),('March','17-18*'),('April','28-29'),('June','16-17*'),('July','28-29'),('September','15-16*'),('October','27-28'),('December','8-9*')]
 return ('<html><h4>2026 FOMC Meetings</h4>'+''.join('<div class="fomc-meeting__month">'+m+'</div><div class="fomc-meeting__date">'+d+'</div>' for m,d in rows)+'</html>').encode()

def fixture(missing=()):
 objects={};entries={};measurements={};originals={}
 for sid,(label,unit,kind) in model.SPECS.items():
  if sid in missing:continue
  days=[date(2025,1,1)+timedelta(days=i) for i in range(626) if (date(2025,1,1)+timedelta(days=i)).weekday()<5]
  definition={'seriess':[{'id':sid,'title':label,'units':unit,'frequency_short':'D','seasonal_adjustment_short':'NSA','seasonal_adjustment':'Not Seasonally Adjusted'}]}
  base=Decimal('4') if kind=='yield' else Decimal('5000')
  rows=[{'date':str(d),'value':str(base+Decimal(i%21)/100)} for i,d in enumerate(days)];rows[10]['value']='.'
  obs={'observations':rows[::-1],'units':'lin','output_type':1,'count':len(rows),'limit':4000,'offset':0};evidence={}
  for part,doc in (('definition',definition),('observations',obs)):
   raw=model.encoded(doc);digest=model.sha(raw);url='https://api.stlouisfed.org/fred/series'+('/observations' if part=='observations' else '')+'?series_id='+sid
   if part=='observations':url+='&units=lin&limit=4000&sort_order=desc'
   key='data/evidence/fred/'+model.sha(url.encode())+'/'+digest+'.bin.gz'
   evidence[part]={'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':STAMP};objects[key]=gzip.compress(raw,mtime=0)
  entries[sid]={'evidence':evidence,'acquired_at':STAMP};originals[sid]={**entries[sid],'definition':definition,'observations':obs}
  measurements[sid]=compiler.measurement(sid,definition,obs,evidence,STAMP,STAMP)
 macro={'contract':compiler.CONTRACT,'generated_at':STAMP,'measurements':measurements}
 raw=Path(compiler.__file__).read_bytes();digest=model.sha(raw);ck='data/report-research/compilers/'+digest+'.py';objects[ck]=raw
 md={'contract':'report-research-replay.v1','generated_at':STAMP,'inputs':entries,'compiler':{'key':ck,'sha256':digest},'output_sha256':model.sha(model.encoded(macro))}
 raw=model.encoded(md);key='data/report-research/runs/'+model.sha(raw)+'.json';objects[key]=raw
 macro['replay']={'manifest_key':key,'output_sha256':md['output_sha256']};objects[store.SOURCES[0]]=model.encoded(macro)
 raw=calendar_raw();ref={'key':'audit-private/20260909-originals/fedwatch-research/'+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)};objects[ref['key']]=raw
 page={'request_url':model.CALENDAR_URL,'http_status':200,'received_at':STAMP,'original':ref}
 fw={'contract':'fedwatch-native-research.v1','generated_at':STAMP,'calendar':model.calendar(raw,page,STAMP),'collection':{'started_at':STAMP},**model.PERMISSIONS}
 raw=model.encoded(fw);digest=model.sha(raw);out={'key':'data/fedwatch-research/outputs/'+digest+'.json','sha256':digest,'bytes':len(raw)};objects[out['key']]=raw
 run={'contract':'fedwatch-native-replay.v1','generated_at':STAMP,'output':out,'output_sha256':digest};raw=model.encoded(run);key='data/fedwatch-research/runs/'+model.sha(raw)+'.json';objects[key]=raw
 fw['replay']={'manifest_key':key,'output_sha256':digest};objects['data/fedwatch.json']=model.encoded(fw)
 for key in ('data/fomc-reaction.json','data/fomc-calibration.json'):objects[key]=b'{"surprise":{"label":"DOVISH"},"prob_up_pct":100,"original":"preserved"}'
 client=Storage(objects);inputs={'contract':'fomc-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),'legacy':{k:store.snapshot(client,'b',k) for k in store.SOURCES[1:]}}
 return client,inputs,macro,originals

def packet(missing=()):
 s,i,_,_=fixture(missing);o=store.compile_output(i,store.reader(s,'b'));return s,i,{**o,'replay':store.retain(s,'b',i,o)}
