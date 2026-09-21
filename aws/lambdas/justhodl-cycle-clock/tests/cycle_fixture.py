"""Portable source originals and in-memory conditional storage; no AWS."""
from pathlib import Path
from datetime import date,datetime,timedelta
from decimal import Decimal
import gzip,io,json,sys
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import cycle_research_model as model
import cycle_research_store as store
import report_observations as compiler
STAMP='2026-09-21T00:20:00+00:00';AT=datetime.fromisoformat(STAMP)

class Failure(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.reads=[];self.writes=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise Failure('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[key]),'ETag':model.sha(self.objects[key])}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(kw)

def fixture(missing=('SAHMCURRENT',)):
    objects={};entries={};measurements={};originals={}
    for sid,(label,unit,freq,adj) in model.SPECS.items():
        if sid in missing:continue
        if freq=='M':days=[model.month('2000-01-01',i) for i in range(320 if sid=='PCEPILFE' else 321)]
        elif freq=='W':days=[str(date(2023,1,4)+timedelta(weeks=i)) for i in range(194)]
        else:days=[str(date(2023,1,2)+timedelta(days=i)) for i in range(1356) if (date(2023,1,2)+timedelta(days=i)).weekday()<5]
        definition={'seriess':[{'id':sid,'title':label,'units':unit,'frequency_short':freq,'seasonal_adjustment_short':adj,
            'seasonal_adjustment':'Seasonally Adjusted' if adj=='SA' else 'Not Seasonally Adjusted'}]}
        base={'WALCL':Decimal('6800000'),'WTREGEN':Decimal('850000'),'RRPONTSYD':Decimal('10'),
            'UNRATE':Decimal('4'),'MCUMFN':Decimal('75'),'SAHMREALTIME':Decimal('-.07'),'SAHMCURRENT':Decimal('-.07')}.get(sid,Decimal('100'))
        rows=[{'date':d,'value':str(base+(Decimal(i)/100 if sid not in ('UNRATE','MCUMFN','SAHMREALTIME','SAHMCURRENT') else Decimal(i%12)/100))} for i,d in enumerate(days)]
        if sid=='CPIAUCSL':next(r for r in rows if r['date']=='2025-10-01')['value']='.'
        obs={'observations':rows[::-1],'units':'lin','output_type':1,'count':len(rows),'limit':4000,'offset':0};evidence={}
        for part,doc in (('definition',definition),('observations',obs)):
            raw=model.encoded(doc);digest=model.sha(raw);url='https://api.stlouisfed.org/fred/series'+('/observations' if part=='observations' else '')+'?series_id='+sid
            if part=='observations':url+='&units=lin&limit=4000&sort_order=desc'
            key='data/evidence/fred/'+model.sha(url.encode())+'/'+digest+'.bin.gz'
            evidence[part]={'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':STAMP}
            objects[key]=gzip.compress(raw,mtime=0)
        entries[sid]={'evidence':evidence,'acquired_at':STAMP};originals[sid]={**entries[sid],'definition':definition,'observations':obs}
        measurements[sid]=compiler.measurement(sid,definition,obs,evidence,STAMP,STAMP)
    macro={'contract':compiler.CONTRACT,'generated_at':STAMP,'measurements':measurements}
    raw=Path(compiler.__file__).read_bytes();digest=model.sha(raw);ck='data/report-research/compilers/'+digest+'.py';objects[ck]=raw
    md={'contract':'report-research-replay.v1','generated_at':STAMP,'inputs':entries,'compiler':{'key':ck,'sha256':digest},'output_sha256':model.sha(model.encoded(macro))}
    raw=model.encoded(md);key='data/report-research/runs/'+model.sha(raw)+'.json';objects[key]=raw
    macro['replay']={'manifest_key':key,'output_sha256':md['output_sha256']};objects[store.SOURCES[0]]=model.encoded(macro)
    for key in model.DEPENDENCIES:
        objects[key]=model.encoded({'generated_at':STAMP,'regime':'GOLDILOCKS','score':99,'recession_prob_pct':100,
            'calls_eligible':True,'observations':[{'series_id':'WALCL','source_url':'https://fred.stlouisfed.org/series/WALCL'}]})
    objects['data/cycle-clock.json']=b'{"cycle":{"phase":"LATE"},"synthesis":{"score":99,"posture":"RISK-ON"},"original":"whole"}'
    objects['data/cycle-clock-history.json']=b'[{"date":"2026-08-01","posture_score":99}]'
    client=Storage(objects);inputs={'contract':'cycle-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),
        'legacy':{key:store.snapshot(client,'b',key) for key in store.SOURCES[1:]}}
    return client,inputs,macro,originals

def packet(missing=('SAHMCURRENT',)):
    s,i,_,_=fixture(missing);out=store.compile_output(i,store.reader(s,'b'))
    return s,i,{**out,'replay':store.retain(s,'b',i,out)}
