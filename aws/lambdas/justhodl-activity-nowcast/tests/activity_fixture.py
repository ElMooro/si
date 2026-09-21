"""Portable source originals and in-memory conditional storage; no AWS."""
from pathlib import Path
from datetime import date,datetime,timedelta
from decimal import Decimal
import gzip,io,json,sys
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import activity_research_model as model
import activity_research_store as store
import report_observations as compiler
STAMP='2026-09-21T01:10:00+00:00';AT=datetime.fromisoformat(STAMP)

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

def fixture(missing=()):
    objects={};entries={};measurements={};originals={}
    for sid,(label,unit,freq,adj) in model.SPECS.items():
        if sid in missing:continue
        start=date(2015,1,3) if freq=='W' else date(2015,1,1)
        end=date(2026,9,5) if sid=='CCSA' else date(2026,9,12) if sid in ('WEI','ICSA') else date(2026,9,11) if freq=='W' else date(2026,9,18)
        if sid=='GDPNOW':days=[str(date(2015+i//4,1+3*(i%4),1)) for i in range(47)]
        else:days=[str(start+timedelta(days=i)) for i in range((end-start).days+1) if (start+timedelta(days=i)).weekday()==model.WEEKDAY[sid]] if freq=='W' else [str(start+timedelta(days=i)) for i in range((end-start).days+1) if (start+timedelta(days=i)).weekday()<5]
        definition={'seriess':[{'id':sid,'title':label,'units':unit,'frequency_short':freq,'seasonal_adjustment_short':adj,
            'seasonal_adjustment':'Seasonally Adjusted' if adj=='SA' else 'Not Seasonally Adjusted'}]}
        base=Decimal('100000') if sid in ('ICSA','CCSA') else Decimal('-0.5') if sid in ('NFCI','STLFSI4') else Decimal('2')
        step=Decimal('10') if sid in ('ICSA','CCSA') else Decimal('0.001')
        rows=[{'date':d,'value':str(base+step*i+step*(i%7))} for i,d in enumerate(days)]
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
    for key in store.SOURCES[1:]:objects[key]=model.encoded({'legacy':'whole','source':key,'activity_index':99,'regime':'ACCELERATING'})
    client=Storage(objects);inputs={'contract':'activity-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),
        'legacy':{key:store.snapshot(client,'b',key) for key in store.SOURCES[1:]}}
    return client,inputs,macro,originals

def packet(missing=()):
    s,i,_,_=fixture(missing);out=store.compile_output(i,store.reader(s,'b'))
    return s,i,{**out,'replay':store.retain(s,'b',i,out)}
