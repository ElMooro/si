"""Synthetic originals only; no provider history or credentials."""
from pathlib import Path
from datetime import date,timedelta
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'source'))
import volatility_research_model as model

STAMP='2026-09-20T15:00:00+00:00'
EVALUATION='2026-09-20'


def originals(label,values=None):
    if values is None:
        start=date(2025,6,1)
        values=[(str(start+timedelta(days=i)),str(10+i%31)) for i in range(474)
                if (start+timedelta(days=i)).weekday()<5]
    if label in model.PUBLISHER:
        rows=['DATE,'+label]+[date.fromisoformat(day).strftime('%m/%d/%Y')+','+value for day,value in values]
        return {'observations':('\n'.join(rows)+'\n').encode()}
    sid=model.CATALOG[label]['fred']
    definition={'id':sid,'title':'CBOE Synthetic Volatility Index','units':'Index',
        'frequency_short':'D','frequency':'Daily, Close','seasonal_adjustment_short':'NSA',
        'seasonal_adjustment':'Not Seasonally Adjusted','realtime_start':EVALUATION,'realtime_end':EVALUATION}
    doc={'realtime_start':EVALUATION,'realtime_end':EVALUATION,
        'observation_start':str(date.fromisoformat(EVALUATION)-timedelta(days=model.HISTORY_DAYS)),
        'observation_end':EVALUATION,'units':'lin','output_type':1,'offset':0,'sort_order':'asc','limit':100000,
        'count':len(values),'observations':[{'date':day,'value':value,'realtime_start':EVALUATION,'realtime_end':EVALUATION} for day,value in values]}
    return {'definition':model.encoded({'seriess':[definition]}),'observations':model.encoded(doc)}


def sources():
    out={}
    for label in model.LABELS:
        out[label]={kind:{'raw':raw} for kind,raw in originals(label).items()}
    return out


def fixture():
    inputs={'contract':'volatility-native-inputs.v1','started_at':'2026-09-20T14:59:00+00:00',
        'generated_at':STAMP,'evaluation_date':EVALUATION,'sources':{}}
    bodies={}
    for label in model.LABELS:
        inputs['sources'][label]={}
        for kind,raw in originals(label).items():
            digest=model.sha(raw);key=model.PRIVATE+digest+'.bin';bodies[key]=raw
            inputs['sources'][label][kind]={'acquired_at':'2026-09-20T14:59:30+00:00',
                'evidence':{'sha256':digest,'bytes':len(raw),'key':key,
                    'provider':'Cboe' if label in model.PUBLISHER else 'fred',
                    'request_url':model.source_url(label,kind,EVALUATION),'access':'protected_AWS_IAM_source_archive'}}
    return inputs,bodies
