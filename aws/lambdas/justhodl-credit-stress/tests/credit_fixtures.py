"""Synthetic FRED-shaped originals. No provider history or credentials included."""
from pathlib import Path
from datetime import date,timedelta
import sys
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'source'),str(ROOT/'aws/shared')]
import credit_research_model as model

STAMP='2026-09-20T14:00:00+00:00'
STARTED='2026-09-20T13:59:00+00:00'
EVALUATION='2026-09-20'


def originals(sid,values=None):
    title=('ICE BofA Synthetic Option-Adjusted Spread' if sid in model.OAS else
        '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity' if sid=='T10Y2Y' else
        'ICE BofA Synthetic Effective Yield')
    d={'id':sid,'title':title,'realtime_start':EVALUATION,'realtime_end':EVALUATION,
       'units':'Percent','frequency_short':'D','frequency':'Daily','seasonal_adjustment_short':'NSA',
       'seasonal_adjustment':'Not Seasonally Adjusted','last_updated':'2026-09-18 09:00:00-05'}
    if values is None:
        end=date(2026,9,17);begin=end-timedelta(days=430)
        days=[begin+timedelta(days=i) for i in range(431) if (begin+timedelta(days=i)).weekday()<5]
        values=[(str(day),str(100+i%33+model.SERIES.index(sid))[:-2]+'.'+str(100+i%33+model.SERIES.index(sid))[-2:]) for i,day in enumerate(days)]
    rows=[{'date':day,'value':value,'realtime_start':EVALUATION,'realtime_end':EVALUATION} for day,value in values]
    observations={'realtime_start':EVALUATION,'realtime_end':EVALUATION,
        'observation_start':str(date.fromisoformat(EVALUATION)-timedelta(days=model.HISTORY_DAYS)),
        'observation_end':EVALUATION,'units':'lin','output_type':1,'offset':0,'sort_order':'asc','limit':100000,
        'count':len(rows),'observations':rows}
    return {'definition':model.encoded({'seriess':[d]}),'observations':model.encoded(observations)}


def fixture():
    inputs={'contract':'credit-native-inputs.v1','started_at':STARTED,'generated_at':STAMP,
        'evaluation_date':EVALUATION,'sources':{},'dealer':None}
    bodies={}
    for sid in model.SERIES:
        pair={}
        for kind,raw in originals(sid).items():
            digest=model.sha(raw);key=model.PRIVATE+digest+'.bin';bodies[key]=raw
            pair[kind]={'acquired_at':'2026-09-20T13:59:30+00:00','evidence':{
                'sha256':digest,'bytes':len(raw),'key':key,'provider':'fred','request_url':model.source_url(sid,kind,EVALUATION),
                'access':'protected_AWS_IAM_source_archive'}}
        inputs['sources'][sid]=pair
    return inputs,bodies
