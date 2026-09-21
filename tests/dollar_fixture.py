"""Synthetic canonical definitions and complete provider-shaped observations."""
from pathlib import Path
from datetime import date,timedelta
from decimal import Decimal
import gzip,sys
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import dollar_research_model as model
import report_observations as compiler
STAMP='2026-09-21T21:50:00+00:00'


def fixture(missing=()):
    originals={};objects={}
    for sid,(label,unit,freq,family) in model.SPECS.items():
        if sid in missing:continue
        end=date(2026,9,18)
        if freq=='D':days=[end-timedelta(days=i) for i in range(800) if (end-timedelta(days=i)).weekday()<5]
        elif freq=='W':days=[date(2026,9,16)-timedelta(weeks=i) for i in range(260)]
        else:days=[compiler.months_before(date(2026,8,1),i) for i in range(64)]
        base=Decimal('1.1') if sid in model.FX else Decimal('3') if unit=='Percent' else Decimal('100')
        rows=[{'date':str(day),'value':str(base+Decimal(len(days)-i)/10000)} for i,day in enumerate(days)]
        definition={'seriess':[{'id':sid,'title':label,'units':unit,'frequency_short':freq,
            'seasonal_adjustment_short':'NSA','seasonal_adjustment':'Not Seasonally Adjusted'}]}
        obs={'observations':rows,'units':'lin','output_type':1,'count':len(rows),'limit':4000,'offset':0}
        evidence={}
        for kind,doc in (('definition',definition),('observations',obs)):
            raw=model.encoded(doc);digest=model.sha(raw)
            url='https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?series_id='+sid
            if kind=='observations':url+='&units=lin&limit=4000&sort_order=desc'
            key='data/evidence/fred/'+model.sha(url.encode())+'/'+digest+'.bin.gz';objects[key]=gzip.compress(raw,mtime=0)
            evidence[kind]={'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,
                'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':STAMP}
        originals[sid]={'definition':definition,'observations':obs,'evidence':evidence,'acquired_at':STAMP}
    contexts={};predecessors={}
    for key in (*model.CONTEXT_KEYS,model.CURRENT,model.HISTORY):
        raw=model.encoded({'source':key,'engine':'justhodl-dollar-radar','schema_version':'3.0',
            'preserve_unknown':[0,None],'regime':'OLD','dollar_pressure':99})
        ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)};objects[ref['key']]=raw
        objects[key]=raw
        if key in model.CONTEXT_KEYS:contexts[key]={'original':ref,'status':'retained_unqualified_context','source_generated_at':None,'independent_votes':0}
        else:predecessors[key]=ref
    packet=canonical(originals,objects)
    return packet,originals,contexts,predecessors,objects


def canonical(originals,objects=None):
    packet=compiler.build({sid:{'category':'fixture','display_name':sid} for sid in model.SERIES},originals,STAMP)
    source=Path(compiler.__file__).read_bytes();digest=model.sha(source)
    manifest={'contract':'report-research-replay.v1','generated_at':STAMP,'catalog':packet['catalog'],
        'inputs':{sid:{'evidence':v['evidence'],'acquired_at':v['acquired_at']} for sid,v in originals.items()},
        'compiler':{'key':'data/report-research/compilers/'+digest+'.py','sha256':digest},'output_sha256':compiler.digest(packet)}
    raw=model.encoded(manifest);key='data/report-research/runs/'+model.sha(raw)+'.json'
    packet['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler_sha256':digest}
    if objects is not None:
        objects[key]=raw;objects[manifest['compiler']['key']]=source;objects['data/report-measurements.json']=model.encoded(packet)
    return packet
