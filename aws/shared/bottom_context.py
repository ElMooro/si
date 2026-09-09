"""Fresh BOTTOM context for existing consumers; no unvalidated scoring uplift."""
from donor_contract import inspect_donor, numeric

STATES={'CLIMAX','TESTING','ST_CONFIRMED','TRIGGERED','MARKUP','FAILED','STOPPED'}

def context_rows(document,now=None):
    health=inspect_donor(document,'data/bottom.json',30,observed_paths=('session',),required_paths=('board_all',),max_observation_age_hours=120,now=now)
    health.pop('fields',None)
    health.update(use='DESCRIPTIVE_CONTEXT_ONLY',execution_eligible=False,calibration_status='INDEPENDENT_OUT_OF_SAMPLE_VALIDATION_REQUIRED')
    if not isinstance(document,dict) or document.get('engine')!='justhodl-bottom':
        health.update(usable=False,status='INVALID',errors=['unexpected producer'])
    if not health['usable']:return {},health
    rows=document.get('board_all')
    if not isinstance(rows,list):return {},{**health,'usable':False,'status':'INVALID','errors':['invalid board rows']}
    result={};invalid=0
    for row in rows:
        if not isinstance(row,dict):invalid+=1;continue
        ticker=row.get('ticker');score=numeric(row.get('score'));bars=numeric(row.get('bars_in_state'))
        if not isinstance(ticker,str) or not ticker or row.get('state') not in STATES or row.get('frame') not in ('D','W') or row.get('grade') not in (None,'A','B','C','D','F') or score is None or not 0<=score<=100 or bars is None or bars<0 or bars!=int(bars):
            invalid+=1;continue
        symbol=ticker.upper()
        if symbol in result:return {},{**health,'usable':False,'status':'INVALID','errors':['duplicate instrument rows']}
        result[symbol]={key:row.get(key) for key in ('state','frame','grade','weekly_state','daily_state','st_depth_class','trigger_date','st_date')}
        result[symbol].update(score=score,bars_in_state=int(bars),st_vol_ratio_sc=numeric(row.get('st_vol_ratio_sc')),use='DESCRIPTIVE_CONTEXT_ONLY')
    health.update(accepted_rows=len(result),invalid_rows=invalid,provider_degraded=bool(document.get('degraded')))
    if invalid:health.update(status='PARTIAL',errors=['invalid rows excluded'])
    if not result:health.update(usable=False,status='INVALID')
    return result,health
