"""Read-only named sentiment allocator/outcome consumer controls before semantic repair.

No engine invocation, provider query, application packet, environment value,
account artifact, log query, schedule mutation or credential material is read
or returned. A changed/incomplete census is a failure, never evidence of absence.
"""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import schedule_evidence,verified_alias
FUNCTIONS=('justhodl-allocator', 'justhodl-signal-logger')

def capture(lam,events,scheduler,function):
    cfg=lam.get_function_configuration(FunctionName=function)
    if cfg.get('FunctionName')!=function or cfg.get('State')!='Active' or cfg.get('LastUpdateStatus')!='Successful':
        raise ValueError('Active stable named function required')
    config_path=ROOT/'aws/lambdas'/function/'config.json'
    conf=json.loads(config_path.read_bytes()) if config_path.exists() else {}
    for key in ('Timeout','MemorySize'):
        if type(cfg.get(key)) is not int or cfg[key]<=0:raise ValueError('Positive typed runtime control required')
    if not isinstance(cfg.get('EphemeralStorage'),dict) or type(cfg['EphemeralStorage'].get('Size')) is not int or cfg['EphemeralStorage']['Size']<=0:
        raise ValueError('Typed ephemeral storage control required')
    for key in ('CodeSha256','Runtime','Handler','Role'):
        if type(cfg.get(key)) is not str or not cfg[key]:raise ValueError('Complete named runtime control required')
    if not isinstance(cfg.get('Architectures'),list) or not cfg['Architectures'] or not all(type(v) is str and v for v in cfg['Architectures']):
        raise ValueError('Architecture inventory required')
    alias=verified_alias(lam,function,cfg,conf)
    result={key:cfg[key] for key in ('FunctionName','CodeSha256','Runtime','Handler','Timeout','MemorySize','Architectures','Role','EphemeralStorage')}
    result['schedules']=sorted(schedule_evidence(events,scheduler,function,cfg['FunctionArn'],conf,alias),key=lambda row:(row['kind'],row.get('group','default'),row['name']))
    result['schedule_observation']='bound_schedules_observed' if result['schedules'] else 'no_binding_observed_in_complete_schedule_inventory'
    result['other_invocation_routes_verified']=False
    if alias:result['active_alias']=alias
    return result


class ScheduleInventory:
    """One complete metadata census per pass; selected details stay uncached.

    list_schedules returns summary names/targets, not invocation input bodies.
    Only schedules belonging to the fixed reviewed functions reach get_schedule.
    Any denied or malformed page aborts the complete pass, never means absence.
    """
    def __init__(self, client):
        self.client=client;self.pages=[];seen=set()
        for page in client.get_paginator('list_schedules').paginate():
            if not isinstance(page,dict) or not isinstance(page.get('Schedules'),list):
                raise ValueError('Complete schedule metadata page required')
            rows=[]
            for item in page['Schedules']:
                if not isinstance(item,dict) or not all(isinstance(item.get(k),str) and item[k] for k in ('Name','GroupName')):
                    raise ValueError('Complete schedule metadata identity required')
                target=(item.get('Target') or {}).get('Arn')
                if not isinstance(target,str) or not target:raise ValueError('Schedule target metadata required')
                key=(item['GroupName'],item['Name'])
                if key in seen:raise ValueError('Duplicate schedule metadata identity')
                seen.add(key);rows.append({'Name':item['Name'],'GroupName':item['GroupName'],'Target':{'Arn':target}})
            self.pages.append({'Schedules':rows})
        if not self.pages:raise ValueError('No schedule metadata response received')
    def get_paginator(self, name):
        if name!='list_schedules':raise ValueError('Only schedule metadata listing permitted')
        return self
    def paginate(self, **kwargs):
        if kwargs:raise ValueError('Complete schedule census cannot be narrowed')
        return iter(self.pages)
    def get_schedule(self, **kwargs):
        return self.client.get_schedule(**kwargs)

def snapshot(lam,events,scheduler):
    schedules=ScheduleInventory(scheduler)
    return {fn:capture(lam,events,schedules,fn) for fn in FUNCTIONS}


def main():
    import boto3
    from ops_report import report
    lam,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler')]
    with report('ops_6443_sentiment_consumer_controls') as out:
        before=snapshot(lam,events,scheduler)
        after=snapshot(lam,events,scheduler)
        if before!=after:raise ValueError('Producer controls changed during inspection')
        out.kv(evidence={'status':'stable_named_sentiment_consumer_control_baseline','functions':before,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,
                        'private_reads':0,'account_reads':0,'native_writes':0,'schedule_changes':0,
                        'source_qualified':False,'normal_publication_verified':False,'investment_authority':False})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
