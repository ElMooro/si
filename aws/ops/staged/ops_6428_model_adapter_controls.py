"""Read-only named model-adapter importer controls before enforcing the no-paid policy.

No engine invocation, provider query, application packet, environment value,
account artifact, log query, schedule mutation or credential material is read
or returned. A changed/incomplete census is a failure, never evidence of absence.
"""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import schedule_evidence,verified_alias
FUNCTIONS=('justhodl-a2a-bus', 'justhodl-ab-test', 'justhodl-ai-brief-router', 'justhodl-ai-chat', 'justhodl-ai-council', 'justhodl-ai', 'justhodl-altseason', 'justhodl-ask-desk', 'justhodl-ask', 'justhodl-asset-discovery', 'justhodl-auction-crisis-ai', 'justhodl-auction-interpreter', 'justhodl-bond-desk', 'justhodl-bottleneck-research', 'justhodl-brain-sync', 'justhodl-cb-stance', 'justhodl-chart-vision', 'justhodl-chat-api', 'justhodl-chokepoint', 'justhodl-crypto-intel', 'justhodl-cryptoquant', 'justhodl-cycle-clock', 'justhodl-debate-engine', 'justhodl-devils-advocate', 'justhodl-digest-trends-ai', 'justhodl-dislocation-ai', 'justhodl-divergence-interpreter', 'justhodl-earnings-nlp', 'justhodl-earnings-sentiment', 'justhodl-ecb-derived', 'justhodl-episode-compass', 'justhodl-eurodollar-plumbing', 'justhodl-fed-nlp', 'justhodl-fed-speak', 'justhodl-financial-secretary', 'justhodl-fleet-monitor', 'justhodl-flows-ai-analysis', 'justhodl-fomc-reaction', 'justhodl-global-flow-desk', 'justhodl-hot-stocks-digest', 'justhodl-industry-case', 'justhodl-institutional-footprint', 'justhodl-interpretation-grader', 'justhodl-investor-agents', 'justhodl-ka-metrics', 'justhodl-khalid-metrics', 'justhodl-llm-health', 'justhodl-meta-improver', 'justhodl-my-brief', 'justhodl-news-sentiment', 'justhodl-news-wire', 'justhodl-nobrainer-rationale', 'justhodl-notes-intel', 'justhodl-page-ai-commentary', 'justhodl-page-ai', 'justhodl-political-ai-investigation', 'justhodl-positioning-analog', 'justhodl-premortem-engine', 'justhodl-prompt-iterator', 'justhodl-pump-earnings-nlp', 'justhodl-research-critique', 'justhodl-research-papers', 'justhodl-rotation-radar', 'justhodl-sec-filing-diff', 'justhodl-signal-backtest', 'justhodl-stock-ai-research', 'justhodl-strategist', 'justhodl-telegram-bot', 'justhodl-ticker-deep-research', 'justhodl-ticket-ai-rationale', 'justhodl-upside-thesis', 'justhodl-watchlist-debate', 'justhodl-weekly-ai-review')

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
    with report('ops_6428_model_adapter_controls') as out:
        before=snapshot(lam,events,scheduler)
        after=snapshot(lam,events,scheduler)
        if before!=after:raise ValueError('Producer controls changed during inspection')
        out.kv(evidence={'status':'stable_named_adapter_control_baseline','functions':before,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,
                        'private_reads':0,'account_reads':0,'native_writes':0,'schedule_changes':0,
                        'source_qualified':False,'normal_publication_verified':False,'investment_authority':False})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
