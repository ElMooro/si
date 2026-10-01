"""Read-only Bond TRACE and website-synthesis controls before a source repair.

No engine invocation, provider query, application packet, environment value,
account artifact, log query, schedule mutation or credential material is read
or returned. A changed/incomplete census is a failure, never evidence of absence.
"""
from pathlib import Path
import json,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import schedule_evidence,verified_alias
FUNCTIONS=('justhodl-bond-trace','justhodl-ai-website-synthesis')

def capture(lam,events,scheduler,function):
    cfg=lam.get_function_configuration(FunctionName=function)
    if cfg.get('FunctionName')!=function or cfg.get('State')!='Active' or cfg.get('LastUpdateStatus')!='Successful':
        raise ValueError('Active stable named function required')
    conf=json.loads((ROOT/'aws/lambdas'/function/'config.json').read_bytes())
    alias=verified_alias(lam,function,cfg,conf)
    result={key:cfg[key] for key in ('FunctionName','CodeSha256','Runtime','Handler','Timeout','MemorySize','Architectures','Role','EphemeralStorage')}
    result['schedules']=sorted(schedule_evidence(events,scheduler,function,cfg['FunctionArn'],conf,alias),key=lambda row:(row['kind'],row.get('group','default'),row['name']))
    if not result['schedules']:raise ValueError('No bound schedule found; original cadence is not established')
    if alias:result['active_alias']=alias
    return result

def main():
    import boto3
    from ops_report import report
    lam,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler')]
    with report('ops_6411_bond_source_controls_baseline') as out:
        before={fn:capture(lam,events,scheduler,fn) for fn in FUNCTIONS}
        after={fn:capture(lam,events,scheduler,fn) for fn in FUNCTIONS}
        if before!=after:raise ValueError('Producer controls changed during inspection')
        out.kv(evidence={'status':'stable_native_control_baseline','functions':before,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,
                        'private_reads':0,'account_reads':0,'native_writes':0,'schedule_changes':0,
                        'source_qualified':False,'normal_publication_verified':False,'investment_authority':False})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
