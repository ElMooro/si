"""Read-only exact storage heartbeat release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys,runpy
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
ScheduleInventory=runpy.run_path(str(ROOT/'aws/ops/staged/ops_6434_crypto_semantics_controls.py'))['ScheduleInventory']
SOURCE_HASHES = {'aws/lambdas/justhodl-feed-heartbeat/source/lambda_function.py': 'd006d930e98a50450273b14c6bb911aaf207d390506fc070d2e6515df4b41267', 'aws/lambdas/justhodl-feed-heartbeat/config.json': '4e8d1a1e88087a61e8e875bd6ecd53a7e5584051ae2c7d79cbad7497e4599caa', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480', 'aws/ops/staged/ops_6434_crypto_semantics_controls.py': '5799191855f86f9ec100204a5b3fbb6db4c7980905618ff0ff716e156aa0a230'}
EXPECTED_CONTROLS = {'justhodl-feed-heartbeat': {'function_name': 'justhodl-feed-heartbeat', 'timeout': 120, 'memory_mb': 256, 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': []}}
EXPECTED_SOURCE_COUNTS = {'justhodl-feed-heartbeat': 1}

MONITORED_TARGETS = {'justhodl-ticker-360': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-ticker-360', 'justhodl-sec-8k-enrich': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-sec-8k-enrich', 'justhodl-short-interest': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-short-interest', 'justhodl-xbrl-fundamentals': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-xbrl-fundamentals', 'justhodl-corporate-actions': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-corporate-actions', 'justhodl-etf-issuer-holdings': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-issuer-holdings'}

def verify_binding_identities(lam):
    result={}
    for function,arn in MONITORED_TARGETS.items():
        cfg=lam.get_function_configuration(FunctionName=function)
        if cfg.get('FunctionName')!=function or cfg.get('FunctionArn')!=arn or cfg.get('State')!='Active':
            raise ValueError('Named monitored function target identity differs')
        result[function]=arn
    return result

def expected_commit():
    return subprocess.check_output(['git','log','-1','--format=%H','--',*SOURCE_HASHES],cwd=ROOT,text=True).strip()

class ReceiptOnly:
    def __init__(self,client):self.client=client
    def get_object(self,**kw):
        allowed=[{'Bucket':BUCKET,'Key':'data/ops/releases/'+fn+'.json'} for fn in EXPECTED_CONTROLS]
        if kw not in allowed:raise ValueError('Exact named public release receipt only')
        return self.client.get_object(**kw)

def normalize(value,function,commit):
    if value.get('function_name')!=function or value.get('receipt')!={'status':'matched','commit':commit}:
        raise ValueError('Exact function and release commit required')
    if type(value.get('source_files_checked')) is not int or value['source_files_checked']!=EXPECTED_SOURCE_COUNTS[function]:
        raise ValueError('Whole expected source closure required')
    for key in ('handler_bytes','timeout','memory_mb','ephemeral_storage_mb'):
        if type(value.get(key)) is not int or value[key]<=0:raise ValueError('Positive typed native counts required')
    schedules=value.get('schedules')
    if not isinstance(schedules,list) or not all(isinstance(row,dict) for row in schedules):
        raise ValueError('Complete schedule census required, including observed absence')
    expected=EXPECTED_CONTROLS[function]
    key=lambda row:(row['kind'],row.get('group','default'),row['name'])
    if sorted(schedules,key=key)!=sorted(expected['schedules'],key=key):
        raise ValueError('Original native schedules changed')
    # Preserve the baseline presentation order only after comparing every
    # observed binding. Ordering alone is not a change to native controls.
    value={**value,'schedules':[dict(row) for row in expected['schedules']]}
    if {k:value.get(k) for k in expected}!=expected:raise ValueError('Original native controls changed')
    return value

def inspect(clients,commit):
    def snapshot():
        lam,s3,events,scheduler=clients
        inventory=ScheduleInventory(scheduler)
        result={}
        for fn in EXPECTED_CONTROLS:
            try:result[fn]=normalize(runtime(lam,s3,events,inventory,fn),fn,commit)
            except Exception as exc:
                # Never leak a signed package URL or credential-bearing error.
                raise ValueError('Named release acceptance failed: '+fn+' '+type(exc).__name__) from None
        return result
    before=snapshot()
    after=snapshot()
    if before!=after:raise ValueError('Native package or controls changed during inspection')
    return before,after

def main():
    import boto3
    from ops_report import report
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!=set(EXPECTED_SOURCE_COUNTS) or len(EXPECTED_CONTROLS)!=1:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6447_heartbeat_acceptance') as out:
        identities_before=verify_binding_identities(lam)
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        identities_after=verify_binding_identities(lam)
        if identities_before!=identities_after:raise ValueError("Monitored target identity changed")
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'monitored_target_identities':identities_before,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0,'other_invocation_routes_verified':False})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
