"""Read-only exact squeeze retained-research release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
SOURCE_HASHES = {'aws/lambdas/justhodl-squeeze-pretrigger/source/lambda_function.py': '59f369f151a79fab73301745b5295e699fdf36e4f24400dabe20eb3650b996a5', 'aws/lambdas/justhodl-squeeze-pretrigger/source/squeeze_research_model.py': '7c3496251eec9472943cc1bdad81a2538e290e1c5a20ed529a2fba75bc07702b', 'aws/shared/context_evidence_store.py': 'd3ac1a61721c5d7926e652ecd85a7371f1655a16cab7cdf43aa6029698ea1ce7', 'aws/shared/managed_secret.py': 'afa2552d71119f547476c327ab2bcbb9329b582591c785781233e4cbc91fa75e', 'aws/shared/short_position_context.py': '4254e629803838e6bc4a3c8ea01b0016451c762df2ffd5f8e10b0e65310b896c', 'aws/shared/short_volume_context.py': '743e4dc0fa294b3be990993be59b1bb8e67123fcda1d52db32101210a30814ff', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480'}
EXPECTED_CONTROLS = {'justhodl-squeeze-pretrigger': {'function_name': 'justhodl-squeeze-pretrigger', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 540, 'memory_mb': 768, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-squeeze-pretrigger-daily', 'state': 'ENABLED', 'expression': 'cron(30 23 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-squeeze-pretrigger': 6}

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
    if not isinstance(schedules,list) or not schedules or not all(isinstance(row,dict) for row in schedules):
        raise ValueError('Complete bound schedule census required')
    value={**value,'schedules':sorted(schedules,key=lambda row:(row['kind'],row.get('group','default'),row['name']))}
    expected=EXPECTED_CONTROLS[function]
    if {k:value.get(k) for k in expected}!=expected:raise ValueError('Original native controls changed')
    return value

def inspect(clients,commit):
    before={fn:normalize(runtime(*clients,fn),fn,commit) for fn in EXPECTED_CONTROLS}
    after={fn:normalize(runtime(*clients,fn),fn,commit) for fn in EXPECTED_CONTROLS}
    if before!=after:raise ValueError('Native package or controls changed during inspection')
    return before,after

def main():
    import boto3
    from ops_report import report
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!={'justhodl-squeeze-pretrigger'}:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6424_squeeze_research_acceptance') as out:
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
