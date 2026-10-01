"""Read-only exact short-interest projection/importer release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
SOURCE_HASHES = {'aws/lambdas/justhodl-short-interest/source/lambda_function.py': '570f68ab5ad828e20ef4c2ec74552df59add7d2a82cc91ff0d4480a8f7661d68', 'aws/shared/context_evidence_store.py': 'd3ac1a61721c5d7926e652ecd85a7371f1655a16cab7cdf43aa6029698ea1ce7', 'aws/shared/offexchange_measurements.py': 'b1d994f5a84a17df8eca2559f1de29cfe73297636aa086829025eb5232d64e18', 'aws/shared/short_interest_context.py': '6944275390de08c425a8d9b1d73d32028a7623369b1c7ba953fbc1995d530a04', 'aws/shared/short_interest_measurements.py': '9002bf37bb778b9737543b3641ff2e6a6d2dbe381b3bf0eaaa4700cf8ecacf63', 'aws/shared/short_interest_producer.py': '73d62965bcef38be200f6c1d39e56a3059ebda5af3f4d0c007a1825ae02cf9b7', 'aws/shared/short_interest_research_model.py': '11716dc109a04b4b7b34b40d5b5602abe26c518e5dd9708649257f091b0ba831', 'aws/shared/short_interest_research_store.py': '5b84503baf1268a4537f16d55f89be903a7ccf24cc64779205cb58d64f1c5dc2', 'aws/shared/short_interest_tickers.py': 'f0955d45924173a455610cfbb4456962f35b8fb25685897a0681abeba96df464', 'aws/lambdas/justhodl-microcap-float-squeeze/source/float_observations.py': '7a545897a6f4a1e6a44725b78b4141cd2a1f7671c24af06bd7c7f6e82d049b7e', 'aws/lambdas/justhodl-microcap-float-squeeze/source/lambda_function.py': '1bc3e2cd0d16292da967deb44fdd330244106823ed0c053352dad8aa881801a1', 'aws/shared/managed_secret.py': 'afa2552d71119f547476c327ab2bcbb9329b582591c785781233e4cbc91fa75e', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480'}
EXPECTED_CONTROLS = {'justhodl-short-interest': {'function_name': 'justhodl-short-interest', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 360, 'memory_mb': 512, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-short-interest-6h', 'state': 'ENABLED', 'expression': 'cron(20 12 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-short-interest-sched', 'state': 'ENABLED', 'expression': 'cron(15 21 ? * MON,WED *)', 'native_targets': 1}]}, 'justhodl-microcap-float-squeeze': {'function_name': 'justhodl-microcap-float-squeeze', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 600, 'memory_mb': 2048, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-microcap-float-squeeze-daily', 'state': 'ENABLED', 'expression': 'cron(0 22 ? * MON-FRI *)', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-short-interest': 9, 'justhodl-microcap-float-squeeze': 8}

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
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!={'justhodl-microcap-float-squeeze', 'justhodl-short-interest'}:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6426_short_interest_projection_acceptance') as out:
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
