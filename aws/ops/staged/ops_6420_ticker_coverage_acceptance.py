"""Read-only exact ticker coverage consumer release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
SOURCE_HASHES = {'aws/lambdas/justhodl-flow-confluence/source/lambda_function.py': 'ea74d2e47a3d0ef5d1ffeac5a63ce5b4734ecb0d160a1b61ef4f7c6cbcce8639', 'aws/shared/capital_research_boundary.py': 'befc98aad1b412eb523506af93bb404e96554fe61268b389e59e26b79761a27f', 'aws/shared/holdings_authority.py': 'b9790df87ed542396333cb75cfd4f7e6a185f733568e4886beb7753220d99355', 'aws/shared/holdings_derived_boundary.py': '1da007ba20f1ae67ac35729276fa0c43a486b80c0a09eff001ad25c08ac35240', 'aws/shared/offexchange_context.py': '9990224a9e3f22c73820268a19fe17f1c866592fca3c7fb95868683163e434a3', 'aws/shared/option_scanner_boundary.py': '3abfb136783cc01aa8a78411fc51830b8aac0b68d251ef3908177d6dae9459b1', 'aws/shared/provider_flow_catalog.py': 'ea0be7634a8d24c28ffba961ee56a8c32aff28b83b5d9e3076dcc19fd5ed9f5b', 'aws/shared/provider_flow_research.py': '534e0208a128de163d378384e7d8ae41dccac46cd93077664a5f274de6598e6a', 'aws/shared/short_interest_context.py': '6944275390de08c425a8d9b1d73d32028a7623369b1c7ba953fbc1995d530a04', 'aws/shared/short_volume_context.py': '743e4dc0fa294b3be990993be59b1bb8e67123fcda1d52db32101210a30814ff', 'aws/shared/ticker_coverage_context.py': '8c6795c7d702e548ba0b19e01e90ba56d0c7f9f49f49ddce64641ff7fbe9f11b', 'aws/lambdas/justhodl-best-ideas/source/lambda_function.py': '9513b332e6df4995a3770008dcb246eef1ba4284809c8ee8444cbf68b7e455c0', 'aws/shared/insider_research.py': 'bd9dfd28a4a5dd072397b708d34b28b1bd8636b98dda81bcd4d1048d1713a15d', 'aws/shared/momentum_research_boundary.py': '54eff7690518c228905fb26bd22ca03d446d4d2915e5b8f3166eefcde46a4d92', 'aws/shared/sec_search_research.py': '6e450928888c5210ebfa8b33c3906a8cdc11971d6b640c9a830e986794aaa483', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480'}
EXPECTED_CONTROLS = {'justhodl-flow-confluence': {'function_name': 'justhodl-flow-confluence', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 120, 'memory_mb': 512, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-flow-confluence-daily', 'state': 'ENABLED', 'expression': 'cron(25 13 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-flow-confluence-sched', 'state': 'ENABLED', 'expression': 'cron(45 22 ? * MON-FRI *)', 'native_targets': 1}]}, 'justhodl-best-ideas': {'function_name': 'justhodl-best-ideas', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 120, 'memory_mb': 320, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'best-ideas-sched', 'state': 'ENABLED', 'expression': 'cron(45 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge rule', 'name': 'best-ideas-daily', 'state': 'ENABLED', 'expression': 'cron(45 14 * * ? *)', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-flow-confluence': 11, 'justhodl-best-ideas': 7}

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
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!={'justhodl-best-ideas', 'justhodl-flow-confluence'}:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6420_ticker_coverage_acceptance') as out:
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
