"""Read-only exact no-paid research narrative release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
SOURCE_HASHES = {'aws/lambdas/justhodl-alpha-research/source/lambda_function.py': '2d12cec7108a70de22c6a1bbea2e8b9019dc5642a249996ab166286fff638a57', 'aws/shared/equity_enrich.py': '2248cfb57a157c59acbb671d02842e3ee9846dd243d163da9a3c8ceb1d506a8c', 'aws/shared/fmp_book.py': 'fa1abf71c006fefe578b4a016290657ee502201ecc8dd8153873eddcce331150', 'aws/shared/managed_secret.py': 'afa2552d71119f547476c327ab2bcbb9329b582591c785781233e4cbc91fa75e', 'aws/shared/short_position_context.py': 'f62260c04d17df94f96eff9f938272c665d1fd4d604e87d955a3eb4203996c5e', 'aws/lambdas/justhodl-opportunities-research/source/lambda_function.py': '67193a4a6777f97fccc1d24a3170633bcf35d9fefbbf86babecfa8b458ef479d', 'aws/lambdas/justhodl-retail-sentiment/source/lambda_function.py': '068cce4f6438e81cf2989df6829054c0bef284ce323195ff6eb534d766932f34', 'aws/lambdas/justhodl-retail-sentiment/source/legacy_retail_sentiment.py': '4039b6d46d221567277885b158cdd99dbbc91b05bfec27e102291c8a6ec3f6d9', 'aws/lambdas/justhodl-retail-sentiment/source/retail_research_model.py': 'b140a908775982fcd6f01432e1dba6f5973220250ed5822623116c37b0ecd430', 'aws/lambdas/justhodl-retail-sentiment/source/retail_research_store.py': '3545f059574dc695adc441477d0617a45c6c85410db18256971c039b0b960977', 'aws/shared/finviz.py': 'a68a6d83a70848e91dc1dbebee8a28ad5e110ce8b672d862c858e08b92346e69', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480'}
EXPECTED_CONTROLS = {'justhodl-alpha-research': {'function_name': 'justhodl-alpha-research', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 600, 'memory_mb': 1024, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-alpha-research-6h', 'state': 'ENABLED', 'expression': 'rate(6 hours)', 'native_targets': 1}]}, 'justhodl-opportunities-research': {'function_name': 'justhodl-opportunities-research', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 600, 'memory_mb': 1024, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-opportunities-research-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 * * ? *)', 'native_targets': 1}]}, 'justhodl-retail-sentiment': {'function_name': 'justhodl-retail-sentiment', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 180, 'memory_mb': 512, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-retail-sentiment-30min', 'state': 'ENABLED', 'expression': 'cron(10 19 * * ? *)', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-alpha-research': 5, 'justhodl-opportunities-research': 5, 'justhodl-retail-sentiment': 9}

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
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!={'justhodl-alpha-research', 'justhodl-opportunities-research', 'justhodl-retail-sentiment'}:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6419_no_paid_research_acceptance') as out:
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
