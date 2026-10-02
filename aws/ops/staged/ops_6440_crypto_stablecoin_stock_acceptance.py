"""Read-only exact Crypto stablecoin stock producer and consumer release and native control acceptance.

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
SOURCE_HASHES = {'aws/lambdas/justhodl-crypto-intel/source/lambda_function.py': 'a89b66c3626f51a3d8c47c9637e4c714a7b128c9f9b3db9121daa115649d2029', 'aws/shared/_sentry_lite.py': 'dad3ef390457a7b0b6b882451bef6f4ef9254bf13aa3c260216ad65ef6e82e44', 'aws/shared/crypto_funding_archive.py': 'aff1c225cf6877f90a493f60714c9680cfb6c8b7c07c9d6cbcaff9fc64dbef53', 'aws/shared/crypto_funding_observations.py': '7ca2f58d527887d1569d4906f5a6135446bb7636a51a4683562c543e80a7e3bc', 'aws/shared/crypto_market_cap_extension.py': '091fa4431f966abdb2f1957ca9d2bd9b194e9e4ef343f2f2f7fca96d879f5886', 'aws/shared/crypto_stablecoin_archive.py': '0fe0032164161baa532264757a9f5bd55b908b797211fef6d3a2807263cb1835', 'aws/shared/crypto_stablecoin_observations.py': '2f35f08de0d2cd276ff6e3f36a0a7c56900eca7ac052a902f784e57e839e6a16', 'aws/shared/crypto_stablecoin_transport.py': '33ef41f66adb81afb5b514159c05eb7918987c0e423bfe1791223a7a3cf54395', 'aws/shared/ka_aliases.py': 'e57211a0cc0779c5a51e67c37bb80f42656e072fb92784ca62fe8422c4b5e76e', 'aws/shared/llm_cost.py': 'b929fe17aa3f42f8f4f96584f6dadcb78740ea141ca70ef2b4cddd6ee9635e84', 'aws/shared/llm_router.py': 'e46303f7cdff7264d59ca3cf51c870a175fffe610ba46a3d5cafe93eee43ab33', 'aws/shared/option_population_context.py': 'ef547b1d1fac62db2e548779e6637907fb44d670ae1a9f62e3623983aeae1b0a', 'aws/shared/xai_voice.py': '14936b2dc7791c42241ac6adddab1e5d590c5cda202b84ef87e99691dcf9ef1a', 'aws/lambdas/justhodl-financial-secretary/source/lambda_function.py': 'c336d2acf0af17fbb0877c62e42b1e091f808af9e2794f3e53a190e6a2421363', 'aws/shared/anthropic_shim.py': 'ec7225a9b2e074684163de36d60ca6fd023701d3624c0869a8b902bf4c41bdd1', 'aws/shared/fmp_book.py': 'fa1abf71c006fefe578b4a016290657ee502201ecc8dd8153873eddcce331150', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480', 'aws/ops/staged/ops_6434_crypto_semantics_controls.py': '5799191855f86f9ec100204a5b3fbb6db4c7980905618ff0ff716e156aa0a230'}
EXPECTED_CONTROLS = {'justhodl-crypto-intel': {'function_name': 'justhodl-crypto-intel', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 180, 'memory_mb': 1024, 'architectures': ['arm64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-crypto-15min', 'state': 'ENABLED', 'expression': 'rate(15 minutes)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-crypto-fanin', 'state': 'ENABLED', 'expression': None, 'native_targets': 1}]}, 'justhodl-financial-secretary': {'function_name': 'justhodl-financial-secretary', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 300, 'memory_mb': 1024, 'architectures': ['arm64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-financial-secretary-daily', 'state': 'ENABLED', 'expression': 'cron(45 12 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'secretary-4h-scan', 'state': 'ENABLED', 'expression': 'rate(4 hours)', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-crypto-intel': 13, 'justhodl-financial-secretary': 10}

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
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!=set(EXPECTED_SOURCE_COUNTS) or len(EXPECTED_CONTROLS)!=2:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6440_crypto_stablecoin_stock_acceptance') as out:
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0,'other_invocation_routes_verified':False})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
