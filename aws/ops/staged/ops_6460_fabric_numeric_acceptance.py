"""Read-only exact Fabric numeric and Best Setups consumer release and native control acceptance.

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
SOURCE_HASHES = {'aws/lambdas/justhodl-best-setups/source/lambda_function.py': '7083181b31fb9a1efe606b67e05982e52b374872ebc6b3bd0886fdf11740131c', 'aws/shared/capital_research_boundary.py': 'befc98aad1b412eb523506af93bb404e96554fe61268b389e59e26b79761a27f', 'aws/shared/consume_brain.py': '5b5ebfe0c4c4a8678893a3755b12a6d508e8cf2bd89c218d6c8fde6d592fe6d3', 'aws/shared/fabric_numeric.py': '234cab8d8e6f083673e5f410ffa3691032689edae450f1d1d59f3717e0f2fd7c', 'aws/shared/holdings_authority.py': 'b9790df87ed542396333cb75cfd4f7e6a185f733568e4886beb7753220d99355', 'aws/shared/holdings_derived_boundary.py': '1da007ba20f1ae67ac35729276fa0c43a486b80c0a09eff001ad25c08ac35240', 'aws/shared/managed_secret.py': 'afa2552d71119f547476c327ab2bcbb9329b582591c785781233e4cbc91fa75e', 'aws/shared/massive_research_context.py': '8cfa803e6d6a53437c0e3d6549f2b75ebf7ff2ddafc570a4911082be37abdf8e', 'aws/shared/option_population_context.py': 'ef547b1d1fac62db2e548779e6637907fb44d670ae1a9f62e3623983aeae1b0a', 'aws/shared/private_artifact.py': '52d5a7c3176b977a0870dcfe35909d598eff434d7a438cdf403fb65dcdc001ba', 'aws/shared/public_brain_projection.py': 'ffeed8b27f259ac1cdf40bc06f1781dee430fac3d68f46689534fbcb259261b2', 'aws/shared/retail_research.py': 'c50fe4ef0b268186a5bca428cec0b457e6d47837fd4e7fbe4cb58f902a235dd3', 'aws/shared/risk_gate_authority.py': '80c71a611e52480ff4c3e4043a2496b3b7a0e878315692da124dec42f90e2090', 'aws/shared/risk_regime_authority.py': '9e9f27c6c62d915fe1d99a6e4fadb40be477662dfc97ce2619718ee3a15fa642', 'aws/shared/sec_ftd_context.py': '79ab547bea3b466cee58e68e61d2e682f5566a49adb2ecacecc07dc9dac2e3e4', 'aws/shared/sector_research.py': 'b2131c339b2305fa4565467485266dbbf22fd495b7281461e460bf06c8b33123', 'aws/shared/short_volume_context.py': '743e4dc0fa294b3be990993be59b1bb8e67123fcda1d52db32101210a30814ff', 'aws/shared/signals_emit.py': '0d456b9231fde0479a6594f64d94c29ce7e0c32feeb7056fddb12c3703144199', 'aws/shared/wl_fusion.py': 'cb8e7c62fc5c82287b274fc27c302c82658c89766c039a742a4ff610b99a3764', 'aws/lambdas/justhodl-signal-fabric/source/lambda_function.py': '92c3f84870dfc0316792c151a394386e0b5872588e173d2cf4de117ed9205582', 'aws/shared/compound_research_context.py': '89da9e7686387747e31295c54c2f6e5cf09dfb46443edd498743468ca610de2e', 'aws/shared/context_evidence_store.py': '48fc8f3470fec9e1fe9b984290858dd956bf70d572de71850ea2c6cc8154fae4', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480', 'aws/ops/staged/ops_6434_crypto_semantics_controls.py': '5799191855f86f9ec100204a5b3fbb6db4c7980905618ff0ff716e156aa0a230'}
EXPECTED_CONTROLS = {'justhodl-best-setups': {'function_name': 'justhodl-best-setups', 'timeout': 120, 'memory_mb': 512, 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-best-setups-hourly', 'state': 'ENABLED', 'expression': 'cron(10 * * * ? *)', 'native_targets': 1}]}, 'justhodl-signal-fabric': {'function_name': 'justhodl-signal-fabric', 'timeout': 300, 'memory_mb': 512, 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-signal-fabric-hourly', 'state': 'ENABLED', 'expression': 'cron(5 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-signal-fabric-cadence', 'state': 'ENABLED', 'expression': 'cron(10 0 * * ? *)', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-best-setups': 19, 'justhodl-signal-fabric': 6}

MONITORED_TARGETS = {'justhodl-best-setups': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-best-setups', 'justhodl-signal-fabric': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-signal-fabric'}

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
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!=set(EXPECTED_SOURCE_COUNTS) or len(EXPECTED_CONTROLS)!=2:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6460_fabric_numeric_acceptance') as out:
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
