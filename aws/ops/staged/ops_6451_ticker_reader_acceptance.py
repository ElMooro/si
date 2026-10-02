"""Read-only exact ticker reader release and native control acceptance.

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
SOURCE_HASHES = {'aws/shared/ticker_360.py': '46519cb54f5a2ce05b2d30357a8a1294badaaa5dbafaea60f23a5c56f5cb0291', 'aws/lambdas/justhodl-ticker-360/source/lambda_function.py': '9761e70c520414631a4c0a1fa46644ef7dfd5082728b6439c9a56d9b3f1839bc', 'aws/lambdas/justhodl-ticker-360/config.json': 'e1e93a0352b5d0017199eee866b67488fec723df545d5c9e8dd0534c157ba0a4', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480', 'aws/ops/staged/ops_6434_crypto_semantics_controls.py': '5799191855f86f9ec100204a5b3fbb6db4c7980905618ff0ff716e156aa0a230', 'aws/shared/capital_structure_context.py': '9f50048d3a197d6231e6dda8709d52d588c47ba07c9bdd8ad669bad7d237fcec', 'aws/shared/dollar_research_context.py': '490c78d24d1cd03674cf1f0d5aa660f3ca8f04946c0dc3ea8de5f31e6cde01ba', 'aws/shared/futures_research_context.py': '9484f264fedb7ded6ab165abcda58fc0446a651e138a19b09af96e52e907bf46', 'aws/shared/fx_research_context.py': 'd08cb1b30078cb82e0a98aa13c39a7bb129bc502b339608b190fe89c34c3fc0c', 'aws/shared/gold_rotation_context.py': '14ab452e042208b99473ca630fbf5051edf336c409d09538d44caa10df6c2a1c', 'aws/shared/offexchange_context.py': '9990224a9e3f22c73820268a19fe17f1c866592fca3c7fb95868683163e434a3', 'aws/shared/pd_fails_context.py': 'fae6d16962c2759b1ba9b51c21761dfdc16d7307db153edbc7136fa2561f9e5a', 'aws/shared/sec_ftd_context.py': '79ab547bea3b466cee58e68e61d2e682f5566a49adb2ecacecc07dc9dac2e3e4', 'aws/shared/short_interest_context.py': '6944275390de08c425a8d9b1d73d32028a7623369b1c7ba953fbc1995d530a04', 'aws/shared/short_volume_context.py': '743e4dc0fa294b3be990993be59b1bb8e67123fcda1d52db32101210a30814ff', 'aws/shared/statement_context.py': '4e851b0c0a7f11b81c7407ca57fdfbef66ce6226240aad32a367d2f4cfefba22', 'aws/shared/context_evidence_store.py': '48fc8f3470fec9e1fe9b984290858dd956bf70d572de71850ea2c6cc8154fae4'}
EXPECTED_CONTROLS = {'justhodl-ticker-360': {'function_name': 'justhodl-ticker-360', 'timeout': 600, 'memory_mb': 1024, 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-ticker-360-schedule', 'state': 'ENABLED', 'expression': 'cron(20 6,18 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge rule', 'name': 'justhodl-ticker-360-schedule', 'state': 'ENABLED', 'expression': 'cron(20 6,18 * * ? *)', 'native_targets': 1}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-ticker-360': 14}

MONITORED_TARGETS = {'justhodl-ticker-360': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-ticker-360'}

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
    with report('ops_6451_ticker_reader_acceptance') as out:
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
