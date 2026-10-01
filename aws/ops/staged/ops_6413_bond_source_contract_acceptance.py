"""Read-only exact FINRA/bond source release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
SOURCE_HASHES = {'aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py': '1a800d4069b4699bc7fa29d3b4d2672bdfd0851e932f9f2b3518eae1f7cfae72', 'aws/lambdas/justhodl-bond-trace/source/_fred_shim.py': 'd08c506bffca294140df149d42a4f2895f445029e26c2d51ca1d3de6cc9b3428', 'aws/lambdas/justhodl-bond-trace/source/lambda_function.py': 'f251da6691fd16fdac1aaadb9ac654fed8fd4f28ade9eedeb390f7575148fb4e', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480', 'aws/shared/_sentry_lite.py': 'dad3ef390457a7b0b6b882451bef6f4ef9254bf13aa3c260216ad65ef6e82e44', 'aws/shared/anthropic_shim.py': 'd930c942c9678bcf3d2887733ecaf459158c30f212d63f6c961f9e7a41fd604c', 'aws/shared/finra_si.py': '1cc0db5afee6c1d9c31e52bd8a11110c304d6646867a76ba5a001713dd2312fb', 'aws/shared/finra_trace.py': '4a4fcf48bef5a083ad087a2a11a44904d5e38f16725532dd549071d959482fda', 'aws/shared/gsi_authority.py': 'c55d98fd46f0805cde0280ba11b1d73a5787e5e9f228721c569201ef88a01aef', 'aws/shared/llm_cost.py': 'b929fe17aa3f42f8f4f96584f6dadcb78740ea141ca70ef2b4cddd6ee9635e84', 'aws/shared/llm_router.py': '24f31bd545b3ac7add99f1b01629231e7d1aac15b208c2f42413fa4e3d45c047', 'aws/shared/managed_secret.py': 'afa2552d71119f547476c327ab2bcbb9329b582591c785781233e4cbc91fa75e', 'aws/shared/signal_board_authority.py': '075524273bc38c2c50363755a1d47c46a5acc54847b3cae41829c111a5c99183', 'aws/shared/xai_voice.py': 'bdbb4a3e47514627195f6cce8ad50ef348b6b6580eb93c4395327f386943a2c8'}
EXPECTED_CONTROLS = {'justhodl-bond-trace': {'function_name': 'justhodl-bond-trace', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 120, 'memory_mb': 256, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge rule', 'name': 'bond-trace-daily', 'state': 'ENABLED', 'expression': 'cron(0 21 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-bond-trace-cadence', 'state': 'ENABLED', 'expression': 'cron(0 21 ? * MON-FRI *)', 'native_targets': 1}]}, 'justhodl-ai-website-synthesis': {'function_name': 'justhodl-ai-website-synthesis', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 180, 'memory_mb': 512, 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-website-synthesis-hourly', 'state': 'ENABLED', 'expression': 'cron(25 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}]}}
EXPECTED_SOURCE_COUNTS = {'justhodl-bond-trace': 4, 'justhodl-ai-website-synthesis': 9}

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
    if not SOURCE_HASHES or set(EXPECTED_CONTROLS)!={'justhodl-bond-trace','justhodl-ai-website-synthesis'}:
        raise ValueError('Reviewed complete acceptance specification required')
    for path,digest in SOURCE_HASHES.items():
        if hashlib.sha256((ROOT/path).read_bytes()).hexdigest()!=digest:raise ValueError('Reviewed source changed: '+path)
    lam,s3,events,scheduler=[boto3.client(name,region_name='us-east-1') for name in ('lambda','s3','events','scheduler')]
    commit=expected_commit()
    with report('ops_6413_bond_source_contract_acceptance') as out:
        before,after=inspect((lam,ReceiptOnly(s3),events,scheduler),commit)
        out.kv(evidence={'status':'exact_native_release_checked','expected_commit':commit,'reviewed_source_hashes':SOURCE_HASHES,
                        'native_before':before,'native_after':after,'original_controls':EXPECTED_CONTROLS,
                        'normal_publication_verified':False,'source_qualified':False,'investment_authority':False,
                        'native_invocations':0,'provider_requests':0,'application_packet_reads':0,'private_reads':0,
                        'account_reads':0,'native_writes':0,'schedule_changes':0,'application_log_queries':0})

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
