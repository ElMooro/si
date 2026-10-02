"""Read-only exact shared signal event numeric release and native control acceptance.

Only deployed code packages, named public receipts and native configuration/
schedule metadata are read. No invocation, provider query, application packet,
account data, environment output or notification is permitted.
"""
from pathlib import Path
import hashlib,json,subprocess,sys,runpy
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from market_runtime_evidence import runtime,BUCKET
ScheduleInventory=runpy.run_path(str(ROOT/'aws/ops/staged/ops_6461_signal_logger_controls.py'))['ScheduleInventory']
SOURCE_HASHES = {'aws/lambdas/justhodl-ai/source/brain_dataset.py': '9ddee60278b4ed8f9919f5ab2db76e37fc540efc024d6ffeaf2d05380dc34069', 'aws/lambdas/justhodl-ai/source/cost_guard.py': '2a1f7642b51492f5b1522d6d08160500e524bb85ad8636e66fbd6ce67519c77a', 'aws/lambdas/justhodl-ai/source/deployment_gates.py': '8889a4ddf624cd8656f0fd7c9cfdb7323df028aa664a41b4e59b14f7672a1f50', 'aws/lambdas/justhodl-ai/source/doctrine_families.py': '9f3fc268c3df5d081db8b2662ecb86c643601d6c54584542a00b6c4c9ad44fd7', 'aws/lambdas/justhodl-ai/source/factory_gateway.py': '64ab1a2c02f8b550ee65311363ef72b63ebf0da61697e406f7a1c18b5fa08e01', 'aws/lambdas/justhodl-ai/source/factory_inference.py': '732d4596d85e8c5081a13ed00b996cc464aa8055966e61bb5d7bec13c00149d6', 'aws/lambdas/justhodl-ai/source/factory_status.py': 'e9f7b48b2dfdb9d93ec3f3c0ecb25483c870525b58f603b2e62ffcdcbf5ae3e2', 'aws/lambdas/justhodl-ai/source/fleet_inputs.py': 'c52fd382f21e99ca2f86dd4d064b1a0e6019f13b1028efb8d051b24c613db16a', 'aws/lambdas/justhodl-ai/source/gear_b.py': '63a3a472395d466c4248b7958e721beb291f52b20e5a2ac42bbcb2e4a22b4af8', 'aws/lambdas/justhodl-ai/source/gear_b_doctrine.py': '6f666ef2f96c38823ee352c5f2daa164aece9529c7b28ea8e81ce19bc2aec86f', 'aws/lambdas/justhodl-ai/source/gear_b_own.py': '3e979cc662e051f1ce62a97e61d6405d0f4126cdd7ef2473c2ab3e5757bf2d7b', 'aws/lambdas/justhodl-ai/source/governance_control.py': 'fcda09977edea2ede24c69815f75c539d7cb6dced1ff5495ff40ec8235abf9e0', 'aws/lambdas/justhodl-ai/source/lambda_function.py': '8af8e404afa80cf45f7893fb675d29f567beee3f136ed6cb6dc3702ce2894a50', 'aws/lambdas/justhodl-ai/source/market_read.py': 'd5cc5600fb4e86d7dd0ff3de4440d9034689158bdafb2fe0803abc86e812edf1', 'aws/lambdas/justhodl-ai/source/model_governance_consumer.py': '40a4ce082156d6420ae8538ecb0bf7a9e0132bf35b5462145f54a33ebb5a61b9', 'aws/lambdas/justhodl-ai/source/model_registry.py': 'b9d097ea3a06b526fb329e37fea8a3667ea1b935c86e4fed096873942d6730c2', 'aws/lambdas/justhodl-ai/source/outcome_labels.py': 'fa20205bc37ccc5e104ed9baeff609179f55bacd28bbca851cbe135c756e11a9', 'aws/lambdas/justhodl-ai/source/pipeline.py': '88865acf981af0809599d803ecf74f97d99d852265e5b040d4c219087360c91c', 'aws/lambdas/justhodl-ai/source/point_in_time.py': 'c23301f8e7d64d8f9113b03deff2e9e6c609d03468c9bf8639fa50c69a71931c', 'aws/lambdas/justhodl-ai/source/prediction_ledger.py': '348c5fe8ffc65d61b84e307d8b5745913fa72d757e6ce24651fa93bd742813bf', 'aws/lambdas/justhodl-ai/source/prediction_ledger_consumer.py': 'd031536979429b0833d658f1894025d5b979fb8f0c22ea0018f5ed98d5711a53', 'aws/lambdas/justhodl-ai/source/queue_consumer_runtime.py': '30a79dafdca043a0b152c7665b5af00e9e5c1d5c627234d2e92ee98f41f26194', 'aws/lambdas/justhodl-ai/source/s3_event_outbox.py': '5767ab4db779d9778b2323c4e9291b6aaba508029e0ed8884cde76575b0f9e6f', 'aws/lambdas/justhodl-ai/source/sagemaker_feature_store.py': '79c70f4b1c2789f31273f1393bf6b76a04384c3d0ba2f84bde99f7ba9b2d09e1', 'aws/lambdas/justhodl-ai/source/self_improve.py': 'd7e58e5d6708bcfd3f237e60e5ba8a82fd1f6d4b3f87083c9fb5a86a581e6270', 'aws/lambdas/justhodl-ai/source/self_improve_ext.py': '301350dae656cfa3eddd5625219038403f131f8147c9db29de416656d172d5a2', 'aws/lambdas/justhodl-ai/source/signal_envelope.py': 'f5052d75b7714fe6c7e69db2c6509a8077a0d7235bafb1f5ee5ab6bd2aae49d3', 'aws/lambdas/justhodl-ai/source/signal_feature_consumer.py': '56f5494013548872f7db328d609d4721c5726a0207790fea937c87055c651919', 'aws/lambdas/justhodl-ai/source/sm_hub.py': '415187dfa052fff325e2196d158a055b0d8b774cc329bacf234164b8d72c6620', 'aws/lambdas/justhodl-ai/source/training.py': '034f182a7d079ceec4f63190e7c666204e7a8da13289903826e6a7800a2343c6', 'aws/lambdas/justhodl-ai/source/training_eligibility.py': '5d5863a3efda87800d43815c69f5280b3821d6cfb3fdc31a2e78c4e277a6b11f', 'aws/lambdas/justhodl-ai/source/validation_splits.py': '1ec6e348b99ac552bba133bc90b7559559b381ac52a59a9643fa0c4a9dc88d3d', 'aws/lambdas/justhodl-ai/source/wall_post.py': 'f2cddd718df3b5a9b122087ed32b6c5cf4341d5e7907c30b1fc2eb64bc696307', 'aws/shared/context_evidence_store.py': '48fc8f3470fec9e1fe9b984290858dd956bf70d572de71850ea2c6cc8154fae4', 'aws/shared/crisis_authority.py': 'b917845d839e6f5c0d314292d5db7e5dc47885fe36528291d6ee10303b5ae73c', 'aws/shared/deterministic_desk.py': 'ee386f30b29af630c4bc05d3b3bef9eb6746b88e36ca1a081b76e2fd53097f43', 'aws/shared/fabric_logging_context.py': 'af8df16f5d582985a5dbb4acfa361c38bc23c6ba496aaecb7a24461253d8d88f', 'aws/shared/factory_core.py': '6d05d66b1d5743342e8d69d1a12c3a3423712c2669e10d638e6c4acc36577ed4', 'aws/shared/factory_discipline.py': 'fcd6fcee4e8f389784050669f444758aaf950dafd6d52b4e94d5122d14f44808', 'aws/shared/factory_doctrine.py': '0788a2b80f2b65dd188f00b5748efb2d4622afa59aaaab05a89a88db36b6c485', 'aws/shared/factory_evidence.py': '5523d9a62db7feaf5b2b8681095ba85c8fa2871b0bfbdea7126e0d253f500ec7', 'aws/shared/factory_forecast.py': '5b7279caef8fad121571d524fb2ec3cb7246e9c3618ecc58bc97b0b073feb2db', 'aws/shared/factory_store.py': 'c8bcfb23f73b3ca889d8db8ec8d0350cc4607b17a64ee5c402df90462eb5f9cd', 'aws/shared/llm_cost.py': 'b929fe17aa3f42f8f4f96584f6dadcb78740ea141ca70ef2b4cddd6ee9635e84', 'aws/shared/llm_router.py': 'e46303f7cdff7264d59ca3cf51c870a175fffe610ba46a3d5cafe93eee43ab33', 'aws/shared/managed_secret.py': 'afa2552d71119f547476c327ab2bcbb9329b582591c785781233e4cbc91fa75e', 'aws/shared/private_artifact.py': '52d5a7c3176b977a0870dcfe35909d598eff434d7a438cdf403fb65dcdc001ba', 'aws/shared/signal_event_inputs.py': '10c135b4a191bd31d0cde8df3ce1bd8d6d40a06e83a363f3a410b521311175dd', 'aws/shared/signals_emit.py': '4aad4d81f5855bbcc0f1ce314c24fe50f0aa765c7e1b78001c4168bcb2cab0c2', 'aws/shared/xai_voice.py': '14936b2dc7791c42241ac6adddab1e5d590c5cda202b84ef87e99691dcf9ef1a', 'aws/ops/checks/market_runtime_evidence.py': 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca', 'aws/ops/checks/release_package_evidence.py': 'a0a45ca05400b0940f1e89621785de66216ef4107c2565aa6bec5336ca869480', 'aws/ops/staged/ops_6461_signal_logger_controls.py': 'bdefb9c4c1b3d738cad79c18dc8f94639990ad9c4452314f5dc59e3218309384'}
EXPECTED_CONTROLS = {'justhodl-ai': {'timeout': 900, 'memory_mb': 3008, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-inventory', 'state': 'ENABLED', 'expression': 'cron(7 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-pipeline', 'state': 'ENABLED', 'expression': 'rate(10 minutes)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-wall-prepare', 'state': 'ENABLED', 'expression': 'cron(5 9 ? * MON *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-wall-post', 'state': 'ENABLED', 'expression': 'cron(31 9 ? * MON *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-market-read', 'state': 'ENABLED', 'expression': 'cron(45 5 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-ai', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 2048}}
EXPECTED_SOURCE_COUNTS = {'justhodl-ai': 50}

MONITORED_TARGETS = {'justhodl-ai': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-ai'}

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
    with report('ops_6466_ai_input_acceptance') as out:
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
