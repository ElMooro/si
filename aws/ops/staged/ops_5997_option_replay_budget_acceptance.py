"""Verify deployed replay reserve and old immutable output; never invoke."""
from pathlib import Path
import json,subprocess,sys,time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5994_option_flow_native_acceptance import public,runtime_commit,FUNCTION,BUCKET
from ops_5975_etf_constituent_source_preflight import runtime
import option_flow_store as store
import option_flow_research as model
PROOF='data/option-flow-budget-verification.json'


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5997_option_replay_budget_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        assert store.POST_CAPTURE_RESERVE_SECONDS==780
        commit=runtime_commit();actual=runtime(lam,s3,events,scheduler,FUNCTION)
        assert actual['receipt']=={'status':'matched','commit':commit}
        assert json.loads(public('data/ops/releases/'+FUNCTION+'.json'))['commit']==commit
        prior=json.loads(public('data/option-flow-scheduled-verification.json'))
        for field in ('function_name','runtime','handler','architectures','role','ephemeral_storage_mb','schedules','memory_mb','timeout'):
            assert actual[field]==prior['runtime_package'][field]
        read=store.reader(s3,BUCKET);started=time.monotonic()
        original=store.replay(prior['publication']['replay'],read)
        assert model.sha(model.encoded(original))==prior['publication']['replay']['output_sha256']
        assert original['generated_at']==prior['publication']['generated_at']
        raw=public(model.CURRENT);packet=json.loads(raw)
        assert model.clock(packet['generated_at'])>=model.clock(original['generated_at'])
        run=store.verified_run(packet['replay'],read)
        assert model.checked(run['output'],read,'outputs')=={k:v for k,v in packet.items() if k!='replay'}
        assert json.loads(public(model.LEGACY))==store.compatibility(packet)
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof={'contract':'option-flow-replay-budget-acceptance.v1','generated_at':store.now(),'commit':commit,
            'runtime_package':actual,'post_capture_reserve_seconds':store.POST_CAPTURE_RESERVE_SECONDS,
            'previous_measured_post_capture_seconds':prior['post_capture_execution_seconds'],
            'original_replay':prior['publication']['replay'],'original_output_unchanged':True,
            'runner_replay_seconds':round(time.monotonic()-started,3),
            'current_publication':{'generated_at':packet['generated_at'],'sha256':model.sha(raw),'replay':packet['replay']},
            'current_compiler_matches_runtime':run['compilers']['option_flow_store']==model.ref(Path(store.__file__).read_bytes(),'compilers'),
            'engine_invocations':0,'provider_requests':0,'source_packet_writes':0,'schedules_changed':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0}
        body=model.encoded(proof)
        s3.put_object(Bucket=BUCKET,Key=PROOF,Body=body,ContentType='application/json',CacheControl='no-store')
        assert public(PROOF)==body
        r.kv(proof_key=PROOF,commit=commit,runtime_package=actual,
            post_capture_reserve_seconds=store.POST_CAPTURE_RESERVE_SECONDS,original_output_unchanged=True,
            runner_replay_seconds=proof['runner_replay_seconds'],current_publication=proof['current_publication'],
            current_compiler_matches_runtime=proof['current_compiler_matches_runtime'],
            engine_invocations=0,provider_requests=0,source_packet_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
