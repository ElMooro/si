"""Replay complete retained issuer evidence and qualify Bond Desk cohort arithmetic.

No provider request, invocation, publication, history modification, notification,
account read or schedule change. The candidate remains private and unintegrated.
"""
from pathlib import Path
import gzip,hashlib,io,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks','aws/shared','aws/lambdas/justhodl-etf-true-flows/source')]
from ops_report import report
from market_runtime_evidence import bounded,schedule_evidence,verified_alias
from ops_6168_bond_desk_baseline import retain,PRIVATE,BUCKET,FUNCTION
import etf_store,etf_research
import bond_flow_candidate as candidate
import verify_bond_flow as independent
import retained_access_evidence as access

BASELINE='36534c93f149632f65e0fcd49566e3396d9ff76f500b52478223f0ca66f82d2e'


def private_read(s3,ref):
    if ref['key']!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Protected retained identity required')
    raw=bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('Retained whole object differs')
    return raw


def reader(s3):
    observations={}
    def read(key):
        if not isinstance(key,str) or not re.fullmatch(r'data/(?:etf-research/(?:runs|inputs|outputs|histories|compilers)/[a-f0-9]{64}\.(?:json|py)|evidence/etf_original/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key):raise ValueError('Only exact original-replay artifacts may be read')
        raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
        if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        receipt={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
        if key in observations and observations[key]!=receipt:raise ValueError('Immutable original changed during replay')
        observations[key]=receipt
        return raw
    return read,observations


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_bond_flow_candidate.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    with report('ops_6169_bond_flow_candidate') as r:
        baseline=json.loads(private_read(s3,{'key':PRIVATE+BASELINE+'.bin','sha256':BASELINE,'bytes':62355}))
        before=lam.get_function_configuration(FunctionName=FUNCTION);expected=baseline['predecessor']['runtime']
        if any(before.get(k)!=v for k,v in expected.items()):raise ValueError('Bond Desk predecessor changed')
        conf=json.loads((ROOT/'aws/lambdas'/FUNCTION/'config.json').read_bytes());alias=verified_alias(lam,FUNCTION,before,conf)
        schedules=schedule_evidence(events,scheduler,FUNCTION,before['FunctionArn'],conf,alias)
        if schedules!=baseline['predecessor']['schedules']:raise ValueError('Bond Desk cadence changed')
        raw=private_read(s3,baseline['captures']['data/etf-true-flows.json']['original']);packet=json.loads(raw)
        read,observations=reader(s3);key=packet['replay']['manifest_key'];manifest_raw=read(key);manifest=json.loads(manifest_raw)
        if key!='data/etf-research/runs/'+etf_research.digest(manifest)+'.json':raise ValueError('Run identity differs')
        reproduced=etf_store.replay(manifest,read)
        independent.same(reproduced,{k:v for k,v in packet.items() if k!='replay'})
        if etf_research.digest(reproduced)!=packet['replay']['output_sha256']:raise ValueError('Packet output identity differs')
        at=baseline['finished_at'];view=candidate.compile_cohorts(raw,at);proof=independent.verify(raw,view,candidate.BUCKETS,at)
        closure={module.__name__:retain(s3,Path(module.__file__).read_bytes()) for module in (candidate,independent)}
        view_ref=retain(s3,candidate.encoded(view));proof_ref=retain(s3,candidate.encoded(proof));input_ref=baseline['captures']['data/etf-true-flows.json']['original']
        evidence={'contract':'bond-flow-candidate-acceptance.v1','baseline':BASELINE,'evaluated_at':at,
            'source':input_ref,'source_run':packet['replay'],'source_artifacts':observations,'compilers':closure,
            'view':view_ref,'proof':proof_ref,'whole_original_replay_passed':True,'candidate_only':True}
        evidence_ref=retain(s3,candidate.encoded(evidence))
        protected=[view_ref,proof_ref,evidence_ref,*closure.values()]
        privacy=access.summarize([access.check(ref['key']) for ref in protected])
        after=lam.get_function_configuration(FunctionName=FUNCTION)
        if any(after.get(k)!=v for k,v in expected.items()) or schedule_evidence(events,scheduler,FUNCTION,after['FunctionArn'],conf,alias)!=schedules:raise ValueError('Predecessor or schedule changed')
        originals={k:v for k,v in observations.items() if k.startswith('data/evidence/')}
        r.kv(acceptance=evidence_ref,candidate=view_ref,independent_proof=proof_ref,checks=proof,source=input_ref,
            source_run=packet['replay'],source_artifacts=len(observations),original_responses=len(originals),
            original_bytes=sum(v['bytes'] for v in originals.values()),native_histories=reproduced['quality']['native_histories'],
            native_observations=reproduced['quality']['native_observations'],whole_original_replay_passed=True,
            five_observation_coverage={name:{k:row['windows']['5_observations'][k] for k in ('included_count','configured_count','status')} for name,row in view['cohorts'].items()},
            **privacy,provider_requests=0,native_invocations=0,public_writes=0,history_writes=0,account_reads=0,notifications_sent=0,
            schedules_changed=0,candidate_only=True,historical_point_in_time_verified=False,forecast_qualified=False,
            scope='Complete upstream issuer-source replay and independently checked descriptive cohort arithmetic. No native Bond Desk integration or investment qualification.')
        if not privacy['all_denied']:raise ValueError('Candidate evidence must remain private')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
