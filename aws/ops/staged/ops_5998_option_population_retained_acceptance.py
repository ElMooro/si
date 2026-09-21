"""Finish retained coefficient qualification after a transport-only HEAD failure."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,re,sys,time,urllib.error
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from ops_5996_option_population_qualification import SOURCE_RUN,BUCKET,FUNCTION,independent
from ops_5975_etf_constituent_source_preflight import runtime,denied
import ops_5987_options_dependency_preflight as baseline
import option_population_model as model
import option_flow_research as upstream
import option_flow_store as store


def denied_with_retry(url,check=denied,pause=time.sleep):
    for attempt in range(3):
        try:return check(url)
        except (urllib.error.URLError,ConnectionResetError,TimeoutError):
            if attempt==2:raise
            pause(attempt+1)
    raise AssertionError('No denial response obtained')


def find_retained(s3,read):
    matches=[];objects=0;probes=0
    start=upstream.clock('2026-09-21T14:53:00Z');end=upstream.clock('2026-09-21T14:58:10Z')
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix=upstream.PRIVATE,
            PaginationConfig={'PageSize':1000}):
        for obj in page.get('Contents',[]):
            objects+=1;assert objects<=30000,'Bounded private evidence inventory required'
            key=obj['Key']
            if not (start<=obj['LastModified']<=end and 1000<=obj['Size']<=512*1024
                    and re.fullmatch(re.escape(upstream.PRIVATE)+r'[a-f0-9]{64}\.bin',key)):continue
            probes+=1;assert probes<=1500,'Bounded candidate discovery required'
            response=s3.get_object(Bucket=BUCKET,Key=key,Range='bytes=0-1023')
            prefix=store.bounded(response['Body'])
            if b'"contract":"option-population-qualification.v1"' not in prefix:continue
            ref={'key':key,'sha256':key.rsplit('/',1)[-1][:-4],'bytes':obj['Size']}
            candidate=json.loads(upstream.protected(ref,read))
            if candidate.get('contract')=='option-population-qualification.v1' and candidate.get('source_run')==SOURCE_RUN:
                assert upstream.protected(candidate['compiler'],read)==Path(model.__file__).read_bytes()
                matches.append((ref,candidate))
    assert len(matches)==1,'Select the exact retained candidate before any further action'
    return matches[0]


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=16));read=store.reader(s3,BUCKET)
    lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5998_option_population_retained_acceptance') as r:
        ref,candidate=find_retained(s3,read)
        r.kv(retained_candidate=ref,prior_arithmetic_counts=candidate['counts'],source_run=SOURCE_RUN)
        actual=runtime(lam,s3,events,scheduler,FUNCTION);assert actual==candidate['predecessor_runtime']
        run=store.verified_run(SOURCE_RUN,read);inputs=upstream.checked(run['input'],read,'inputs')
        packet={**upstream.checked(run['output'],read,'outputs'),'replay':SOURCE_RUN}
        protected={ref['key'],candidate['compiler']['key']}|{x['original']['key'] for x in candidate['predecessors'].values()}
        protected.update(v['key'] for v in candidate['private_delivery'].values())
        def delivery(key):return upstream.protected(candidate['private_delivery'][key],read)
        started=time.monotonic();results={}
        for symbol in model.UNDERLYINGS:
            recorded=candidate['results'][symbol];retained=model.restore(recorded['chain'],delivery)
            regenerated=model.chain(packet,symbol,read);assert regenerated==retained
            assert upstream.sha(upstream.encoded(regenerated))==recorded['expanded_sha256']
            source=inputs['chains'][symbol];summary=upstream.checked(packet['chains'][symbol]['chain'],read,'chains')
            result=independent(regenerated,summary,source,read);assert result==recorded['coefficient_checks'];results[symbol]=result
            protected.update(p['original']['key'] for p in source['pages'] if p.get('original'))
        proof={'contract':'option-population-retained-acceptance.v1','generated_at':store.now(),
            'retained_candidate':ref,'source_run':SOURCE_RUN,'results':results,'counts':candidate['counts'],
            'predecessor_runtime':actual,'replay_seconds':round(time.monotonic()-started,3),
            'original_candidate_reproduced':True,'scope':'No source recollection, native invocation, public head or schedule mutation.'}
        accepted=baseline.retain(s3,upstream.encoded(proof));protected.add(accepted['key'])
        r.kv(retained_acceptance=accepted,replay_seconds=proof['replay_seconds'],privacy_checks_pending=len(protected))
        def check(key):
            assert denied_with_retry('https://justhodl.ai/'+key)
            assert denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof.update(privacy_verified=True,protected_artifacts_checked=len(protected),originals_anonymously_denied=True)
        final=baseline.retain(s3,upstream.encoded(proof));check(final['key'])
        r.kv(accepted_candidate=ref,accepted_manifest=final,results=results,counts=candidate['counts'],
            protected_artifacts_checked=len(protected)+1,originals_anonymously_denied=True,
            elapsed_s=candidate['elapsed_s'],peak_runner_rss_kib=candidate['peak_runner_rss_kib'],
            provider_requests=0,engine_invocations=0,public_head_writes=0,schedules_changed=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
