"""Qualify one complete desk candidate without publishing mutable public packets.

Runner IAM only. Existing provider subscription; durable one-attempt claim before
collection. No Lambda invocation, account reads, AI, notifications or schedule
changes. Originals, inputs and candidate are retained before later checks.
"""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json,resource,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import etf_desk_store as store
import etf_desk_model as model
import etf_desk_catalog as catalog
from ops_5975_etf_constituent_source_preflight import denied,runtime
from ops_5984_etf_profile_original_replay import independent
BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-etf-global-desk'
ATTEMPT=store.PRIVATE+'ops-5985-candidate-attempt.json'


def original_keys(value):
    found=set()
    if isinstance(value,dict):
        for child in value.values():found.update(original_keys(child))
    elif isinstance(value,list):
        for child in value:found.update(original_keys(child))
    elif isinstance(value,str) and value.startswith('audit-private/') and value.endswith('.bin'):found.add(value)
    return found


def public_hash(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
            headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=35) as response:
        body=store.bounded(response)
    return {'bytes':len(body),'sha256':model.sha(body)}


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    attempt={'contract':'etf-desk-candidate-attempt.v1','status':'running','phase':'preflight','started_at':store.now()}
    with report('ops_5985_etf_desk_candidate_qualification') as r:
        for path in ('aws/shared/tests/test_etf_desk_model.py','aws/shared/tests/test_etf_profile_collect.py',
                'aws/shared/tests/test_etf_desk_store.py'):
            subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
        before_runtime=runtime(lam,s3,events,scheduler,FUNCTION)
        before_public={key:public_hash(key) for key in store.ALIASES}
        try:s3.head_object(Bucket=BUCKET,Key=model.CURRENT)
        except Exception as exc:
            if not store.missing(exc):raise
        else:raise RuntimeError('Native desk already published; review instead of collecting again')
        try:store.status_write(s3,BUCKET,ATTEMPT,attempt,IfNoneMatch='*')
        except Exception as exc:
            if store.conflict(exc):raise RuntimeError('Candidate already attempted; inspect its retained inputs without recollecting') from None
            raise
        def checkpoint(**fields):
            attempt.update(fields);store.status_write(s3,BUCKET,ATTEMPT,attempt)
        try:
            started=time.monotonic();read=store.reader(s3,BUCKET)
            contexts=store.preserve(s3,BUCKET,read)
            flow=store.snapshot(s3,BUCKET,store.flow_model.CURRENT,read)
            holdings=store.snapshot(s3,BUCKET,store.holdings_model.CURRENT,read)
            checkpoint(phase='collect_originals',preserved_contexts=len(contexts))
            env=lam.get_function_configuration(FunctionName=FUNCTION).get('Environment',{}).get('Variables',{})
            credential=env.get('POLYGON_KEY') or env.get('POLYGON_API_KEY') or env.get('MASSIVE_API_KEY');del env
            assert credential,'Existing desk provider credential required'
            collection_started=time.monotonic()
            try:collections=store.collect(s3,BUCKET,credential,read,time.monotonic()+540)
            finally:del credential
            timings={'collection_seconds':round(time.monotonic()-collection_started,3)}
            inputs={'contract':'etf-desk-inputs.v1','generated_at':store.now(),'contexts':contexts,
                'canonical_flows':flow,'canonical_holdings':holdings,'previous':None,**collections}
            raw=model.encoded(inputs);digest=model.sha(raw);key=model.PREFIX+'inputs/'+digest+'.json'
            store.immutable(s3,BUCKET,key,raw,read=read)
            checkpoint(phase='compile',retained_input={'key':key,'sha256':digest,'bytes':len(raw)},
                provider_requests=inputs['provider_requests'],original_provider_bytes=inputs['original_provider_bytes'])
            phase=time.monotonic()
            with store.ArtifactWriter(s3,BUCKET,read) as emit:output=store.compile_output(inputs,read,emit)
            timings['compile_seconds']=round(time.monotonic()-phase,3)
            checkpoint(phase='retained_replay');phase=time.monotonic()
            ref=store.retain(s3,BUCKET,inputs,output,read,lambda candidate:checkpoint(candidate_replay=candidate))
            timings['retention_and_replay_seconds']=round(time.monotonic()-phase,3)
            timings['production_path_seconds']=round(time.monotonic()-started,3)
            checkpoint(phase='independent_checks',candidate_replay=ref,timings=timings)
            assert len(output['funds'])==len(catalog.DESK)==116
            assert output['quality']['canonical_overlap']==100 and len(output['quality']['additional_funds'])==16
            assert output['call'] is None and output['portfolio_action']=='WAIT'
            assert all(output[k] is False for k in model.PERMISSIONS)
            profile_counts=Counter();profile_statuses=Counter();coverage={};protected=original_keys(inputs)|{ATTEMPT}
            for ticker,fund in output['funds'].items():
                coverage[ticker]={'profile':fund['profiles']['current']['quality'],
                    'profile_effective_date':fund['profiles']['current']['effective_date'],
                    'holdings_effective_dates':fund['holdings']['current']['effective_dates'],
                    'holdings_rows':fund['holdings']['current']['quality'].get('returned_rows'),
                    'flow_current_eligible':fund['quality']['flow_current_eligible'],
                    'source_basis':fund['source_basis']}
                for role in ('current','prior'):
                    snapshot=model.checked(fund['profiles'][role]['snapshot'],read)
                    profile_statuses[role+':'+snapshot['quality']['status']]+=1
                    if snapshot['quality']['status']=='complete_returned_profile_snapshot':
                        profile_counts.update(independent(inputs['profiles'][ticker][role],snapshot,read))
                    protected.update(original_keys(snapshot))
            for canonical_ref in (flow,holdings):
                packet=json.loads(store.protected(canonical_ref,read));run=json.loads(read(packet['replay']['manifest_key']))
                source_inputs=model.checked(run['input'],read);protected.update(original_keys(source_inputs))
            # Every old packet and full history remains byte-identical in S3.
            for old_key,old_ref in contexts.items():
                if old_ref:assert store.bounded(s3.get_object(Bucket=BUCKET,Key=old_key)['Body'])==store.protected(old_ref,read)
            assert {key:public_hash(key) for key in store.ALIASES}==before_public
            try:s3.head_object(Bucket=BUCKET,Key=model.CURRENT)
            except Exception as exc:
                if not store.missing(exc):raise
            else:raise AssertionError('Candidate must not publish a mutable current desk')
            assert runtime(lam,s3,events,scheduler,FUNCTION)==before_runtime
            def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            with ThreadPoolExecutor(max_workers=8) as pool:
                for _ in pool.map(check,sorted(protected)):pass
            evidence={'contract':'etf-desk-candidate-qualification.v1','candidate_replay':ref,
                'generated_at':output['generated_at'],'quality':output['quality'],'coverage':coverage,
                'independent_profile_counts':dict(profile_counts),'profile_statuses':dict(profile_statuses),
                'provider_requests':inputs['provider_requests'],'original_provider_bytes':inputs['original_provider_bytes'],
                'timings':timings,'runner_max_rss_kb':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                'unchanged_contexts':len(contexts),'unchanged_public_packets':before_public,
                'protected_artifacts_checked':len(protected),'originals_anonymously_denied':True,
                'predecessor_runtime':before_runtime,'mutable_desk_published':False,'producer_invocations':0,
                'private_account_reads':0,'paid_ai_calls':0,'portfolio_writes':0,'notifications_sent':0,'signals_emitted':0}
            retained=store.protect(s3,BUCKET,model.encoded(evidence),read)
            checkpoint(status='qualified',phase='complete',completed_at=store.now(),qualification=retained)
            r.kv(**evidence,retained_qualification=retained)
        except Exception:
            checkpoint(status='failed',failed_at=store.now())
            raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
