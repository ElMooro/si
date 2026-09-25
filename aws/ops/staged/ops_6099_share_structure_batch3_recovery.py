"""Explicitly recover part 3 by adopting retained responses after quota review.

All 597 successes are reused, plus the successful reviewed canary and existing
accounting/probe originals. New starts are capped at 0.5 per second. Any failure
stops the new campaign; this script cannot retry a failed request or publish an
engine. The old failed journal survives whole before a conditional control link.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,sys,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import share_structure_sources as source
import share_structure_campaign as campaign
import share_structure_batch_runner as batches
import financial_statement_campaign as prior_sources
import ops_6081_share_structure_retained_baseline as baseline
import ops_6082_share_structure_original_probe as probe
import ops_6098_share_structure_batch3_limit_check as diagnostic

REQUEST='chatgpt-share-structure-batch3-reviewed-recovery-6099'


def limited_transport():
    rate=source.Rate(interval=2.0);opener=urllib.request.build_opener(source.NoRedirect())
    def open_one(request,timeout):
        rate.acquire()
        return opener.open(request,timeout=timeout)
    return open_one


def adopter(s3,failed,canary,canary_origin,original_probe,accounting):
    def adoption(request):
        retained=failed['captures'].get(request['url'])
        if retained:
            cap=json.loads(source.read(s3,retained))
            return cap,source.read(s3,cap['original']),diagnostic.FAILED
        if request==canary['spec']:
            return canary,source.read(s3,canary['original']),canary_origin
        retained=original_probe['captures'].get(request['url'])
        if retained:
            cap=json.loads(source.read(s3,retained))
            return cap,source.read(s3,cap['original']),batches.PROBE
        retained=accounting['captures'].get(request['url'])
        if retained:
            cap=json.loads(prior_sources.read(s3,retained))
            return cap,prior_sources.read(s3,cap['original']),probe.ACCOUNTING
        return None
    return adoption


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=12,retries={'max_attempts':2}))
    lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler'))
    with report('ops_6099_share_structure_batch3_recovery') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6098_share_structure_batch3_limit_check.md').read_text(encoding='utf-8')
        check=campaign.read_journal(s3,diagnostic.STATUS)
        assert check['status']=='complete' and check['http_status']==200 and check['outcome']=='single_source_request_recovered'
        assert check['elapsed_cooldown_seconds']>=1800 and check['failed_batch']==diagnostic.FAILED
        canary_origin=source.retain(s3,source.encode(check));canary=json.loads(source.read(s3,check['capture']))
        control=source.request_key(batches.PARENT,'batch:3');response=s3.get_object(Bucket=source.BUCKET,Key=control)
        failed_raw=campaign.bounded(response['Body'],source.MAX);etag=response['ETag']
        assert failed_raw==source.read(s3,diagnostic.FAILED)
        failed=json.loads(failed_raw);assert failed['status']=='failed' and len(failed['captures'])==597
        assert canary['spec']['url'] in failed['source_errors'] and canary['http_status']==200
        plan_ref=failed['plan'];plan=json.loads(source.read(s3,plan_ref));campaign.specifications(plan,3)
        original=json.loads(source.read(s3,probe.BASELINE));before=runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        assert before==original['runtime']
        original_probe=json.loads(source.read(s3,batches.PROBE));accounting=json.loads(prior_sources.read(s3,probe.ACCOUNTING))
        assert original_probe['status']=='complete' and accounting['status']=='complete'
        env=lam.get_function_configuration(FunctionName=baseline.FUNCTION).get('Environment',{}).get('Variables',{})
        credential=env.get('FMP_API_KEY') or env.get('FMP_KEY')
        if not credential:credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
        ref,executed=campaign.run_batch(s3,REQUEST,plan_ref,3,credential,baseline.now,
            adopter(s3,failed,canary,canary_origin,original_probe,accounting),remaining_seconds=3300,transport=limited_transport())
        assert executed,'Accepted or ambiguous recovery must not be rerun'
        batch=json.loads(source.read(s3,ref));campaign.verify_batch(s3,plan,batch)
        protected={control,diagnostic.STATUS,diagnostic.FAILED['key'],canary_origin['key'],check['capture']['key'],ref['key'],
            source.request_key(REQUEST,'batch:3'),plan_ref['key']}
        for captured in batch['captures'].values():
            cap=json.loads(source.read(s3,captured));protected.update((captured['key'],cap['original']['key'],cap['request_status_key']))
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=6) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
        recovered=campaign.read_journal(s3,source.request_key(REQUEST,'batch:3'))
        assert recovered['status']=='complete' and recovered['manifest']==ref and recovered['plan']==plan_ref
        linked={**recovered,'recovered_from':diagnostic.FAILED,'accepted_by_ops':'ops_6099_share_structure_batch3_recovery'}
        s3.put_object(Bucket=source.BUCKET,Key=control,Body=source.encode(linked),ContentType='application/json',CacheControl='no-store',IfMatch=etag)
        assert campaign.read_journal(s3,control)==linked
        r.kv(request_id=REQUEST,part=3,batch_manifest=ref,counts=batch['counts'],original_failed_journal=diagnostic.FAILED,
            prior_successes_adopted=597,effective_max_provider_requests_per_second=0.5,original_sources_reparsed=True,
            protected_artifacts_checked=len(protected),provider_requests=batch['counts']['provider_requests'],
            producer_invocations=0,consumer_invocations=0,private_account_reads=0,public_writes=0,
            signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
            complete_population_claimed=False,forecast_qualified=False,sizing_qualified=False)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
