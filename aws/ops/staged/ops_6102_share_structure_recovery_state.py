"""Inspect the interrupted 6099 recovery without retrying any source request.

Retain whole control/journal bytes, report exact completion/error counts and
the last confirmed manifest, and confirm native state remains unchanged.
No source acquisition, current/publication, invocation, account or schedule.
"""
from pathlib import Path
import json, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
import share_structure_sources as source
import share_structure_campaign as campaign
import share_structure_batch_runner as batches
import ops_6081_share_structure_retained_baseline as baseline
import ops_6082_share_structure_original_probe as probe
import ops_6099_share_structure_batch3_recovery as recovery


def main():
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6102_share_structure_recovery_state') as r:
        old=json.loads(source.read(s3,probe.BASELINE))
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert before==old['runtime']
        observations={};protected=set()
        for label,request in (('original_control',batches.PARENT),('interrupted_recovery',recovery.REQUEST)):
            key=source.request_key(request,'batch:3')
            response=s3.get_object(Bucket=source.BUCKET,Key=key)
            raw=bounded(response['Body'],source.MAX);state=json.loads(raw)
            assert state['part']==3
            if state['request_id']!=request:
                assert label=='original_control' and state['request_id']==recovery.REQUEST
                assert state.get('accepted_by_ops')=='ops_6099_share_structure_batch3_recovery'
            ref=source.retain(s3,raw);protected.update((key,ref['key']))
            plan=json.loads(source.read(s3,state['plan']))
            specs={spec['url']:spec for spec in campaign.specifications(plan,3)}
            captures=state.get('captures',{});errors=state.get('source_errors',{})
            assert set(captures)<=set(specs) and set(errors)<=set(specs)
            observation={'status':state['status'],'request_id':state['request_id'],'whole_journal':ref,'etag':response['ETag'],
                'planned_sources':len(specs),'retained_capture_count':len(captures),'source_error_count':len(errors),
                'unconfirmed_source_count':len(set(specs)-set(captures)),
                'generated_at':state.get('generated_at'),'completed_at':state.get('completed_at'),
                'error_type':state.get('error_type'),'counts':state.get('counts'),'manifest':state.get('manifest'),
                'source_errors':[]}
            if state.get('manifest'):
                manifest=json.loads(source.read(s3,state['manifest']))
                assert manifest['plan']==state['plan'] and manifest['part']==3
                assert manifest['status']=='complete' and set(manifest['captures'])==set(specs)
                observation['manifest_capture_count']=len(manifest['captures'])
                observation['manifest_counts']=manifest['counts'];protected.add(state['manifest']['key'])
            for url in sorted(errors):
                failure_key=source.request_key(state['request_id'],url)
                failure_raw=bounded(s3.get_object(Bucket=source.BUCKET,Key=failure_key)['Body'],source.MAX)
                failure=json.loads(failure_raw);assert failure['spec']==specs[url]
                failure_ref=source.retain(s3,failure_raw);protected.update((failure_key,failure_ref['key']))
                observation['source_errors'].append({'spec':specs[url],'status':failure.get('status'),
                    'http_status':failure.get('http_status'),'error_type':failure.get('error_type'),
                    'requested_at':failure.get('requested_at'),'received_at':failure.get('received_at'),
                    'whole_request_journal':failure_ref,'original':failure.get('original')})
            observations[label]=observation
        for key in sorted(protected):
            assert denied_with_retry('https://justhodl.ai/'+key)
            assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
        assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
        r.kv(observations=observations,interrupted_workflow_run=36149403141,
            protected_artifacts_checked=len(protected),whole_original_journals_preserved=True,
            all_source_bodies_reparsed=False,source_population_accepted=False,native_package_unchanged=True,
            provider_requests=0,producer_invocations=0,consumer_invocations=0,private_account_reads=0,
            public_writes=0,signal_writes=0,portfolio_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
