"""Finish verification of already complete 6099 originals, with no acquisition.

The cancelled runner retained all 2000 responses. Do not rerun it. Verify the
exact diagnosed journal, reparse every original, check every protected path,
then conditionally link the preserved parent control to this acceptance.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,sys
import boto3
from botocore.config import Config
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

REQUEST='chatgpt-share-structure-completed-recovery-acceptance-6103'
STATUS=source.request_key(REQUEST,'acceptance')
COMPLETE={'key':source.PRIVATE+'0bb0bbea859ca6bc6669e76711cc5cf3a7a8a84e2ca3594b521e909ff84b2f2c.bin',
    'sha256':'0bb0bbea859ca6bc6669e76711cc5cf3a7a8a84e2ca3594b521e909ff84b2f2c','bytes':616991}
MANIFEST={'key':source.PRIVATE+'b27a7dae4ccd1a43abd50c1611b893833169da1da6390a16c2078670b51c22fe.bin',
    'sha256':'b27a7dae4ccd1a43abd50c1611b893833169da1da6390a16c2078670b51c22fe','bytes':616752}


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=12,retries={'max_attempts':2}))
    lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler'))
    with report('ops_6103_share_structure_recovery_acceptance') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6102_share_structure_recovery_state.md').read_text(encoding='utf-8')
        original=json.loads(source.read(s3,probe.BASELINE))
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert before==original['runtime']
        control=source.request_key(batches.PARENT,'batch:3')
        response=s3.get_object(Bucket=source.BUCKET,Key=control)
        old=bounded(response['Body'],source.MAX);etag=response['ETag']
        assert old==source.read(s3,recovery.diagnostic.FAILED)
        recovered_key=source.request_key(recovery.REQUEST,'batch:3')
        recovered_raw=bounded(s3.get_object(Bucket=source.BUCKET,Key=recovered_key)['Body'],source.MAX)
        assert recovered_raw==source.read(s3,COMPLETE)
        recovered=json.loads(recovered_raw)
        assert recovered['status']=='complete' and recovered['manifest']==MANIFEST
        assert len(recovered['captures'])==2000 and not recovered['source_errors']
        progress={'request_id':REQUEST,'status':'claimed','started_at':baseline.now(),
            'whole_completed_journal':COMPLETE,'source_manifest':MANIFEST,'original_failed_journal':recovery.diagnostic.FAILED}
        source.journal(s3,STATUS,progress,True)
        try:
            batch=json.loads(source.read(s3,MANIFEST));plan=json.loads(source.read(s3,recovered['plan']))
            counts=campaign.verify_batch(s3,plan,batch)
            assert counts==recovered['counts'] and counts['complete_sources']==2000
            assert counts['provider_requests']==1259 and counts['reused_sources']==741
            protected={STATUS,control,recovered_key,COMPLETE['key'],MANIFEST['key'],
                recovery.diagnostic.FAILED['key'],recovered['plan']['key']}
            for ref in batch['captures'].values():
                cap=json.loads(source.read(s3,ref));protected.update((ref['key'],cap['original']['key'],cap['request_status_key']))
            def deny(key):
                assert denied_with_retry('https://justhodl.ai/'+key)
                assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
            with ThreadPoolExecutor(max_workers=6) as pool:
                for _ in pool.map(deny,sorted(protected)):pass
            assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
            assert bounded(s3.get_object(Bucket=source.BUCKET,Key=recovered_key)['Body'],source.MAX)==recovered_raw
            linked={**recovered,'recovered_from':recovery.diagnostic.FAILED,
                'completed_recovery_journal':COMPLETE,'accepted_by_ops':'ops_6103_share_structure_recovery_acceptance'}
            assert batches.accepted_batch_report(3,linked)=='ops_6103_share_structure_recovery_acceptance.md'
            s3.put_object(Bucket=source.BUCKET,Key=control,Body=source.encode(linked),
                ContentType='application/json',CacheControl='no-store',IfMatch=etag)
            assert campaign.read_journal(s3,control)==linked
            progress.update(status='complete',completed_at=baseline.now(),counts=counts,protected_artifacts_checked=len(protected))
            source.journal(s3,STATUS,progress)
            r.kv(**progress,all_original_sources_reparsed=True,all_protected_paths_checked=True,
                native_package_unchanged=True,interrupted_workflow_run=36149403141,
                provider_requests=0,producer_invocations=0,consumer_invocations=0,private_account_reads=0,
                public_writes=0,signal_writes=0,portfolio_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
                complete_population_claimed=False,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            source.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
