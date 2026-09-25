"""Reparse all six accepted source batches; retain the complete population.

Private artifacts only. No source acquisition, native/consumer invocation,
account read, public/current write, signal, notification or schedule change.
"""
from pathlib import Path
from collections import Counter
import json, subprocess, sys
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
import ops_6081_share_structure_retained_baseline as baseline
import ops_6082_share_structure_original_probe as probe

REQUEST='chatgpt-share-structure-complete-population-6089'
STATUS=source.request_key(REQUEST,'complete')


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=12,retries={'max_attempts':2}))
    lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler'))
    with report('ops_6089_share_structure_complete_population') as r:
        for name in ('sources','campaign'):
            subprocess.run([sys.executable,str(ROOT/'tests'/('test_share_structure_'+name+'.py'))],cwd=ROOT,check=True)
        original=json.loads(source.read(s3,probe.BASELINE))
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert before==original['runtime']
        plan=campaign.read_journal(s3,batches.PLAN_STATUS);assert plan['status']=='complete'
        refs=[]
        for part in range(1,7):
            report_path=ROOT/'aws/ops/reports/latest'/('ops_'+str(6082+part)+'_share_structure_sources_part_'+str(part)+'.md')
            assert '**Status:** success' in report_path.read_text(encoding='utf-8')
            state=campaign.read_journal(s3,source.request_key(batches.PARENT,'batch:'+str(part)))
            assert state['status']=='complete' and state['plan']==plan['plan'];refs.append(state['manifest'])
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/staged/ops_6089_share_structure_complete_population.py'],cwd=ROOT,text=True).strip()
        progress={'request_id':REQUEST,'source_commit':commit,'status':'claimed','plan':plan['plan'],'generated_at':baseline.now()}
        source.journal(s3,STATUS,progress,True)
        try:
            complete=campaign.complete_population(s3,plan['plan'],refs)
            assert len(complete['reported_symbols'])==1615 and complete['counts']['complete_sources']==11305
            ref=source.retain(s3,source.encode(complete))
            for key in (STATUS,ref['key']):
                assert denied_with_retry('https://justhodl.ai/'+key)
                assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
            assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
            progress.update(status='complete',completed_at=baseline.now(),source_manifest=ref,counts=complete['counts'])
            source.journal(s3,STATUS,progress)
            r.kv(**progress,all_original_responses_reparsed=True,protected_artifacts_checked=2,
                complete_reported_names=1615,native_package_unchanged=True,provider_requests=0,
                producer_invocations=0,consumer_invocations=0,private_account_reads=0,public_writes=0,
                signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
                identity_qualified=False,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            source.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
