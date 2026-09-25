"""Qualify original capital-structure research without replacing a live engine.

Original bytes, row coverage, current ticker/CIK corroboration, independent
rational arithmetic, frozen compiler replay and public artifact readback.
No provider request, engine/consumer invocation, account or portfolio access,
current-head publication, signal, notification, AI or schedule mutation.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import gc,json,resource,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6081_share_structure_retained_baseline as baseline
import ops_6082_share_structure_original_probe as probe
import ops_6089_share_structure_complete_population as completed
import share_structure_campaign as campaign
import share_structure_sources as capture
import capital_structure_source as source
import capital_structure_research as model
import capital_structure_store as store
import capital_structure_arithmetic as arithmetic

BUCKET=capture.BUCKET
REQUEST='chatgpt-capital-structure-research-candidate-6090'
STATUS=capture.request_key(REQUEST,'candidate')
IDENTITY={'key':'audit-private/20260909-originals/financial-statement-research/be68296c89773d354b80b976434de5fc3b4c5538e6c2d4f62e1f51fe56bb04d8.bin',
    'sha256':'be68296c89773d354b80b976434de5fc3b4c5538e6c2d4f62e1f51fe56bb04d8','bytes':883}


def fingerprint(compiled):
    return {'packet':source.sha(source.encoded(compiled['packet'])),
        'shards':{k:source.sha(source.encoded(v)) for k,v in compiled['shards'].items()}}


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=12,retries={'max_attempts':2}))
    lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler'))
    with report('ops_6090_capital_structure_research_candidate') as r:
        for name in ('measurements','research','store','arithmetic'):
            subprocess.run([sys.executable,str(ROOT/'tests'/('test_capital_structure_'+name+'.py'))],cwd=ROOT,check=True)
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6089_share_structure_complete_population.md').read_text(encoding='utf-8')
        accepted=campaign.read_journal(s3,completed.STATUS);assert accepted['status']=='complete'
        SOURCE=accepted['source_manifest'];assert accepted['counts']['complete_sources']==11305
        read=store.reader(s3,BUCKET);original=source.strict(source.original(probe.BASELINE,read))
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert before==original['runtime']
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/staged/ops_6090_capital_structure_research_candidate.py'],cwd=ROOT,text=True).strip()
        progress={'request_id':REQUEST,'source_commit':commit,'status':'claimed','source_manifest':SOURCE,'identity_capture':IDENTITY}
        capture.journal(s3,STATUS,progress,True)
        try:
            started=time.monotonic();compiled=model.compile_output(SOURCE,IDENTITY,read);packet=compiled['packet']
            assert packet['reported_names']==1615 and packet['provider_responses']==11305
            assert packet['provider_rows']==accepted['counts']['provider_rows']
            profile={'compile_seconds':round(time.monotonic()-started,3),'index_bytes':len(source.encoded(packet)),
                'shard_count':len(compiled['shards']),'shard_bytes':sum(len(source.encoded(v)) for v in compiled['shards'].values()),
                'max_shard_bytes':max(len(source.encoded(v)) for v in compiled['shards'].values())}
            progress.update(status='compiled',profile=profile);capture.journal(s3,STATUS,progress)
            started=time.monotonic();proof=arithmetic.verify(SOURCE,IDENTITY,compiled,read)
            profile['independent_check_seconds']=round(time.monotonic()-started,3)
            expected=fingerprint(compiled);ref=store.retain(s3,BUCKET,SOURCE,IDENTITY,compiled)
            progress.update(status='retained',replay=ref,qualification=proof);capture.journal(s3,STATUS,progress)
            del compiled,packet,read;gc.collect()
            started=time.monotonic();read=store.reader(s3,BUCKET);compiled=store.replay(ref,read)
            profile.update(replay_seconds=round(time.monotonic()-started,3),max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            assert fingerprint(compiled)==expected
            run=store.verified_run(ref,read)
            public={ref['manifest_key'],run['input']['key'],run['output']['key'],compiled['packet']['identity_index']['original']['key'],
                *(v['key'] for v in run['compilers'].values()),*(v['record']['key'] for v in compiled['packet']['issuers'])}
            def check_public(key):
                request=urllib.request.Request('https://justhodl.ai/'+key,
                    headers={'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache'})
                assert store.bounded(urllib.request.urlopen(request,timeout=30))==store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            with ThreadPoolExecutor(max_workers=4) as pool:
                for _ in pool.map(check_public,sorted(public)):pass
            for key in (STATUS,SOURCE['key'],IDENTITY['key']):
                assert denied_with_retry('https://justhodl.ai/'+key)
                assert denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
            packet=compiled['packet'];counts={key:packet[key] for key in ('reported_names','provider_responses','provider_rows',
                'empty_responses','record_statuses','metric_statuses','unrequestable_reported_labels')}
            progress.update(status='complete',profile=profile,qualification=proof,counts=counts,
                public_artifacts_checked=len(public),frozen_compilers={key:value['sha256'] for key,value in run['compilers'].items()})
            capture.journal(s3,STATUS,progress)
            r.kv(**progress,native_package_unchanged=True,provider_requests=0,producer_invocations=0,
                consumer_invocations=0,current_head_writes=0,private_account_reads=0,paid_ai_calls=0,
                notifications_sent=0,signal_writes=0,portfolio_writes=0,schedules_changed=0,
                native_runtime_capacity_verified=False,original_sec_filings_verified=False,
                historical_security_continuity_verified=False,forecast_qualified=False,sizing_qualified=False)
        except Exception as exc:
            capture.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
