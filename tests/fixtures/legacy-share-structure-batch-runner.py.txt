"""Run one disjoint part of the reviewed complete share-structure population.

Private source capture only. No current publication, Lambda invocation,
private-account access, signal, notification, AI or schedule mutation.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,subprocess,sys
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6081_share_structure_retained_baseline as baseline
import ops_6082_share_structure_original_probe as probe
import share_structure_sources as source
import share_structure_campaign as campaign
import financial_statement_campaign as prior_sources

PARENT='chatgpt-share-structure-population-6083'
PLAN_STATUS=source.request_key(PARENT,'plan')
PROBE={'key':source.PRIVATE+'69d0590616f29a7d94e50b6c282e9c51e69304d3c607bf34fda0ac42ecf8c8fb.bin',
    'sha256':'69d0590616f29a7d94e50b6c282e9c51e69304d3c607bf34fda0ac42ecf8c8fb','bytes':10286}


def get_plan(client,part,captured):
    try:state=campaign.read_journal(client,PLAN_STATUS)
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('404','NoSuchKey'):raise
        state=None
    if state:
        if state.get('status')!='complete':raise ValueError('Inspect retained ambiguous source-plan state')
        document=json.loads(source.read(client,state['plan']))
        campaign.specifications(document,part)
        if document['baseline']!=probe.BASELINE or document['probe']!=PROBE or document['accounting']!=probe.ACCOUNTING:
            raise ValueError('Reviewed source-plan origins differ')
        return state['plan'],document
    if part!=1:raise ValueError('Reviewed source plan must precede later batches')
    inventory=json.loads(source.read(client,captured['inventory']))
    assert inventory['candidate_provider_label_count']==1615 and inventory['all_current_rows_conserved']
    document=campaign.plan(inventory['candidate_provider_labels'],probe.BASELINE,PROBE,probe.ACCOUNTING,baseline.now)
    assert document['batches']==6 and document['planned_sources']==11305
    claim={'contract':'capital-structure-plan-request.v1','request_id':PARENT,'status':'claimed',
        'generated_at':document['generated_at'],'planned_sources':11305,'batches':6}
    source.journal(client,PLAN_STATUS,claim,True)
    ref=source.retain(client,source.encode(document))
    source.journal(client,PLAN_STATUS,{**claim,'status':'complete','plan':ref})
    return ref,document


def main(part):
    if type(part) is not int or not 1<=part<=6:raise ValueError('Reviewed population part required')
    name='ops_'+str(6082+part)+'_share_structure_sources_part_'+str(part)
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=12,retries={'max_attempts':2}))
    lam,events,scheduler=(boto3.client(service,region_name='us-east-1') for service in ('lambda','events','scheduler'))
    with report(name) as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_share_structure_campaign.py')],cwd=ROOT,check=True)
        captured=json.loads(source.read(s3,probe.BASELINE));diagnostic=json.loads(source.read(s3,PROBE))
        assert captured['status']=='complete' and diagnostic['status']=='complete' and diagnostic['counts']['complete_sources']==28
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION);assert before==captured['runtime']
        plan_ref,document=get_plan(s3,part,captured)
        # A failed earlier batch is a stop, never permission to skip its names.
        for earlier in range(1,part):
            state=campaign.read_journal(s3,source.request_key(PARENT,'batch:'+str(earlier)))
            assert state['status']=='complete' and state['plan']==plan_ref
            accepted=ROOT/'aws/ops/reports/latest'/('ops_'+str(6082+earlier)+'_share_structure_sources_part_'+str(earlier)+'.md')
            assert '**Status:** success' in accepted.read_text(encoding='utf-8'), 'Earlier source and privacy acceptance must pass'
        previous=json.loads(prior_sources.read(s3,probe.ACCOUNTING))
        assert previous['status']=='complete'
        def adoption(request):
            existing=diagnostic['captures'].get(request['url'])
            if existing:
                capsule=json.loads(source.read(s3,existing));return capsule,source.read(s3,capsule['original']),PROBE
            existing=previous['captures'].get(request['url'])
            if existing:
                capsule=json.loads(prior_sources.read(s3,existing))
                return capsule,prior_sources.read(s3,capsule['original']),probe.ACCOUNTING
            return None
        env=lam.get_function_configuration(FunctionName=baseline.FUNCTION).get('Environment',{}).get('Variables',{})
        credential=env.get('FMP_API_KEY') or env.get('FMP_KEY')
        if not credential:credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
        ref,executed=campaign.run_batch(s3,PARENT,plan_ref,part,credential,baseline.now,adoption)
        batch=json.loads(source.read(s3,ref))
        protected={PLAN_STATUS,source.request_key(PARENT,'batch:'+str(part)),plan_ref['key'],ref['key'],
            probe.BASELINE['key'],PROBE['key'],probe.ACCOUNTING['key']}
        examples={}
        for value in batch['captures'].values():
            capsule=json.loads(source.read(s3,value))
            protected.update((value['key'],capsule['original']['key'],capsule['request_status_key']))
            spec=capsule['spec']
            if part==1 and spec['symbol'] in ('AAPL','BRK-B'):
                rows=source.strict(source.read(s3,capsule['original']))
                fields=('date','symbol','cik','reportedCurrency','period','fiscalYear','filingDate','acceptedDate',
                    'weightedAverageShsOut','weightedAverageShsOutDil','commonStockIssued','commonStockIssuance',
                    'commonStockRepurchased','netCommonStockIssuance','stockBasedCompensation','commonDividendsPaid',
                    'price','marketCap','timestamp','floatShares','outstandingShares','freeFloat','numerator','denominator','splitType')
                examples[spec['url']]={'original':capsule['original'],'source_row':0 if rows else None,
                    'first_reported_row_fields':{field:{'present':field in rows[0],'reported':source.typed(rows[0].get(field))} for field in fields} if rows else {},
                    'example_only_not_population_qualification':True}
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=6) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
        commit=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/checks/share_structure_batch_runner.py'],cwd=ROOT,text=True).strip()
        r.kv(source_commit=commit,plan=plan_ref,part=part,batch_manifest=ref,counts=batch['counts'],
            source_examples=examples,
            protected_artifacts_checked=len(protected),original_sources_reparsed=True,
            provider_requests_this_run=batch['counts']['provider_requests'] if executed else 0,
            producer_invocations=0,consumer_invocations=0,private_account_reads=0,signal_writes=0,
            public_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
            complete_population_claimed=False,forecast_qualified=False,sizing_qualified=False)
