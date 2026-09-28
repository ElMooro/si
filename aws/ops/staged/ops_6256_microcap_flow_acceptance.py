"""Read-only Microcap package, actual fanout route and whole public-source replay.

No native/provider/consumer invocation, private output/account/learning reads,
schedule mutation, notification or publication write.
"""
from pathlib import Path
import hashlib,importlib.util,json,re,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6232_sec_search_research_acceptance import check_runtime
FN='justhodl-microcap-float-squeeze';KEY='data/microcap-float-squeeze.json';BUCKET='justhodl-dashboard-live'


def compiler():
    path=ROOT/'aws/lambdas'/FN/'source/float_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_float_observations',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def archive_ref(raw):
    return {'key':'data/microcap-float-squeeze/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def read_public_archive(s3,ref):
    if not isinstance(ref,dict) or not re.fullmatch(r'data/microcap-float-squeeze/history/[a-f0-9]{64}\.json',str(ref.get('key',''))):
        raise ValueError('Only declared public Microcap history permitted')
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'],64*1024*1024)
    if obj.get('ContentLength')!=len(raw) or archive_ref(raw)!=ref:raise ValueError('Whole immutable archive differs')
    return raw


def source_acquisitions(p):
    yield p['universe_acquisition']
    yield from p['finra_acquisitions']
    for row in p['request_records']:yield from row['acquisitions']


def read_sources(s3,p):
    m=compiler();sources={};total=0
    for a in source_acquisitions(p):
        if a.get('status') not in ('received','invalid_original'):
            if 'original_ref' in a:raise ValueError('Unexpected source reference on unavailable acquisition')
            continue
        ref=m.validate_ref(a.get('original_ref'))
        if ref['key'] in sources:continue
        total+=ref['bytes']
        if total>160*1024*1024:raise ValueError('Declared original source budget exceeded')
        obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'],8*1024*1024)
        if obj.get('ContentLength')!=len(raw) or m.source_ref(raw,ref['format'])!=ref:raise ValueError('Whole public source identity differs')
        sources[ref['key']]=raw
    return sources


def publication(raw,prior_raw=None,sources=None):
    m=compiler();p=m.strict(raw);sources={} if sources is None else sources
    if not isinstance(p,dict):raise ValueError('Whole packet required')
    out={'status':'pending_original_fanout_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'generated_at':p.get('generated_at'),'version':p.get('version')}
    if p.get('measurement_contract')!=m.CONTRACT:return out
    paths={name:ROOT/'aws/lambdas'/FN/'source'/name for name in ('lambda_function.py','float_observations.py')}
    paths['offexchange_measurements.py']=ROOT/'aws/shared/offexchange_measurements.py'
    expected_sources={name:{'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()} for name,path in paths.items() for content in [path.read_bytes()]}
    if p.get('source_files')!=expected_sources:raise ValueError('Exact packet compiler/orchestration source identity differs')
    if any(p.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','private_state_read_or_written','independent_evidence_eligible')) or p.get('call') is not None:
        raise ValueError('Explicit research-only permission required')
    if p.get('all_qualifying')!=[] or p.get('summary')!={'top_25_overall':[],'tier_s':[]} or p.get('signals_logged')!=0 or p.get('notifications_sent')!=0:
        raise ValueError('No unsupported tiers or signals permitted')
    stamp=m.clock(p.get('generated_at'));started=m.clock(p.get('acquisition_started_at'))
    if stamp is None or started is None or stamp<started or m.day(p.get('checked_as_of'))!=started.date():raise ValueError('Publication clocks invalid')
    if p.get('previous_publication')!=(archive_ref(prior_raw) if prior_raw is not None else None):raise ValueError('Whole previous public packet differs')
    previous=m.strict(prior_raw) if prior_raw else {}
    if previous.get('measurement_contract')==m.CONTRACT:
        prior_clock=m.clock(previous.get('generated_at'))
        if prior_clock is None or prior_clock>=started:raise ValueError('Previous clock invalid')
    references=set()
    for a in source_acquisitions(p):
        if a.get('status') in ('received','invalid_original'):
            received=m.clock(a.get('received_at'))
            if received is None or not started<=received<=stamp:raise ValueError('Receipt clock outside run')
            m.content(a,sources);references.add(a['original_ref']['key'])
        elif 'original_ref' in a:raise ValueError('Unavailable acquisition cannot hide retained bytes')
    if references!=set(sources) or p['retained_unique_source_bytes']!=sum(len(v) for v in sources.values()):raise ValueError('Whole unique source population differs')
    capture=p['universe_acquisition']
    if capture['endpoint']!='data/universe.json':raise ValueError('Original universe source scope required')
    membership=m.universe(capture,p['universe_membership']['request_limit'],sources)
    if p['universe_membership']!=membership:raise ValueError('Complete original universe replay differs')
    records=p['request_records'];selected=membership['selected']
    if len(records)!=len(selected):raise ValueError('Selected request population differs')
    flow=m.finra_files(p['finra_acquisitions'],sources,selected,p['checked_as_of'])
    if p['finra_file_coverage']!={k:v for k,v in flow.items() if k!='by_literal_symbol'}:raise ValueError('Whole CNMS file replay differs')
    window=p['finra_request_window'];back=window.get('calendar_days_examined')
    if type(back) is not int or not 0<=back<=44 or window.get('maximum_calendar_days')!=44 or window.get('target_nonempty_files')!=20:raise ValueError('Original FINRA acquisition bounds differ')
    from datetime import timedelta
    expected_dates=[(started.date()-timedelta(days=i)).isoformat() for i in range(1,back+1) if (started.date()-timedelta(days=i)).weekday()<5]
    if [a['observation_date'] for a in p['finra_acquisitions']]!=expected_dates:raise ValueError('Original ordered weekday requests differ')
    parsed=sum(f['status']=='whole_cnms_file_parsed' and bool(f['reported_rows']) for f in flow['files'])
    if window.get('nonempty_parsed_files')!=parsed or parsed>20:raise ValueError('Nonempty file count differs')
    stop=window.get('stop_reason')
    if stop not in ('original_twenty_file_target_reached','runtime_or_source_byte_reserve','rate_limited_no_retry','original_lookback_exhausted'):raise ValueError('Unknown stop reason')
    if stop=='original_twenty_file_target_reached' and parsed!=20 or stop=='original_lookback_exhausted' and back!=44:raise ValueError('Acquisition stop differs')
    if stop=='rate_limited_no_retry' and (not p['finra_acquisitions'] or p['finra_acquisitions'][-1]['status']!='rate_limited'):raise ValueError('Rate-limit stop differs')
    count=0;prices=0
    for i,(member,row) in enumerate(zip(selected,records)):
        group=row['acquisitions']
        if [a['endpoint'] for a in group] not in (['historical-price-eod/full'],['quote','historical-price-eod/full']):raise ValueError('Unreviewed acquisition endpoint scope')
        expected=m.dossier(member,group,sources,flow,p['checked_as_of']);expected['request_index']=i
        if row!=expected:raise ValueError('Whole market/flow observation replay differs')
        count+=len(row['finra_observations']);prices+=row['price_evidence'].get('records') or 0
    if p['n_finra_observations']!=count or p['n_price_records']!=prices or p['stats']!={'n_universe':len(selected),'n_evaluated':None,'n_filtered_out':None,'n_tier_s':None,'n_tier_a':None,'n_tier_b':None,'n_finra_tickers':None}:raise ValueError('Root population counts or old ranks differ')
    out.update(status='published_microcap_originals_replayed',universe_occurrences=len(membership['occurrences']),request_occurrences=len(records),finra_observations=count,price_records=prices,source_blobs=len(sources),first_release_history_verified=False,independent_evidence=False,investment_authority=False)
    return out


def manifest_membership(raw):
    p=compiler().strict(raw)
    if not isinstance(p,dict) or not isinstance(p.get('ticks'),dict):raise ValueError('Live fanout ticks required')
    if FN in (p.get('disabled') or []):raise ValueError('Microcap producer disabled in fanout')
    ticks=[]
    for tick,names in p['ticks'].items():
        if not isinstance(names,list):raise ValueError('Malformed fanout member list')
        if names.count(FN)>1:raise ValueError('Duplicate Microcap fanout membership')
        if FN in names:ticks.append(tick)
    if not ticks:raise ValueError('No existing Microcap fanout membership')
    return sorted(ticks)


def fanout(clients):
    lam,s3,events,scheduler=(clients[n] for n in ('lambda','s3','events','scheduler'))
    router=runtime(lam,s3,events,scheduler,'justhodl-scheduler')
    cfg=lam.get_function_configuration(FunctionName='justhodl-scheduler')
    key=(cfg.get('Environment',{}).get('Variables',{}).get('FANOUT_MANIFEST_KEY') or 'config/fanout-manifest.json')
    if key!='config/fanout-manifest.json':raise ValueError('Router uses an unreviewed manifest path')
    obj=s3.get_object(Bucket=BUCKET,Key=key);raw=bounded(obj['Body'],4*1024*1024)
    if obj.get('ContentLength')!=len(raw) or not obj.get('ETag'):raise ValueError('Whole live fanout manifest required')
    ticks=manifest_membership(raw);arn=cfg['FunctionArn'];routes=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=arn):
        for name in page['RuleNames']:
            rule=events.describe_rule(Name=name)
            for group in events.get_paginator('list_targets_by_rule').paginate(Rule=name):
                for target in group['Targets']:
                    if target.get('Arn')!=arn:continue
                    try:payload=json.loads(target.get('Input','null'))
                    except ValueError:continue
                    if not isinstance(payload,dict):continue
                    tick=payload.get('tick') or ((payload.get('Input') or {}).get('tick') if isinstance(payload.get('Input'),dict) else None)
                    if tick in ticks:routes.append({'tick':tick,'kind':'EventBridge rule','name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),'timezone':'UTC'})
    for tick in ticks:
        if not any(r['tick']==tick and r['state']=='ENABLED' and r['expression'] for r in routes):raise ValueError('No verified enabled fanout tick rule: '+tick)
    return {'manifest_key':key,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'etag':obj['ETag'],
            'matching_ticks':ticks,'routes':sorted(routes,key=lambda r:(r['tick'],r['name'])),
            'router_code_sha256':router['code_sha256'],'router_sources_checked':router['source_files_checked']}


def main():
    subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FN/'tests/run_tests.py')],cwd=ROOT,check=True)
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/microcap-float-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6256_microcap_flow_acceptance') as r:
        before=runtime(*args,FN);r.kv(actual_runtime=before);check_runtime(before,baseline,expected,4)
        route=fanout(clients);r.kv(fanout_route=route)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'],64*1024*1024)
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==compiler().CONTRACT:
            if read_public_archive(clients['s3'],archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=read_public_archive(clients['s3'],p['previous_publication'])
        sources=read_sources(clients['s3'],p) if p.get('measurement_contract')==compiler().CONTRACT else {}
        result=publication(raw,prior,sources)
        if runtime(*args,FN)!=before or fanout(clients)!=route:raise ValueError('Runtime or routing changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,fanout_route=route,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,scope='Exact producer package; actual router code, selected Microcap fanout membership and enabled tick payload. Whole declared public packet, its own history and exact content-addressed provider blobs only; no predictive qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
