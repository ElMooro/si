"""Read-only EPS package, actual fanout route and whole public-source replay.

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
FN='justhodl-eps-revision-velocity';KEY='data/eps-revision-velocity.json';BUCKET='justhodl-dashboard-live'


def compiler():
    path=ROOT/'aws/lambdas'/FN/'source/eps_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_eps_observations',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def archive_ref(raw):
    return {'key':'data/eps-revision-velocity/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def read_public_archive(s3,ref):
    if not isinstance(ref,dict) or not re.fullmatch(r'data/eps-revision-velocity/history/[a-f0-9]{64}\.json',str(ref.get('key',''))):
        raise ValueError('Only declared public EPS history permitted')
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'])
    if obj.get('ContentLength')!=len(raw) or archive_ref(raw)!=ref:raise ValueError('Whole immutable archive differs')
    return raw


def publication(raw,prior_raw=None):
    m=compiler();p=m.strict(raw)
    if not isinstance(p,dict):raise ValueError('Whole packet required')
    out={'status':'pending_original_fanout_publication','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'generated_at':p.get('generated_at'),'version':p.get('version')}
    if p.get('measurement_contract')!=m.CONTRACT:return out
    expected_sources={name:{'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()} for name in ('lambda_function.py','eps_observations.py') for content in [(ROOT/'aws/lambdas'/FN/'source'/name).read_bytes()]}
    if p.get('source_files')!=expected_sources:raise ValueError('Exact packet compiler/orchestration source identity differs')
    if any(p.get(k) is not False for k in ('calls_eligible','forecast_qualified','sizing_eligible','execution_eligible','private_state_read_or_written','independent_evidence_eligible')) or p.get('call') is not None:
        raise ValueError('Explicit research-only permission required')
    if p.get('all_qualifying')!=[] or p.get('summary')!={'top_25_overall':[],'tier_a':[],'tier_b_symbols':[]} or p.get('signals_logged')!=0 or p.get('notifications_sent')!=0:
        raise ValueError('No unsupported tiers or signals permitted')
    stamp=m.clock(p.get('generated_at'));started=m.clock(p.get('acquisition_started_at'))
    if stamp is None or started is None or stamp<started or m.day(p.get('checked_as_of'))!=started.date():raise ValueError('Publication clocks invalid')
    if p.get('previous_publication')!=(archive_ref(prior_raw) if prior_raw is not None else None):raise ValueError('Whole previous public packet differs')
    previous=m.strict(prior_raw) if prior_raw else {};old={}
    if previous.get('measurement_contract')==m.CONTRACT:
        if m.clock(previous.get('generated_at')) is None or m.clock(previous['generated_at'])>=started:raise ValueError('Previous clock invalid')
        for row in previous['request_records']:old.setdefault(row['ticker'],[]).append(row)
    def acquisition(a):
        if a.get('status') in ('received','invalid_original'):
            received=m.clock(a.get('received_at'))
            if received is None or not started<=received<=stamp:raise ValueError('Receipt clock outside run')
        return m.original(a)
    captures=p['universe_acquisitions']
    if [a['endpoint'] for a in captures]!=['data/universe.json','screener/data.json']:raise ValueError('Original universe source scope required')
    for a in captures:acquisition(a)
    # Read only the static literal, never import a Lambda or managed secret.
    import ast
    tree=ast.parse((ROOT/'aws/lambdas'/FN/'source/lambda_function.py').read_bytes())
    backup=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SP500_BACKUP' for t in n.targets))
    if p['legacy_static_backup']!=backup:raise ValueError('Static selection lineage changed')
    policy=p['acquisition_policy'];membership=m.universe(captures,backup,policy['request_limit'])
    if policy.get('threshold_currency_verified') is not False or policy.get('quote_gate_is_budget_selection_not_valuation') is not True:raise ValueError('Unreported cap currency cannot become USD valuation')
    if p['universe_membership']!=membership:raise ValueError('Full universe membership replay differs')
    selected=membership['selected_symbols'];records=p['request_records']
    if len(records)!=len(selected):raise ValueError('Missing selected occurrence')
    n_est=0;n_rating=0
    for i,(ticker,row) in enumerate(zip(selected,records)):
        group=row['acquisitions'];endpoints=[a['endpoint'] for a in group]
        if endpoints not in (['quote'],['quote','analyst-estimates'],['quote','analyst-estimates','grades']):raise ValueError('Unexpected provider request scope')
        for a in group:acquisition(a)
        matches=old.get(ticker,[]);prior=matches[0]['acquisitions'] if len(matches)==1 else None
        expected=m.dossier(ticker,group,p['checked_as_of'],prior);expected['request_index']=i
        if row!=expected:raise ValueError('Full forecast/rating replay differs')
        n_est+=len(row['estimate_observations']);n_rating+=len(row['rating_observations'])
    if p['n_estimate_observations']!=n_est or p['n_rating_observations']!=n_rating or p['stats']!={'n_universe':len(selected),'n_qualifying':None,'n_tier_a':None,'n_tier_b':None}:raise ValueError('Population counts differ')
    out.update(status='published_eps_originals_replayed',universe_occurrences=len(membership['occurrences']),request_occurrences=len(records),estimate_observations=n_est,rating_observations=n_rating,first_release_history_verified=False,independent_evidence=False,investment_authority=False)
    return out


def manifest_membership(raw):
    p=compiler().strict(raw)
    if not isinstance(p,dict) or not isinstance(p.get('ticks'),dict):raise ValueError('Live fanout ticks required')
    if FN in (p.get('disabled') or []):raise ValueError('EPS producer disabled in fanout')
    ticks=[]
    for tick,names in p['ticks'].items():
        if not isinstance(names,list):raise ValueError('Malformed fanout member list')
        if names.count(FN)>1:raise ValueError('Duplicate EPS fanout membership')
        if FN in names:ticks.append(tick)
    if not ticks:raise ValueError('No existing EPS fanout membership')
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
    baseline=json.loads((ROOT/'docs/audit/2026-09-27/eps-revision-velocity-original-baseline.json').read_bytes())['actual_producers'][FN]
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    with report('ops_6250_eps_target_acceptance') as r:
        before=runtime(*args,FN);r.kv(actual_runtime=before);check_runtime(before,baseline,expected,3)
        route=fanout(clients);r.kv(fanout_route=route)
        obj=clients['s3'].get_object(Bucket=BUCKET,Key=KEY);raw=bounded(obj['Body'])
        if obj.get('ContentLength')!=len(raw):raise ValueError('Whole current publication required')
        p=compiler().strict(raw);prior=None;archived=False
        if p.get('measurement_contract')==compiler().CONTRACT:
            if read_public_archive(clients['s3'],archive_ref(raw))!=raw:raise ValueError('Current archive differs')
            archived=True
            if p.get('previous_publication') is not None:prior=read_public_archive(clients['s3'],p['previous_publication'])
        result=publication(raw,prior)
        if runtime(*args,FN)!=before or fanout(clients)!=route:raise ValueError('Runtime or routing changed during acceptance')
        r.kv(expected_commit=expected,actual_runtime=before,fanout_route=route,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,scope='Exact producer package; actual router code, selected EPS fanout membership and enabled tick payload. Whole declared public packet and its own history only; no predictive qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
