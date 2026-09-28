"""Read-only Activist Filings package, actual fanout route and whole public-source replay.

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
FN='justhodl-activist-filings-scanner';KEY='data/activist-filings.json';BUCKET='justhodl-dashboard-live'


def compiler():
    path=ROOT/'aws/lambdas'/FN/'source/filing_observations.py'
    spec=importlib.util.spec_from_file_location('isolated_filing_observations',path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m


def archive_ref(raw):
    return {'key':'data/activist-filings/history/'+hashlib.sha256(raw).hexdigest()+'.json','sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def read_public_archive(s3,ref):
    if not isinstance(ref,dict) or not re.fullmatch(r'data/activist-filings/history/[a-f0-9]{64}\.json',str(ref.get('key',''))):
        raise ValueError('Only declared public Activist Filings history permitted')
    obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'],64*1024*1024)
    if obj.get('ContentLength')!=len(raw) or archive_ref(raw)!=ref:raise ValueError('Whole immutable archive differs')
    return raw


def source_acquisitions(p):
    a=p['acquisitions']
    yield a['mapping'];yield a['universe'];yield from a['feeds']


def read_sources(s3,p):
    m=compiler();sources={};total=0
    for a in source_acquisitions(p):
        if a.get('status')!='received':
            if 'original_ref' in a:raise ValueError('Unexpected original on unavailable attempt')
            continue
        ref=m.validate_ref(a.get('original_ref'))
        if ref['key'] in sources:continue
        total+=ref['bytes']
        if total>80*1024*1024:raise ValueError('Declared originals exceed bound')
        obj=s3.get_object(Bucket=BUCKET,Key=ref['key']);raw=bounded(obj['Body'],8*1024*1024)
        if obj.get('ContentLength')!=len(raw) or m.source_ref(raw,ref['format'])!=ref:raise ValueError('Whole original source differs')
        sources[ref['key']]=raw
    return sources


def publication(raw,prior_raw=None,sources=None):
    m=compiler();p=m.strict(raw);sources={} if sources is None else sources
    if not isinstance(p,dict):raise ValueError('Whole public packet required')
    out={'status':'pending_original_fanout_publication','bytes':len(raw),'sha256':m.sha(raw),'generated_at':p.get('generated_at'),'version':p.get('version')}
    if p.get('measurement_contract')!=m.CONTRACT:return out
    paths={name:ROOT/'aws/lambdas'/FN/'source'/name for name in ('lambda_function.py','filing_observations.py')}
    paths['sec_atom_model.py']=ROOT/'aws/shared/sec_atom_model.py'
    identity={name:{'bytes':len(raw),'sha256':m.sha(raw)} for name,path in paths.items() for raw in [path.read_bytes()]}
    if p.get('source_files')!=identity:raise ValueError('Exact packet source identity differs')
    if any(p.get(k) is not False for k in m.FLAGS) or p.get('call') is not None:raise ValueError('Explicit research permissions required')
    if p.get('all_filings')!=[] or any(value!=[] for value in p.get('summary',{}).values()) or p.get('signals_logged')!=0 or p.get('notifications_sent')!=0:raise ValueError('Legacy rankings and alerts cannot gain authority')
    stamp=m.clock(p.get('generated_at'));started=m.clock(p.get('acquisition_started_at'))
    if stamp is None or started is None or stamp<started:raise ValueError('Publication clocks invalid')
    if p.get('previous_publication')!=(archive_ref(prior_raw) if prior_raw is not None else None):raise ValueError('Whole prior public packet differs')
    previous=m.strict(prior_raw) if prior_raw else {}
    if previous.get('measurement_contract')==m.CONTRACT:
        prior_clock=m.clock(previous.get('generated_at'))
        if prior_clock is None or prior_clock>=started:raise ValueError('Previous publication clock invalid')
    for a in source_acquisitions(p):
        if 'requested_at' in a:
            requested=m.clock(a.get('requested_at'));received=m.clock(a.get('received_at'))
            if requested is None or received is None or not started<=requested<=received<=stamp:raise ValueError('Attempt outside acquisition clock')
    replay=m.build(p['acquisitions'],sources,p['generated_at'],p['window_days'])
    if any(p.get(k)!=v for k,v in replay.items()):raise ValueError('Complete filing identity/source replay differs')
    if p.get('retained_unique_source_bytes')!=sum(len(v) for v in sources.values()):raise ValueError('Unique original byte count differs')
    out.update(status='published_ownership_originals_replayed',source_blobs=len(sources),entry_occurrences=len(p['entry_occurrences']),
        accession_groups=len(p['filing_groups']),feed_coverage=p['feed_coverage'],quality=p['quality'],
        first_release_history_verified=False,independent_evidence=False,investment_authority=False)
    return out


def manifest_membership(raw):
    p=compiler().strict(raw)
    if not isinstance(p,dict) or not isinstance(p.get('ticks'),dict):raise ValueError('Live fanout ticks required')
    if FN in (p.get('disabled') or []):raise ValueError('Activist Filings producer disabled in fanout')
    ticks=[]
    for tick,names in p['ticks'].items():
        if not isinstance(names,list):raise ValueError('Malformed fanout member list')
        if names.count(FN)>1:raise ValueError('Duplicate Activist Filings fanout membership')
        if FN in names:ticks.append(tick)
    if not ticks:raise ValueError('No existing Activist Filings fanout membership')
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
    for path in ['aws/lambdas/'+FN+'/tests/run_tests.py','aws/lambdas/justhodl-compound-aggregator/tests/run_tests.py','tests/ops/test_activist_filings_acceptance.py']:
        subprocess.run([sys.executable,str(ROOT/path)],cwd=ROOT,check=True)
    baselines=json.loads((ROOT/'docs/audit/2026-09-28/activist-filings-original-baseline.json').read_bytes())['actual_producers']
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
    clients={n:boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')};args=[clients[n] for n in ('lambda','s3','events','scheduler')]
    functions={FN:3,'justhodl-compound-aggregator':4}
    with report('ops_6260_activist_filings_acceptance') as r:
        before={fn:runtime(*args,fn) for fn in functions};r.kv(actual_packages=before)
        for fn,count in functions.items():check_runtime(before[fn],baselines[fn],expected,count)
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
        if any(runtime(*args,fn)!=before[fn] for fn in functions) or fanout(clients)!=route:raise ValueError('Runtime or routing changed during acceptance')
        r.kv(expected_commit=expected,actual_packages=before,fanout_route=route,native_publication=result,current_archive_verified=archived,
             native_invocations=0,provider_requests=0,private_state_reads=0,consumer_output_reads=0,account_reads=0,learning_log_reads=0,
             public_writes=0,history_writes=0,schedule_changes=0,scope='Exact producer and one consumer package only; actual router code, selected Activist Filings fanout membership and enabled tick payload. Whole declared public producer packet, its own history and exact source blobs only; no consumer output read or predictive qualification.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
