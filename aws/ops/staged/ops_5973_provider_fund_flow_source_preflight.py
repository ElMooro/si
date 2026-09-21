"""Audit ETF fund-flow originals, public contexts and the capital radar runtime.

Existing source subscription only; six bounded read-only provider requests.
No producer invocation, account reads, signals, notifications or cadence changes.
"""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from collections import Counter
from decimal import Decimal
import ast,base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,urllib.parse,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import shared_imports
BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/provider-fund-flow-research/'
FUNCTIONS=('justhodl-etf-fund-flows','justhodl-capital-flow-radar')
PACKETS=('etf-flows/measurements.json','etf-flows/daily.json','etf-flows/composite.json',
    'etf-flows/event-study.json','etf-flows/rotation.json','etf-flows/per-ticker-context.json',
    'etf-flows/ai-analysis.json','etf-flows/constituent-pressure.json','data/capital-flow-radar.json',
    'data/capital-flow-radar-state.json','data/etf-true-flows.json','data/sector-rotation.json')
PROBE=('SPY','VOO','XLC','SOXL','SQQQ','TLT')
ENDPOINT='https://api.polygon.io/etf-global/v1/fund-flows'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def bounded(stream,limit=64*1024*1024):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    assert len(raw)<=limit,'Reviewed byte bound exceeded'
    return raw
def code(exc):return getattr(exc,'response',{}).get('Error',{}).get('Code')
def retain(s3,raw):
    key=PREFIX+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
    assert bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def runtime(lam,s3,events,scheduler,FN):
    source=ROOT/'aws/lambdas'/FN/'source';deployed=lam.get_function(FunctionName=FN);cfg=deployed['Configuration']
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40),64*1024*1024)
    assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==cfg['CodeSha256']
    paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in paths}
    expected.update({p.name:p for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        for name,p in expected.items():assert z.read(name)==p.read_bytes(),'Actual packaged source differs: '+name
    try:
        receipt=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key='data/ops/releases/'+FN+'.json')['Body']))
        assert receipt['code_sha256']==cfg['CodeSha256']
        receipt_status={'status':'matched','commit':receipt['commit']}
    except Exception as exc:
        if code(exc) not in ('NoSuchKey','404'):raise
        receipt_status={'status':'missing_predecessor_receipt'}
    schedules=[]
    for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):
        for name in page['RuleNames']:
            rule=events.describe_rule(Name=name);targets=events.list_targets_by_rule(Rule=name)['Targets']
            schedules.append({'kind':'EventBridge rule','name':name,'state':rule['State'],'expression':rule.get('ScheduleExpression'),
                'native_targets':sum(t.get('Arn')==cfg['FunctionArn'] for t in targets)})
    cfgpath=source.parent/'config.json';conf=json.loads(cfgpath.read_bytes()) if cfgpath.exists() else {}
    declared=conf.get('eventbridge_scheduler') or {}
    if declared.get('schedule_name'):
        actual=scheduler.get_schedule(Name=declared['schedule_name']);assert actual['Target']['Arn']==cfg['FunctionArn']
        schedules.append({'kind':'EventBridge Scheduler','name':declared['schedule_name'],'state':actual['State'],
            'expression':actual['ScheduleExpression'],'timezone':actual['ScheduleExpressionTimezone'],'native_targets':1})
    for page in scheduler.get_paginator('list_schedules').paginate(NamePrefix=FN):
        for item in page.get('Schedules',[]):
            if item.get('Target',{}).get('Arn')!=cfg['FunctionArn']:continue
            actual=scheduler.get_schedule(Name=item['Name'],GroupName=item['GroupName'])
            schedules.append({'kind':'EventBridge Scheduler','name':actual['Name'],'group':actual['GroupName'],
                'state':actual['State'],'expression':actual['ScheduleExpression'],
                'timezone':actual['ScheduleExpressionTimezone'],'native_targets':1})
    runtime={'code_sha256':cfg['CodeSha256'],'source_files_checked':len(expected),'handler_bytes':len((source/'lambda_function.py').read_bytes()),
        'timeout':cfg['Timeout'],'memory_mb':cfg['MemorySize'],'receipt':receipt_status,'schedules':schedules,
        'function_name':cfg['FunctionName'],'runtime':cfg['Runtime'],'handler':cfg['Handler'],
        'architectures':cfg['Architectures'],'role':cfg['Role'],'ephemeral_storage_mb':cfg['EphemeralStorage']['Size']}
    return runtime


def assignments(path,names):
    tree=ast.parse(path.read_text(encoding='utf-8'));out={}
    for node in tree.body:
        if isinstance(node,ast.Assign):
            for target in node.targets:
                if getattr(target,'id',None) in names:out[target.id]=ast.literal_eval(node.value)
    assert set(out)==set(names);return out

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Provider redirect not permitted')

def probe(s3,credential,ticker):
    now=datetime.now(timezone.utc);params={'composite_ticker':ticker,'processed_date.gte':(now.date()-timedelta(days=110)).isoformat(),
        'processed_date.lte':now.date().isoformat(),'sort':'processed_date.desc','limit':5000}
    url=ENDPOINT+'?'+urllib.parse.urlencode(params)
    req=urllib.request.Request(url,headers={'User-Agent':'JustHodl-SourceAudit/1.0','Authorization':'Bearer '+credential})
    try:
        response=urllib.request.build_opener(NoRedirect).open(req,timeout=30)
        raw=bounded(response,8*1024*1024)
    except urllib.error.HTTPError as exc:return {'status':'provider_http_error','http_status':exc.code,'ticker':ticker,'request':params}
    except Exception:return {'status':'provider_request_failed','ticker':ticker,'request':params}
    assert credential.encode() not in raw,'Provider response must not contain a credential'
    p=json.loads(raw,parse_float=Decimal);assert isinstance(p,dict) and p.get('status')=='OK'
    ref=retain(s3,raw);rows=p.get('results');assert isinstance(rows,list)
    for row in rows:assert isinstance(row,dict)
    fields=Counter(k for row in rows for k in row);identities=Counter(row.get('composite_ticker') or '<missing>' for row in rows)
    effective=[x.get('effective_date') for x in rows if isinstance(x.get('effective_date'),str)]
    processed=[x.get('processed_date') for x in rows if isinstance(x.get('processed_date'),str)]
    versions=Counter((x.get('effective_date'),x.get('processed_date')) for x in rows)
    select=sorted(rows,key=lambda r:(r.get('effective_date') or '',r.get('processed_date') or ''))
    sample=[{k:(str(v) if isinstance(v,Decimal) else v) for k,v in row.items() if k in ('composite_ticker','effective_date','processed_date','fund_flow','nav','shares_outstanding')} for row in select[-3:]]
    return {'status':'retained','ticker':ticker,'request':params,'acquired_at':now.isoformat(),'original':ref,'rows':len(rows),
        'reported_count':p.get('count'),'next_page_present':bool(p.get('next_url')),'fields':dict(fields),
        'ticker_counts':dict(identities),'effective_date_min':min(effective) if effective else None,'effective_date_max':max(effective) if effective else None,
        'processed_date_min':min(processed) if processed else None,'processed_date_max':max(processed) if processed else None,
        'duplicate_effective_processed_pairs':sum(v-1 for v in versions.values() if v>1),'sample_latest_effective_rows':sample}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5973_provider_fund_flow_source_preflight') as r:
        runtimes={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes)
        source=ROOT/'aws/lambdas/justhodl-etf-fund-flows/source/lambda_function.py'
        catalog=assignments(source,('ETF_UNIVERSE',))['ETF_UNIVERSE']
        radar=assignments(ROOT/'aws/lambdas/justhodl-capital-flow-radar/source/lambda_function.py',('COMPLEXES','SINGLE_STOCK_LEV'))
        catalog_ref=retain(s3,encoded(catalog));radar_ref=retain(s3,encoded(radar))
        refs={};inventory={};packets={}
        for key in PACKETS:
            try:raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            except Exception as exc:
                if code(exc) not in ('NoSuchKey','404'):raise
                inventory[key]={'status':'missing'};continue
            refs[key]=retain(s3,raw);p=json.loads(raw);packets[key]=p
            summary={'bytes':len(raw),'root_fields':list(p),'generated_at':p.get('generated_at'),'contract':p.get('contract'),
                'methodology_version':p.get('methodology_version'),'quality':p.get('quality'),'n_ok':p.get('n_ok'),'universe_size':p.get('universe_size')}
            if isinstance(p.get('metrics'),list):
                rows=p['metrics'];summary.update(metrics_count=len(rows),ticker_count=len({row.get('ticker') for row in rows}),
                    metric_quality=dict(Counter((row.get('quality') or {}).get('status','none') for row in rows)),
                    metric_fields=sorted({field for row in rows for field in row}),
                    first_selected=[{k:row.get(k) for k in ('ticker','observation_date','processed_date','quality','daily_flow_usd','date_basis')} for row in rows if row.get('ticker') in PROBE])
            inventory[key]=summary
        stamp=(packets.get('etf-flows/daily.json',{}).get('generated_at') or '')[:10]
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}',stamp):
            key='etf-flows/history/'+stamp+'.json'
            try:raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']);refs[key]=retain(s3,raw);inventory[key]={'bytes':len(raw),'status':'whole_latest_archive_retained'}
            except Exception as exc:
                if code(exc) not in ('NoSuchKey','404'):raise
                inventory[key]={'status':'missing'}
        credential=lam.get_function_configuration(FunctionName=FUNCTIONS[0]).get('Environment',{}).get('Variables',{}).get('POLYGON_KEY','')
        assert credential,'Existing provider configuration absent'
        probes={ticker:probe(s3,credential,ticker) for ticker in PROBE};del credential
        manifest={'contract':'provider-fund-flow-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'packets':refs,'packet_inventory':inventory,'catalog':catalog_ref,'radar_catalog':radar_ref,'provider_probes':probes,
            'scope':'Full old public contexts retained. Six original provider response samples. Static tags are not investor identity; creations/redemptions are not exchange turnover or underlying demand.'}
        manifest_ref=retain(s3,encoded(manifest));keys={manifest_ref['key'],catalog_ref['key'],radar_ref['key'],*(v['key'] for v in refs.values()),*(p['original']['key'] for p in probes.values() if p.get('original'))}
        for key in sorted(keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        r.kv(retained_manifest=manifest_ref,packet_inventory=inventory,configured_etfs=len(catalog),radar_complexes=len(radar['COMPLEXES']),
            single_stock_groups=len(radar['SINGLE_STOCK_LEV']),provider_probes=probes,protected_artifacts_checked=len(keys),
            originals_anonymously_denied=True,provider_requests=len(PROBE),producer_invocations=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0,schedule_changes=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
