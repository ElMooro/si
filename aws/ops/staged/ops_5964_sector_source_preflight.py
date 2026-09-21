"""Preserve Sector Rotation and Tilt predecessors and replay existing market originals.

Read-only research audit; protected retention only. No producer, notification,
provider, credential, account, portfolio or schedule call.
"""
from pathlib import Path
from datetime import datetime,timezone
import ast,base64,gzip,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-daily-report-v3/source')]
from ops_report import report
from release_package_evidence import shared_imports
import daily_market_model,daily_macro_model
from daily_market_store import verify_sources
BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/sector-research/'
FUNCTIONS=('justhodl-sector-rotation','justhodl-sector-tilt')
PACKETS=('data/sector-rotation.json','data/sector-tilt.json','data/report.json','data/macro-nowcast.json','data/etf-true-flows.json','data/etf-desk.json','etf-flows/daily.json','data/finviz-groups.json','signals/anomalies.json')
SYMBOLS=('XLK','XLF','XLV','XLY','XLP','XLE','XLI','XLU','XLB','XLRE','XLC','SMH','XBI','KRE','XOP','SPY','QQQ','IWM','DIA','TLT','IEF','HYG','LQD','GLD','SLV','DBC','USO','UUP')

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def bounded(stream,limit=32*1024*1024):
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


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5964_sector_source_preflight') as r:
        runtimes={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
        refs={};packets={};inventory={};originals={}
        def read(key):
            assert (key in PACKETS or re.fullmatch(r'data/(?:daily-research/(?:runs|inputs|compilers)/[a-f0-9]{64}\.(?:json|py)|evidence/polygon/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key)),'Unreviewed source path'
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)))
            originals[key]=retain(s3,raw)
            return raw
        for key in PACKETS:
            try:raw=read(key)
            except Exception as exc:
                if code(exc) not in ('404','NoSuchKey'):raise
                inventory[key]={'status':'missing'};continue
            p=json.loads(raw);assert isinstance(p,dict),'Expected source object'
            refs[key]=originals[key];packets[key]=p
            meta={'bytes':len(raw),'contract':p.get('contract'),'generated_at':p.get('generated_at') or p.get('as_of'),
                'root_fields':sorted(p),'calls_eligible':p.get('calls_eligible'),'sizing_eligible':p.get('sizing_eligible')}
            if key=='data/sector-rotation.json':
                meta.update(sectors=len(p.get('sectors',[])),sector_fields=sorted(p['sectors'][0]) if p.get('sectors') else [],
                    identities=[{'symbol':x.get('symbol'),'ticker':x.get('ticker'),'name':x.get('name'),'rs_1m_pct':x.get('rs_1m_pct'),'rs_3m_pct':x.get('rs_3m_pct')} for x in p.get('sectors',[])],
                    ratios=len(p.get('ratios',[])),macro_context=p.get('macro_context'),risk_appetite=p.get('risk_appetite'))
            if key=='data/sector-tilt.json':
                meta.update(tilts=len(p.get('tilts',[])),regime=p.get('regime'),summary=p.get('summary'),
                    identities=[{k:x.get(k) for k in ('ticker','name','current_state','rs_20d','rs_63d','last_close','implication')} for x in p.get('tilts',[])])
            inventory[key]=meta
        report_packet=packets['data/report.json'];replay=report_packet['replay'];key=replay['manifest_key'];raw=read(key)
        assert key=='data/daily-research/runs/'+sha(raw)+'.json'
        manifest=json.loads(raw);assert manifest['contract']=='daily-research-replay.v1' and manifest['generated_at']==report_packet['generated_at']
        ref=manifest['input'];raw=read(ref['key']);assert len(raw)==ref['bytes'] and sha(raw)==ref['sha256']
        inputs=json.loads(raw);sources=inputs['auxiliary']['market_sources']
        for module in (daily_market_model,daily_macro_model):
            compiler=Path(module.__file__).read_bytes();digest=sha(compiler);ref=manifest['compilers'][module.__name__]
            assert ref=={'key':'data/daily-research/compilers/'+digest+'.py','sha256':digest} and read(ref['key'])==compiler
        assert sources['contract']==daily_market_model.CONTRACT
        restored={};market_inventory={}
        for symbol in SYMBOLS:
            source=sources['equities'].get(symbol)
            if source is None:market_inventory[symbol]={'status':'absent','source_error':sources.get('errors',{}).get(symbol)};continue
            verify_sources({'contract':sources['contract'],'equities':{symbol:source}},read)
            row=daily_market_model.equity(source,manifest['generated_at']);assert row['symbol']==symbol
            expected=report_packet.get('stocks',{}).get(symbol) or report_packet.get('sectors',{}).get(symbol)
            projected={**row,**{field:None for field in daily_macro_model.STOCK_AUTHORITY},'source_collected_at':inputs['auxiliary']['collected_at']}
            assert expected==projected,'Public market row differs from retained original: '+symbol
            restored[symbol]=row
            market_inventory[symbol]={'status':'original_replayed','name':row['name'],'date':row['date'],'price':row['price'],
                'unit':row['unit'],'adjustment':row['adjustment'],'acquired_at':row['acquired_at'],'history_scope':row['history_scope'],'quality':row['quality'],
                'source_request':source['request'],'evidence':source['evidence']}
        common=set.intersection(*(set(x['d'] for x in restored[s]['history']) for s in SYMBOLS[:11]+('SPY',) if s in restored)) if restored else set()
        manifest={'contract':'sector-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'packets':refs,'packet_inventory':inventory,'source_artifacts':originals,'market_inventory':market_inventory,
            'core_common_dates':sorted(common),'daily_source_run':replay['manifest_key'],'daily_output_digest_checked':False,
            'scope':'The selected daily market compiler and exact original responses are verified; unrelated report fields are retained context, not revalidated.'}
        ref=retain(s3,encoded(manifest));protected_keys={ref['key'],*(v['key'] for v in originals.values())}
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        r.kv(runtimes=runtimes,retained_manifest=ref,packet_inventory=inventory,market_inventory=market_inventory,
            retained_artifacts=len(originals),protected_artifacts_checked=len(protected_keys),core_common_date_count=len(common),
            core_first_date=min(common) if common else None,core_last_date=max(common) if common else None,
            engine_invocations=0,provider_requests=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,
            originals_anonymously_denied=True,next_work='Build matched-period sector price and risk research from retained originals; retain every predecessor context, separate NAV/share changes from price-volume indicators, and remove unsupported regime allocation authority. No legacy Rotation invocation: it contains a notification side effect.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
