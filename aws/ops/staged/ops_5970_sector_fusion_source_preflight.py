"""Retain whole Sector Flow contexts and audit native price, volume and issuer roots.

Source/runtime inspection and protected retention only. No source collection,
producer invocation, account reads, notifications or schedule changes.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,gzip,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-etf-true-flows/source')]
from ops_report import report
from release_package_evidence import shared_imports
import etf_native,etf_research,etf_store
import sector_research_store as sector_store
import money_volume_store as volume_store
BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/sector-flow-research/'
FUNCTIONS=('justhodl-sector-flow-state','justhodl-sector-capital-fusion','justhodl-sector-rotation','justhodl-money-flow-state','justhodl-etf-true-flows')
PACKETS=('data/etf-true-flows.json','data/sector-rotation.json','data/etf-flows.json','data/flow-lookthrough.json',
    'data/rotation-chains.json','data/dark-pool.json','data/13f-positions.json','data/insider-aggregate-history.json',
    'data/liquidity-flow.json','data/smart-beta.json','data/sector-flow-state.json','data/finviz-groups.json',
    'data/money-flow-state.json','data/political-trades.json','data/risk-regime.json','data/polygon-fx-regime.json',
    'data/dollar-radar.json','data/capital-inflows.json','data/gold-equity-rotation.json','data/tic-flows.json',
    'data/sector-capital-fusion.json','data/etf-desk.json','data/universe.json','data/chart-patterns.json',
    'data/accumulation-radar.json','flow-data.json','data/capital-flow-radar.json')
SYMBOLS=('XLK','XLF','XLV','XLY','XLP','XLE','XLI','XLU','XLB','XLRE','XLC')
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


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5970_sector_fusion_source_preflight') as r:
        runtimes={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
        refs={};packets={};inventory={};originals={}
        def read(key):
            allowed=key in PACKETS or sector_store.allowed(key) or volume_store.allowed(key) or bool(re.fullmatch(
                r'data/(?:etf-research/(?:runs|inputs|outputs|compilers|histories)/[a-f0-9]{64}\.(?:json|py)|evidence/etf_original/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)',key))
            assert allowed,'Unreviewed source path'
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'],64*1024*1024)
            if key.endswith('.gz'):raw=bounded(gzip.GzipFile(fileobj=io.BytesIO(raw)),64*1024*1024)
            originals[key]=retain(s3,raw)
            return raw
        for key in PACKETS:
            try:raw=read(key)
            except Exception as exc:
                if code(exc) not in ('404','NoSuchKey'):raise
                inventory[key]={'status':'missing'};continue
            packet=json.loads(raw);assert isinstance(packet,(dict,list))
            refs[key]=originals[key];packets[key]=packet
            inventory[key]={'bytes':len(raw),'type':type(packet).__name__}
            if isinstance(packet,dict):inventory[key].update(contract=packet.get('contract'),generated_at=packet.get('generated_at') or packet.get('as_of'),
                root_fields=sorted(packet),calls_eligible=packet.get('calls_eligible'),sizing_eligible=packet.get('sizing_eligible'))
        native_replays={}
        for key,module in [('data/sector-rotation.json',sector_store),('data/money-flow-state.json',volume_store)]:
            packet=packets[key];restored=module.replay(packet['replay'],read)
            assert {k:v for k,v in packet.items() if k!='replay'}==restored
            native_replays[key]={'contract':packet['contract'],'generated_at':packet['generated_at'],'replay':packet['replay'],'quality':packet['quality']}
        etf=packets['data/etf-true-flows.json'];ref=etf['replay'];raw=read(ref['manifest_key']);run=json.loads(raw)
        assert ref['manifest_key']==etf_research.PREFIX+'runs/'+sha(raw)+'.json' and run['contract']=='etf-original-replay.v1'
        assert set(run['compilers'])=={m.__name__ for m in etf_store.COMPILERS}
        for module in etf_store.COMPILERS:
            body=Path(module.__file__).read_bytes();digest=sha(body);expected={'key':etf_research.PREFIX+'compilers/'+digest+'.py','sha256':digest}
            assert run['compilers'][module.__name__]==expected and read(expected['key'])==body
        def checked(name):
            reference=run[name];body=read(reference['key']);assert reference['key']==etf_research.PREFIX+name+'s/'+sha(body)+'.json'
            assert reference['sha256']==sha(body) and reference['bytes']==len(body)
            return json.loads(body)
        inputs=checked('input');output=checked('output');assert output=={k:v for k,v in etf.items() if k!='replay'}
        assert etf_research.digest(output)==ref['output_sha256']==run['output_sha256'] and etf['generated_at']==run['generated_at']
        assert inputs['contract']=='etf-original-inputs.v1';original=inputs['originals'];at=run['generated_at']
        sc=etf_native.ssga_catalog(etf_native.original(original['ssga_catalog'],read,at,etf_native.SSGA_URL),SYMBOLS)
        ic=etf_native.ishares_catalog(etf_native.original(original['ishares_catalog'],read,at,etf_native.ISHARES_URL),('IVV',))
        assert set(sc)==set(SYMBOLS) and set(ic)=={'IVV'}
        ivv=etf_native.ishares_history(etf_native.original(original['ishares_IVV'],read,at,etf_native.DOWNLOAD.format(pid=ic['IVV']['portfolio_id'])),{**ic['IVV'],'currency':'USD'},at)
        dates=sorted({x['date'] for x in ivv['rows']});end=etf['aggregation_period']['end_date'];issuance={};matched_prices={}
        for ticker in SYMBOLS:
            row=etf['by_etf'][ticker];identity=sc[ticker]
            parsed=etf_native.ssga_history(etf_native.original(original['ssga_'+ticker],read,at,identity['history_url']),identity,at)
            native_rows=etf_native.flow_history(parsed['rows'],dates,[])
            href=row['history'];body=read(href['key']);history=json.loads(body)
            assert href['key']==etf_research.PREFIX+'histories/'+sha(body)+'.json' and href['sha256']==sha(body) and len(body)==href['bytes']
            assert history['rows']==native_rows and history['identity']==identity and history['ticker']==ticker and history['source']==original['ssga_'+ticker]
            common=[v for v in native_rows if v['date']<=end]
            windows={str(n)+'d':etf_native.window(common,dates,n) for n in (1,5,20)}
            assert windows==row['flow_windows']
            selected=windows['5d'];value=etf_native.dec(selected['value_decimal'],True);sensitivity=etf_native.dec(selected.get('precision_sensitivity_decimal'),True)
            issuance[ticker]={'issuer':identity['issuer'],'source_acquired_at':original['ssga_'+ticker]['acquired_at'],
                'history_observations':len(native_rows),'five_observation_window':selected,
                'absolute_estimate_exceeds_reported_display_sensitivity':abs(value)>sensitivity if value is not None and sensitivity is not None else None,
                'source_comparison':row['source_comparison'],'public_history_reconstructed':True}
            observation=packets['data/sector-rotation.json']['observations'][ticker]
            price_rows={v['date']:v for v in observation['history']}
            start=price_rows.get(selected.get('start_date'));finish=price_rows.get(selected.get('end_date'))
            matched_prices[ticker]={'start_date':selected.get('start_date'),'end_date':selected.get('end_date'),
                'exact_price_endpoints_available':bool(start and finish),'start':start,'end':finish}
        manifest={'contract':'sector-fusion-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'packets':refs,'packet_inventory':inventory,'source_artifacts':originals,'native_replays':native_replays,
            'issuer_replay':{'source_run':ref,'selected_histories':issuance,'unselected_funds':'Retained current packet; not replayed in this selected-source audit.'},
            'matched_issuer_period_prices':matched_prices,
            'scope':'Separate dated ETF price performance, issuer NAV-valued share changes and stock price-volume proxies. Other whole engine/page packets are retained contexts; no independent vote or allocation claim.'}
        reference=retain(s3,encoded(manifest));protected_keys={reference['key'],*(v['key'] for v in originals.values())}
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        r.kv(runtimes=runtimes,retained_manifest=reference,packet_inventory=inventory,native_replays=native_replays,issuer_replay=issuance,
            matched_issuer_period_prices=matched_prices,retained_artifacts=len(originals),protected_artifacts_checked=len(protected_keys),
            engine_invocations=0,provider_requests=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,
            originals_anonymously_denied=True,next_work='Replace both sector fusion models with one typed evidence matrix: same-period price/issuer comparisons, separately dated stock turnover, precision sensitivity and shared roots; retain all other contexts without score or allocation authority.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
