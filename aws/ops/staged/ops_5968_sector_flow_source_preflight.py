"""Audit the three sector-flow predecessors and six existing original tape sessions.

Protected retention and read-only runtime/source inspection. No provider or engine
invocation, notification, credential, account, portfolio or schedule changes.
"""
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal
import base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-market-internals/source')]
from ops_report import report
from release_package_evidence import shared_imports
import breadth_research_model as breadth
import breadth_research_store as breadth_store
BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/sector-flow-research/'
FUNCTIONS=('justhodl-money-flow-state','justhodl-sector-flow-state','justhodl-sector-capital-fusion')
PACKETS=('data/money-flow-state.json','data/sector-flow-state.json','data/sector-capital-fusion.json',
    'data/universe.json','data/13f-positions.json','data/market-internals.json','data/sector-rotation.json',
    'data/etf-true-flows.json','data/capital-flow-radar.json','data/dark-pool.json','data/liquidity-flow.json',
    'data/risk-regime.json','data/chart-patterns.json','data/accumulation-radar.json','flow-data.json')
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
    with report('ops_5968_sector_flow_source_preflight') as r:
        runtimes={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS}
        refs={};packets={};inventory={};originals={}
        def read(key):
            assert key in PACKETS or breadth_store.allowed(key),'Unreviewed source path'
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            originals[key]=retain(s3,raw)
            return raw
        for key in PACKETS:
            try:raw=read(key)
            except Exception as exc:
                if code(exc) not in ('404','NoSuchKey'):raise
                inventory[key]={'status':'missing'};continue
            p=json.loads(raw);assert isinstance(p,(dict,list)),'Expected source object or universe array'
            refs[key]=originals[key];packets[key]=p
            meta={'bytes':len(raw),'root_type':type(p).__name__}
            if isinstance(p,dict):
                meta.update(contract=p.get('contract'),generated_at=p.get('generated_at') or p.get('as_of'),
                    root_fields=sorted(p),calls_eligible=p.get('calls_eligible'),sizing_eligible=p.get('sizing_eligible'))
                for name in ('sectors','stocks_in','stocks_out','industries_in','industries_out','institutional_sector_tilt','most_bought','most_sold'):
                    rows=p.get(name)
                    if isinstance(rows,list):meta[name]={'count':len(rows),'first_fields':sorted(rows[0]) if rows and isinstance(rows[0],dict) else []}
            inventory[key]=meta
        universe=packets['data/universe.json']
        rows=universe if isinstance(universe,list) else universe.get('stocks') or universe.get('universe') or []
        symbols={};duplicates=[];missing_labels=[];invalid=[]
        for index,row in enumerate(rows):
            ticker=row.get('symbol') or row.get('ticker')
            if not isinstance(ticker,str) or not re.fullmatch(r'[A-Za-z0-9.\-]{1,20}',ticker):invalid.append(index);continue
            if ticker in symbols:duplicates.append(ticker)
            symbols[ticker]={'row_index':index,'sector':row.get('sector'),'industry':row.get('industry'),'name':row.get('name')}
            if not row.get('sector') or not row.get('industry'):missing_labels.append(ticker)
        universe_inventory={'rows':len(rows),'unique_symbols':len(symbols),'duplicates':duplicates,'invalid_row_indices':invalid,
            'missing_labels':missing_labels,'label_pairs':sorted({(str(v['sector']),str(v['industry'])) for v in symbols.values()}),
            'classification_basis':'Current retained configured universe; historical classification and security continuity are not established.'}
        packet=packets['data/market-internals.json'];ref=packet['replay'];run=json.loads(read(ref['manifest_key']))
        assert ref['manifest_key']==breadth_store.PREFIX+'runs/'+sha(encoded(run))+'.json'
        assert run['contract']=='breadth-native-replay.v1' and run['generated_at']==packet['generated_at']
        for module in breadth_store.COMPILERS:
            raw=Path(module.__file__).read_bytes();digest=sha(raw);compiler=run['compilers'][module.__name__]
            assert compiler=={'key':breadth_store.PREFIX+'compilers/'+digest+'.py','sha256':digest} and read(compiler['key'])==raw
        inputs=breadth_store.checked(run['input'],'inputs',read);output=breadth_store.checked(run['output'],'outputs',read)
        assert sha(encoded(output))==ref['output_sha256']==run['output_sha256']
        assert {k:v for k,v in packet.items() if k!='replay'}==output
        expected=breadth.sessions(inputs['generated_at'],6)
        assert expected==inputs['expected_days'][-6:]
        tape={};common=set(symbols)
        for day in expected:
            item=inputs['sources'][day]
            if item.get('error'):tape[day]={'status':'source_missing','error':item['error']};common=set();continue
            evidence=item['evidence'];raw=read(evidence['key'])
            assert evidence['key']==breadth_store.PRIVATE+sha(raw)+'.bin' and evidence['sha256']==sha(raw) and evidence['bytes']==len(raw)
            assert evidence['source_url']==breadth_store.source_url(day) and evidence['provider']=='massive'
            assert breadth.stamp(inputs['started_at'])<=breadth.stamp(item['acquired_at'])<=breadth.stamp(inputs['generated_at'])
            parsed=breadth.parse_session(raw,day,item['acquired_at']);doc=json.loads(raw,parse_float=Decimal)
            selected=set(parsed)&set(symbols);common&=selected
            valid_vwap=0;missing_vwap=[]
            for ticker in sorted(selected):
                original=doc['results'][parsed[ticker]['source_row_index']]
                try:
                    value=breadth.number(original.get('vw'));assert value>0
                    valid_vwap+=1
                except (AssertionError,ValueError,TypeError):missing_vwap.append(ticker)
            tape[day]={'status':'original_replayed','source_rows':len(parsed),'configured_universe_rows':len(selected),
                'vwap_available':valid_vwap,'vwap_missing':missing_vwap,'acquired_at':item['acquired_at'],'source':evidence}
        institutional=packets.get('data/13f-positions.json',{})
        buys={x.get('ticker') for x in institutional.get('most_bought',[]) if x.get('ticker')}
        sells={x.get('ticker') for x in institutional.get('most_sold',[]) if x.get('ticker')}
        inst={'most_bought_tickers':len(buys),'most_sold_tickers':len(sells),'overlapping_tickers':sorted(buys&sells),
            'basis':'Selected summaries retained; fund accession, amendment and quarter-level reconciliations are not revalidated by this audit.'}
        manifest={'contract':'sector-flow-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'packets':refs,'packet_inventory':inventory,'source_artifacts':originals,
            'universe_inventory':universe_inventory,'selected_sessions':tape,'common_universe_symbols':sorted(common),
            'institutional_inventory':inst,'breadth_run':ref,
            'scope':'Six exact original grouped sessions and retained universe are audited. Other packets are retained context; no independent signal or actual capital-flow claim.'}
        manifest_ref=retain(s3,encoded(manifest));protected_keys={manifest_ref['key'],*(v['key'] for v in originals.values())}
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        r.kv(runtimes=runtimes,retained_manifest=manifest_ref,packet_inventory=inventory,universe_inventory=universe_inventory,
            selected_sessions=tape,common_universe_symbols=len(common),institutional_inventory=inst,
            retained_artifacts=len(originals),protected_artifacts_checked=len(protected_keys),
            engine_invocations=0,provider_requests=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,
            originals_anonymously_denied=True,next_work='Native matched-session price-volume research with explicit proxy units and coverage; issuer share changes and 13F holdings remain separately typed evidence. No independent votes from shared price data.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
