"""Retain ETF holdings sources before replacing inferred stock-flow claims.

Read existing public research and six bounded current provider snapshots, plus
two prior snapshots. Existing subscription only; no producer invocations,
private-account reads, paid AI, signals, notifications or cadence mutations.
"""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from collections import Counter
from decimal import Decimal,localcontext
import base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,urllib.parse,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import shared_imports
BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/etf-constituent-research/'
FUNCTIONS=('justhodl-etf-constituents','justhodl-flow-lookthrough')
PACKETS=('etf-flows/constituent-pressure.json','etf-flows/stock-exposure-lookup.json',
    'data/flow-lookthrough.json','data/impact/etf-holdings-index.json',
    'data/etf-holdings-complete.json','data/provider-fund-flow-research.json','data/etf-true-flows.json')
PROBE=('SPY','VOO','TLT','BND','SOXL','EFA')
ENDPOINT='https://api.polygon.io/etf-global/v1/constituents'
AUDIT=('d00064f8e28d27ba8332310b45e990ac261e246695a85d8f38f1601e352d2e0a',18101)
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

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise ValueError('Provider redirect not permitted')


def checked_url(url):
    u=urllib.parse.urlsplit(url)
    assert (u.scheme=='https' and u.hostname in ('api.polygon.io','api.massive.com')
        and u.path=='/etf-global/v1/constituents' and not u.username and not u.password
        and u.port in (None,443) and not u.fragment),'Unreviewed provider destination'
    query=urllib.parse.parse_qsl(u.query,keep_blank_values=True)
    allowed={'composite_ticker','processed_date','processed_date.lte','sort','limit','cursor'}
    assert query and len({k for k,v in query})==len(query) and all(k in allowed and v for k,v in query),'Unreviewed provider query'
    return url


def request_original(s3,credential,url,budget):
    checked_url(url);budget['requests']+=1
    assert budget['requests']<=120,'Reviewed source request bound'
    req=urllib.request.Request(url,headers={'User-Agent':'JustHodl-HoldingsSourceAudit/1.0','Authorization':'Bearer '+credential})
    try:
        response=urllib.request.build_opener(NoRedirect).open(req,timeout=25)
        raw=bounded(response,8*1024*1024)
    except urllib.error.HTTPError as exc:
        status=exc.code;exc.close();return None,{'status':'provider_http_error','http_status':status}
    except Exception:return None,{'status':'provider_request_failed'}
    assert credential.encode() not in raw,'Credential reflection rejected'
    budget['bytes']+=len(raw);assert budget['bytes']<=128*1024*1024,'Reviewed total original-response bound'
    ref=retain(s3,raw)
    try:
        doc=json.loads(raw,parse_float=Decimal)
        assert isinstance(doc,dict) and doc.get('status')=='OK' and isinstance(doc.get('results'),list)
    except Exception:return None,{'status':'provider_shape_rejected','original':ref}
    return doc,{'status':'retained','url':url,'original':ref,'acquired_at':datetime.now(timezone.utc).isoformat()}


def snapshot_probe(s3,credential,ticker,cutoff,budget,selection=None):
    query={'composite_ticker':ticker,'processed_date.lte':cutoff,'sort':'processed_date.desc','limit':1}
    if selection is None:
        peek,selection=request_original(s3,credential,ENDPOINT+'?'+urllib.parse.urlencode(query),budget)
    else:
        assert selection['url']==ENDPOINT+'?'+urllib.parse.urlencode(query)
        ref=selection['original'];raw=bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
        assert ref['key']==PREFIX+ref['sha256']+'.bin' and len(raw)==ref['bytes'] and sha(raw)==ref['sha256']
        peek=json.loads(raw,parse_float=Decimal)
        assert isinstance(peek,dict) and peek.get('status')=='OK' and isinstance(peek.get('results'),list)

    out={'ticker':ticker,'cutoff':cutoff,'selection':selection,'pages':[],'status':'unavailable'}
    if not peek or not peek['results']:return out
    row=peek['results'][0];processed=row.get('processed_date')
    assert row.get('composite_ticker')==ticker and isinstance(processed,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',processed) and processed<=cutoff
    query={'composite_ticker':ticker,'processed_date':processed,'limit':5000}
    url=ENDPOINT+'?'+urllib.parse.urlencode(query);seen=set();rows=[]
    for _ in range(20):
        assert url not in seen,'Repeated source page';seen.add(url)
        doc,ref=request_original(s3,credential,url,budget)
        if not doc:out.update(status='incomplete',failure=ref);return out
        for row in doc['results']:
            assert isinstance(row,dict) and row.get('composite_ticker')==ticker and row.get('processed_date')==processed
        rows.extend(doc['results']);out['pages'].append(ref)
        if not doc.get('next_url'):break
        url=checked_url(doc['next_url'])
    else:out.update(status='pagination_bound');return out
    identities=Counter(tuple(str(row.get(k) or '') for k in ('effective_date','figi','isin','us_code','sedol','constituent_ticker','constituent_rank')) for row in rows)
    finite_weights=[Decimal(str(row['weight'])) for row in rows if isinstance(row.get('weight'),(int,Decimal)) and not isinstance(row.get('weight'),bool)]
    assert all(value.is_finite() for value in finite_weights)
    sample=sorted(rows,key=lambda row:Decimal(str(row.get('weight') or 0)),reverse=True)[:4]
    def public_row(row):return {k:str(v) if isinstance(v,Decimal) else v for k,v in row.items()}
    out.update(status='complete_returned_snapshot',processed_date=processed,acquired_at=datetime.now(timezone.utc).isoformat(),
        rows=len(rows),fields=dict(Counter(k for row in rows for k in row)),
        effective_dates=dict(Counter(row.get('effective_date') or '<missing>' for row in rows)),
        asset_classes=dict(Counter(row.get('asset_class') or '<missing>' for row in rows)),
        security_types=dict(Counter(row.get('security_type') or '<missing>' for row in rows)),
        traded_currencies=dict(Counter(row.get('currency_traded') or '<missing>' for row in rows)),
        missing_ticker=sum(not row.get('constituent_ticker') for row in rows),
        missing_identifiers=sum(not any(row.get(k) for k in ('isin','figi','us_code','sedol')) for row in rows),
        duplicate_identity_and_rank_rows=sum(n-1 for n in identities.values() if n>1),
        raw_weight_sum=str(sum(finite_weights,Decimal(0))),weight_unit_certified=False,market_value_currency_certified=False,
        top_raw_rows=[public_row(row) for row in sample])
    return out


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5977_etf_constituent_snapshot_recovery') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_etf_constituent_snapshot_recovery.py')],cwd=ROOT,check=True)
        runtimes={fn:runtime(lam,s3,events,scheduler,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes)
        refs={};inventory={}
        keys=[*PACKETS,*(f'etf-flows/constituents/{ticker}.json' for ticker in PROBE),*(f'etf-constituents-v2/{ticker}.json' for ticker in PROBE)]
        for key in keys:
            try:raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            except Exception as exc:
                if code(exc) not in ('NoSuchKey','404'):raise
                inventory[key]={'status':'missing'};continue
            refs[key]=retain(s3,raw);packet=json.loads(raw)
            inventory[key]={'bytes':len(raw),'root_fields':list(packet)[:60],
                'generated_at':packet.get('generated_at'),'contract':packet.get('contract'),
                'processed_date':packet.get('processed_date'),'quality':packet.get('quality'),
                'funds':len(packet.get('funds',{})),'by_etf':len(packet.get('by_etf',{})),
                'per_stock_exposure':len(packet.get('per_stock_exposure',{}))}
        cfg=lam.get_function_configuration(FunctionName=FUNCTIONS[0]);env=cfg.get('Environment',{}).get('Variables',{})
        credential=env.get('POLYGON_KEY') or env.get('POLYGON_API_KEY');assert credential,'Existing provider configuration unavailable'
        del cfg,env
        audit_raw=bounded(s3.get_object(Bucket=BUCKET,Key=PREFIX+AUDIT[0]+'.bin')['Body'])
        assert len(audit_raw)==AUDIT[1] and sha(audit_raw)==AUDIT[0]
        audit=json.loads(audit_raw);assert audit['contract']=='etf-constituent-source-preflight.v1'
        budget={'requests':0,'bytes':0}
        probes={ticker:snapshot_probe(s3,credential,ticker,p['cutoff'],budget,p['selection'])
            for ticker,p in audit['current_probes'].items()}
        prior={ticker:snapshot_probe(s3,credential,ticker,p['cutoff'],budget,p['selection'])
            for ticker,p in audit['prior_probes'].items()};del credential

        manifest={'contract':'etf-constituent-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'packets':refs,'packet_inventory':inventory,'current_probes':probes,'prior_probes':prior,
            'provider_requests':budget['requests'],'original_provider_bytes':budget['bytes'],
            'recovered_selection_manifest':{'sha256':AUDIT[0],'bytes':AUDIT[1]},
            'scope':'Whole source snapshots and current contexts. Weight scale, valuation currency, historical availability and corporate actions require separate qualification.'}
        manifest_ref=retain(s3,encoded(manifest));protected_keys={manifest_ref['key'],PREFIX+AUDIT[0]+'.bin',*(ref['key'] for ref in refs.values())}
        for probe in [*probes.values(),*prior.values()]:
            for ref in [probe['selection'],*probe['pages'],probe.get('failure',{})]:
                if ref.get('original'):protected_keys.add(ref['original']['key'])
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        r.kv(retained_manifest=manifest_ref,packet_inventory=inventory,current_probes=probes,prior_probes=prior,
            protected_artifacts_checked=len(protected_keys),originals_anonymously_denied=True,
            provider_requests=budget['requests'],original_provider_bytes=budget['bytes'],producer_invocations=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,signals_emitted=0,portfolio_writes=0,schedule_changes=0)
        assert all(p['status']=='complete_returned_snapshot' for p in [*probes.values(),*prior.values()]),'Not every reviewed source snapshot completed; inspect retained report'


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
