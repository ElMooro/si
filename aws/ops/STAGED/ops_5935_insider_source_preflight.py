"""Inspect the existing insider aggregate and three bounded provider pages.

All whole packets/responses are retained privately. No engine invocation,
account input, paid AI, notification, schedule or permission change.
"""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import base64,hashlib,io,json,subprocess,sys,time,urllib.request,urllib.error,urllib.parse,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import shared_imports
FN='justhodl-insider-aggregate';BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/insider-research/'
PACKETS=('data/insider-aggregate.json','data/insider-aggregate-history.json','data/insider-trades.json')
ENDPOINT='https://financialmodelingprep.com/stable/insider-trading/latest'

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

def page_summary(rows):
    docs=[r for r in rows if isinstance(r,dict)]
    dates={}
    for k in ('filingDate','transactionDate'):
        vals=sorted(str(r[k]) for r in docs if r.get(k))
        dates[k]={'present':len(vals),'min':vals[0] if vals else None,'max':vals[-1] if vals else None}
    return {'rows':len(rows),'object_rows':len(docs),'fields':dict(sorted(Counter(k for r in docs for k in r).items())),
        'field_types':{k:dict(Counter(type(r[k]).__name__ for r in docs if k in r)) for k in sorted({k for r in docs for k in r})},
        'transaction_codes':dict(Counter(str(r.get('transactionType')) for r in docs)),
        'security_types':dict(Counter(str(r.get('securityName')) for r in docs).most_common(12)),
        'dates':dates,'rows_with_filing_link':sum(bool(r.get('link')) for r in docs),
        'known_currency_rows':sum(bool(r.get('currency')) for r in docs),
        'plan_field_names':sorted({k for r in docs for k in r if 'plan' in k.lower() or '10b5' in k.lower()}),
        'quantity_missing_but_owned_present':sum(r.get('securitiesTransacted') is None and r.get('securitiesOwned') is not None for r in docs)}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5935_insider_source_preflight') as r:
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
                schedules.append({'name':name,'state':rule['State'],'schedule':rule.get('ScheduleExpression'),
                    'targets':sum(t.get('Arn')==cfg['FunctionArn'] for t in targets)})
        r.kv(runtime={'code_sha256':cfg['CodeSha256'],'source_files_checked':len(expected),
            'handler_bytes':len((source/'lambda_function.py').read_bytes()),'timeout':cfg['Timeout'],'memory_mb':cfg['MemorySize'],
            'receipt':receipt_status,'schedules':schedules},engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        refs={};metadata={}
        for key in PACKETS:
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']);p=json.loads(raw)
            refs[key]={**retain(s3,raw),'acquired_at':datetime.now(timezone.utc).isoformat()}
            metadata[key]={'contract':p.get('contract'),'generated_at':p.get('generated_at'),'updated_at':p.get('updated_at'),
                'bytes':len(raw),'top_level_keys':sorted(p),'regime':p.get('regime'),'coverage_days':p.get('data_coverage_days'),
                'transactions':len(p.get('transactions') or []),'history_rows':len(p.get('snapshots') or []),'quality':p.get('quality')}
        # Reuse only this producer's already configured provider credential in memory.
        credential=(cfg.get('Environment',{}).get('Variables',{}).get('FMP_KEY') or '')
        assert credential,'Existing insider provider credential unavailable'
        pages=[]
        for page in range(3):
            query={'page':str(page),'limit':'1000'};safe_url=ENDPOINT+'?'+urllib.parse.urlencode(query)
            req=urllib.request.Request(safe_url+'&'+urllib.parse.urlencode({'apikey':credential}),headers={'User-Agent':'JustHodl-Insider-Source-Audit/1.0'})
            acquired=datetime.now(timezone.utc).isoformat()
            try:
                with urllib.request.urlopen(req,timeout=30) as response:raw=bounded(response,8*1024*1024)
            except Exception as exc:
                r.kv(provider_failure={'page':page,'kind':type(exc).__name__,'status':getattr(exc,'code',None)})
                raise RuntimeError('Bounded provider request failed; credential-bearing request omitted') from None
            ref=retain(s3,raw)
            try:rows=json.loads(raw)
            except Exception:raise RuntimeError('Provider response is not JSON; original protected') from None
            assert isinstance(rows,list),'Provider response is not a transaction array; original protected'
            pages.append({'request_url':safe_url,'acquired_at':acquired,'original':ref,'summary':page_summary(rows)})
            if not rows:break
            time.sleep(.3)
        manifest={'contract':'insider-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtime_code_sha256':cfg['CodeSha256'],'packets':refs,'metadata':metadata,'provider_pages':pages,
            'coverage':'Three bounded pages only; no whole-market, complete-window, amendment, plan or currency qualification.'}
        ref=retain(s3,encoded(manifest))
        for item in (refs[PACKETS[0]],ref,*[p['original'] for p in pages]):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        r.kv(retained_manifest=ref,packet_inventory=metadata,provider_pages=pages,provider_requests=len(pages),originals_anonymously_denied=True,
            next_work='Reconstruct finite reported sample with exact classification, date basis, original row identities and uncertainty; no unsupported market-timing or SMART_MONEY authority.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
