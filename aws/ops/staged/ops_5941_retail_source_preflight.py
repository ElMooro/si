"""Inspect retail attention sources and preserve legacy research without invocation.

All whole packets/responses are retained privately. No engine invocation,
account input, paid AI, notification, schedule or permission change.
"""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,urllib.parse,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import shared_imports
FN='justhodl-retail-sentiment';BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/retail-research/'
PACKETS=('data/retail-sentiment.json','data/retail-sentiment-history.json','data/retail-attention-history.json',
 'data/news-velocity.json','data/ticker-trends.json','data/options-flow-scanner.json','data/finviz-news.json',
 'data/short-interest.json','data/13f-positions.json','data/estimate-revisions-latest.json','data/rotation-chains.json','data/finviz-short.json')

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

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5941_retail_source_preflight') as r:
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
            try:raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            except Exception as exc:
                if code(exc) not in ('NoSuchKey','404'):raise
                metadata[key]={'status':'missing'};continue
            p=json.loads(raw);assert isinstance(p,(dict,list))
            refs[key]={**retain(s3,raw),'acquired_at':datetime.now(timezone.utc).isoformat()}
            metadata[key]={'bytes':len(raw),'shape':type(p).__name__}
            if isinstance(p,dict):
                metadata[key].update({k:p.get(k) for k in ('contract','generated_at','updated_at','version','n_all_stocks','n_wsb','n_stocks','n_investing','n_with_stwt_data','n_with_price','market_regime')})
                metadata[key].update(top_level_keys=sorted(p),history_rows=len(p.get('snapshots') or []),ticker_histories=len(p.get('by_ticker') or {}))
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs):return None
        opener=urllib.request.build_opener(NoRedirect());pages=[];provider_requests=0
        def capture(url,label):
            nonlocal provider_requests
            assert re.fullmatch(r'https://(?:apewisdom\.io/api/v1\.0/filter/(?:all-stocks|wallstreetbets|stocks|investing)/page/[12]|api\.stocktwits\.com/api/2/(?:trending/symbols|streams/symbol/[A-Z][A-Z0-9.-]{0,12})\.json)',url)
            acquired=datetime.now(timezone.utc).isoformat();status=None;raw=None;provider_requests+=1
            req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 JustHodl-Retail-Research/1.0','Accept':'application/json'})
            try:
                with opener.open(req,timeout=25) as response:status=response.status;raw=bounded(response,8*1024*1024)
            except urllib.error.HTTPError as exc:status=exc.code;raw=bounded(exc,8*1024*1024)
            except (TimeoutError,urllib.error.URLError):
                pages.append({'label':label,'request_url':url,'acquired_at':acquired,'http_status':None,'status':'transport_unavailable'});return None
            item={'label':label,'request_url':url,'acquired_at':acquired,'http_status':status,'original':retain(s3,raw),'status':'received'}
            try:p=json.loads(raw)
            except (ValueError,UnicodeDecodeError):p=None
            if isinstance(p,dict):
                item['schema']={'top_level_keys':sorted(p)}
                for field in ('results','messages','symbols'):
                    rows=p.get(field)
                    if not isinstance(rows,list):continue
                    docs=[r for r in rows if isinstance(r,dict)]
                    item['schema'][field]={'rows':len(rows),'fields':dict(sorted(Counter(k for r in docs for k in r).items())),
                        'field_types':{k:dict(Counter(type(r[k]).__name__ for r in docs if k in r)) for k in sorted({k for r in docs for k in r})}}
                    if field=='results':
                        item['schema'][field].update(positive_comparison_rows=sum(type(r.get('mentions_24h_ago')) is int and r['mentions_24h_ago']>0 for r in docs),
                            zero_comparison_rows=sum(type(r.get('mentions_24h_ago')) is int and r['mentions_24h_ago']==0 for r in docs),
                            null_comparison_rows=sum(r.get('mentions_24h_ago') is None for r in docs))
                item['pagination']={k:p[k] for k in ('count','pages','current_page') if type(p.get(k)) is int}
            else:item['status']='non_json_response'
            pages.append(item);return p if status==200 else None
        all_stocks=None
        for category,n in (('all-stocks',2),('wallstreetbets',1),('stocks',1),('investing',1)):
            for page in range(1,n+1):
                p=capture('https://apewisdom.io/api/v1.0/filter/'+category+'/page/'+str(page),category+'-'+str(page))
                if category=='all-stocks' and page==1:all_stocks=p
        capture('https://api.stocktwits.com/api/2/trending/symbols.json','stocktwits-trending')
        candidates=[r.get('ticker') for r in (all_stocks or {}).get('results',[]) if isinstance(r,dict) and isinstance(r.get('ticker'),str) and re.fullmatch('[A-Z][A-Z0-9.-]{0,12}',r['ticker'])]
        if candidates:capture('https://api.stocktwits.com/api/2/streams/symbol/'+candidates[0]+'.json','stocktwits-one-public-symbol')
        manifest={'contract':'retail-source-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtime_code_sha256':cfg['CodeSha256'],'packets':refs,'metadata':metadata,'provider_pages':pages,
            'coverage':'Existing public source endpoints only; five bounded ApeWisdom pages, one trending endpoint and at most one symbol stream. No source access controls bypassed; no account, watchlist, alert state, DDB or signal-consumer input.'}
        ref=retain(s3,encoded(manifest))
        for item in (ref,refs['data/retail-sentiment.json'],*[p['original'] for p in pages if p.get('original')]):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        r.kv(retained_manifest=ref,packet_inventory=metadata,provider_pages=pages,provider_requests=provider_requests,originals_anonymously_denied=True,
            next_work='Reproducible vendor attention samples, explicit zero/missing baselines, population and clock limits; no inferred retail flow, crowding trade, account flags or automatic notification.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
