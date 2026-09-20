"""Inspect three bounded current option-chain pages and protect their exact originals.

Uses the existing managed Polygon credential only on the runner. No account, notification, schedule or engine changes.
"""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import hashlib,json,sys,urllib.request,urllib.error,urllib.parse
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from managed_secret import managed_secret

FN='justhodl-tail-risk';BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/tail-research/'
PACKETS=('data/tail-risk.json','data/tail-risk-history.json')


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

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):raise ValueError('Provider redirect refused')

def main():
    s3=boto3.client('s3',region_name='us-east-1');opener=urllib.request.build_opener(NoRedirect())
    with report('ops_5948_tail_option_originals') as r:
        secret=managed_secret(('POLYGON_KEY','POLYGON_API_KEY','POLY_KEY'),('/justhodl/polygon/api-key',))
        assert secret,'Existing provider configuration unavailable'
        started=datetime.now(timezone.utc);refs={};summaries={}
        first=(started.date()+timedelta(days=25)).isoformat();last=(started.date()+timedelta(days=75)).isoformat()
        for ticker in ('SPY','QQQ','IWM'):
            public_url='https://api.polygon.io/v3/snapshot/options/'+ticker+'?'+urllib.parse.urlencode({'expiration_date.gte':first,'expiration_date.lte':last,'sort':'ticker','order':'asc','limit':250})
            request=urllib.request.Request(public_url+'&apiKey='+urllib.parse.quote(secret,safe=''),headers={'User-Agent':'JustHodl-Tail-Research-Audit/2.0','Accept':'application/json','Accept-Encoding':'identity'})
            try:
                response=opener.open(request,timeout=20);status=response.status;raw=bounded(response,4*1024*1024)
            except urllib.error.HTTPError as exc:status=exc.code;raw=bounded(exc,4*1024*1024)
            except Exception:raise RuntimeError('Reviewed provider transport failed; no URL logged') from None
            acquired=datetime.now(timezone.utc).isoformat();ref=retain(s3,raw)
            refs[ticker]={'request_url':public_url,'acquired_at':acquired,'http_status':status,'original':ref}
            doc=json.loads(raw);rows=doc.get('results',[]) if isinstance(doc,dict) else [];assert isinstance(rows,list)
            selected=[]
            for row in rows[:2]:
                selected.append({k:{x:v for x,v in (row.get(k) or {}).items() if x in fields} for k,fields in {
                    'details':('contract_type','exercise_style','expiration_date','shares_per_contract','strike_price','ticker'),
                    'last_quote':('ask','bid','ask_size','bid_size','last_updated','timeframe'),
                    'last_trade':('sip_timestamp','price','timeframe'),
                    'underlying_asset':('ticker','price','last_updated','timeframe'),
                    'day':('last_updated','close','volume'),'greeks':('delta','gamma','vega','theta')}.items()})
                selected[-1]['implied_volatility']=row.get('implied_volatility')
            summaries[ticker]={'http_status':status,'rows':len(rows),'has_next_page':bool(doc.get('next_url')),
                'expiration_dates':sorted({x.get('details',{}).get('expiration_date','unknown') for x in rows}),
                'with_quote':sum(bool(x.get('last_quote')) for x in rows),'with_trade':sum(bool(x.get('last_trade')) for x in rows),
                'with_underlying_clock':sum(bool((x.get('underlying_asset') or {}).get('last_updated')) for x in rows),
                'with_iv':sum(x.get('implied_volatility') is not None for x in rows),'sample_contracts':selected,
                'next_page_query_keys':sorted({k for k,v in urllib.parse.parse_qsl(urllib.parse.urlsplit(doc.get('next_url') or '').query)})}
            r.kv(source_page={ticker:summaries[ticker]},retained_original=ref)
            assert status==200 and rows,'Reviewed snapshot unavailable'
        manifest={'contract':'tail-option-source-preflight.v1','started_at':started.isoformat(),'generated_at':datetime.now(timezone.utc).isoformat(),'pages':refs,'summary':summaries}
        ref=retain(s3,encoded(manifest))
        for item in (ref,*[x['original'] for x in refs.values()]):
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        r.kv(retained_manifest=ref,source_summary=summaries,provider_requests=3,engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,originals_anonymously_denied=True,
            limits='Only the first bounded page of each reviewed chain. No chain-completeness, IV observation-time, executable quote, density, forecast or investment claim.')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
