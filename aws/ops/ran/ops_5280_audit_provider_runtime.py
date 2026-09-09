# Verify explicit direct-FRED fallback without reporting raw observations.
# Retry after verified market CORS correction and completed NASDAQ deployment.
"""Runner-only source-bound public provider acceptance. Metadata only; no secrets or payload reports."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from release_package_evidence import check_packages
FUNCTIONS=('nasdaq-datalink-agent','alphavantage-technical-analysis','alphavantage-market-agent')

def probe(function,url,path):
    row={'path':path};start=time.monotonic()
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args):return None
    request=urllib.request.Request(url.rstrip('/')+path,headers={'Origin':'https://justhodl.ai','Accept':'application/json','User-Agent':'JustHodl-Audit/20260909'})
    try:
        try:response=urllib.request.build_opener(NoRedirect).open(request,timeout=130)
        except urllib.error.HTTPError as exc:response=exc
        with response:
            row['http_status']=response.status;row['cors_origins']=response.headers.get_all('Access-Control-Allow-Origin',[])
            row['cors_valid']=row['cors_origins'] in (['*'],['https://justhodl.ai'])
            raw=response.read(4_000_001);row['response_bytes']=len(raw)
            if len(raw)>4_000_000:raise ValueError()
            doc=json.loads(raw)
            if not isinstance(doc,dict):raise ValueError()
            row['traceback_present']=any(k in doc for k in ('trace','traceback','stack'))
            row['agent_matches']=doc.get('agent')==function
            row['provider_status']=doc.get('status') if doc.get('status') in ('HANDLER_READY','READY','PARTIAL','UNAVAILABLE') else None
            row['handler_error_present']=bool(doc.get('error'))
            if function=='nasdaq-datalink-agent' and path=='/':
                rows=[r for group in doc.get('categories',{}).values() if isinstance(group,dict) for r in group.values() if isinstance(r,dict)]
                row.update(fallback_datasets=doc.get('fallback_datasets'),datasets=len(rows),history_rows=sum(len(r.get('history',[])) for r in rows),available=doc.get('metrics_ok'),unavailable=doc.get('metrics_err'))
                row['schema_verified']=len(rows)==24 and doc.get('metrics_ok',-1)+doc.get('metrics_err',-1)==24
                row['provider_http_counts']={str(code):sum(r.get('http_status')==code for r in rows) for code in (200,401,403,404,429,500,502,503)}
            elif function=='alphavantage-technical-analysis' and path=='/':
                quote=doc.get('Global Quote');row['schema_verified']=isinstance(quote,dict) and bool(quote)
            elif function=='alphavantage-market-agent' and path=='/':
                coverage=doc.get('coverage',{});row['observed_quotes']=coverage.get('observed_quotes');row['expected_quotes']=coverage.get('expected_quotes')
                row['schema_verified']=coverage.get('expected_quotes')==17 and doc.get('execution_eligible') is False
            else:row['schema_verified']=row['agent_matches'] and doc.get('provider_data_verified') is False
            row['acceptance_verified']=response.status==200 and row['cors_valid'] and row['schema_verified'] and not row['traceback_present'] and not row['handler_error_present']
    except Exception as exc:row.update(acceptance_verified=False,error_type=type(exc).__name__)
    row['elapsed_s']=round(time.monotonic()-start,2);return row

def main():
    if os.environ.get('GITHUB_ACTIONS')!='true':raise RuntimeError('runner_only')
    import boto3
    client=boto3.client('lambda',region_name='us-east-1')
    report={'ops':5280,'read_only':True,'raw_provider_payloads_reported':0,'secret_values_reported':0,
            'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'functions':{}}
    for function in FUNCTIONS:
        row={};report['functions'][function]=row
        try:
            row['package']=check_packages(client,ROOT,[function])[0]
            if not row['package']['pass']:row['status']='SOURCE_PARITY_FAILED';continue
            config=client.get_function_url_config(FunctionName=function);url=config.get('FunctionUrl','')
            if not re.fullmatch(r'https://[a-z0-9]+\.lambda-url\.us-east-1\.on\.aws/',url):raise ValueError('unexpected_url')
            row.update(auth_type=config.get('AuthType'),cors=config.get('Cors',{}))
            if config.get('AuthType')!='NONE':row['status']='AUTHENTICATED_RUNTIME_FIXTURE_REQUIRED';continue
            row['requests']=[probe(function,url,path) for path in ('/health','/')]
            row['transport_ok']=all(r['acceptance_verified'] for r in row['requests'])
            row['status']='VERIFIED' if row['transport_ok'] else 'ACCEPTANCE_FAILED'
        except Exception as exc:row.update(status='PROBE_FAILED',error_type=type(exc).__name__)
    report['ok']=all(row.get('transport_ok') for row in report['functions'].values())
    (ROOT/'aws/ops/reports/5280_provider_runtime.json').write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    print(json.dumps({'ops':5280,'ok':report['ok'],'statuses':{name:row['status'] for name,row in report['functions'].items()}}))
    return 0 if report['ok'] else 1

if __name__=='__main__':raise SystemExit(main())
