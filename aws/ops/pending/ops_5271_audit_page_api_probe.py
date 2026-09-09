"""Read-only FMP request/CORS probe. No raw response or credential is reported."""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import urllib.error

ROOT=Path(__file__).resolve().parents[3]
FUNCTION='fmp-fundamentals-agent'
HOST='nwjtcrf4xwkc6n5r6u3vw7ub6m0wgpiv.lambda-url.us-east-1.on.aws'


def probe(path):
    row={'path':path};started=time.monotonic()
    request=urllib.request.Request('https://'+HOST+path,headers={
        'User-Agent':'JustHodl-Release-Audit/20260909','Origin':'https://justhodl.ai','Accept':'application/json'})
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args):return None
    try:
        try:response=urllib.request.build_opener(NoRedirect).open(request,timeout=130)
        except urllib.error.HTTPError as exc:response=exc
        with response:
            row.update(status=response.status,content_type=response.headers.get('Content-Type'),
                cors_allow_origin=response.headers.get_all('Access-Control-Allow-Origin',[]))
            raw=response.read(4_000_001);row['bytes']=len(raw)
            if len(raw)<=4_000_000:
                try:
                    doc=json.loads(raw)
                    if isinstance(doc,dict):
                        row.update(agent_matches=doc.get('agent')==FUNCTION,
                            handler_error_present=bool(doc.get('error')),trace_field_present='trace' in doc,
                            quote_count=len(doc.get('watchlist_quotes',{})) if isinstance(doc.get('watchlist_quotes'),dict) else None,
                            sector_count=len(doc.get('sector_performance',[])) if isinstance(doc.get('sector_performance'),list) else None)
                except (ValueError,TypeError):row['invalid_json']=True
    except Exception as exc:row['error_type']=type(exc).__name__
    row['elapsed_s']=round(time.monotonic()-started,2);return row


def main():
    if os.environ.get('GITHUB_ACTIONS')!='true':raise RuntimeError('runner_only')
    import boto3
    client=boto3.client('lambda',region_name='us-east-1')
    config=client.get_function_url_config(FunctionName=FUNCTION)
    if config.get('FunctionUrl')!='https://'+HOST+'/':raise RuntimeError('unexpected_url_identity')
    result={'operation':5271,'read_only':True,'function':FUNCTION,'auth_type':config.get('AuthType'),
        'cors':config.get('Cors',{}),'raw_response_bodies_reported':0,
        'source_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()}
    result['requests']=[probe('/health'),probe('/')]
    result['ok']=all(r.get('status')==200 and r.get('agent_matches') and not r.get('handler_error_present') for r in result['requests'])
    destination=ROOT/'aws/ops/reports/5271_audit_page_api_probe.json';destination.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result));return 0


if __name__=='__main__':raise SystemExit(main())
