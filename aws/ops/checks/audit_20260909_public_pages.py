"""Read public route HTML only; retain no bodies, credentials or application data."""
import json
import re
import subprocess
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from html.parser import HTMLParser
from pathlib import Path

class Markers(HTMLParser):
    def __init__(self):super().__init__();self.build=None;self.inspector=False
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='meta' and attrs.get('name')=='jh-build-commit':
            candidate=attrs.get('content','')
            if re.fullmatch('[a-f0-9]{40}',candidate):self.build=candidate
        if tag=='script' and re.fullmatch(r'(?:/)?jh-data-inspector\.js(?:\?[^#]*)?',attrs.get('src','')):self.inspector=True

class OwnedRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        from urllib.parse import urlsplit
        url=urlsplit(newurl)
        if url.scheme!='https' or url.hostname!='justhodl.ai':raise ValueError('redirect_outside_reviewed_host')
        return super().redirect_request(req,fp,code,msg,headers,newurl)

def inspect(route,opener):
    if not re.fullmatch(r'[A-Za-z0-9_./-]+\.html',route) or '..' in route.split('/') or route.startswith('/'):
        raise ValueError('invalid_public_route')
    row={'route':route,'status':'UNAVAILABLE','http_status':None,'build_sha':None,'inspector_present':False}
    try:
        request=urllib.request.Request('https://justhodl.ai/'+route,headers={'User-Agent':'JustHodl-Release-Audit/1.0','Accept-Encoding':'identity'})
        with opener.open(request,timeout=25) as response:
            row['http_status']=response.status;body=response.read(4_000_001);row['bytes_read']=len(body)
            if len(body)>4_000_000:row['status']='RESPONSE_LIMIT_EXCEEDED';return row
            if 'text/html' not in response.headers.get('Content-Type','').lower():row['status']='NOT_HTML';return row
        markers=Markers();markers.feed(body.decode('utf-8','replace'))
        row.update(build_sha=markers.build,inspector_present=markers.inspector)
        row['status']='HTML_VERIFIED' if row['http_status']==200 and markers.build and markers.inspector else 'MISSING_BUILD_OR_INSPECTOR'
    except urllib.error.HTTPError as exc:row.update(http_status=exc.code,error_type='HTTPError')
    except Exception as exc:row['error_type']=type(exc).__name__
    return row

def same_product_source(root,expected,observed):
    if not re.fullmatch('[a-f0-9]{40}',str(observed)):return False
    if expected==observed:return True
    def git(*args):return subprocess.run(['git',*args],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
    if git('cat-file','-e',observed+'^{commit}').returncode:
        if git('fetch','--no-tags','--depth=1','origin',observed).returncode:return False
    if git('merge-base','--is-ancestor',expected,observed).returncode:return False
    # Later ops receipts/cadence snapshots must not masquerade as new product code.
    changed=git('diff','--name-only',expected,observed,'--','.',':!aws/ops/**',':!docs/**',':!STATE.md',':!engine-manifest.json',':!config/schedule-manifest.json')
    return changed.returncode==0 and not changed.stdout.strip()

def sweep(root,expected):
    contracts=json.loads((root/'config/page-data-contracts.json').read_text())
    routes=sorted(contracts['pages'])
    def run(route):return inspect(route,urllib.request.build_opener(OwnedRedirect()))
    with ThreadPoolExecutor(max_workers=6) as pool:rows=list(pool.map(run,routes))
    builds={row['build_sha'] for row in rows if row['build_sha']}
    source_match={build:same_product_source(root,expected,build) for build in builds}
    for row in rows:
        row['product_source_matches_release']=source_match.get(row['build_sha'],False)
        if row['status']=='HTML_VERIFIED' and not row['product_source_matches_release']:row['status']='OTHER_BUILD_SOURCE'
    failures=[row['route'] for row in rows if row['status']!='HTML_VERIFIED']
    return {'schema_version':'public-page-html-audit.v1','expected_product_release':expected,'observed_at':datetime.now(timezone.utc).isoformat(),
            'ok':not failures,'routes_checked':len(rows),'verified_routes':len(rows)-len(failures),'failed_routes':failures,
            'source_equivalent_builds':source_match,'scope':'PUBLIC_HTML_HTTP_BUILD_AND_INSPECTOR_ONLY',
            'runtime_api_payloads_requested':0,'browser_interaction_claim':False,'page_bodies_reported':0,'rows':rows}
