"""ops 4368 — edge truth: fetch https://justhodl.ai/crypto/ and the S3 data URL
exactly as a browser would; hash-compare against the repo file; capture
cf-cache-status + CORS headers; grep for v5 markers. Ends the guessing."""
import json, os, hashlib, urllib.request
R={"ops":4368}
def get(url, hdrs=None):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache",**(hdrs or {})})
    with urllib.request.urlopen(req,timeout=20) as r:
        body=r.read()
        return body, dict(r.headers)
try:
    body,h=get("https://justhodl.ai/crypto/")
    R["page"]={"bytes":len(body),
               "sha12":hashlib.sha256(body).hexdigest()[:12],
               "cf_cache_status":h.get("Cf-Cache-Status") or h.get("CF-Cache-Status"),
               "age":h.get("Age"),"cache_control":h.get("Cache-Control"),
               "last_modified":h.get("Last-Modified"),
               "markers":{m:(m.encode() in body) for m in
                          ("pane-cq","data-tab=\"altseason\"","paneFleetAll","COVERAGE RATCHET",
                           "autoWalk","_iso(paneCQ","cqCatalog")}}
except Exception as e:
    R["page_err"]=str(e)[:200]
try:
    repo=open("crypto/index.html","rb").read()
    R["repo"]={"bytes":len(repo),"sha12":hashlib.sha256(repo).hexdigest()[:12]}
except Exception as e:
    R["repo_err"]=str(e)[:100]
try:
    body,h=get("https://justhodl-dashboard-live.s3.amazonaws.com/crypto-intel.json",
               {"Origin":"https://justhodl.ai"})
    d=json.loads(body)
    R["data"]={"bytes":len(body),"version":d.get("version"),
               "generated_at":d.get("generated_at"),
               "cors_allow_origin":h.get("Access-Control-Allow-Origin"),
               "content_type":h.get("Content-Type")}
except Exception as e:
    R["data_err"]=str(e)[:200]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4368_edge_truth.json","w"),indent=1)
pg=R.get("page",{})
open("aws/ops/reports/4368_edge_truth.md","w").write(
    "# ops 4368 — edge truth\n"
    f"- edge page: {pg.get('bytes')}B sha={pg.get('sha12')} cf={pg.get('cf_cache_status')} age={pg.get('age')} cc={pg.get('cache_control')}\n"
    f"- repo page: {json.dumps(R.get('repo'))}\n"
    f"- markers: {json.dumps(pg.get('markers'))}\n"
    f"- data: {json.dumps(R.get('data') or R.get('data_err'))}\n"
    f"- page_err: {R.get('page_err')}\n")
print(json.dumps(R,indent=1))
