import json, urllib.request, time
out={}
try:
    h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/auth.js?v=6&t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
    out["css_matches_html"]=".jh-auth-card{" in h and ".jh-oauth-btn{" in h and ".jh-auth-primary{" in h
    out["modal_gated"]=".jh-auth-modal.open{display:flex}" in h
    out["google_oauth"]="signInWithOAuth" in h
except Exception as e: out["err"]=str(e)[:80]
b=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/brain.html?t=%d"%int(time.time()),headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
out["brain_authv6"]="auth.js?v=6" in b
out["brain_cookie_id"]="_setCookie" in b
open("aws/ops/reports/1381_a.json","w").write(json.dumps(out,indent=2,default=str));print("done")
