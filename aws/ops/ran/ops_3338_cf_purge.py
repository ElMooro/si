"""ops 3338 — Cloudflare edge is serving stale JS (GEN 3310, 4h TTL) to all
visitors, so the guard bump never reaches clients. Attempt a CF cache purge
with the token in run-ops env. If the token lacks Zone>Cache Purge (known
401), report that clearly so we pivot to the version-bump workaround."""
import os, json, urllib.request, urllib.error
from pathlib import Path
from ops_report import report

TOKEN=os.environ.get("CLOUDFLARE_API_TOKEN","")
def cf(method,url,body=None):
    data=json.dumps(body).encode() if body else None
    req=urllib.request.Request(url,data=data,method=method,headers={
        "Authorization":f"Bearer {TOKEN}","Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req,timeout=25) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read())
        except Exception: return e.code, {}
    except Exception as e:
        return None, {"err":str(e)}

with report("3338_cf_purge") as rep:
    if not TOKEN:
        rep.fail("no CLOUDFLARE_API_TOKEN in env"); rep.kv(RESULT="NO_TOKEN"); raise SystemExit(1)
    rep.kv(token_present=True, token_suffix=TOKEN[-4:])
    # 1. token verify + perms
    st,v=cf("GET","https://api.cloudflare.com/client/v4/user/tokens/verify")
    rep.kv(token_verify_status=st, token_ok=v.get("success"))
    # 2. get zone id
    st,z=cf("GET","https://api.cloudflare.com/client/v4/zones?name=justhodl.ai")
    zid=None
    if z.get("result"): zid=z["result"][0]["id"]
    rep.kv(zone_status=st, zone_id=zid[:8]+"…" if zid else None)
    if not zid:
        rep.fail("could not resolve zone"); rep.kv(RESULT="NO_ZONE"); raise SystemExit(1)
    # 3. purge specific stale JS files (targeted first — needs same perm)
    files=[f"https://justhodl.ai/{f}" for f in ("jh-nav-drawer.js","sw.js","jh-page-ai.js")]
    st,p=cf("POST",f"https://api.cloudflare.com/client/v4/zones/{zid}/purge_cache",{"files":files})
    rep.kv(purge_files_status=st, purge_files_ok=p.get("success"), errors=p.get("errors"))
    if not p.get("success"):
        # try purge_everything
        st2,p2=cf("POST",f"https://api.cloudflare.com/client/v4/zones/{zid}/purge_cache",{"purge_everything":True})
        rep.kv(purge_all_status=st2, purge_all_ok=p2.get("success"), errors2=p2.get("errors"))
        if p2.get("success"):
            rep.ok("purge_everything SUCCEEDED — stale JS cleared sitewide"); rep.kv(RESULT="PURGED_ALL")
        else:
            rep.fail("purge FAILED (token lacks Zone>Cache Purge) — pivot to version-bump workaround")
            rep.kv(RESULT="PURGE_DENIED")
            raise SystemExit(1)
    else:
        rep.ok("targeted purge SUCCEEDED — jh-nav-drawer.js + sw.js cleared")
        rep.kv(RESULT="PURGED_FILES")
