"""ops 3339 — confirm the versioned drawer URL serves GEN 3335 (CF MISS ->
fresh) and benzinga.html now references ?v=3335, so clients self-heal."""
import urllib.request, re, time
from pathlib import Path
from ops_report import report
def get(url):
    r=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"}),timeout=20)
    return r.status, dict(r.getheaders()), r.read().decode()
with report("3339_versioned_verify") as rep:
    # versioned JS
    try:
        st,h,js=get("https://justhodl.ai/jh-nav-drawer.js?v=3335")
        m=re.search(r'var GEN = "(\d+)"',js)
        rep.kv(versioned_js={"gen":m.group(1) if m else None,"status":st,"cf_cache":h.get("Cf-Cache-Status") or h.get("cf-cache-status")})
    except Exception as e:
        rep.kv(versioned_js=f"err {e}")
    # benzinga.html references the versioned URL?
    try:
        st,h,b=get("https://justhodl.ai/benzinga.html")
        rep.kv(benzinga_versioned=("jh-nav-drawer.js?v=3335" in b),
               benzinga_has_funcurl=("qgmut34alss5" in b),
               benzinga_cf=h.get("Cf-Cache-Status") or h.get("cf-cache-status"))
    except Exception as e:
        rep.kv(benzinga=f"err {e}")
    rep.kv(RESULT="DONE")
