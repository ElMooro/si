"""ops 3337 — the guard bump deployed (Pages success) but live JS still
GEN 3310. Inspect cache headers on jh-nav-drawer.js to find which layer is
stale (GH Pages max-age vs CF), and test a cache-busted URL to confirm the
new file IS deployed underneath."""
import urllib.request, re
from pathlib import Path
from ops_report import report
def fetch(url):
    r=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache","Pragma":"no-cache"}),timeout=20)
    js=r.read().decode()
    m=re.search(r'var GEN = "(\d+)"',js)
    return r.status, dict(r.getheaders()), (m.group(1) if m else None)
with report("3337_cache_headers") as rep:
    import time
    bare="https://justhodl.ai/jh-nav-drawer.js"
    bust=f"https://justhodl.ai/jh-nav-drawer.js?cb={int(time.time())}"
    for label,url in [("BARE",bare),("CACHE_BUSTED",bust)]:
        try:
            st,h,gen=fetch(url)
            rep.kv(**{label:{"gen":gen,"status":st,"cache_control":h.get("Cache-Control") or h.get("cache-control"),"cf_cache":h.get("Cf-Cache-Status") or h.get("cf-cache-status"),"age":h.get("Age") or h.get("age"),"etag":h.get("ETag") or h.get("etag")}})
        except Exception as e:
            rep.kv(**{label:f"err {e}"})
    rep.kv(RESULT="DONE")
