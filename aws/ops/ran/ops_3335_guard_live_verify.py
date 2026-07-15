"""ops 3335 — confirm the bumped JH_FRESH_GUARD GEN is live on justhodl.ai
so stale clients (benzinga.html) will self-heal on next visit."""
import urllib.request, re
from pathlib import Path
from ops_report import report
with report("3335_guard_live_verify") as rep:
    try:
        req=urllib.request.Request("https://justhodl.ai/jh-nav-drawer.js",headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"})
        js=urllib.request.urlopen(req,timeout=20).read().decode()
        m=re.search(r'var GEN = "(\d+)"',js)
        rep.kv(live_GEN=m.group(1) if m else None, expected="3335")
        # also confirm benzinga.html served + its function URL present
        b=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/benzinga.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode()
        rep.kv(benzinga_has_funcurl=("qgmut34alss5" in b))
        if m and m.group(1)=="3335":
            rep.ok("GEN 3335 LIVE — clients self-heal on next visit"); rep.kv(RESULT="LIVE")
        else:
            rep.warn("GEN not yet 3335 (pages deploy still rolling)"); rep.kv(RESULT="ROLLING")
    except Exception as e:
        rep.fail(f"check err: {e}"); rep.kv(RESULT="ERR")
