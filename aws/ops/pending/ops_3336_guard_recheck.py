import urllib.request, re
from pathlib import Path
from ops_report import report
with report("3336_guard_recheck") as rep:
    try:
        js=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/jh-nav-drawer.js",headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache","Pragma":"no-cache"}),timeout=20).read().decode()
        m=re.search(r'var GEN = "(\d+)"',js)
        rep.kv(live_GEN=m.group(1) if m else None)
        rep.kv(RESULT="LIVE" if m and m.group(1)=="3335" else "ROLLING")
    except Exception as e:
        rep.fail(str(e)); rep.kv(RESULT="ERR")
