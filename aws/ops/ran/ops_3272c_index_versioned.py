"""ops 3272c — homepage drawer tag versioned. reskin skips index.html
by design ('native'); a dedicated pages.yml step now versions its
drawer tag with the same content hash. Verify the served homepage."""
import sys
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3272c)"}

with report("3272c_index_versioned") as rep:
    fails = []
    ok = False
    for i in range(26):
        try:
            h = urllib.request.urlopen(urllib.request.Request(
                f"https://justhodl.ai/?t={int(time.time())}",
                headers=UA), timeout=15).read().decode("utf-8",
                                                       "replace")
            ok = "jh-nav-drawer.js?v=" in h
        except Exception:
            pass
        if ok:
            rep.ok(f"homepage serves versioned drawer tag "
                   f"(~{(i + 1) * 15}s) — favorites/sidebar cache "
                   "chain fully closed")
            break
        time.sleep(15)
    if not ok:
        fails.append("still unversioned after rebake window")
    rep.kv(verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
