"""ops 3246 — why the 3245 markers aren't served: pages-deploy succeeded
inside the verify window, so either CF serves stale HTML (cf-cache-status
/age headers decide) or the bake stripped the snippet (served tail
decides). One page, full evidence."""
import time
import urllib.request

from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3246)"}

with report("3246_surface_diag") as rep:
    rep.heading("ops 3246 — served-HTML evidence for defcon.html")
    req = urllib.request.Request(
        f"https://justhodl.ai/defcon.html?t={int(time.time())}",
        headers={**UA, "Cache-Control": "no-cache"})
    r = urllib.request.urlopen(req, timeout=20)
    h = r.read().decode("utf-8", "replace")
    for k in ("cf-cache-status", "age", "last-modified", "etag",
              "x-github-request-id", "cf-ray"):
        rep.log(f"  {k}: {r.headers.get(k)}")
    rep.kv(bytes=len(h),
           has_marker=("jh-fusion-btp_bund_canary" in h),
           has_ops3245=("ops 3245" in h))
    tail = h[-500:].replace("\n", " ")
    rep.log("  tail: …" + tail[-360:])
    rep.kv(verdict="PASS")
