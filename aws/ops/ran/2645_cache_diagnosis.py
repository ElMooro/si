"""ops 2645 — confirm origin is fresh + inspect actual cache headers being served."""
import urllib.request, time
def get_with_headers(u):
    req = urllib.request.Request(u, headers={"User-Agent":"M","Cache-Control":"no-cache","Pragma":"no-cache"})
    r = urllib.request.urlopen(req, timeout=25)
    return r.read().decode("utf-8","ignore"), dict(r.getheaders())

print("=== live origin content (cache-busted) ===")
html, hdrs = get_with_headers(f"https://justhodl.ai/index.html?cb={int(time.time())}")
print("index.html has drawer tag:", 'src="/jh-nav-drawer.js"' in html)
print("bytes:", len(html))

print("\n=== actual response headers on a NORMAL request (no cache-bust, mimics a real browser visit) ===")
html2, hdrs2 = get_with_headers("https://justhodl.ai/index.html")
print("normal-request has drawer tag:", 'src="/jh-nav-drawer.js"' in html2)
for k in ["cache-control","cf-cache-status","age","last-modified","etag","expires"]:
    for hk,hv in hdrs2.items():
        if hk.lower()==k: print(f"  {hk}: {hv}")

print("\n=== jh-nav-drawer.js itself — is IT cached/stale? ===")
js, hdrs3 = get_with_headers("https://justhodl.ai/jh-nav-drawer.js")
print("js has toggle logic:", "toggleDrawer" not in js and "toggle" in js, "| bytes:", len(js))
for k in ["cache-control","cf-cache-status","age"]:
    for hk,hv in hdrs3.items():
        if hk.lower()==k: print(f"  {hk}: {hv}")
print("DONE 2645")
