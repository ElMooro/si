"""ops 2647 — deep diagnosis: full headers, content-type correctness, service-worker
check, cache consistency across repeat hits, byte-integrity of served JS vs repo source."""
import urllib.request, time, hashlib

def get(u, ua=None):
    hdrs = {"User-Agent": ua or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36"}
    req = urllib.request.Request(u, headers=hdrs)
    r = urllib.request.urlopen(req, timeout=25)
    return r.read(), dict(r.getheaders()), r.status

print("=== 1) FULL raw headers on the 3 key URLs (mimicking a real Chrome UA) ===")
for path in ["index.html", "jh-nav-drawer.js", "nav-manifest.json"]:
    body, hdrs, status = get(f"https://justhodl.ai/{path}")
    print(f"\n--- /{path} (status {status}, {len(body)} bytes) ---")
    for k, v in hdrs.items():
        print(f"  {k}: {v}")

print("\n=== 2) Content-Type sanity check (wrong MIME can silently block execution) ===")
_, h1, _ = get("https://justhodl.ai/jh-nav-drawer.js")
_, h2, _ = get("https://justhodl.ai/nav-manifest.json")
ct_js = next((v for k,v in h1.items() if k.lower()=="content-type"), None)
ct_json = next((v for k,v in h2.items() if k.lower()=="content-type"), None)
print("jh-nav-drawer.js Content-Type:", ct_js, "| OK for JS:", bool(ct_js and ("javascript" in ct_js or "ecmascript" in ct_js)))
print("nav-manifest.json Content-Type:", ct_json, "| OK for JSON:", bool(ct_json and "json" in ct_json))

print("\n=== 3) service worker present? (would explain staleness surviving hard-refresh) ===")
try:
    sw, _, sws = get("https://justhodl.ai/service-worker.js")
    print("service-worker.js:", sws, len(sw), "bytes — EXISTS, this could be the real cause")
except Exception as e:
    print("service-worker.js: not found (", str(e)[:50], ") — good, not the cause")
body_idx, _, _ = get("https://justhodl.ai/index.html")
print("index.html registers a service worker?:", "serviceWorker" in body_idx.decode("utf-8","ignore"))

print("\n=== 4) cache consistency across 4 consecutive hits (any edge-node flakiness / stale HIT?) ===")
for i in range(4):
    _, h, _ = get(f"https://justhodl.ai/index.html")
    cf = next((v for k,v in h.items() if k.lower()=="cf-cache-status"), "?")
    age = next((v for k,v in h.items() if k.lower()=="age"), "?")
    print(f"  hit {i+1}: cf-cache-status={cf} age={age}")
    time.sleep(2)

print("\n=== 5) byte-for-byte integrity — served JS hash vs local repo source hash ===")
served, _, _ = get("https://justhodl.ai/jh-nav-drawer.js")
local = open("jh-nav-drawer.js","rb").read()
print("served sha256:", hashlib.sha256(served).hexdigest()[:16])
print("local  sha256:", hashlib.sha256(local).hexdigest()[:16])
print("IDENTICAL:", served == local)

print("\n=== 6) is /screener/ (intentionally excluded) what's actually being checked? ===")
scr, _, _ = get("https://justhodl.ai/screener/")
print("screener/ has drawer tag (should be False, it was excluded on purpose):", b'jh-nav-drawer.js' in scr)

print("\nDONE 2647")
