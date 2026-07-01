"""ops 2648 — confirm the new service worker version is actually live."""
import urllib.request, time
def get(u):
    return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M"}),timeout=25).read().decode("utf-8","ignore")
sw = get(f"https://justhodl.ai/service-worker.js?cb={int(time.time())}")
print("live SW version line:", [l for l in sw.split(chr(10)) if "VERSION =" in l])
print("has skipWaiting:", "skipWaiting" in sw, "| has clients.claim:", "clients.claim" in sw)
print("cache-name deletion logic present:", "caches.delete" in sw)
print("DONE 2648")
