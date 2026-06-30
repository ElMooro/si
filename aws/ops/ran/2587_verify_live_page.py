"""ops 2587 — verify live attention.html serves the new institutional build + feeds resolve."""
import urllib.request, time
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}),timeout=25)
        return r.read().decode("utf-8","ignore"), r.status
    except Exception as e: return str(e)[:80], 0
html,st=get(f"https://justhodl.ai/attention.html?cb={int(time.time())}")
print("live attention.html status:", st, "bytes:", len(html))
markers={
 "new title":"Smart Accumulation vs Crowd Attention",
 "divergence bar css":"dfill smart",
 "stealth section":"Stealth Accumulation",
 "igniting":"🔥 Igniting",
 "distribution":"🚫 Distribution",
 "options panel":"Unusual Options Flow",
 "search panel":"Search Attention Spikes",
 "loads confluence":"data/attention-confluence.json",
 "loads search":"data/search-attention.json",
 "OLD single-feed (should be ABSENT)":"attention-signals.json",
}
for k,v in markers.items():
    print(f"  [{'OK' if (v in html) else 'MISS'}] {k}")
# confirm both feeds resolve via proxy
for f in ["attention-confluence.json","search-attention.json"]:
    body,s=get(f"https://justhodl-data-proxy.raafouis.workers.dev/data/{f}?t={int(time.time())}")
    print(f"  feed {f}: http {s}, {len(body)} bytes")
print("DONE 2587")
