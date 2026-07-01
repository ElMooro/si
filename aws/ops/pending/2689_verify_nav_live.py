"""ops 2689 — confirm the live nav-manifest.json actually has both new pages under
Equity Signals, and the drawer script fetches from the right place."""
import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=25).read().decode()
m = json.loads(get(f"https://justhodl.ai/nav-manifest.json?cb={int(time.time())}"))
print("live total pages:", m["n_pages"])
for cat in m["categories"]:
    if cat["name"] == "⚡ Equity Signals":
        found = [p for p in cat["pages"] if "signal-genealogy" in p["href"] or "early-signals" in p["href"]]
        print(f"Equity Signals count: {cat['count']}")
        for p in found:
            print(f"  LIVE: {p['href']} -> {p['title']}")
print("DONE 2689")
