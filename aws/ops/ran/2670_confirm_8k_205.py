"""ops 2670 — confirm 8-K Item 2.05 (restructuring) full-text search works, and check the
signal-scorecard's existing SPY-history helper so block 1 reuses proven code."""
import urllib.request, json
def get(url):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl.AI research"}),timeout=20).read())
    except Exception as e: return {"__err__": str(e)[:150]}
d = get('https://efts.sec.gov/LATEST/search-index?q=%22Item+2.05%22&forms=8-K&startdt=2026-06-01&enddt=2026-06-30')
print("8-K Item 2.05, June 2026:", d.get("hits",{}).get("total"))
for h in (d.get("hits",{}) or {}).get("hits",[])[:3]:
    src = h.get("_source",{})
    print(" ", src.get("display_names"), src.get("file_date"), src.get("_id") or h.get("_id"))
print("DONE 2670")
