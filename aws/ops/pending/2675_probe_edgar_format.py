"""ops 2675 — exact SEC EDGAR full-text search result format (ticker extraction, all fields)
before writing block 2's parser."""
import urllib.request, json
def get(url):
    return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl.AI research"}),timeout=20).read())

d = get('https://efts.sec.gov/LATEST/search-index?q=%22Item+2.05%22&forms=8-K&startdt=2026-06-01&enddt=2026-06-30')
print("total hits:", d.get("hits",{}).get("total"))
for h in d.get("hits",{}).get("hits",[])[:5]:
    print(json.dumps(h, indent=1)[:900])
    print("---")

print("\n=== capex/datacenter language search ===")
d2 = get('https://efts.sec.gov/LATEST/search-index?q=%22data+center%22+%22megawatts%22&forms=8-K,10-Q,10-K&startdt=2026-06-20&enddt=2026-06-30')
print("total hits:", d2.get("hits",{}).get("total"))
for h in d2.get("hits",{}).get("hits",[])[:3]:
    src = h.get("_source",{})
    print(f"  {src.get('display_names')} | {src.get('form')} | {src.get('file_date')} | {src.get('root_forms')}")
print("DONE 2675")
