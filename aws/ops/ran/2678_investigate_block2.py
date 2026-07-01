"""ops 2678 — investigate: is the 0-restructuring count genuine, and are BACC/DLR real
duplicate filings or a dedup bug?"""
import urllib.request, json, urllib.parse
def edgar(q, forms, startdt, enddt):
    url = "https://efts.sec.gov/LATEST/search-index?q=" + urllib.parse.quote(q) + f"&forms={forms}&startdt={startdt}&enddt={enddt}"
    return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl.AI research"}),timeout=20).read())

print("=== exact same 14-day window the engine used ===")
d = edgar('"Item 2.05"', "8-K", "2026-06-17", "2026-07-01")
print("total hits for restructuring window:", d.get("hits",{}).get("total"))
for h in d.get("hits",{}).get("hits",[])[:5]:
    src = h.get("_source",{})
    print(f"  {src.get('display_names')} | items={src.get('items')} | {src.get('file_date')}")

print("\n=== check BACC across all 4 buildout terms — same doc or different? ===")
import sys
sys.path.insert(0, "/dev/null")
TERMS = ['"data center" "megawatts"', '"power purchase agreement" "data center"', '"gigawatt" "data center"', '"hyperscale"']
bacc_hits = []
for term in TERMS:
    d2 = edgar(term, "8-K,10-Q,10-K", "2026-06-17", "2026-07-01")
    for h in d2.get("hits",{}).get("hits",[]):
        src = h.get("_source",{})
        if "BACC" in str(src.get("display_names")):
            bacc_hits.append((term, h.get("_id"), src.get("form"), src.get("adsh"), src.get("file_date")))
print(f"BACC found in {len(bacc_hits)} (term, hit) combinations:")
for t, hid, form, adsh, fd in bacc_hits:
    print(f"  term='{t}' _id={hid} form={form} adsh={adsh} date={fd}")
print("DONE 2678")
