import json, urllib.request, urllib.parse
def g(url,t=40):
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com"})
    with urllib.request.urlopen(req,timeout=t) as r: return json.loads(r.read())
params={"$where":"cftc_contract_market_code='133741'","$order":"report_date_as_yyyy_mm_dd DESC","$limit":"1"}
url="https://publicreporting.cftc.gov/resource/gpe5-46if.json?"+urllib.parse.urlencode(params)
r=g(url)[0]
# print all position-related fields + values
print("Bitcoin row position fields:")
for k in sorted(r.keys()):
    if any(w in k for w in ["positions","traders","open_interest","pct_of_oi","change"]) and "spread" not in k:
        print(f"  {k} = {r[k]}")
print("DONE 2397")
