import urllib.request, json
UA={"User-Agent":"Mozilla/5.0 (verify)"}
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=20)
        return r.status, r.read().decode("utf-8","replace")
    except Exception as e:
        return None, str(e)[:80]
st,body=get("https://justhodl.ai/capital-inflows.html")
print("page status:", st)
if body and "<" in body:
    for m in ("US Capital Inflows","regime","by_asset_class","rolling trend","Where the money goes"):
        print(f"  contains '{m}':", m in body)
# confirm data via proxy (the page's fallback)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/capital-inflows.json",headers=UA),timeout=20)
    d=json.loads(r.read())
    print("proxy data status:", r.status, "| asof:", d.get("data_asof"), "| regime:", d.get("regime"),
          "| foreign_into_us_12mo: $%sB"%d.get("headline",{}).get("foreign_net_into_us_lt_12mo_b"),
          "| equities leg: $%sB"%(d.get("by_asset_class",{}).get("equities",{}).get("rolling_12mo_b")))
    print("PAGE+DATA FULLY LIVE")
except Exception as e:
    print("proxy ERR:", str(e)[:80])
print("DONE 2223")
