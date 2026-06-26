import urllib.request, json
UA={"User-Agent":"Mozilla/5.0 (verify)"}
def get(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=20)
        return r.status, r.read().decode("utf-8","replace")
    except Exception as e:
        return None, str(e)[:80]
st,body=get("https://justhodl.ai/crypto-confluence.html")
print("page status:", st)
if body and "<" in body:
    for m in ("Crypto Confluence","market_context","confluence_book","Multi-dimension"):
        print(f"  contains '{m}':", m in body)
st2,b2=get("https://justhodl.ai/data/crypto-confluence.json")
print("data json status:", st2)
if st2==200:
    try:
        d=json.loads(b2); print("  data ok: regime",d.get("market_context",{}).get("regime"),"| bullish",d.get("counts",{}).get("bullish_any"))
    except Exception as e: print("  parse:",str(e)[:50])
print("DONE 2215")
