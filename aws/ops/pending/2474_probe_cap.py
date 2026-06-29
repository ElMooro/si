import urllib.request, urllib.parse, json, boto3
def fts(q, forms, a, b):
    p=urllib.parse.urlencode({"q":q,"forms":forms,"startdt":a,"enddt":b})
    url=f"https://efts.sec.gov/LATEST/search-index?{p}&from=0"
    req=urllib.request.Request(url,headers={"User-Agent":"JustHodl Research contact@justhodl.ai","Accept":"application/json"})
    return json.loads(urllib.request.urlopen(req,timeout=20).read())
# test form-based counting (empty q vs wildcard)
for q in ["", "*"]:
    try:
        j=fts(q,"S-1","2026-03-01","2026-06-29")
        tot=((j.get("hits") or {}).get("total") or {}).get("value")
        hits=(j.get("hits") or {}).get("hits") or []
        print(f"q={q!r} S-1 total={tot} nhits={len(hits)}")
        if hits:
            src=hits[0].get("_source") or {}
            print("  _source keys:",sorted(src.keys()))
            print("  display_names:",src.get("display_names"))
            print("  sics:",src.get("sics"))
    except Exception as e:
        print(f"q={q!r} ERR",str(e)[:80])
# credit-stress.json schema
s3=boto3.client("s3","us-east-1")
try:
    cs=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/credit-stress.json")["Body"].read())
    print("credit-stress top keys:",list(cs.keys())[:20])
    print("sample:",json.dumps(cs)[:400])
except Exception as e:
    print("credit-stress ERR",str(e)[:80])
print("DONE 2474")
