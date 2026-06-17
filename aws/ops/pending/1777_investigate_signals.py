import json, boto3, urllib.request
s3=boto3.client("s3",region_name="us-east-1")
def s3get(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:80]

print("=== A) ecb-derived.json signals ===")
der=s3get("data/ecb-derived.json")
ind=der.get("indicators",{}) if isinstance(der,dict) else {}
for k,v in ind.items():
    if isinstance(v,dict) and "signal" in v:
        print(f"  {k:30} signal={v.get('signal'):10} value={v.get('value',v.get('metric',''))}  keys={list(v.keys())[:6]}")
print("  (non-signal keys:", [k for k,v in ind.items() if not(isinstance(v,dict) and 'signal' in v)],")")

print("\n=== B) ecb-hist manifest series ids ===")
man=s3get("data/ecb-hist/_manifest.json")
ser=man.get("series",[]) if isinstance(man,dict) else []
print("  count:", len(ser))
print("  ids:", ", ".join(sorted(x.get("id","?") for x in ser)))

print("\n=== C) ECB dataflow catalog — find survey/confidence/sentiment flows ===")
st,body=get("https://data-api.ecb.europa.eu/service/dataflow/ECB")
if st==200:
    import re
    # match <str:Dataflow ... id="XXX"> ... <com:Name ...>NAME</com:Name>
    flows=re.findall(r'id="([A-Z0-9_]+)"[^>]*>\s*(?:<[^>]+>\s*)*<com:Name[^>]*>([^<]+)</com:Name>', body)
    hits=[(i,n) for i,n in flows if re.search(r'survey|confiden|sentiment|business|consumer|economic indicator',n,re.I)]
    print(f"  catalog ok ({len(body)} bytes, {len(flows)} flows). Survey/confidence matches:")
    for i,n in hits[:25]: print(f"    {i:12} {n[:70]}")
    if not hits:
        print("  no name matches; sample flows:", [f"{i}:{n[:25]}" for i,n in flows[:15]])
else:
    print("  catalog http:", st, str(body)[:80])
