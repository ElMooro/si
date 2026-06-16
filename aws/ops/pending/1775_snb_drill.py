import urllib.request, json, boto3
def get(url,t=30,hdr=None):
    h={"User-Agent":"JustHodl raafouis@gmail.com"}; h.update(hdr or {})
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=h),timeout=t) as r: return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:80]

print("=== SNB capchstocki (share price indices) — structure ===")
st,body=get("https://data.snb.ch/api/cube/capchstocki/data/csv/en")
lines=body.splitlines() if isinstance(body,str) else []
for ln in lines[:40]: print("  "+ln[:90])

print("\n=== SNB snbbipo (balance sheet — find sight deposits) dimensions ===")
st,body=get("https://data.snb.ch/api/cube/snbbipo/dimensions/json/en")
try:
    j=json.loads(body)
    def walk(n,d=0):
        if d>2: return
        for k in (n if isinstance(n,list) else [n]):
            if isinstance(k,dict):
                if k.get("id") or k.get("name"): print(f"    {'  '*d}{k.get('id','')}: {str(k.get('name',''))[:50]}")
                for ch in (k.get("dimensions") or k.get("entities") or k.get("children") or []): walk(ch,d+1)
    walk(j)
except Exception as e: print("  ",st,str(body)[:100])

print("\n=== SNB rendoblid (yields) + devkua (CHF) latest sample ===")
for cid in ["rendoblid","devkua"]:
    st,body=get(f"https://data.snb.ch/api/cube/{cid}/data/csv/en")
    ls=body.splitlines() if isinstance(body,str) else []
    print(f"  {cid}: {len(ls)} lines; tail:")
    for ln in ls[-4:]: print("     "+ln[:80])

print("\n=== FRED Swiss unemployment + IP (find FRESH ids) ===")
FRED="2f057499936072679d8843d7fce99989"
for nm,sid in {"unemp q":"CHEUNTOTQDSMEI","unemp SLU":"LMUNRRTTCHQ156S","IP q OECD":"CHEPROINDQISMEI",
               "IP PRINTO q":"PRINTO01CHQ657S","mfg PRMNTO q":"PRMNTO01CHQ657S","IP MEI":"PRINTO01CHQ659S"}.items():
    st,body=get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=1")
    try: o=json.loads(body)["observations"][0]; print(f"  {nm:14} {sid:18} -> {o['value']} ({o['date']})")
    except Exception: print(f"  {nm:14} {sid:18} -> none")

print("\n=== existing ECB esi.json (what's in it?) ===")
s3=boto3.client("s3",region_name="us-east-1")
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-hist/esi.json")["Body"].read())
    print("  esi.json:", {k:d[k] for k in ("id","label","flow_key","freq","latest","latest_date") if k in d})
except Exception as e: print("  esi.json:", type(e).__name__)
