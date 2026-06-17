import urllib.request, json
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:90]
print("=== TIC MFH full text (for parser) ===")
st,b=get("https://ticdata.treasury.gov/Publish/mfh.txt")
print(b[:2400])
print("...TRUNC...")
print("\n=== NY Fed AMBS details sample ===")
st,b=get("https://markets.newyorkfed.org/api/ambs/all/results/details/last/3.json")
try:
    j=json.loads(b); arr=j["ambs"]; print("n=",len(arr)); 
    if arr: print("keys:",list(arr[0].keys())); print("sample:",{k:arr[0][k] for k in list(arr[0].keys())[:12]})
except Exception as e: print("err",e,str(b)[:120])
print("\n=== NY Fed seclending details sample ===")
st,b=get("https://markets.newyorkfed.org/api/seclending/all/results/details/last/3.json")
try:
    j=json.loads(b); arr=j["seclending"]; print("n=",len(arr))
    if arr: print("keys:",list(arr[0].keys())); print("sample:",{k:arr[0][k] for k in list(arr[0].keys())[:12]})
except Exception as e: print("err",e,str(b)[:120])
print("\n=== seclending summary (aggregate by day?) ===")
st,b=get("https://markets.newyorkfed.org/api/seclending/all/results/summary/last/3.json")
try:
    j=json.loads(b); arr=j["seclending"]; print("n=",len(arr)); print("sample:",arr[0] if arr else "empty")
except Exception as e: print("err",e,str(b)[:120])
