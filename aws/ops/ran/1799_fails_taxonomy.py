import urllib.request, json, collections
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
def get(url,t=60):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
NY="https://markets.newyorkfed.org/api/pd"
j=json.loads(get(NY+"/list/timeseries.json"))
arr=j["pd"]["timeseries"]
fails=[x for x in arr if 'fail' in (x.get("label","")+x.get("keyid","")).lower()]
print("total fails series:",len(fails))
# classify by deliver/receive + asset class
def cls(k,lab):
    L=lab.upper()
    if k.endswith("-TD") or k.endswith("-FDT"): side="DELIVER"
    elif k.endswith("-TR") or k.endswith("-FRT"): side="RECEIVE"
    else: side="?"
    if "MBS" in L: a="MBS"
    elif "TIPS" in L or k.startswith("PDSI"): a="TIPS"
    elif "FRN" in L or "PDFRN" in k: a="FRN"
    elif "CORP" in L: a="CORP"
    elif "AGENCY" in L or "AGCY" in L: a="AGENCY"
    elif k.startswith("PDST"): a="UST_COUPON"
    else: a="OTHER"
    return side,a
cnt=collections.Counter()
for x in fails: cnt[cls(x["keyid"],x.get("label",""))]+=1
print("breakdown:",dict(cnt))
# print distinct UST coupon + TIPS + FRN keys (the Treasury complex)
print("\nTreasury-complex fails keys (TD side):")
for x in fails:
    if x["keyid"].endswith("-TD") and (x["keyid"].startswith("PDST") or x["keyid"].startswith("PDSI") or x["keyid"].startswith("PDFRN")):
        print("  ",x["keyid"],"|",x.get("label","")[:55])
# probe one series data shape + history depth
print("\n=== sample series PDST10F-TD (10yr UST fails to deliver) ===")
b=get(NY+"/get/PDST10F-TD.json")
if b:
    jj=json.loads(b); ts=jj["pd"]["timeseries"]
    print("  n obs:",len(ts),"| first:",ts[0],"| last:",ts[-1])
