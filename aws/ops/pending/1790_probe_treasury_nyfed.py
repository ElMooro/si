import urllib.request, json
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com","Accept":"application/json, text/plain, */*"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:90]
FD="https://api.fiscaldata.treasury.gov/services/api/fiscal_service"

print("=== 1) AVG INTEREST RATE on Treasury debt ===")
st,b=get(FD+"/v2/accounting/od/avg_interest_rates?sort=-record_date&page[size]=3")
if st==200:
    j=json.loads(b); 
    for r in j["data"][:3]: print("  ",{k:r[k] for k in r if k in ('record_date','security_type_desc','security_desc','avg_interest_rate_amt')})
else: print("  http",st,str(b)[:80])

print("\n=== 2) MTS — deficit/receipts/outlays (mts_table_1) ===")
st,b=get(FD+"/v1/accounting/mts/mts_table_1?sort=-record_date&page[size]=4")
if st==200:
    j=json.loads(b)
    print("  fields:", list(j["data"][0].keys())[:14])
    for r in j["data"][:4]: print("  ",{k:r.get(k) for k in ('record_date','classification_desc','current_month_net_rcpt_amt','current_month_net_outly_amt','current_month_dfct_sur_amt')})
else: print("  http",st,str(b)[:120])

print("\n=== 3) Debt to the Penny ===")
st,b=get(FD+"/v2/accounting/od/debt_to_penny?sort=-record_date&page[size]=1")
print("  ", (json.loads(b)["data"][0] if st==200 else ("http "+str(st))))

print("\n=== 4) TIC — Major Foreign Holders of US Treasuries ===")
for url in ["https://ticdata.treasury.gov/Publish/mfh.txt",
            "https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/mfh.txt",
            "https://home.treasury.gov/system/files/206/mfh.txt"]:
    st,b=get(url); print(f"  {url[-40:]:42} http={st} {('len='+str(len(b))+' head='+repr(b[:60])) if st==200 else str(b)[:50]}")
print("  FRED FDHBFIN (foreign-held federal debt, fallback):")
st,b=get("https://api.stlouisfed.org/fred/series/observations?series_id=FDHBFIN&api_key=2f057499936072679d8843d7fce99989&file_type=json&sort_order=desc&limit=2")
print("   ", (json.loads(b)["observations"][:2] if st==200 else "http "+str(st)))

print("\n=== 5) NY Fed AMBS + Securities Lending ===")
for url in ["https://markets.newyorkfed.org/api/ambs/all/results/summary/lastTwoWeeks.json",
            "https://markets.newyorkfed.org/api/ambs/all/results/details/latest.json",
            "https://markets.newyorkfed.org/api/seclending/all/results/summary/lastTwoWeeks.json",
            "https://markets.newyorkfed.org/api/seclending/all/results/details/latest.json"]:
    st,b=get(url)
    head=""
    if st==200:
        try: j=json.loads(b); k=list(j.keys()); head=str(k)+" | "+str(list((j.get(k[0]) or [{}])[0].keys())[:8] if isinstance(j.get(k[0]),list) and j.get(k[0]) else "")
        except: head=b[:60]
    print(f"  {url.split('/api/')[1][:45]:46} http={st} {head[:90]}")
