import urllib.request, json, ssl
ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
H={"User-Agent":"JustHodl raafouis@gmail.com"}
def es(dataset,dims,geo="EA20",n=6):
    q="&".join(f"{k}={v}" for k,v in dims.items())
    u=f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}?format=JSON&lang=EN&geo={geo}&{q}&lastTimePeriod={n}"
    try:
        j=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers=H),timeout=35,context=ctx).read())
        idx=j.get("dimension",{}).get("time",{}).get("category",{}).get("index",{}); vals=j.get("value",{})
        pts=sorted((t,vals[str(i)]) for t,i in idx.items() if str(i) in vals)
        return pts[-1] if pts else None, len(pts)
    except Exception as e: return f"ERR {str(e)[:60]}", 0

print("=== Eurostat BCS confidence — ei_bssi_m_r2 (indic codes) ===")
for nm,code in [("industrial","BS-ICI"),("services","BS-SCI"),("consumer","BS-CSMCI"),("retail","BS-RCI"),
                ("construction","BS-CCI"),("ESI","BS-ESI-I"),("employment EEI","BS-EEI")]:
    last,n=es("ei_bssi_m_r2",{"indic":code,"s_adj":"SA","unit":"BAL"} if code!="BS-ESI-I" else {"indic":code,"s_adj":"SA","unit":"I15"})
    print(f"  {nm:14} {code:10} -> {last}  (n={n})")

print("\n=== Eurostat IP by sector/MIG — sts_inpr_m (index, YoY computable) ===")
for nm,nace in [("total B-D","B-D"),("manufacturing C","C"),("intermediate MIG_ING","MIG_ING"),
                ("capital MIG_CAG","MIG_CAG"),("durable MIG_DCOG","MIG_DCOG"),
                ("nondurable MIG_NDCOG","MIG_NDCOG"),("energy MIG_NRG","MIG_NRG")]:
    last,n=es("sts_inpr_m",{"indic_bt":"PROD","nace_r2":nace,"s_adj":"SCA","unit":"I21"},n=14)
    print(f"  {nm:22} -> {last}  (n={n})")
