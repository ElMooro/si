import urllib.request, json, ssl
ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
H={"User-Agent":"JustHodl raafouis@gmail.com"}
def struct(dataset,extra=""):
    u=f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}?format=JSON&lang=EN&geo=EA20&lastTimePeriod=1{extra}"
    try:
        j=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers=H),timeout=40,context=ctx).read())
        dims=j.get("dimension",{}); order=j.get("id",[])
        print(f"  dims: {order}")
        for d in order:
            if d in ("time","geo"): continue
            cat=dims.get(d,{}).get("category",{}); lab=cat.get("label",{})
            items=list(lab.items())[:14]
            print(f"    {d}: "+", ".join(f"{k}={v[:22]}" for k,v in items))
        return True
    except Exception as e:
        print(f"  ERR {str(e)[:90]}"); return False
print("=== ei_bssi_m_r2 (sentiment/confidence) structure ===")
struct("ei_bssi_m_r2")
print("\n=== sts_inpr_m (industrial production) structure ===")
struct("sts_inpr_m")
