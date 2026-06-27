import boto3, json, urllib.request
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
ip=d.get("industry_pressure") or {}
ranks=d.get("ranks") or []
print("groups available:",list(ip.keys()))
def windowfor(tk):
    rk=next((x for x in ranks if x.get("ticker")==tk),None)
    if not rk: return f"{tk}: not a candidate"
    g=rk.get("pressure_group"); v=ip.get(g) or {}
    sc=v.get("pressure_0_100") or (50+(v.get("ip_yoy_z") or 0)*10 if v.get("ip_yoy_z") is not None else None)
    return f"{tk}: {g} -> score={round(sc) if sc is not None else '—'} dir={v.get('direction')} t6={v.get('trend_6mo')}"
for tk in ["NVDA","MU","TDG","KTOS","ETN","ROK","VST","LDOS"]:
    print("  "+windowfor(tk))
# live deploy check
UA={"User-Agent":"Mozilla/5.0 (verify)"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
print("\nlive page has 'Industry bottleneck window':", "Industry bottleneck window" in html, "| pressureViz:", "function pressureViz" in html)
print("DONE 2311")
