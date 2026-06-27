import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:40]}
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="Event",Payload=b"{}")
print("regen boom; polling for finer groups...")
d=None
for i in range(20):
    time.sleep(13); cur=doc(); ip=cur.get("industry_pressure") or {}
    if "MACHINERY" in ip or "AEROSPACE_DEFENSE" in ip: d=cur; print(f"  t+{(i+1)*13}s finer groups PRESENT"); break
    print(f"  t+{(i+1)*13}s...")
if d:
    ip=d.get("industry_pressure") or {}
    print("\nPressure groups now:")
    for g,v in ip.items():
        print(f"  {g}: score={v.get('pressure_0_100') or ('semis z='+str(v.get('ip_yoy_z')))} dir={v.get('direction')} trend6={v.get('trend_6mo')} hist={len(v.get('history') or [])}")
    # candidate re-mapping
    cands=d.get("candidates") or d.get("results") or []
    if isinstance(cands,dict): cands=list(cands.values())
    print("\nCandidate -> pressure_group (industrials):")
    for c in cands:
        tk=c.get("ticker"); g=c.get("pressure_group"); ind=c.get("industry")
        if tk in ("ETN","ROK","TDG","PH","EMR","GE","HON","CAT","DE","PWR","VRT","ATKR","NVT","HUBB","LDOS"):
            print(f"  {tk}: {g}  ({ind})")
print("DONE 2307")
