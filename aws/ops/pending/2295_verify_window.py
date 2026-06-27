import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:40]}
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="Event",Payload=b"{}")
print("regen boom engine; polling for pressure history/direction...")
d=None
for i in range(20):
    time.sleep(13); cur=doc(); ip=cur.get("industry_pressure") or {}
    if any(isinstance(v,dict) and v.get("history") for v in ip.values()):
        d=cur; print(f"  t+{(i+1)*13}s history PRESENT"); break
    print(f"  t+{(i+1)*13}s waiting...")
if d:
    ip=d.get("industry_pressure") or {}
    for g,v in ip.items():
        h=v.get("history") or []
        print(f"{g}: score={v.get('pressure_0_100') or ('semis '+str(v.get('ip_yoy_z')))} dir={v.get('direction')} trend6={v.get('trend_6mo')} hist_pts={len(h)} {('['+h[0]['d']+'→'+h[-1]['d']+']') if h else ''} last3={[x['p'] for x in h[-3:]]}")
print("DONE 2295")
