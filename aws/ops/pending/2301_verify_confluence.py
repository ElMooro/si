import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:40]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("regen; polling for confluence...")
d=None
for i in range(16):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4: d=cur; print(f"  t+{(i+1)*12}s wrote (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if d:
    bt=d.get("by_ticker") or {}
    rows=[(tk,r.get("confluence"),r.get("confluence_avail"),r.get("boom_score") if False else None,r.get("confluence_signals")) for tk,r in bt.items()]
    rows=[r for r in rows if r[1] is not None]
    rows.sort(key=lambda x:-(x[1] or 0))
    print(f"\nconfluence present on {len(rows)}/{len(bt)} names. Top by confirmations:")
    for tk,cf,ca,_,sg in rows[:10]:
        print(f"  {tk}: {cf}/{ca}  [{', '.join(sg or [])}]")
print("DONE 2301")
