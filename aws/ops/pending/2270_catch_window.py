import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(t): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"equity-research/{t}.json")["Body"].read())
# fire async regen for LDOS + a couple cold names to raise the odds of catching an AI-up window
befores={}
for t in ["LDOS","ACN","NOC"]:
    try: befores[t]=doc(t).get("generated_at")
    except Exception: befores[t]=None
    lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
               Payload=json.dumps({"ticker":t,"force_refresh":True,"_internal":"1"}).encode())
print("fired regen for LDOS, ACN, NOC; polling for a populated relationships block...")
found=None
for i in range(16):
    time.sleep(13)
    for t in ["LDOS","ACN","NOC"]:
        try:
            d=doc(t)
            if d.get("generated_at")!=befores.get(t):
                rel=d.get("relationships") or {}
                ai_ok=not str(d.get('executive_summary') or '').startswith('AI synthesis failed')
                ncust=len(rel.get("customers") or []); npart=len(rel.get("partners") or [])
                print(f"t+{(i+1)*13}s {t} regenerated | AI_ok={ai_ok} | customers={ncust} partners={npart}")
                if rel and (ncust or npart):
                    found=(t,d,rel); break
        except Exception: pass
    if found: break
if found:
    t,d,rel=found
    print(f"\n=== {t} RELATIONSHIPS (grounded) ===")
    print("summary:", str(rel.get("summary"))[:220])
    for kind in ("customers","partners","suppliers"):
        for x in (rel.get(kind) or [])[:6]:
            print(f"  [{kind}] {x.get('name')} | {str(x.get('detail'))[:50]} | conc={x.get('concentration')} | src={x.get('source')}")
else:
    print("\nNo AI-up window caught — relationships still empty (Anthropic credits out). Renderer is live + safe; will populate on next good run.")
print("DONE 2270")
