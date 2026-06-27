import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen v2.6...")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.6": d=cur; print(f"  t+{(i+1)*12}s v2.6 (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if not d: print("NO v2.6:",json.dumps(doc())[:150]); print("DONE 2342"); raise SystemExit
sy=d.get("synthesis") or {}; tr=d.get("trajectory") or {}
print("\n=== SYNTHESIS (the read) ===")
print("  posture:",sy.get("posture"),"| score:",sy.get("score"),"| conviction:",sy.get("conviction"),"|",sy.get("n_risk_off"),"off vs",sy.get("n_risk_on"),"on")
print("  bottom_line:",sy.get("bottom_line"))
print("  bullish:",[c["label"] for c in (sy.get("bullish_drivers") or [])])
print("  bearish:",[c["label"] for c in (sy.get("bearish_drivers") or [])])
print("  own (leading):",sy.get("own_whats_leading"),"| reduce (lagging):",sy.get("reduce_whats_lagging"))
print("  key_risk:",(sy.get("key_risk") or "")[:90])
print("\n=== TRAJECTORY / SELF-HISTORY ===")
print("  days logged:",tr.get("n_days_logged"),"| series len:",len(tr.get("series") or []))
print("  deltas: posture5d",tr.get("posture_score_5d"),"recession21d",tr.get("recession_prob_21d"),"squeeze21d",tr.get("squeeze_21d"))
print("  latest snapshot:",json.dumps((tr.get("series") or [{}])[-1]))
# verify history file written
try:
    h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock-history.json")["Body"].read())
    print("  history file entries:",len(h))
except Exception as e: print("  history file:",str(e)[:50])
print("  fed path scenario now:",((d.get('rates_fed_vol') or {}).get('fed_path') or {}).get('summary_6mo'))
print("DONE 2342")
