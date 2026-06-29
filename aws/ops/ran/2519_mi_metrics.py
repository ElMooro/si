import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
rl=g("learning/morning_run_log.json")
# run log may be a list of runs
runs=rl if isinstance(rl,list) else rl.get("runs") or [rl]
last=runs[-1] if runs else {}
m=last.get("metrics") or last.get("m") or {}
print("run keys:",list(last.keys())[:12])
flow={k:v for k,v in m.items() if k.startswith("flow_") or k.startswith("roro_")}
if flow:
    print("=== cross-asset flow metrics in last run ===")
    for k,v in flow.items(): print("  %-24s %s"%(k,json.dumps(v)[:80]))
else:
    print("metrics not stored in runlog; confirming via deployed prompt instead")
print("DONE 2519")
