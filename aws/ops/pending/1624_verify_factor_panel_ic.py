"""Verify: alpha-score writes dated factor panel; signal-backtest factor-ic runs."""
import json, time, boto3
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
today = datetime.now(timezone.utc).date().isoformat()
pkey = f"screener/alpha-panel/{today}.json"

def exists(k):
    try: s3.head_object(Bucket=B, Key=k); return True
    except Exception: return False

# 1) alpha-score -> panel
print("panel before:", exists(pkey))
try:
    lam.invoke(FunctionName="justhodl-alpha-score", InvocationType="Event")
    print("invoked alpha-score (async)")
except Exception as e:
    print("alpha-score invoke err:", str(e)[:140])

panel_ok = False
for i in range(15):
    time.sleep(20)
    if exists(pkey):
        p = json.loads(s3.get_object(Bucket=B, Key=pkey)["Body"].read())
        print(f"PANEL WRITTEN after ~{(i+1)*20}s: n={p.get('n')} factors={p.get('factors')}")
        sample = list((p.get('rows') or {}).items())[:2]
        print("  sample row:", json.dumps(dict(sample))[:260])
        panel_ok = True
        break
if not panel_ok:
    print("panel not yet written (alpha-score still scoring; scheduled run will write it)")

# 2) signal-backtest -> factor-ic.json
try:
    lam.invoke(FunctionName="justhodl-signal-backtest", InvocationType="Event")
    print("\ninvoked signal-backtest (async)")
except Exception as e:
    print("signal-backtest invoke err:", str(e)[:140])

for i in range(12):
    time.sleep(20)
    try:
        fic = json.loads(s3.get_object(Bucket=B, Key="data/factor-ic.json")["Body"].read())
        if fic.get("generated_at","") >= today:
            print(f"\nfactor-ic.json: maturity={fic.get('maturity')} panels_total={fic.get('panels_total')} "
                  f"panels_matured={fic.get('panels_matured')} dates_used={fic.get('dates_used')}")
            print("  note:", (fic.get('note') or fic.get('eta',''))[:150])
            break
    except Exception:
        pass
    print(f"  ...waiting factor-ic ({(i+1)*20}s)")
else:
    print("factor-ic.json not refreshed yet (backtest still running)")
