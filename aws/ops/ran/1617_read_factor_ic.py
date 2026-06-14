"""Read alpha-calibrator diagnostics: per-factor forward IC + sample size.
This is the literal 'do my signals have forward edge' answer (for current factors)."""
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")

def load(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:160]}

cal = load("data/calibration-latest.json")
if "_err" in cal:
    print("calibration-latest.json:", cal["_err"]); raise SystemExit(0)

print("generated_at:", cal.get("generated_at"))
# surface sample size / warm-up status
for k in ("n_trades","n_obs","sample_size","n","warm_up","warmup","status","mode","trades_analyzed"):
    if k in cal: print(f"{k}:", cal[k])
# per-factor IC — keys vary, so search for any IC-like structure
def show_ic(o, path=""):
    if isinstance(o, dict):
        for kk, vv in o.items():
            kl = kk.lower()
            if ("ic" == kl or "information_coef" in kl or "forward_return" in kl or "ic_" in kl or "_ic" in kl) and isinstance(vv,(int,float,dict,list)):
                print(f"  {path}{kk}: {vv if not isinstance(vv,(dict,list)) else json.dumps(vv)[:200]}")
            else:
                show_ic(vv, path+kk+".")
    elif isinstance(o, list) and o and isinstance(o[0], dict):
        show_ic(o[0], path+"[0].")
print("--- IC-related fields ---")
show_ic(cal)
print("--- top-level keys ---")
print(list(cal.keys()))
