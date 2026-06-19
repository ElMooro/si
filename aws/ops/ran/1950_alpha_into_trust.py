"""ops 1950 — CAPSTONE: wire net-of-cost FDR alpha into the auto-demotion gate.
engine-trust.effective_trust now folds alpha_status (ALPHA_PROVEN x1.20,
ALPHA_NEGATIVE x0.40). The signal-harvester already consumes engine_trust.trust(),
so value-destroying engines are now down-weighted in the live cascade.
"""
import io, json, time, zipfile, os, glob
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
B = "justhodl-dashboard-live"
ROOT = os.getcwd()

def zb(main_path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(main_path, "lambda_function.py")
        for sp in glob.glob(f"{ROOT}/aws/shared/*.py"):
            z.write(sp, os.path.basename(sp))
    buf.seek(0); return buf.read()

fn = "justhodl-engine-trust"
data = zb(f"{ROOT}/aws/lambdas/{fn}/source/lambda_function.py")
for i in range(24):
    try:
        lam.update_function_code(FunctionName=fn, ZipFile=data, Publish=False)
        print(f"{fn}: code update OK (attempt {i})"); break
    except lam.exceptions.ResourceConflictException:
        time.sleep(5)
for _ in range(40):
    c = lam.get_function_configuration(FunctionName=fn)
    if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)

r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:160])
time.sleep(2)
et = json.loads(s3.get_object(Bucket=B, Key="data/engine-trust.json")["Body"].read())
ag = et.get("alpha_gate", {})
print(f"\nregime={et.get('current_regime')} | engines={et.get('n_engines')} | counts={et.get('counts')}")
print(f"\nALPHA-BOOSTED (proven, x1.20):")
for e in ag.get("proven_boosted", []):
    print(f"  {e['signal_type'][:30]:30s} net_t={e['net_t']} net_excess={e['net_excess_pct']}% -> trust={e['effective_trust']}")
print(f"\nALPHA-DEMOTED (value-destroying, x0.40):")
for e in ag.get("negative_demoted", [])[:15]:
    print(f"  {e['signal_type'][:30]:30s} net_t={e['net_t']} net_excess={e['net_excess_pct']}% -> trust={e['effective_trust']}")
print(f"\ntotal demoted on alpha: {len(ag.get('negative_demoted', []))} | boosted: {len(ag.get('proven_boosted', []))}")
print("\nDONE 1950")
