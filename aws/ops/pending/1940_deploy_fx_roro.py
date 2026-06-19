"""ops 1940 — Deploy fx-regime v2 (FX RORO) + verify the RORO score.

Force update (bundle massive.py so the entitled Massive FX path is used), invoke,
print fx_roro_score + regime + per-driver contributions to sanity-check the
weighting before the synthesizer is built on top of it.
"""
import io, json, time, zipfile, os
import boto3

lam = boto3.client("lambda", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = os.getcwd()

def zb(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for arc, p in files.items():
            z.write(p, arc)
    buf.seek(0); return buf.read()

def update(fn, data):
    for i in range(24):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=data, Publish=False)
            print(f"  update OK attempt {i}"); return
        except lam.exceptions.ResourceConflictException:
            time.sleep(5)

def wait(fn):
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return c
        time.sleep(3)
    return lam.get_function_configuration(FunctionName=fn)

FN = "justhodl-polygon-fx-regime"
update(FN, zb({
    "lambda_function.py": f"{ROOT}/aws/lambdas/justhodl-polygon-fx-regime/source/lambda_function.py",
    "massive.py": f"{ROOT}/aws/shared/massive.py",
}))
c = wait(FN)
print("state:", c["State"], c.get("LastUpdateStatus"), "size:", c["CodeSize"])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:300])

d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/polygon-fx-regime.json")["Body"].read())
roro = d.get("fx_roro", {})
print("\nversion:", d.get("version"), "| source:", d.get("source"), "| n_pairs:", d.get("n_pairs"))
print("FX RORO score:", roro.get("fx_roro_score"), "| regime:", roro.get("fx_roro_regime"))
print("em_basket_5d:", roro.get("em_basket_5d_pct"), "| gold/silver chg:", roro.get("gold_silver_ratio_chg_5d"),
      "| havens_bid:", roro.get("havens_bid_count"))
print("\nDrivers (contribution, + = risk-on):")
for dr in sorted(roro.get("drivers", []), key=lambda x: -abs(x["contribution"])):
    print(f"  {dr['driver']:24s} 5d={dr['ret_5d_pct']:+.2f}%  contrib={dr['contribution']:+.2f}  w={dr['weight']}")
print("\nTells:")
for t in roro.get("tells", []):
    print("  •", t)
# show a few key pairs fresh
print("\nKey pairs (latest, 5d%):")
for k in ("USD_JPY","USD_CHF","AUD_JPY","XAU_USD","USD_MXN","AUD_USD"):
    p = d.get("pair_data", {}).get(k, {})
    print(f"  {k:8s} {p.get('latest_price')}  5d={p.get('return_5d_pct')}")
print("\nDONE 1940")
