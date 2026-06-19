"""ops 1951 — Operational: stale-feed gate in master-ranker.
Fast decision-critical feeds (risk-regime 48h, capital-flow 60h, options/massive 72h,
cross-asset 72h, nowcast 96h) are now excluded from the ranking if stale, with full
feed-freshness transparency. Prevents silent data-rot from contaminating decisions.
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

fn = "justhodl-master-ranker"
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
mr = json.loads(s3.get_object(Bucket=B, Key="data/master-ranker.json")["Body"].read())
print(f"\ntop_tickers={len(mr.get('top_tickers', []))} | as_of={mr.get('as_of')}")
print(f"stale_feeds_excluded: {mr.get('stale_feeds_excluded')}")
print(f"missing_feeds: {mr.get('missing_feeds')}")
print("\nfeed freshness (oldest first):")
for f in (mr.get("feed_freshness") or [])[:14]:
    tag = "STALE-EXCLUDED" if f.get("stale") else ("MISSING" if f.get("missing") else "ok")
    print(f"  {f['key']:38s} age={f.get('age_h')}h  {tag}")
print("\nDONE 1951")
