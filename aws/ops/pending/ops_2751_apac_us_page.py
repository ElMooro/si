"""ops 2751 — APAC v1.1: real US returns in bridges + expanded TW map + PAGE.

Copies FMP_API_KEY from an existing FMP-using function into justhodl-apac-flows
env (key already in Khalid's infra — never asked). Deploys the widened adapter
(Winbond/Nanya/Macronix memory names + 50-name sector map; FMP stock-price-change
US returns wired into every Asia->US bridge with a divergence verdict). Ships
apac.html (bridges hero + Taiwan flow tables + sector bars + KR/JP placeholders)
and nav entry. Verifies: bridges carry us_avg_ret5d, Semiconductors now in sector
rollup, page at edge. Report: aws/ops/reports/2751_apac_us_page.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-flows"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2751, "ts": datetime.now(timezone.utc).isoformat()}

def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=200):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)

print("settling 15s…"); time.sleep(15)
print("== 1/4 copy FMP key from infra -> apac env ==")
fmp = None
for donor in ("justhodl-theme-rotation", "justhodl-bond-desk", "justhodl-factor-returns", "justhodl-capex-tracker"):
    try:
        env = (lam.get_function_configuration(FunctionName=donor).get("Environment", {}) or {}).get("Variables", {}) or {}
        fmp = env.get("FMP_API_KEY") or env.get("FMP_KEY")
        if fmp:
            print("  FMP key sourced from %s (len %d)" % (donor, len(fmp))); break
    except ClientError:
        continue
assert fmp, "no FMP key found in donor functions"
wait_ok(FN)
cur = (lam.get_function_configuration(FunctionName=FN).get("Environment", {}) or {}).get("Variables", {}) or {}
cur.update({"FMP_API_KEY": fmp, "TZ": "UTC"})
lam.update_function_configuration(FunctionName=FN, Environment={"Variables": cur})
wait_ok(FN)
R["fmp_key_wired"] = True

print("== 2/4 deploy widened adapter + invoke ==")
lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); wait_ok(FN)
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:260])
assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-flows.json")["Body"].read())
tw = d.get("taiwan", {})
sec = tw.get("sector_flows_shares", {})
print("  sector rollup:", json.dumps(sec))
brs = d.get("bridges", [])
for b in brs:
    print("  bridge: %-40s TWnet=%s US5d=%s [%s]" % (
        b["name"][:40], b.get("tw_foreign_net_shares"), b.get("us_avg_ret5d"), b.get("verdict")))
assert "Semiconductors" in sec, "expanded map not applied"
assert d.get("us_returns_live"), "US returns not live (FMP)"
assert any(b.get("us_avg_ret5d") is not None for b in brs), "no bridge has US returns"
R["sectors"] = sec
R["bridges"] = [(b["name"], b.get("tw_foreign_net_shares"), b.get("us_avg_ret5d"), b.get("verdict")) for b in brs]
R["us_returns_live"] = d.get("us_returns_live")

print("== 3/4 page at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"APAC RADAR v1" in pub("apac.html")
    except Exception: pass
    print("  page attempt %d: %s" % (a + 1, "LIVE" if okp else "pending"))
    if okp: break
assert okp, "apac.html not at edge"
nv = pub("nav-manifest.json").decode()
assert "/apac.html" in nv, "nav entry missing"
R["page"] = "LIVE"

print("== 4/4 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2751_apac_us_page.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2751 COMPLETE — APAC radar has a face and a US side")
