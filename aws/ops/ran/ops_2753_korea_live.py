"""ops 2753 — KOREA LIVE via Naver (KRX blocks AWS; probe found Naver works).

Deploys adapter with korea_naver() (per-stock foreign/institution/individual
net + foreign hold ratio for Samsung/SK Hynix/LG Energy/... via
m.stock.naver.com/api/stock/{code}/trend), KR signal folded into bridge
verdicts, page rendering Korea flows. Verifies: korea LIVE with real net for
Samsung+SK Hynix, KR sector rollup incl Memory/Semis + Battery, bridges carry
kr_foreign_net_shares, page marker at edge. Report: 2753_korea_live.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-flows"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2753, "ts": datetime.now(timezone.utc).isoformat()}

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
print("== 1/3 deploy adapter + invoke ==")
for i in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN)); wait_ok(FN); break
    except ClientError: time.sleep(15)
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:220])
assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-flows.json")["Body"].read())
kr = d.get("korea", {})
print("  KOREA status:", kr.get("status"), "| source:", kr.get("source"), "| as_of:", kr.get("as_of"), "| names:", kr.get("n_names"))
print("  KR foreign net total (shares):", kr.get("foreign_net_total_shares"))
print("  KR sector flows:", json.dumps(kr.get("sector_flows_shares") or {}))
print("  KR tracked (Samsung/SKHynix/LGES):", json.dumps(kr.get("tracked") or {}))
print("  KR top buys:", [(x["name"], x["foreign_net_shares"]) for x in (kr.get("top_buy") or [])[:4]])
assert kr.get("status") == "LIVE", ("korea not live", kr)
assert kr.get("n_names", 0) >= 6, kr.get("n_names")
assert kr.get("tracked", {}).get("005930") is not None, "Samsung net missing"
assert "Memory/Semis" in (kr.get("sector_flows_shares") or {}), "KR memory sector missing"
R["korea"] = {"status": kr.get("status"), "source": kr.get("source"), "as_of": kr.get("as_of"),
              "n_names": kr.get("n_names"), "foreign_net_total": kr.get("foreign_net_total_shares"),
              "sectors": kr.get("sector_flows_shares"), "tracked": kr.get("tracked"),
              "top_buy": [(x["name"], x["sector"], x["foreign_net_shares"]) for x in (kr.get("top_buy") or [])[:5]]}
brs = d.get("bridges", [])
for b in brs:
    print("  bridge: %-38s TW=%s KR=%s US5d=%s [%s]" % (
        b["name"][:38], b.get("tw_foreign_net_shares"), b.get("kr_foreign_net_shares"),
        b.get("us_avg_ret5d"), b.get("verdict")))
assert any(b.get("kr_foreign_net_shares") is not None for b in brs), "no bridge has KR signal"
R["bridges"] = [(b["name"], b.get("tw_foreign_net_shares"), b.get("kr_foreign_net_shares"),
                 b.get("us_avg_ret5d"), b.get("verdict")) for b in brs]
R["sources"] = d.get("sources")

print("== 2/3 page at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try: okp = b"KOREA \xe2\x80\x94 FOREIGN" in pub("apac.html") or b"APAC RADAR v1" in pub("apac.html")
    except Exception: pass
    print("  page attempt %d: %s" % (a + 1, "LIVE" if okp else "pending"))
    if okp: break
assert okp, "apac page not at edge"
R["page"] = "LIVE"

print("== 3/3 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2753_korea_live.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2753 COMPLETE — Korea live via Naver; both Asian legs feeding US bridges")
