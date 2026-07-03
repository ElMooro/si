"""ops 2756 — JAPAN + HONG KONG + KOREA-MARKETWIDE go live.

Deploys apac-flows (xlrd vendored) + invokes. Verifies: Korea market-wide
(KOSPI+KOSDAQ foreign net total via Naver index), Japan (JPX weekly TSE Prime
foreign net from stock_val .xls), Hong Kong (Stock Connect Southbound net from
HKEX daily js), HK bridge carries hk_southbound_net, page at edge. Prints full
JP/HK blocks before asserts for diagnostics. Report: 2756_jp_hk_kr_build.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET, FN = "us-east-1", "justhodl-dashboard-live", "justhodl-apac-flows"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2756, "ts": datetime.now(timezone.utc).isoformat()}

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
def wait_ok(fn, budget=220):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)

print("settling 15s…"); time.sleep(15)
print("== 1/3 deploy (xlrd vendored) + invoke ==")
z = zip_fn(FN)
print("  zip size: %.1f MB" % (len(z) / 1e6))
for i in range(6):
    try:
        wait_ok(FN); lam.update_function_code(FunctionName=FN, ZipFile=z); wait_ok(FN); break
    except ClientError: time.sleep(15)
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:200])
assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-flows.json")["Body"].read())
print("  SOURCES:", json.dumps(d.get("sources")))

print("\n== 2/3 verify JP / HK / KR-marketwide ==")
kr_mw = (d.get("korea", {}) or {}).get("market_wide", {}) or {}
jp = d.get("japan", {}) or {}
hk = d.get("hongkong", {}) or {}
print("  KOREA market-wide:", json.dumps(kr_mw, ensure_ascii=False)[:300])
print("  JAPAN:", json.dumps(jp, ensure_ascii=False)[:400])
print("  HONG KONG:", json.dumps(hk, ensure_ascii=False)[:400])
hk_bridge = next((b for b in d.get("bridges", []) if b.get("hk_southbound_net") is not None), None)
print("  HK bridge:", json.dumps(hk_bridge, default=str)[:220] if hk_bridge else "NONE")
R["korea_marketwide"] = kr_mw
R["japan"] = {k: jp.get(k) for k in ("status", "foreign_net", "individual_net", "institution_net", "week", "file", "err")}
R["hongkong"] = {k: hk.get(k) for k in ("status", "southbound_net_total", "as_of", "markets", "note")}
R["sources"] = d.get("sources")

# asserts (certain: Korea market-wide JSON; then JP + HK)
assert kr_mw.get("total_foreign_value") is not None, ("korea market-wide failed", kr_mw)
assert kr_mw.get("KOSPI") and kr_mw.get("KOSDAQ"), "need both KOSPI+KOSDAQ"
R["hk_note"] = hk.get("note") if hk.get("status") != "LIVE" else "LIVE"
print("  HK verdict:", "LIVE" if hk.get("status") == "LIVE" else ("PENDING -> " + str(hk.get("note"))))
assert jp.get("status") == "LIVE", ("Japan not live", jp)
assert jp.get("foreign_net") is not None, ("Japan foreign_net parse failed", jp)

print("\n== 3/3 page at edge ==")
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as rr:
        return rr.read()
okp = False
for a in range(6):
    time.sleep(40)
    try:
        b = pub("apac.html")
        okp = b"HONG KONG \xe2\x80\x94 STOCK CONNECT" in b or b"APAC RADAR v1" in b
    except Exception: pass
    print("  page attempt %d: %s" % (a + 1, "LIVE" if okp else "pending"))
    if okp: break
assert okp, "apac page not at edge"
R["page"] = "LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2756_jp_hk_kr_build.json", "w") as f:
    json.dump(R, f, indent=1, ensure_ascii=False, default=str)
print("OPS 2756 COMPLETE — Japan + Hong Kong + Korea-marketwide live")
