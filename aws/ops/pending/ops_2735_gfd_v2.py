"""ops 2735 — GLOBAL FLOW DESK v2 (Khalid: page needs more improvements).

Engine v1.1.0: AI brief under the footprint OUTPUT CONTRACT (_clean_brief
strips reasoning scaffold — the live page was rendering a full 4KB
"Analyze the Request" chain — sentence-complete <=700c, deterministic
3-sentence fallback, source tag); WARMING gate (exact-$0 5d flow = degenerate
fresh history -> warming list, no fake zeros for India/Brazil); aaii_spread_pct
(kills 0.08800000000000002); daily history (inst/retail/eq_us/tips, 400d S3 +
60d in feed) for sparklines; edge cache 900->60s. Page v2: hero dials w/
sparklines, hot-money hbar map w/ per-country ETF/FX/TIC components + WARMING
chip, ladder+sector hbars, capex card, brief card w/ src tag. First-party
pages.yml deploys the page independently (~90s) — hard marker gate is now
reliable in-ops. Report: aws/ops/reports/2735_gfd_v2.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2735, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 Chrome/126", "Cache-Control": "no-cache"}
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
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(5)
def retry(call, what, tries=6):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(what)
def fetch(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=25) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as he:
        return he.code, he.read()[:200]
    except Exception as e:
        return None, str(e)[:100].encode()

print("settling 30s…"); time.sleep(30)
print("== 1/3 engine v1.1.0 ==")
retry(lambda: (wait_ok("justhodl-global-flow-desk"), lam.update_function_code(FunctionName="justhodl-global-flow-desk", ZipFile=zip_fn("justhodl-global-flow-desk")))[-1], "gfd")
wait_ok("justhodl-global-flow-desk")
r = lam.invoke(FunctionName="justhodl-global-flow-desk", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:260])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-flow-desk.json")["Body"].read())
assert d["version"] == "1.1.0"
br = d.get("ai_brief") or ""
R["brief"] = {"src": d.get("ai_brief_src"), "len": len(br),
              "clean": br.rstrip().endswith((".", "!", "?")) and "Analyze the Request" not in br and "**" not in br,
              "head": br[:200]}
print("  brief[%s|%d]: %s" % (R["brief"]["src"], len(br), br[:170]))
assert 90 <= len(br) <= 760 and R["brief"]["clean"], "brief contract violated"
IR = d["inst_vs_retail"]
assert "aaii_spread_pct" in IR
zero_ctry = [c for c, e in d["hot_money"]["countries"].items() if e.get("etf_5d_usd_m") == 0.0]
R["hot"] = {"scored": d["hot_money"]["n_scored"], "warming": len(d["hot_money"].get("warming_etfs") or []),
            "zero_flow_countries": zero_ctry, "aaii_pct": IR["aaii_spread_pct"]}
print("  countries:", R["hot"])
assert not zero_ctry, "fake-zero countries survived: %s" % zero_ctry
assert len(d.get("history") or []) >= 1 and d["history"][-1].get("inst") is not None
print("  history:", len(d["history"]), "| sectors ranked:", len(d["sectors"]["ranked"]))

print("== 2/3 page v2 at edge (first-party pages.yml, ~90s) ==")
okp = False
for a in range(5):
    time.sleep(45)
    st, b = fetch("https://justhodl.ai/global-flow-desk.html?v=%d" % a)
    okp = st == 200 and b"FLOW DESK v2" in b and b"S.ranked" in b
    print("  attempt %d: %s %s" % (a + 1, st, "v2 LIVE" if okp else "pending"))
    if okp: break
assert okp, "page v2 not at edge"
st2, b2 = fetch("https://justhodl.ai/data/global-flow-desk.json?v=9")
json.loads(b2.decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
assert st2 == 200
R["page"], R["feed_strict"] = "LIVE_v2", True

print("== 3/3 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2735_gfd_v2.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2735 COMPLETE — the flow desk earns its name")
