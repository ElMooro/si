"""ops 2730 — SURVEILLANCE DESK v2.0 (Khalid: major page improvements + AI + visuals).

Engine v1.2.0: posture HISTORY (sparkline substrate, 400d S3 + 60d in feed);
AI dossier hardened — sentence-complete truncation guard + DETERMINISTIC
fallback synthesis when LLM offline (never empty, never mid-sentence);
wholesaler extremes filtered to real names (no De-Minimis artifacts, >=5
firms, >=50M sh); 13F adds/exits deduped w/ position $; PD tenor curve
embedded. Page v2.0: sparkline dials, PD curve SVG chart, dark-$/sector +
class + complex bar charts, formatted CFTC k-contracts, 13F $ tags, feed
health strip — and the $NaNB bug (stale dark_usd_5d key) eliminated.
Report: aws/ops/reports/2730_desk_v2.json.
"""
import os, io, json, time, zipfile, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2730, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 Chrome/126", "Accept": "*/*", "Cache-Control": "no-cache"}
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
print("== 1/3 engine v1.2.0 ==")
retry(lambda: (wait_ok("justhodl-institutional-footprint"), lam.update_function_code(FunctionName="justhodl-institutional-footprint", ZipFile=zip_fn("justhodl-institutional-footprint")))[-1], "fp")
wait_ok("justhodl-institutional-footprint")
r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
assert d["version"] == "1.2.0"
br = d.get("ai_dossier") or ""
R["dossier"] = {"src": d.get("ai_dossier_src"), "len": len(br), "ends_clean": br.rstrip().endswith((".", "!", "?")),
                "head": br[:180]}
print("  dossier:", json.dumps(R["dossier"], default=str)[:280])
assert len(br) >= 120 and R["dossier"]["ends_clean"], "dossier still weak"
R["history_n"] = len(d.get("history") or [])
assert R["history_n"] >= 1 and d["history"][-1].get("now") is not None
wl = d["dark_pool_footprint"].get("wholesaler_extremes") or []
R["wholesaler"] = [(w["ticker"], w["top_firm"][:18], w["top_pct"]) for w in wl[:5]]
print("  wholesaler (filtered):", R["wholesaler"])
assert all("de minimis" not in str(w.get("top_firm", "")).lower() for w in wl)
bu = d["stocks_usd_13f"]["buys"]
assert len(bu) == len({b["t"] for b in bu}), "13F dupes survived"
assert sum(1 for b in bu if b.get("usd_m") is not None) >= 3
assert (d.get("pd") or {}).get("coupons_by_tenor_b"), "pd curve missing"
assert isinstance(d.get("feeds_total"), int)
print("  history:", R["history_n"], "| pd tenors:", len(d["pd"]["coupons_by_tenor_b"]),
      "| adds deduped:", len(d["stocks"]["institutions_buying_13f"]))

print("== 2/3 page v2.0 marker + strict feed ==")
okp = okf = False
for attempt in range(4):
    time.sleep(65 if attempt == 0 else 45)
    st, b = fetch("https://justhodl.ai/institutional-footprint.html?v=%d" % attempt)
    okp = st == 200 and b"PD TREASURY CURVE" in b and b"dark_daily_usd_m_est" in b
    st2, b2 = fetch("https://justhodl.ai/data/institutional-footprint.json?v=%d" % attempt)
    try:
        json.loads(b2.decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        okf = st2 == 200
    except Exception:
        okf = False
    print("  attempt %d: page=%s(%s) feed_strict=%s" % (attempt + 1, st, "v2" if okp else "old", okf))
    if okp and okf: break
R["page"] = "LIVE_v2" if okp else "stale"
R["feed_strict"] = okf
assert okp and okf, "page/feed not proven at edge"

print("== 3/3 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2730_desk_v2.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2730 COMPLETE — the desk looks like it belongs on a trading floor")
