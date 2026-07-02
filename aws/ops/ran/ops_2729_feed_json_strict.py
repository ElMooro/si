"""ops 2729 — SURVEILLANCE PAGE DOWN: strict-JSON diagnosis + hardening.

Khalid: institutional-footprint.html shows "Feed unavailable" — the browser's
strict JSON.parse rejects the feed while every server-side (Python) assert
passes. Prime suspect: NaN/Infinity emitted by json.dumps. This ops (a)
fetches the LIVE public URL from the runner and strict-parses it, (b) walks
the S3 object for non-finite values with paths, (c) deploys _finite()
sanitizer + allow_nan=False into footprint v1.1.4 and gfd v1.0.3, (d)
re-proves the public URL parses strictly. Report: 2729_feed_json_strict.json.
"""
import os, io, json, math, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2729, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126", "Accept": "application/json",
      "Cache-Control": "no-cache"}
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
            b = r.read()
            return r.status, b
    except urllib.error.HTTPError as he:
        return he.code, he.read()[:300]
    except Exception as e:
        return None, str(e)[:120].encode()
def strict(b):
    try:
        json.loads(b.decode("utf-8"), parse_constant=lambda x: (_ for _ in ()).throw(ValueError("nonfinite:" + x)))
        return "VALID", None
    except Exception as e:
        return "FAIL", str(e)[:110]
def scan_nonfinite(obj, path="$", out=None):
    if out is None: out = []
    if len(out) >= 8: return out
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        out.append(path)
    elif isinstance(obj, dict):
        for k, v in obj.items(): scan_nonfinite(v, path + "." + str(k)[:24], out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:60]): scan_nonfinite(v, path + "[%d]" % i, out)
    return out

print("settling 30s…"); time.sleep(30)
print("== 1/4 LIVE URL diagnosis (runner egress) ==")
for name in ("institutional-footprint", "global-flow-desk", "naaim"):
    st, b = fetch("https://justhodl.ai/data/%s.json?v=diag%d" % (name, int(time.time())))
    verdict, err = strict(b) if st == 200 else ("HTTP_%s" % st, (b or b"")[:90].decode("utf-8", "ignore"))
    toks = (b.count(b"NaN"), b.count(b"Infinity")) if isinstance(b, bytes) else (0, 0)
    print("  %-24s HTTP %s %6dB NaN/Inf=%s strict=%s %s" % (name, st, len(b or b""), toks, verdict, err or ""))
    R.setdefault("live", {})[name] = {"http": st, "bytes": len(b or b""), "nan_inf": toks, "strict": verdict, "err": err}

print("== 2/4 S3 object non-finite scan ==")
for key in ("data/institutional-footprint.json", "data/global-flow-desk.json"):
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    paths = scan_nonfinite(doc)
    print("  %s -> nonfinite paths: %s" % (key, paths or "NONE"))
    R.setdefault("s3_scan", {})[key] = paths

print("== 3/4 deploy sanitized engines + rerun ==")
for fn in ("justhodl-institutional-footprint", "justhodl-global-flow-desk"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn)
    r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
    assert not r.get("FunctionError"), (fn, r["Payload"].read()[:200])
    print("  reran", fn)
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
assert d["version"] == "1.1.4" and d["posture"]["risk_now"] is not None
assert not scan_nonfinite(d), "nonfinite survived sanitizer"

print("== 4/4 PROVE public URL strict-valid ==")
ok = False
for attempt in range(4):
    time.sleep(20)
    st, b = fetch("https://justhodl.ai/data/institutional-footprint.json?v=fix%d" % attempt)
    verdict, err = strict(b) if st == 200 else ("HTTP_%s" % st, None)
    print("  attempt %d: HTTP %s %dB strict=%s %s" % (attempt + 1, st, len(b or b""), verdict, err or ""))
    if st == 200 and verdict == "VALID":
        body = json.loads(b)
        assert body.get("posture", {}).get("risk_now") is not None
        ok = True; R["public_final"] = {"http": st, "strict": "VALID", "risk_now": body["posture"]["risk_now"]}
        break
assert ok, "public feed still not strict-valid"
st, b = fetch("https://justhodl.ai/institutional-footprint.html")
R["page_html"] = "OK" if (st == 200 and b"SURVEILLANCE DESK" in b) else "HTTP_%s" % st
print("  page html:", R["page_html"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2729_feed_json_strict.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2729 COMPLETE — feed strict-JSON valid at the edge")
