"""ops 2703 — NAAIM data-quality fix (v1.1) + patent-velocity Google-egress DIAGNOSIS: file-first with hard-gated page
scrape + contamination scrub. v1's loose regex ingested a stray "1.0" dated in
the FUTURE (2026-08-01), fabricating a WASHED_OUT z=-3.33 panic signal. This op
redeploys, re-runs, hard-asserts sanity, and re-proves the fused consumers with
the corrected print. Report: aws/ops/reports/2703_naaim_quality_fix.json.

Patent diagnosis (data before decisions): tail the engine's CloudWatch for the
exact [patent] google errors AND run one identical count query from THIS runner
(different egress than Lambda) — splits IP-blocking from contract error.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2703, "ts": datetime.now(timezone.utc).isoformat()}

def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"):
                z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()

def wait_ok(fn, budget=180):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return
        time.sleep(5)

def retry(call, what, tries=5):
    for i in range(tries):
        try:
            return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(15)
            else:
                raise
    raise RuntimeError(what)


print("== 0/4 PATENT GOOGLE-EGRESS DIAGNOSIS ==")
logs = boto3.client("logs", region_name=REGION)
try:
    ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-patent-velocity",
                                startTime=int((time.time() - 3600) * 1000),
                                filterPattern='"[patent]"', limit=60)
    lines = sorted({e["message"].strip()[:150] for e in ev.get("events", [])})
    R["patent_cloudwatch"] = lines[:20]
    print("  cloudwatch [patent] lines (%d unique):" % len(lines))
    for L in lines[:14]:
        print("   ", L)
except Exception as e:
    R["patent_cloudwatch"] = ["log read failed: " + str(e)[:100]]
    print("  ", R["patent_cloudwatch"][0])

import urllib.request, urllib.parse, re as _re2
def runner_count(assignee="NVIDIA Corporation"):
    inner = ('q=(assignee:"%s")&country=US&status=GRANT&type=PATENT'
             "&after=publication:20260401&before=publication:20260630") % assignee
    url = "https://patents.google.com/xhr/query?url=" + urllib.parse.quote(inner, safe="")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                                                   "Accept": "application/json",
                                                   "Referer": "https://patents.google.com/"})
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8", "ignore")
        mm = _re2.search(r'"total_num_results"\s*:\s*(\d+)', body)
        return {"http": 200, "count": int(mm.group(1)) if mm else None,
                "body_head": body[:180].replace("\n", " ")}
    except urllib.error.HTTPError as e:
        return {"http": e.code, "body_head": (e.read() or b"")[:160].decode("utf-8", "ignore")}
    except Exception as e:
        return {"err": str(e)[:120]}
R["runner_probe"] = runner_count()
print("  runner-side probe:", json.dumps(R["runner_probe"])[:260])

print("== 1/4 redeploy naaim v1.1 + rerun ==")
wait_ok("justhodl-naaim")
retry(lambda: lam.update_function_code(FunctionName="justhodl-naaim", ZipFile=zip_fn("justhodl-naaim")), "naaim")
wait_ok("justhodl-naaim")
r = lam.invoke(FunctionName="justhodl-naaim", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:200])
assert not r.get("FunctionError"), pay
nj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/naaim.json")["Body"].read())
v = (nj.get("latest") or {}).get("value")
d = (nj.get("latest") or {}).get("date")
z = nj.get("z")
today = datetime.now(timezone.utc).date()
assert isinstance(v, (int, float)) and 3 <= v <= 200, "value still insane: %s" % v
assert d and datetime.fromisoformat(d).date() <= today + timedelta(days=2), "future date persists: %s" % d
assert z is None or abs(z) <= 3.2, "z outlier persists: %s" % z
bogus = [h for h in nj.get("history", []) if h["date"] > (today + timedelta(days=2)).isoformat() or h["value"] < 0]
assert not bogus, "contaminated rows persist: %s" % bogus[:3]
R["naaim"] = {"value": v, "date": d, "z": z, "pctile": nj.get("pctile"),
              "state": nj.get("state"), "signal": nj.get("signal"),
              "history_n": nj.get("history_n"), "provisional": nj.get("provisional")}
print("  SANE:", json.dumps(R["naaim"]))

print("== 2/4 re-prove fused consumers on corrected print ==")
r = lam.invoke(FunctionName="justhodl-put-call-extreme", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "put-call-extreme errored"
pce = json.loads(s3.get_object(Bucket=BUCKET, Key="data/put-call-extreme.json")["Body"].read())
comp = next((c for c in pce.get("signals", []) if c.get("id") == "NAAIM_EXPOSURE"), None)
assert comp and comp.get("ok"), "NAAIM component not ok: %s" % comp
assert abs(comp.get("z_raw") or 9) <= 3.2
R["composite"] = {"naaim_component": comp, "n_total": pce.get("n_total_signals"),
                  "composite_z": pce.get("composite_z"), "state": pce.get("state")}
print("  composite:", json.dumps(R["composite"])[:400])
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "signal-board errored"
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "NAAIM" in sb
import re as _re
m = _re.search(r'\{[^{}]*NAAIM[^{}]*\}', sb)
R["signal_board_row"] = m.group(0)[:220] if m else "present"
print("  board:", R["signal_board_row"])

print("== 3/4 report ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2703_naaim_quality_fix.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2703 COMPLETE")
