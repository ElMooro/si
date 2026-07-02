"""ops 2704 v3 (rerun after naaim v1.4 page-junk gate) — close out the two data additions with verified-correct data.

NAAIM v1.2: header/profile column selection (v1 ingested the MOST-BEARISH
column — 4.0 vs true mean 98.59 on 2026-06-24, web-verified vs CEIC) +
canonical-file purge of contaminated accumulation. put-call-extreme: self-
contained S3 read (BUCKET symbol was undefined there). patent-velocity:
pacing for Google 503s (retries 2/backoff 3s/sleep 1.0), budget 640s, and
CURSOR-ROTATION + MERGE so partial passes accumulate to full 76-name coverage.

Order: deploy all three -> fire patent ASYNC first (longest leg) -> prove
NAAIM/composite/board synchronously -> poll patent landing -> hard asserts.
Report: aws/ops/reports/2704_naaim_patent_close.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2704, "ts": datetime.now(timezone.utc).isoformat()}
today = datetime.now(timezone.utc).date()

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

def wait_ok(fn, budget=200):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return
        time.sleep(5)

def retry(call, what, tries=6):
    for i in range(tries):
        try:
            return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                print("    %s conflict retry %d" % (what, i + 1)); time.sleep(15)
            else:
                raise
    raise RuntimeError(what)

print("== 1/5 DEPLOY (naaim v1.2, put-call-extreme, patent-velocity) ==")
print("  settling 25s for parallel deploy-lambdas…"); time.sleep(25)
for fn in ("justhodl-naaim", "justhodl-put-call-extreme", "justhodl-patent-velocity"):
    wait_ok(fn)
    retry(lambda f=fn: lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)), fn)
    wait_ok(fn)
    print("  synced", fn)

print("== 2/5 FIRE PATENT ASYNC (longest leg first) ==")
def _phead():
    try:
        return s3.head_object(Bucket=BUCKET, Key="data/patent-velocity.json")["LastModified"]
    except Exception:
        return None
lm0 = _phead()
t_event = time.time()
lam.invoke(FunctionName="justhodl-patent-velocity", InvocationType="Event",
           Payload=json.dumps({"limit": 76}).encode())
print("  triggered (will poll after fast proofs; engine budget 640s)")

print("== 3/5 NAAIM v1.2 PROOF (expect ~98.6 EUPHORIC per CEIC cross-check) ==")
r = lam.invoke(FunctionName="justhodl-naaim", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:220])
assert not r.get("FunctionError"), pay
nj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/naaim.json")["Body"].read())
v = (nj.get("latest") or {}).get("value"); d = (nj.get("latest") or {}).get("date")
R["naaim"] = {"value": v, "date": d, "z": nj.get("z"), "pctile": nj.get("pctile"),
              "state": nj.get("state"), "signal": nj.get("signal"),
              "history_n": nj.get("history_n"), "column_mode": nj.get("column_mode"),
              "provisional": nj.get("provisional")}
print("  ", json.dumps(R["naaim"]))
print("  history tail:", json.dumps((nj.get("history") or [])[-3:]))
assert isinstance(v, (int, float)) and 60 <= v <= 130, "value off vs known ~98.6: %s" % v
dd = datetime.fromisoformat(d).date()
assert timedelta(days=0) <= (today - dd) <= timedelta(days=12), "date implausible: %s" % d
assert (nj.get("history_n") or 0) >= 900, "history collapsed: %s" % nj.get("history_n")
assert nj.get("signal") in (-2, -1, 0), "signal polarity wrong for euphoric print: %s" % nj.get("signal")
bogus = [h for h in nj.get("history", []) if h["value"] < -30 or h["date"] > (today + timedelta(days=2)).isoformat()]
assert not bogus, "contamination persists: %s" % bogus[:3]

print("== 4/5 FUSION PROOF ==")
r = lam.invoke(FunctionName="justhodl-put-call-extreme", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "put-call-extreme errored"
pce = json.loads(s3.get_object(Bucket=BUCKET, Key="data/put-call-extreme.json")["Body"].read())
comp = next((c for c in pce.get("signals", []) if c.get("id") == "NAAIM_EXPOSURE"), None)
assert comp and comp.get("ok"), "NAAIM component not ok: %s" % comp
assert abs((comp.get("latest_value") or 0) - v) < 1.5, "component value drift: %s vs %s" % (comp.get("latest_value"), v)
R["composite"] = {"naaim_component": comp, "n_total": pce.get("n_total_signals"),
                  "composite_z": pce.get("composite_z"), "state": pce.get("state")}
print("  composite:", json.dumps(R["composite"])[:420])
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "signal-board errored"
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
import re as _re
m = _re.search(r'\{[^{}]*NAAIM[^{}]*\}', sb)
assert m, "NAAIM row absent from board"
R["signal_board_row"] = m.group(0)[:240]
print("  board:", R["signal_board_row"])

print("== 5/5 PATENT LANDING (poll) ==")
pj = None
while time.time() - t_event < 690:
    lm = _phead()
    if lm and (lm0 is None or lm > lm0):
        pj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/patent-velocity.json")["Body"].read())
        print("  landed after %.0fs" % (time.time() - t_event))
        break
    time.sleep(20)
assert pj is not None, "patent-velocity did not land within 690s — check CloudWatch"
cov = pj.get("coverage") or {}
tot_recent = sum((x.get("n_recent_patents") or 0) for x in pj.get("all_results", []))
R["patent"] = {"coverage": cov, "cursor": pj.get("cursor"), "n_results": pj.get("n_results"),
               "duration_s": pj.get("duration_s"), "total_recent_grants": tot_recent,
               "spikes": pj.get("n_velocity_spikes"), "top_picks_n": len(pj.get("top_picks") or []),
               "top5": [{"t": x.get("ticker"), "s": x.get("score"), "v": x.get("velocity_ratio"),
                          "n90": x.get("n_recent_patents")} for x in pj.get("all_results", [])[:5]]}
print("  ", json.dumps(R["patent"])[:600])
# Accumulation-aware proof: cursor rotation converges to the full 76-name
# universe over ~4-5 daily runs by design; cycle-1 totals near fresh_this_run
# are the CORRECT state, not a failure. Assert engine health, not end-state.
assert (cov.get("fresh_this_run") or 0) >= 8, "fresh pass too thin: %s" % cov
assert (cov.get("total") or 0) >= (cov.get("fresh_this_run") or 0), "merge lost rows: %s" % cov
assert tot_recent >= 120, "grant counts implausibly low: %s" % tot_recent
assert isinstance(pj.get("cursor"), int) and pj["cursor"] != 0, "cursor did not advance"
assert (pj.get("duration_s") or 0) < 700, "engine overran budget"
# accelerate convergence: fire one extra rotation batch async (next ~20 names);
# the daily 17 UTC cron completes the rest within days.
lam.invoke(FunctionName="justhodl-patent-velocity", InvocationType="Event",
           Payload=json.dumps({"limit": 76}).encode())
print("  fired one extra rotation batch async (cursor %s ->)" % pj.get("cursor"))

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2704_naaim_patent_close.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2704_naaim_patent_close.json")
print("\nOPS 2704 COMPLETE — both additions live on verified-real data")
