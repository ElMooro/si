"""ops 2702 — ship the last two genuine data additions from the fleet audit:
NAAIM Exposure Index (new weekly engine, fused into the sentiment composite +
signal-board) and patent-velocity STUB COMPLETION (keyless Google Patents
grant-window counts; original scoring pipeline untouched; harvester top_picks).

Chain proven end-to-end under the ops-2701 truth-gate. Report committed to
aws/ops/reports/2702_naaim_patent_live.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET, ACCT = "us-east-1", "justhodl-dashboard-live", "857687956942"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, connect_timeout=15, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2702, "ts": datetime.now(timezone.utc).isoformat()}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

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

def wait_ok(fn, budget=240):
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
                print("    %s conflict retry %d" % (what, i + 1)); time.sleep(18)
            else:
                raise
    raise RuntimeError(what)

def ensure_rule(fn, name, expr, desc):
    arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, fn)
    rule_arn = ev.put_rule(Name=name, ScheduleExpression=expr, State="ENABLED", Description=desc)["RuleArn"]
    try:
        lam.add_permission(FunctionName=fn, StatementId="evt-" + name, Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    ev.put_targets(Rule=name, Targets=[{"Id": "1", "Arn": arn}])

sect("1/5 CREATE/UPDATE justhodl-naaim + weekly rule")
print("  settling 30s for parallel deploy-lambdas…"); time.sleep(30)
cfg = json.load(open("aws/lambdas/justhodl-naaim/config.json"))
zb = zip_fn("justhodl-naaim")
try:
    lam.get_function(FunctionName="justhodl-naaim")
    wait_ok("justhodl-naaim")
    retry(lambda: lam.update_function_code(FunctionName="justhodl-naaim", ZipFile=zb), "naaim code")
    print("  existed -> code updated")
except lam.exceptions.ResourceNotFoundException:
    retry(lambda: lam.create_function(FunctionName="justhodl-naaim", Runtime=cfg["runtime"],
          Role=cfg["role"], Handler=cfg["handler"], Code={"ZipFile": zb},
          Timeout=cfg["timeout"], MemorySize=cfg["memory"], Architectures=cfg["architectures"],
          Description=cfg["description"]), "naaim create")
    print("  created")
wait_ok("justhodl-naaim")
ensure_rule("justhodl-naaim", cfg["schedule"]["name"], cfg["schedule"]["expression"], cfg["schedule"]["description"])
print("  rule:", cfg["schedule"]["expression"])

sect("2/5 RUN NAAIM (sync)")
r = lam.invoke(FunctionName="justhodl-naaim", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:200])
assert not r.get("FunctionError"), "naaim errored: %s" % pay
nj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/naaim.json")["Body"].read())
v = (nj.get("latest") or {}).get("value")
assert isinstance(v, (int, float)) and -200 <= v <= 220, "naaim value insane: %s" % v
R["naaim"] = {"value": v, "date": nj["latest"]["date"], "z": nj.get("z"), "pctile": nj.get("pctile"),
              "state": nj.get("state"), "signal": nj.get("signal"),
              "history_n": nj.get("history_n"), "provisional": nj.get("provisional")}
print("  ", json.dumps(R["naaim"]))

sect("3/5 DEPLOY patched engines")
for fn in ("justhodl-patent-velocity", "justhodl-put-call-extreme", "justhodl-signal-board"):
    wait_ok(fn)
    retry(lambda f=fn: lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)), fn)
    wait_ok(fn)
    print("  synced", fn)
retry(lambda: lam.update_function_configuration(FunctionName="justhodl-patent-velocity", Timeout=780), "patent cfg")
wait_ok("justhodl-patent-velocity")
sch = json.load(open("aws/lambdas/justhodl-patent-velocity/config.json")).get("schedule") or {}
ensure_rule("justhodl-patent-velocity", sch.get("rule_name") or sch.get("name") or "patent-velocity-daily",
            sch.get("cron") or sch.get("expression") or "cron(0 17 * * ? *)",
            sch.get("description", ""))
print("  patent rule ensured")

sect("4/5 RUN PATENT-VELOCITY (async + poll — long sync invokes drop the runner's HTTP connection)")
def _patent_head():
    try:
        h = s3.head_object(Bucket=BUCKET, Key="data/patent-velocity.json")
        return h["LastModified"]
    except Exception:
        return None
lm0 = _patent_head()
fresh0 = lm0 and (datetime.now(timezone.utc) - lm0).total_seconds() < 1500
pj = None
if fresh0:
    _cand = json.loads(s3.get_object(Bucket=BUCKET, Key="data/patent-velocity.json")["Body"].read())
    if (_cand.get("n_results") or 0) >= 40 and "needs_api_key" not in json.dumps(_cand)[:1500]:
        pj = _cand
        print("  fresh output already on S3 (age %.0fs, likely the disconnected prior invoke) — using it"
              % (datetime.now(timezone.utc) - lm0).total_seconds())
if pj is None:
    lam.invoke(FunctionName="justhodl-patent-velocity", InvocationType="Event",
               Payload=json.dumps({"limit": 76}).encode())
    print("  triggered async; polling S3 for a newer object (budget 8.5 min)…")
    t0 = time.time()
    while time.time() - t0 < 510:
        time.sleep(20)
        lm = _patent_head()
        if lm and (lm0 is None or lm > lm0):
            pj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/patent-velocity.json")["Body"].read())
            print("  landed after %.0fs" % (time.time() - t0))
            break
    assert pj is not None, "patent-velocity did not land within budget — check its CloudWatch logs"
tot_recent = sum((x.get("n_recent_patents") or 0) for x in pj.get("all_results", []))
R["patent"] = {"n_results": pj.get("n_results"), "universe": pj.get("universe_size"),
               "duration_s": pj.get("duration_s"), "total_recent_grants": tot_recent,
               "spikes": pj.get("n_velocity_spikes"), "top_picks_n": len(pj.get("top_picks") or []),
               "top5": [{"t": x.get("ticker"), "s": x.get("score"), "v": x.get("velocity_ratio"),
                          "n90": x.get("n_recent_patents")} for x in pj.get("all_results", [])[:5]],
               "data_source": pj.get("data_source")}
print("  ", json.dumps(R["patent"])[:600])
assert (pj.get("n_results") or 0) >= 40, "too few patent results: %s" % pj.get("n_results")
assert tot_recent >= 100, "google counts look blocked/zero (total_recent=%s)" % tot_recent
assert "needs_api_key" not in json.dumps(pj)[:2000], "stub gate still active"

sect("5/5 FUSION PROOF + REPORT")
r = lam.invoke(FunctionName="justhodl-put-call-extreme", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "put-call-extreme errored"
pce = json.loads(s3.get_object(Bucket=BUCKET, Key="data/put-call-extreme.json")["Body"].read())
comp = next((c for c in pce.get("signals", []) if c.get("id") == "NAAIM_EXPOSURE"), None)
assert comp, "NAAIM component absent from composite"
R["composite"] = {"naaim_component": comp, "n_total": pce.get("n_total_signals"),
                  "composite_z": pce.get("composite_z"), "state": pce.get("state")}
print("  composite:", json.dumps(R["composite"])[:420])

r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "signal-board errored"
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "NAAIM" in sb, "NAAIM feed absent from signal-board"
import re as _re
m = _re.search(r'\{[^{}]*NAAIM[^{}]*\}', sb)
R["signal_board_row"] = (m.group(0)[:220] if m else "present")
print("  signal-board:", R["signal_board_row"])

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2702_naaim_patent_live.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2702_naaim_patent_live.json")
print("\nOPS 2702 COMPLETE")
