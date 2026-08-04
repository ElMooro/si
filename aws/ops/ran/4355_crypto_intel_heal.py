"""ops 4355 — crypto-intel feed frozen at 2026-07-31 14:55 UTC. Diagnose + heal.

Context: /crypto/ page renders a 4-day-old snapshot. Capacity audit (4252)
shows work_collapse ~07-30 (58.3s -> 5.2s) alongside 4 other LLM engines;
4253 shows credential_ok:false. The engine's S3 put is at the very end of
the handler, so a frozen generated_at means invocations stopped, an
unhandled exception fires pre-put, or the put itself errors every run.

Phases (each fault-isolated; report is honest about anything unfixed):
 1. Function config: state + env key NAMES vs config.json expectation.
 2. Schedule binding: rule state -> targets -> lambda permission (ops 1955
    dead-binding pattern).
 3. CloudWatch: last 36h of events; last invocation time; error signatures.
 4. Heal: merge-restore missing env keys from SSM (names only, never values;
    NEVER replace-wipe Variables); rebuild broken rule bindings; enable rule.
 5. Force invoke (RequestResponse) -> re-head crypto-intel.json -> did
    LastModified advance past script start? Fetch generated_at + ok-source
    count to quantify remaining degradation.
Idempotent. Writes aws/ops/reports/4355_crypto_intel_heal.{json,md}.
"""
import json, os, re, time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config

REGION = "us-east-1"
FN = "justhodl-crypto-intel"
RULE = "justhodl-crypto-15min"
BUCKET = "justhodl-dashboard-live"
KEY = "crypto-intel.json"
EXPECTED_ENV = ["ANTHROPIC_API_KEY", "CMC_API_KEY", "S3_BUCKET"]

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240, retries={"max_attempts": 0}))
ev = boto3.client("events", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

t0 = datetime.now(timezone.utc)
R = {"ops": 4355, "started": t0.isoformat(), "phases": {}, "healed": [], "blocked": []}

def phase(name):
    R["phases"][name] = {}
    return R["phases"][name]

# ---------- 1. function config ----------
p = phase("function")
try:
    cfg = lam.get_function_configuration(FunctionName=FN)
    cur_vars = (cfg.get("Environment", {}) or {}).get("Variables", {}) or {}
    p["state"] = cfg.get("State")
    p["last_modified"] = cfg.get("LastModified")
    p["timeout"] = cfg.get("Timeout")
    p["env_keys_present"] = sorted(cur_vars.keys())
    p["env_keys_missing"] = [k for k in EXPECTED_ENV if k not in cur_vars]
    p["env_keys_empty"] = [k for k in EXPECTED_ENV if cur_vars.get(k, None) == ""]
except Exception as e:
    p["error"] = f"{type(e).__name__}: {e}"
    cur_vars = {}

# ---------- 2. schedule binding ----------
p = phase("schedule")
rule_arn = None
try:
    r = ev.describe_rule(Name=RULE)
    rule_arn = r["Arn"]
    p["rule_state"] = r.get("State")
    p["schedule"] = r.get("ScheduleExpression")
    tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
    p["targets"] = [t.get("Arn", "")[-60:] for t in tg]
    p["target_hits_fn"] = any(FN in (t.get("Arn") or "") for t in tg)
    try:
        pol = json.loads(lam.get_policy(FunctionName=FN)["Policy"])
        p["permission_for_rule"] = any(
            rule_arn == (s.get("Condition", {}).get("ArnLike", {}) or {}).get("AWS:SourceArn")
            for s in pol.get("Statement", []))
    except lam.exceptions.ResourceNotFoundException:
        p["permission_for_rule"] = False
except Exception as e:
    p["error"] = f"{type(e).__name__}: {e}"

# ---------- 3. logs: what actually happened ----------
p = phase("logs")
try:
    lg = f"/aws/lambda/{FN}"
    since = int((t0 - timedelta(hours=36)).timestamp() * 1000)
    events, tok = [], None
    while True:
        kw = dict(logGroupName=lg, startTime=since, limit=800)
        if tok: kw["nextToken"] = tok
        resp = logs.filter_log_events(**kw)
        events += resp.get("events", [])
        tok = resp.get("nextToken")
        if not tok or len(events) > 4000: break
    p["events_36h"] = len(events)
    starts = [e for e in events if "START RequestId" in e["message"]]
    reports = [e["message"] for e in events if e["message"].startswith("REPORT")]
    p["invocations_36h"] = len(starts)
    p["last_invocation"] = (datetime.fromtimestamp(starts[-1]["timestamp"] / 1000, timezone.utc).isoformat()
                            if starts else None)
    durs = [float(m.group(1)) for m in (re.search(r"Duration: ([\d.]+) ms", x) for x in reports) if m]
    p["recent_durations_s"] = [round(d / 1000, 1) for d in durs[-6:]]
    sig = {}
    for e2 in events:
        m = e2["message"]
        if any(k in m for k in ("ERROR", "Error", "Traceback", "S3 ERR", "errorMessage", "Task timed out")):
            key = m.strip()[:160]
            sig[key] = sig.get(key, 0) + 1
    p["error_signatures"] = dict(sorted(sig.items(), key=lambda kv: -kv[1])[:12])
    tb = [e2["message"] for e2 in events if "Traceback" in e2["message"] or '"stackTrace"' in e2["message"]]
    p["last_traceback"] = tb[-1][:2000] if tb else None
except Exception as e:
    p["error"] = f"{type(e).__name__}: {e}"

# ---------- 3b. S3 staleness (server-side truth) ----------
p = phase("s3_before")
try:
    h = s3.head_object(Bucket=BUCKET, Key=KEY)
    p["last_modified"] = h["LastModified"].isoformat()
    p["bytes"] = h["ContentLength"]
    p["stale_hours"] = round((t0 - h["LastModified"]).total_seconds() / 3600, 1)
except Exception as e:
    p["error"] = f"{type(e).__name__}: {e}"

# ---------- 4. heal: env (merge-only, from SSM, names only) ----------
missing = R["phases"]["function"].get("env_keys_missing", []) + \
          R["phases"]["function"].get("env_keys_empty", [])
if missing:
    found = {}
    try:
        params, tok = [], None
        while True:
            kw = dict(Path="/justhodl/", Recursive=True, WithDecryption=True, MaxResults=10)
            if tok: kw["NextToken"] = tok
            resp = ssm.get_parameters_by_path(**kw)
            params += resp.get("Parameters", [])
            tok = resp.get("NextToken")
            if not tok: break
        for want in missing:
            for pa in params:
                if want.lower() in pa["Name"].lower() and pa.get("Value"):
                    found[want] = pa["Value"]; break
    except Exception as e:
        R["blocked"].append(f"SSM lookup failed: {type(e).__name__}: {e}")
    if found:
        try:
            merged = {**cur_vars, **found}          # merge — never replace-wipe
            lam.update_function_configuration(FunctionName=FN,
                                              Environment={"Variables": merged})
            waiter = lam.get_waiter("function_updated_v2")
            waiter.wait(FunctionName=FN)
            R["healed"].append(f"env restored from SSM (names only): {sorted(found.keys())}")
        except Exception as e:
            R["blocked"].append(f"env restore failed: {type(e).__name__}: {e}")
    still = [k for k in missing if k not in found]
    if still:
        R["blocked"].append(f"env keys absent + not in SSM /justhodl/: {still} — needs key from Khalid")

# ---------- 4b. heal: schedule binding ----------
sch = R["phases"]["schedule"]
try:
    if sch.get("rule_state") == "DISABLED":
        ev.enable_rule(Name=RULE); R["healed"].append("rule ENABLED")
    if sch.get("rule_state") and not sch.get("target_hits_fn"):
        fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
        ev.put_targets(Rule=RULE, Targets=[{"Id": "crypto-intel", "Arn": fn_arn}])
        R["healed"].append("rule target rebuilt")
    if rule_arn and sch.get("permission_for_rule") is False:
        try:
            lam.add_permission(FunctionName=FN, StatementId=f"ops4355-{RULE}",
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com", SourceArn=rule_arn)
            R["healed"].append("invoke permission re-added")
        except lam.exceptions.ResourceConflictException:
            pass
except Exception as e:
    R["blocked"].append(f"binding heal failed: {type(e).__name__}: {e}")

# ---------- 5. force invoke + verify fresh write ----------
p = phase("invoke")
try:
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    p["status_code"] = inv.get("StatusCode")
    p["function_error"] = inv.get("FunctionError")
    body = inv["Payload"].read().decode()[:1200]
    p["payload_head"] = body
except Exception as e:
    p["error"] = f"{type(e).__name__}: {e}"

p = phase("s3_after")
try:
    time.sleep(3)
    h = s3.head_object(Bucket=BUCKET, Key=KEY)
    lm = h["LastModified"]
    p["last_modified"] = lm.isoformat()
    p["bytes"] = h["ContentLength"]
    p["fresh_write"] = lm > t0
    if p["fresh_write"]:
        obj = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
        p["generated_at"] = obj.get("generated_at")
        p["version"] = obj.get("version")
        p["fetch_time_s"] = obj.get("fetch_time")
        oks = [k for k, v in obj.items() if isinstance(v, dict) and v.get("status") == "ok"]
        p["sources_ok"] = len(oks)
        p["sources_ok_names"] = sorted(oks)[:25]
except Exception as e:
    p["error"] = f"{type(e).__name__}: {e}"

# ---------- 5b. if the invoke crashed, grab the fresh traceback ----------
if R["phases"]["invoke"].get("function_error"):
    try:
        resp = logs.filter_log_events(logGroupName=f"/aws/lambda/{FN}",
                                      startTime=int(t0.timestamp() * 1000), limit=400)
        msgs = [e["message"] for e in resp.get("events", [])]
        R["phases"]["invoke"]["fresh_error_tail"] = "".join(msgs)[-3000:]
    except Exception as e:
        R["phases"]["invoke"]["fresh_log_error"] = f"{type(e).__name__}: {e}"

R["finished"] = datetime.now(timezone.utc).isoformat()
R["verdict"] = ("HEALED — fresh write confirmed"
                if R["phases"].get("s3_after", {}).get("fresh_write")
                else "NOT WRITING — see error_signatures / fresh_error_tail / blocked")

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/4355_crypto_intel_heal.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
md = [f"# ops 4355 — crypto-intel heal — {R['verdict']}",
      f"- feed before: {R['phases'].get('s3_before',{}).get('last_modified')} "
      f"({R['phases'].get('s3_before',{}).get('stale_hours')}h stale)",
      f"- invocations last 36h: {R['phases'].get('logs',{}).get('invocations_36h')} "
      f"(last: {R['phases'].get('logs',{}).get('last_invocation')})",
      f"- recent durations: {R['phases'].get('logs',{}).get('recent_durations_s')}",
      f"- env missing/empty: {R['phases'].get('function',{}).get('env_keys_missing')}"
      f" / {R['phases'].get('function',{}).get('env_keys_empty')}",
      f"- rule: {R['phases'].get('schedule',{}).get('rule_state')} "
      f"target_ok={R['phases'].get('schedule',{}).get('target_hits_fn')} "
      f"perm_ok={R['phases'].get('schedule',{}).get('permission_for_rule')}",
      f"- healed: {R['healed'] or 'nothing needed / nothing healable'}",
      f"- blocked: {R['blocked'] or 'none'}",
      f"- after: fresh_write={R['phases'].get('s3_after',{}).get('fresh_write')} "
      f"generated_at={R['phases'].get('s3_after',{}).get('generated_at')} "
      f"sources_ok={R['phases'].get('s3_after',{}).get('sources_ok')}"]
with open("aws/ops/reports/4355_crypto_intel_heal.md", "w") as f:
    f.write("\n".join(md) + "\n")
print(json.dumps(R, indent=1, default=str))
