#!/usr/bin/env python3
"""ops 2930 — LAYER D: engine-side availability. Full S3 inventory x registry
mapping, freshness vs cadence, remediation of stale schedules, ici diagnostic."""
import boto3, json, re, sys, time
sys.path.insert(0, "aws/ops")
from ops_report import report

s3 = boto3.client("s3"); lam = boto3.client("lambda")
ev = boto3.client("events"); sched = boto3.client("scheduler")
logs = boto3.client("logs")
B = "justhodl-dashboard-live"
ok = True; out = {}

def s3_age_h(key):
    try:
        return (time.time() - s3.head_object(Bucket=B, Key=key)["LastModified"].timestamp())/3600
    except Exception:
        return None

with report("2930") as r:
    # ── inventory ──
    inv = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix="data/"):
        for o in page.get("Contents", []):
            inv[o["Key"]] = (time.time() - o["LastModified"].timestamp())/3600
    r.ok(f"S3 inventory: {len(inv)} objects under data/")

    reg = json.loads(s3.get_object(Bucket=B, Key="data/engine-registry.json")["Body"].read())
    entries = reg["engines"] if isinstance(reg, dict) else reg
    sample = sorted(entries[0].keys()) if entries and isinstance(entries[0], dict) else [type(entries[0]).__name__]
    r.ok(f"registry: {len(entries)} engines | entry fields: {sample}")

    refs = set(json.load(open("aws/ops/reports/feeds_ref_2929.json")))
    def feed_of(e):
        if isinstance(e, dict):
            for k in ("feed","output","s3_key","data"):
                if e.get(k): return e[k] if str(e[k]).startswith("data/") else "data/"+str(e[k])
            stem = re.sub(r"^justhodl-","", e.get("name",""))
        else:
            stem = re.sub(r"^justhodl-","", str(e))
        for cand in (f"data/{stem}.json", f"data/{stem.replace('-','_')}.json"):
            if cand in inv: return cand
        pre = [k for k in inv if k.startswith(f"data/{stem}")]
        return min(pre, key=lambda k: inv[k]) if pre else None

    fresh, stale, unmatched = [], [], []
    for e in entries:
        name = e.get("name") if isinstance(e, dict) else str(e)
        f = feed_of(e)
        cad = (e.get("cadence_h") if isinstance(e, dict) else None) or 24
        if f is None: unmatched.append(name); continue
        (stale if inv[f] > 2*max(cad,1) else fresh).append((name, f, round(inv[f],1)))
    um_ref = sorted(n for n in unmatched
                    if any(re.sub(r'^justhodl-','',n).replace('-','') in x.replace('-','').replace('_','') for x in refs))
    stale.sort(key=lambda t:-t[2])
    out["engines"] = {"total": len(entries), "fresh": len(fresh), "stale": len(stale),
                      "unmatched": len(unmatched), "unmatched_page_referenced": um_ref,
                      "stale_top": stale[:8]}
    r.ok(f"mapping: fresh={len(fresh)} stale={len(stale)} unmatched={len(unmatched)} "
         f"(page-referenced unmatched: {um_ref})")

    # ── remediation: inventory-drawdown + history-index ──
    def remediate(fn, feed):
        state = "none"
        try:
            for rule in ev.list_rules(NamePrefix=f"{fn}")["Rules"]:
                state = rule["State"]
                if state == "DISABLED":
                    ev.enable_rule(Name=rule["Name"]); state = "re-enabled " + rule["Name"]
        except Exception as ex:
            state = f"rules-err {ex}"
        if state in ("none",):
            try:
                sched.create_schedule(Name=f"{fn}-daily", ScheduleExpression="cron(15 6 * * ? *)",
                    FlexibleTimeWindow={"Mode":"OFF"},
                    Target={"Arn": lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"],
                            "RoleArn": boto3.client("iam").get_role(RoleName="justhodl-scheduler-invoke")["Role"]["Arn"],
                            "Input": "{}"})
                state = "schedule-created"
            except Exception as ex:
                state = f"sched-err {ex}"
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        for _ in range(20):
            a = s3_age_h(feed)
            if a is not None and a < 0.15: return state, round(a,2)
            time.sleep(9)
        return state, s3_age_h(feed)
    for fn, feed in (("justhodl-inventory-drawdown","data/inventory-drawdown.json"),
                     ("justhodl-history-index","data/history-index.json")):
        st, age = remediate(fn, feed)
        good = age is not None and age < 0.5
        ok &= good
        out.setdefault("remediation", {})[fn] = {"schedule": st, "post_age_h": age}
        (r.ok if good else r.fail)(f"remediate {fn}: {st} -> feed age {age}h")

    # ── ici-flows diagnostic ──
    lam.invoke(FunctionName="justhodl-ici-flows", InvocationType="Event", Payload=b"{}")
    time.sleep(25)
    tail = []
    try:
        st = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-ici-flows",
                                    startTime=int((time.time()-300)*1000), limit=50)
        tail = [e["message"].strip() for e in st["events"] if any(
                k in e["message"] for k in ("ERROR","Error","Traceback","404","403","Exception"))][:8]
    except Exception as ex:
        tail = [f"log-read: {ex}"]
    out["ici_diagnostic"] = tail
    r.ok("ici-flows diagnostic captured: " + (tail[0][:90] if tail else "no error lines(!)"))
    json.dump(out, open("aws/ops/reports/layerD_2930.json","w"), indent=2, default=str)
print("DONE 2930", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
