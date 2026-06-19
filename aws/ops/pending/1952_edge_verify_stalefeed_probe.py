"""
1952 — READ-ONLY verification of the deployed edge-accuracy program + diagnosis
of the three 11.5-day-stale feeds passing the master-ranker gate.
No mutations. Reports:
  (A) engine-alpha.json  : proven / negative engines, net-of-cost
  (B) meta-labeler.json  : warming vs active, n, uplift
  (C) scorecard alpha blk : leaderboard head
  (D) for pairs-scanner / future-intelligence / divergence-v2 :
      producing Lambda, EventBridge rule State + schedule, last run, recent errors
"""
import boto3, json, datetime, time

s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

def getj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {e}"}

print("="*70); print("(A) data/engine-alpha.json"); print("="*70)
ea = getj("data/engine-alpha.json")
if "_error" in ea:
    print(ea["_error"])
else:
    print("generated_at:", ea.get("generated_at"), "| benchmark:", ea.get("benchmark"),
          "| fdr_q:", ea.get("fdr_q"))
    print("n_alpha_proven:", ea.get("n_alpha_proven"), "| n_alpha_negative:", ea.get("n_alpha_negative"))
    print("PROVEN  :", ea.get("alpha_proven_signals"))
    print("NEGATIVE:", ea.get("alpha_negative_signals"))
    engs = ea.get("engines", {})
    rows = [(k, v.get("net_t_stat"), v.get("net_mean_excess_pct"), v.get("alpha_n"), v.get("alpha_status"))
            for k, v in engs.items() if v.get("net_t_stat") is not None]
    rows.sort(key=lambda r: -(r[1] or -99))
    print("\n  top/bottom by NET t-stat (signal | net_t | net_excess% | n | status):")
    for r in rows[:6]:  print("   +", r)
    for r in rows[-6:]: print("   -", r)

print("\n" + "="*70); print("(B) data/meta-labeler.json"); print("="*70)
ml = getj("data/meta-labeler.json")
if "_error" in ml:
    print(ml["_error"])
else:
    print("status:", ml.get("status"), "| n_training_rows:", ml.get("n_training_rows"),
          "| min_rows_to_activate:", ml.get("min_rows_to_activate"))
    mdl = ml.get("model", {})
    print("uplift_pp:", mdl.get("uplift_pp"), "| brier:", mdl.get("brier"),
          "| generated_at:", ml.get("generated_at"))

print("\n" + "="*70); print("(C) data/signal-scorecard.json  -> alpha block"); print("="*70)
sc = getj("data/signal-scorecard.json")
if "_error" in sc:
    print(sc["_error"])
else:
    a = sc.get("alpha", {})
    print("n_engines_tested:", a.get("n_engines_tested"),
          "| proven:", a.get("n_alpha_proven"), "| negative:", a.get("n_alpha_negative"),
          "| cost%:", a.get("round_trip_cost_pct"))
    lb = a.get("leaderboard", [])[:5]
    for r in lb: print("   ", r)

print("\n" + "="*70); print("(D) STALE-FEED SCHEDULE PROBE"); print("="*70)
feeds = ["pairs-scanner", "future-intelligence", "divergence-v2"]
# build name->rules index once
def rules_for_function(fn_arn_substr):
    hits = []
    paginator = events.get_paginator("list_rules")
    for page in paginator.paginate():
        for r in page["Rules"]:
            try:
                tg = events.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
            except Exception:
                tg = []
            for t in tg:
                if fn_arn_substr in t.get("Arn", ""):
                    hits.append((r["Name"], r.get("State"), r.get("ScheduleExpression")))
    return hits

for feed in feeds:
    print(f"\n--- feed: data/{feed}.json ---")
    # candidate lambda names
    cands = [f"justhodl-{feed}", f"justhodl-{feed.replace('-v2','')}", f"justhodl-{feed}-engine"]
    fn = None
    for c in cands:
        try:
            meta = lam.get_function(FunctionName=c)
            fn = c
            cfg = meta["Configuration"]
            print(f"  lambda: {c}  | last_modified: {cfg.get('LastModified')}  | state: {cfg.get('State')}")
            break
        except Exception:
            continue
    if not fn:
        print(f"  no lambda found among {cands}")
        continue
    rules = rules_for_function(fn)
    if rules:
        for rn, st, sched in rules:
            flag = "  <-- DISABLED!" if st != "ENABLED" else ""
            print(f"  rule: {rn}  state={st}  sched={sched}{flag}")
    else:
        print("  NO EventBridge rule targets this function  <-- orphaned schedule")
    # recent errors in logs
    lg = f"/aws/lambda/{fn}"
    try:
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                            descending=True, limit=3).get("logStreams", [])
        if streams:
            last_ts = streams[0].get("lastEventTimestamp", 0)
            age_h = (time.time()*1000 - last_ts)/3.6e6 if last_ts else None
            print(f"  last log event: {age_h:.1f}h ago" if age_h is not None else "  no log ts")
            # scan newest stream for ERROR/Task timed out
            ev = logs.get_log_events(logGroupName=lg, logStreamName=streams[0]["logStreamName"],
                                     limit=30, startFromHead=False).get("events", [])
            errs = [e["message"][:160] for e in ev if any(k in e["message"]
                    for k in ("ERROR", "Traceback", "Task timed out", "errorMessage"))]
            for e in errs[:3]:
                print("    ERR:", e.strip())
            if not errs:
                print("    (no obvious errors in newest stream)")
        else:
            print("  no log streams")
    except Exception as e:
        print(f"  log probe failed: {type(e).__name__}: {e}")

print("\nDONE 1952")
