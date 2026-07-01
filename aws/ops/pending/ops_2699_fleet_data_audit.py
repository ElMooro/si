"""ops 2699 — FLEET DATA-HEALTH AUDIT (the hedge-fund audit: freshness + lineage).

With 657 Lambdas and ~450 data feeds, edge decays through silent rot, not missing
sources. This op builds the authoritative producer -> feed -> consumer graph from
the repo checkout, joins live S3 LastModified + EventBridge schedules, and issues
per-feed verdicts:
  STALE_PRODUCER   feed older than its schedule implies, rule ENABLED  (broken engine)
  DEAD_INPUT       feed consumed by live engines but no producer schedule (silent rot)
  MISSING_FEED     feed referenced by consumers but absent from S3
  ORPHAN_COMPUTE   scheduled producer whose feed no engine/page consumes (waste)
Report: aws/ops/reports/2699_fleet_data_audit.json (auto-committed).
"""
import os, re, json, time, glob
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
NOW = datetime.now(timezone.utc)
R = {"ops": 2699, "ts": NOW.isoformat()}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

# ── 1) repo lineage graph ────────────────────────────────────────────────
sect("1/4 LINEAGE FROM REPO")
producers, consumers = {}, {}
KEY_RE = re.compile(r'["\'](data/[A-Za-z0-9._/-]+?\.json)["\']')
PUT_RE = re.compile(r'put_object\s*\(', re.S)
for path in glob.glob("aws/lambdas/*/source/*.py") + glob.glob("aws/shared/*.py"):
    fn = path.split("/")[2] if path.startswith("aws/lambdas/") else "shared:" + os.path.basename(path)
    src = open(path, encoding="utf-8", errors="ignore").read()
    keys = set(KEY_RE.findall(src))
    # crude but effective: a key on a line near put_object( within same file = produced
    prod = set()
    for m in re.finditer(r'put_object\s*\((?:[^()]|\([^()]*\))*?Key\s*=\s*["\'](data/[^"\']+?\.json)["\']', src, re.S):
        prod.add(m.group(1))
    for k in prod:
        producers.setdefault(k, set()).add(fn)
    for k in keys - prod:
        consumers.setdefault(k, set()).add(fn)
# pages consume feeds too
for path in glob.glob("*.html") + ["jh-enhance.js"]:
    try:
        src = open(path, encoding="utf-8", errors="ignore").read()
    except Exception:
        continue
    for k in set(KEY_RE.findall(src)):
        consumers.setdefault(k, set()).add("page:" + path)
print("  producers=%d feeds, consumer-edges=%d feeds" % (len(producers), len(consumers)))

# ── 2) EventBridge schedules -> lambda ──────────────────────────────────
sect("2/4 EVENTBRIDGE SCHEDULES")
fn_sched = {}
pag = ev.get_paginator("list_rules")
rules = []
for pg in pag.paginate():
    rules.extend(pg["Rules"])
for r in rules:
    expr, state = r.get("ScheduleExpression"), r.get("State")
    if not expr:
        continue
    try:
        tg = ev.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
    except Exception:
        continue
    for t in tg:
        arn = t.get("Arn", "")
        if ":function:" in arn:
            fn = arn.split(":function:")[1].split(":")[0]
            fn_sched.setdefault(fn, []).append({"rule": r["Name"], "expr": expr, "state": state})
print("  schedule rules=%d, scheduled fns=%d" % (len(rules), len(fn_sched)))

def expected_age_h(exprs):
    """max acceptable age from the tightest ENABLED schedule (2.5x period heuristic)."""
    best = None
    for s in exprs:
        if s["state"] != "ENABLED":
            continue
        e = s["expr"]
        per = None
        m = re.match(r"rate\((\d+)\s+(minute|hour|day)", e)
        if m:
            per = int(m.group(1)) * {"minute": 1/60, "hour": 1, "day": 24}[m.group(2)]
        elif e.startswith("cron("):
            parts = e[5:-1].split()
            hours = parts[1] if len(parts) > 1 else "*"
            dow = parts[4] if len(parts) > 4 else "?"
            if dow not in ("?", "*"):
                per = 24 * 7 / max(1, len(hours.split(",")))
            elif hours == "*":
                per = 1
            else:
                per = 24 / max(1, len(hours.split(",")))
        if per:
            best = per if best is None else min(best, per)
    return round(best * 2.5 + 6, 1) if best else None

# ── 3) live S3 freshness (top-level data/*.json only) ───────────────────
sect("3/4 S3 FRESHNESS")
last = {}
pag = s3.get_paginator("list_objects_v2")
for pg in pag.paginate(Bucket=BUCKET, Prefix="data/", Delimiter="/"):
    for o in pg.get("Contents", []):
        if o["Key"].endswith(".json"):
            last[o["Key"]] = o["LastModified"]
print("  top-level feeds on S3:", len(last))

# ── 4) verdicts ─────────────────────────────────────────────────────────
sect("4/4 VERDICTS")
stale, dead_input, missing, orphan = [], [], [], []
allkeys = set(producers) | set(consumers) | set(last)
for k in sorted(allkeys):
    if "/" in k[5:]:      # skip archive subdirs
        continue
    prods = sorted(producers.get(k, []))
    cons = sorted(consumers.get(k, []))
    lm = last.get(k)
    age_h = round((NOW - lm).total_seconds() / 3600, 1) if lm else None
    scheds = [s for p in prods for s in fn_sched.get(p, [])]
    exp = expected_age_h(scheds)
    rec = {"key": k, "age_h": age_h, "expected_h": exp,
           "producers": prods, "n_consumers": len(cons),
           "consumers": cons[:8], "schedules": [s["expr"] for s in scheds][:3]}
    if cons and lm is None and prods:
        missing.append(rec)
    elif age_h is not None and exp and age_h > exp and prods:
        stale.append(rec)
    elif cons and not scheds and prods and age_h and age_h > 80:
        dead_input.append(rec)
    elif prods and scheds and not cons and k not in ("data/finviz-universe.json",):
        orphan.append(rec)
stale.sort(key=lambda x: -(x["age_h"] or 0))
dead_input.sort(key=lambda x: -(x["age_h"] or 0))
R["summary"] = {"feeds_seen": len([k for k in allkeys if "/" not in k[5:]]),
                "stale_producer": len(stale), "dead_input": len(dead_input),
                "missing_feed": len(missing), "orphan_compute": len(orphan)}
R["stale_producer"] = stale[:60]
R["dead_input"] = dead_input[:60]
R["missing_feed"] = missing[:40]
R["orphan_compute"] = [{"key": x["key"], "producers": x["producers"], "schedules": x["schedules"]} for x in orphan[:60]]
print(json.dumps(R["summary"]))
print("  worst stale:")
for x in stale[:15]:
    print("   %-44s age=%6sh exp=%5sh prod=%s cons=%d" % (x["key"][:44], x["age_h"], x["expected_h"], ",".join(x["producers"])[:34], x["n_consumers"]))
print("  dead inputs (consumed, unscheduled, old):")
for x in dead_input[:10]:
    print("   %-44s age=%6sh cons=%d" % (x["key"][:44], x["age_h"], x["n_consumers"]))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2699_fleet_data_audit.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("\n  wrote aws/ops/reports/2699_fleet_data_audit.json")
print("OPS 2699 COMPLETE")
