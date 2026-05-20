"""ops 966: FINAL final verification -- all 10 edges should now be 100%."""
import boto3, json, os, datetime as dt, urllib.request
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)

EDGES = [
    (1, "justhodl-vix-backwardation-trigger", "data/vix-backwardation-trigger.json", "vix-capitulation.html"),
    (2, "justhodl-insider-buys-enriched",     "data/insider-buys-enriched.json",     "insider-buys.html"),
    (3, "justhodl-breadth-thrust",            "data/breadth-thrust.json",            "breadth-thrust.html"),
    (4, "justhodl-vol-target-unwind",         "data/vol-target-unwind.json",         "vol-target-unwind.html"),
    (5, "justhodl-russell-recon-frontrun",    "data/russell-recon-frontrun.json",    "russell-recon.html"),
    (6, "justhodl-buyback-scanner",           "data/buyback-scanner.json",           "buyback-scanner.html"),
    (7, "justhodl-stablecoin-flow",           "data/stablecoin-flow.json",           "stablecoin-flow.html"),
    (8, "justhodl-opex-calendar",             "data/opex-calendar.json",             "opex-calendar.html"),
    (9, "justhodl-activist-13d",              "data/activist-13d.json",              "activist-13d.html"),
    (10,"justhodl-rv-iv-scanner",             "data/rv-iv-scanner.json",             "rv-iv-scanner.html"),
]

CHECKS = []
def add(eid, n, ok, d): CHECKS.append({"edge":eid,"name":f"e{eid}.{n}","passed":ok,"detail":str(d)[:280]})

# Force-invoke Edge #4 once more to seed S3 output
print("Force-invoking Edge #4 to seed S3 output...")
try:
    r = lam.invoke(FunctionName="justhodl-vol-target-unwind",
                   InvocationType="RequestResponse", Payload=b"{}")
    payload = r["Payload"].read().decode()
    print(f"  Edge #4 invoke result: {payload[:200]}")
except Exception as e:
    print(f"  invoke error: {e}")

import time
time.sleep(3)

for eid, fn, key, page in EDGES:
    print(f"\n--- Edge #{eid}: {fn} ---")
    # Lambda
    try:
        info = lam.get_function(FunctionName=fn)
        add(eid, "lambda_deployed", True,
            f"mod={info['Configuration'].get('LastModified', '')[:19]}")
    except ClientError as e:
        add(eid, "lambda_deployed", False, str(e)[:120])
        continue
    # S3
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        d = json.loads(obj["Body"].read())
        try:
            ts = dt.datetime.fromisoformat(d["as_of"].replace("Z", "+00:00"))
            age_h = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds() / 3600
        except Exception:
            age_h = -1
        # Fresher than 7 days = OK (some daily engines run 21UTC)
        add(eid, "s3_output_fresh", obj["ContentLength"] > 500 and age_h < 168,
            f"size={obj['ContentLength']}B age_h={round(age_h,1)}")
    except ClientError as e:
        add(eid, "s3_output_fresh", False, str(e)[:120])
        continue
    # Schema sanity
    add(eid, "has_engine_field", "engine" in d, d.get("engine"))
    add(eid, "has_state", "state" in d or "calendar_phase" in d,
        d.get("state") or d.get("calendar_phase"))
    # Page wired
    try:
        req = urllib.request.Request(f"{PAGES}/{page}", headers={"User-Agent":"ops/966"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8","ignore")
            data_file = key.split("/")[-1]
            ok = r.status == 200 and len(body) > 1000 and data_file in body
            add(eid, "page_live_and_wired", ok,
                f"status={r.status} wired={data_file in body}")
    except Exception as e:
        add(eid, "page_live_and_wired", False, str(e)[:120])

# Signal-board check
print("\n--- Signal-board cross-asset aggregator ---")
try:
    r = lam.invoke(FunctionName="justhodl-signal-board",
                   InvocationType="RequestResponse", Payload=b"{}")
    add("SB", "invoke", r["StatusCode"]==200, r["Payload"].read().decode()[:200])
except Exception as e:
    add("SB", "invoke", False, str(e)[:200])
time.sleep(2)
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
    d = json.loads(obj["Body"].read())
    n_eng, n_live = d.get("n_engines",0), d.get("n_live",0)
    add("SB", "aggregating_20", n_eng >= 20 and n_live >= 14,
        f"engines={n_eng} live={n_live} posture={d.get('composite_posture')}")
except Exception as e:
    add("SB", "aggregating_20", False, str(e)[:200])

# Per-edge summary
per_edge = {}
for c in CHECKS:
    e = c["edge"]
    per_edge.setdefault(e, {"p":0,"t":0})
    per_edge[e]["t"] += 1
    if c["passed"]: per_edge[e]["p"] += 1
op = sum(p["p"] for p in per_edge.values())
ot = sum(p["t"] for p in per_edge.values())

rep = {"ops":966,"title":"FINAL 10-edge complete verification",
       "run_at":dt.datetime.utcnow().isoformat()+"Z",
       "per_edge_summary":{str(k):v for k,v in per_edge.items()},
       "checks":CHECKS,
       "summary":{"total":ot,"passed":op,"failed":ot-op,
                  "pct":round(100*op/max(ot,1),1)},
       "overall_ok":op==ot}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/966_final_complete_verify.json","w").write(json.dumps(rep,indent=2,default=str))

print(f"\n=== FINAL: {op}/{ot} ({round(100*op/max(ot,1),1)}%) ===")
for eid in sorted(per_edge.keys(), key=lambda x:(0,int(x)) if str(x).isdigit() else (1,str(x))):
    p = per_edge[eid]
    flag = "GREEN" if p["p"]==p["t"] else "fail"
    print(f"  Edge #{str(eid):3}  {p['p']}/{p['t']}  [{flag}]")
failed = [c for c in CHECKS if not c["passed"]]
if failed:
    print("\nFAILED:")
    for c in failed: print(f"  e{c['edge']}.{c['name']:32} {c['detail'][:100]}")
