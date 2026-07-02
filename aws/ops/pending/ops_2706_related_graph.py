"""ops 2706 — Polygon related-companies fused into supply-chain-graph v2.0.

Closes the "paid full-universe relationship graph" gap named in v1's caveats:
Polygon's keyless relatedness (news co-mention + return similarity) now
(a) market-CONFIRMS each curated edge (mutual/one_way/none), (b) DISCOVERS
liquidity-gated peer nodes/edges beyond the curated hubs (0.8x-haircut pick
tier, measure-before-trust), and (c) publishes data/polygon-related-graph.json
as a reusable adjacency for future pairs/theme/contagion engines.

Probe-before-integrate: step 1 verifies the endpoint LIVE under our key before
anything deploys. Report: aws/ops/reports/2706_related_graph.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2706, "ts": datetime.now(timezone.utc).isoformat()}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

sect("1/4 LIVE ENTITLEMENT PROBE — v1/related-companies")
probe = {}
for t in ("NVDA", "AAPL", "CAT", "XOM", "LLY"):
    try:
        with urllib.request.urlopen(urllib.request.Request(
                "https://api.polygon.io/v1/related-companies/%s?apiKey=%s" % (t, POLY),
                headers={"User-Agent": "jh/1"}), timeout=15) as r:
            j = json.loads(r.read())
        probe[t] = [x.get("ticker") for x in (j.get("results") or [])]
    except Exception as e:
        probe[t] = "ERR " + str(e)[:60]
    print("  %-5s -> %s" % (t, str(probe[t])[:110]))
ok_n = sum(1 for v in probe.values() if isinstance(v, list) and len(v) >= 3)
R["probe"] = probe
assert ok_n >= 3, "endpoint not usable under this key (%d/5 healthy) — DO NOT integrate" % ok_n
print("  ENTITLED (%d/5 healthy)" % ok_n)

sect("2/4 DEPLOY v2.0")
print("  settling 30s for parallel deploy-lambdas…"); time.sleep(30)
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
FN = "justhodl-supply-chain-graph"
for _try in range(6):
    try:
        wait_ok(FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN))
        break
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
            time.sleep(18)
        else:
            raise
wait_ok(FN)
for _try in range(6):
    try:
        lam.update_function_configuration(FunctionName=FN, Timeout=300)
        break
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
            time.sleep(15)
        else:
            raise
wait_ok(FN)
print("  engine synced, timeout=300")

sect("3/4 RUN + PROVE (async + poll)")
OUT = "data/supply-chain-graph.json"
lm0 = s3.head_object(Bucket=BUCKET, Key=OUT)["LastModified"]
lam.invoke(FunctionName=FN, InvocationType="Event")
doc = None
t0 = time.time()
while time.time() - t0 < 260:
    time.sleep(15)
    if s3.head_object(Bucket=BUCKET, Key=OUT)["LastModified"] > lm0:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
        print("  landed after %.0fs" % (time.time() - t0))
        break
assert doc, "engine did not land within budget — check CloudWatch"
gs = doc.get("graph_stats") or {}
poly_edges = [e for e in doc.get("edges", []) if e.get("source") == "polygon"]
conf_sample = [(e["supplier"], e["customer"]) for e in doc.get("edges", []) if e.get("confirm") == "mutual"][:8]
rg = json.loads(s3.get_object(Bucket=BUCKET, Key="data/polygon-related-graph.json")["Body"].read())
mi_picks = [p for p in doc.get("top_picks", []) if p.get("edge_source") == "polygon"]
R["after"] = {"version": doc.get("version"), "graph_stats": gs,
              "n_nodes": doc.get("n_nodes"), "n_edges": doc.get("n_edges"),
              "poly_edges": len(poly_edges), "related_artifact_n": rg.get("n"),
              "mutual_pairs_market": len(rg.get("mutual_pairs") or []),
              "confirmed_sample": conf_sample,
              "discovered_sample": [(e["supplier"], e["customer"]) for e in poly_edges[:8]],
              "picks_total": len(doc.get("top_picks") or []),
              "market_inferred_picks": len(mi_picks),
              "elapsed_s": doc.get("elapsed_s")}
print(json.dumps(R["after"], indent=1)[:1000])
assert doc.get("version") == "2.0.0", "version not bumped"
assert (gs.get("confirmed_mutual") or 0) >= 5, "market confirms curated edges too weakly: %s" % gs
assert 5 <= (gs.get("discovered_nodes") or 0) <= 40, "discovery out of band: %s" % gs
assert all(("source" in e and "confirm" in e) for e in doc.get("edges", [])[:5]), "edge provenance missing"
assert (rg.get("n") or 0) >= 120, "related-graph artifact thin: %s" % rg.get("n")
p0 = (doc.get("top_picks") or [{}])[0]
assert all(k in p0 for k in ("ticker", "direction", "score")), "consumer schema broken: %s" % p0
lam.invoke(FunctionName="justhodl-equity-confluence", InvocationType="Event")
print("  retriggered equity-confluence (structural-family consumer)")

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2706_related_graph.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2706_related_graph.json")
print("\nOPS 2706 COMPLETE — dual-source supply-chain graph live")
