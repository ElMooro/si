"""ops 2742 — ON-CHAIN CONSUMER FUSION + FIRST EMPIRICAL READ (Khalid: Go).

(1) cq adapter: 1.15s throttle + 429 retry -> 8/8 metrics incl stablecoin.
(2) crypto-exchange-flows + crypto-miners: CryptoQuant blocks joined.
(3) onchain-ratios RESURRECTED: daily 21:20 rule + cq fusion block.
(4) signal-logger: onchain_composite_risk CANDIDATE (BTC-USD, 1/3/7/14/21d)
    -> DynamoDB via log_sig; proof via invoke log tail.
(5) FIRST-LOOK IC (1y, INDICATIVE): each cq metric vs fwd-21d BTC return
    (Coin Metrics community PriceUSD), spearman rank IC + Q4-Q1 spread,
    risk_sign-adjusted. The scorecard's BH-FDR forward grading remains the
    real gate. Report: aws/ops/reports/2742_onchain_fusion.json.
"""
import os, io, json, re, time, base64, zipfile, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2742, "ts": datetime.now(timezone.utc).isoformat()}

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
def deploy(fn):
    for i in range(6):
        try:
            wait_ok(fn); lam.update_function_code(FunctionName=fn, ZipFile=zip_fn(fn)); wait_ok(fn); return
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(fn)
def invoke(fn, payload=None, tail=False):
    kw = dict(FunctionName=fn, InvocationType="RequestResponse")
    if payload is not None: kw["Payload"] = json.dumps(payload).encode()
    if tail: kw["LogType"] = "Tail"
    r = lam.invoke(**kw)
    pay = json.loads(r["Payload"].read() or b"{}")
    lg = base64.b64decode(r.get("LogResult", "") or b"").decode("utf-8", "ignore") if tail else ""
    assert not r.get("FunctionError"), (fn, json.dumps(pay)[:220])
    return pay, lg

print("settling 30s…"); time.sleep(30)

print("== 1/6 cq adapter -> 8/8 ==")
deploy("justhodl-cryptoquant")
pay, _ = invoke("justhodl-cryptoquant", {"backfill": True})
print("  cq ->", json.dumps(pay, default=str)[:220])
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cryptoquant-onchain.json")["Body"].read())
M = d["metrics"]
print("  metrics:", len(M), "| errors:", d.get("errors"), "| composite:", d["composite_onchain_risk_z"])
assert len(M) == 8 and not d.get("errors"), (list(M), d.get("errors"))
assert M["stablecoin_exchange_reserve"]["z365"] is not None
R["cq"] = {"n": 8, "composite": d["composite_onchain_risk_z"],
           "stablecoin": M["stablecoin_exchange_reserve"]}

print("== 2/6 onchain-ratios resurrection ==")
deploy("justhodl-onchain-ratios")
cfg = json.load(open("aws/lambdas/justhodl-onchain-ratios/config.json"))
sch = cfg["schedule"]
ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                 Description=sch["description"])["RuleArn"]
try:
    lam.add_permission(FunctionName="justhodl-onchain-ratios", StatementId="evt-" + sch["name"],
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=ra)
except ClientError as e:
    if e.response["Error"]["Code"] != "ResourceConflictException": raise
ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1",
    "Arn": "arn:aws:lambda:%s:857687956942:function:justhodl-onchain-ratios" % REGION}])
envv = (lam.get_function_configuration(FunctionName="justhodl-onchain-ratios")
        .get("Environment", {}) or {}).get("Variables", {}) or {}
okey = envv.get("S3_KEY", "data/onchain-ratios.json")
pay, _ = invoke("justhodl-onchain-ratios")
rd = json.loads(s3.get_object(Bucket=BUCKET, Key=okey)["Body"].read())
assert rd.get("cryptoquant", {}).get("composite_onchain_risk_z") is not None, okey
print("  ratios LIVE @", okey, "| cq block ok | resurrected:", rd.get("resurrected"))
R["ratios"] = {"feed": okey, "resurrected": rd.get("resurrected"),
               "whale": rd["cryptoquant"]["btc_whale_ratio"]}

print("== 3/6 exchange-flows fusion ==")
deploy("justhodl-crypto-exchange-flows")
pay, _ = invoke("justhodl-crypto-exchange-flows")
xd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/crypto-exchange-flows.json")["Body"].read())
assert xd.get("cryptoquant", {}).get("composite_onchain_risk_z") is not None
print("  xf cq block ok | stablecoin:", json.dumps(xd["cryptoquant"]["stablecoin_reserve"], default=str)[:120])
R["xf"] = xd["cryptoquant"]["composite_onchain_risk_z"]

print("== 4/6 miners fusion ==")
deploy("justhodl-crypto-miners")
pay, _ = invoke("justhodl-crypto-miners")
md = json.loads(s3.get_object(Bucket=BUCKET, Key="data/crypto-miners.json")["Body"].read())
assert md.get("cryptoquant_mpi", {}).get("value") is not None
print("  miners MPI:", json.dumps(md["cryptoquant_mpi"], default=str)[:140])
R["miners_mpi"] = md["cryptoquant_mpi"]["value"]

print("== 5/6 signal-logger CANDIDATE ==")
deploy("justhodl-signal-logger")
t0_ms = int(time.time() * 1000) - 5000
pay, lg = invoke("justhodl-signal-logger", tail=True)
print("  logger payload:", json.dumps(pay)[:120])
logs = boto3.client("logs", region_name=REGION)
hit_line = None
for attempt in range(4):
    time.sleep(6)
    evs = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-signal-logger",
                                 startTime=t0_ms, filterPattern="onchain_composite_risk",
                                 limit=5).get("events", [])
    if evs:
        hit_line = evs[-1]["message"].strip()[:170]; break
print("  cw-proof:", hit_line)
assert hit_line, "onchain signal absent from CloudWatch since invoke"
R["logger"] = "onchain_composite_risk LOGGED"

print("== 6/6 FIRST-LOOK IC (1y, indicative) ==")
u = ("https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
     "?assets=btc&metrics=PriceUSD&frequency=1d&page_size=500&start_time=%s"
     % (datetime.now(timezone.utc) - timedelta(days=430)).strftime("%Y-%m-%d"))
req = urllib.request.Request(u, headers={"User-Agent": "JustHodl/1.0"})
with urllib.request.urlopen(req, timeout=30) as r:
    px = {row["time"][:10]: float(row["PriceUSD"]) for row in json.loads(r.read())["data"]
          if row.get("PriceUSD")}
pdates = sorted(px)
fwd = {}
for i, dt in enumerate(pdates):
    if i + 21 < len(pdates):
        fwd[dt] = px[pdates[i + 21]] / px[dt] - 1.0
hist = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/cryptoquant.json")["Body"].read())
spec = json.loads(s3.get_object(Bucket=BUCKET, Key="data/config/cryptoquant-spec.json")["Body"].read())
signs = {m["name"]: m.get("risk_sign", 0) for m in spec["metrics"]}
def spearman(xs, ys):
    def rk(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        rr = [0.0] * len(v)
        for pos, i in enumerate(order): rr[i] = pos + 1.0
        return rr
    rx, ry = rk(xs), rk(ys); n = len(xs)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    dx = sum((r - mx) ** 2 for r in rx) ** 0.5; dy = sum((r - my) ** 2 for r in ry) ** 0.5
    return num / (dx * dy) if dx and dy else 0.0
ic_tbl = {}
print("  %-28s %5s %7s %7s %9s" % ("metric", "n", "IC", "IC_adj", "Q4-Q1 bps"))
for name, ser in hist.items():
    pairs = [(v, fwd[dt]) for dt, v in ser.items() if dt in fwd]
    if len(pairs) < 250: continue
    xs = [p[0] for p in pairs]; ys = [p[1] for p in pairs]
    ic = spearman(xs, ys)
    adj = ic * (-signs.get(name, 0) or 1)
    srt = sorted(pairs); q = len(srt) // 4
    spread = (sum(y for _, y in srt[-q:]) / q - sum(y for _, y in srt[:q]) / q)
    spread_adj = spread * (-signs.get(name, 0) or 1) * 10000
    ic_tbl[name] = {"n": len(pairs), "ic": round(ic, 3), "ic_sign_adj": round(adj, 3),
                    "q4q1_fwd21_bps_adj": round(spread_adj)}
    print("  %-28s %5d %+7.3f %+7.3f %+9.0f" % (name, len(pairs), ic, adj, spread_adj))
assert len(ic_tbl) >= 5, list(ic_tbl)
R["ic_1y_indicative"] = ic_tbl
R["ic_note"] = "1y in-sample rank IC vs fwd-21d BTC; sign-adjusted so + = hypothesis-supportive; BH-FDR forward grading via scorecard remains the gate"

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2742_onchain_fusion.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2742 COMPLETE — on-chain flows through the fleet")

# rev2 env-resolved ratios key

# rev3 cloudwatch proof
