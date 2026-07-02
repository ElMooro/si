"""ops 2728 — NY FED PRIMARY DEALER POSITIONS: the real engine (top follow-up).

Catalog-driven discovery (FINRA-monthly pattern) against markets.newyorkfed.org
/api/pd: net outright positions by security class, weekly, $M. Spec written to
data/config/nyfed-pd-spec.json; engine self-heals if catalog drifts. Feed:
data/nyfed-primary-dealer.json with flat net_treasury_total_b alias so the
footprint desk's extractor joins it verbatim; TREASURIES ledger gains the pd
column — an independent dealer-book cross-check on CFTC +419k spec longs.
Report: aws/ops/reports/2728_nyfed_pd.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2728, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com", "Accept": "application/json"}
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
def retry(call, what, tries=6):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(what)
def ensure_fn(name):
    cfg = json.load(open("aws/lambdas/%s/config.json" % name)); zb = zip_fn(name)
    try:
        lam.get_function(FunctionName=name); wait_ok(name)
        retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name); wait_ok(name)
    except lam.exceptions.ResourceNotFoundException:
        retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg.get("runtime", "python3.12"),
              Role=cfg["role"], Handler=cfg["handler"], Code={"ZipFile": zb},
              Timeout=int(cfg["timeout"]), MemorySize=int(cfg["memory"]),
              Architectures=cfg.get("architectures") or ["x86_64"],
              Description=(cfg.get("description") or "")[:250]), name + " create")
        wait_ok(name); print("  CREATED", name)
    sch = cfg.get("schedule")
    if sch:
        arn = "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, name)
        ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                         Description=sch.get("description", ""))["RuleArn"]
        try:
            lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"], Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com", SourceArn=ra)
        except lam.exceptions.ResourceConflictException: pass
        ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": arn}])
        print("  schedule:", sch["expression"])

print("settling 30s…"); time.sleep(30)
print("== 0/3 catalog probe (runner-side truth) ==")
req = urllib.request.Request("https://markets.newyorkfed.org/api/pd/list/timeseries.json", headers=UA)
with urllib.request.urlopen(req, timeout=35) as r:
    cat = json.loads(r.read())
rows = cat.get("pd", {}).get("timeseries", [])
netpos = [x for x in rows if "net" in str(x.get("description", "")).lower()
          and ("position" in str(x.get("description", "")).lower() or "outright" in str(x.get("description", "")).lower())]
print("  catalog: %d series total, %d net-position candidates" % (len(rows), len(netpos)))
for x in netpos[:12]:
    print("   ", x.get("keyid"), "-", str(x.get("description"))[:96])
R["catalog"] = {"total": len(rows), "netpos": len(netpos),
                "sample": [(x.get("keyid"), str(x.get("description"))[:80]) for x in netpos[:10]]}
assert len(netpos) >= 6, "catalog netpos too thin"

print("== 1/3 CREATE + RUN nyfed-pd ==")
ensure_fn("justhodl-nyfed-pd")
r = lam.invoke(FunctionName="justhodl-nyfed-pd", InvocationType="RequestResponse",
               Payload=json.dumps({"rediscover": True}).encode())
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:300])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/nyfed-primary-dealer.json")["Body"].read())
NB, TSY = d["net_positions_usd_b"], d["net_treasury_total_b"]
R["pd_feed"] = {"as_of": d["as_of"], "net_b": NB, "tsy_total_b": TSY,
                "wow": d["wow_usd_b"], "z52": d["z_52w"], "read": d["read"]}
print("  net_b:", json.dumps(NB))
print("  UST total: $%sB | as_of %s | %s" % (TSY, d["as_of"], d["read"]))
assert len(NB) >= 5, "classes thin: %s" % list(NB)
assert isinstance(TSY, (int, float)) and 20 <= abs(TSY) <= 900, "UST total implausible: %s" % TSY
as_of_dt = datetime.strptime(d["as_of"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
assert datetime.now(timezone.utc) - as_of_dt <= timedelta(days=21), "stale as_of: %s" % d["as_of"]
hist = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/nyfed-pd.json")["Body"].read())
R["hist_depth"] = {c: len(v) for c, v in list(hist.items())[:6]}
print("  history depth:", R["hist_depth"])
assert max(R["hist_depth"].values()) >= 100, "history shallow"

print("== 2/3 FOOTPRINT joins PD ==")
retry(lambda: (wait_ok("justhodl-institutional-footprint"), lam.update_function_code(FunctionName="justhodl-institutional-footprint", ZipFile=zip_fn("justhodl-institutional-footprint")))[-1], "fp")
wait_ok("justhodl-institutional-footprint")
r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
assert not r.get("FunctionError") and pay.get("ok"), pay
f = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
R["fp_pd"] = {"net": f.get("primary_dealer_net"), "note": f.get("primary_dealer_note"),
              "tsy_row": f["asset_ledger"].get("TREASURIES")}
print("  footprint PD:", json.dumps(R["fp_pd"], default=str)[:300])
assert f.get("primary_dealer_net") is not None, "footprint PD still null"
assert "nyfed-primary-dealer" in str(f.get("primary_dealer_note", "")), "PD source note wrong: %s" % f.get("primary_dealer_note")
assert isinstance((f["asset_ledger"].get("TREASURIES") or {}).get("pd_net_usd_b"), (int, float)), "TREASURIES pd column missing"
assert f["version"] == "1.1.3"

print("== 3/3 REPORT ==")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2728_nyfed_pd.json", "w") as f2:
    json.dump(R, f2, indent=1, default=str)
print("OPS 2728 COMPLETE — the dealers' own book is on the ledger")
