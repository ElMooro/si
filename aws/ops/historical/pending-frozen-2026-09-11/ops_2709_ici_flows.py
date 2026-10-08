"""ops 2709 — GAP-HUNT flagship: Fund Flows & Dry Powder (ICI weekly).

Audit verdicts this arc: SLOOS already owned (allocator consumes DRTSCILM);
ISM = dead NAPM* FRED IDs in daily-report (real next-tier gap, scrape-only);
ICI weekly MMF + long-term fund flows = GENUINELY ABSENT (crisis-canaries still
queries WRMFSL/WIMFSL — both killed in the 2021 H.6 revamp).

This op probes BOTH ICI files by executing the ENGINE'S OWN fetch/parse code on
the runner (zero seed/lambda drift), seeds S3 histories, boto3-creates the new
function (new-dir no-op gotcha) + weekly rule, proves the full chain, fuses
signal-board, and verifies nav registration. Report: 2709_ici_flows.json.
"""
import os, io, json, time, zipfile, importlib.util, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=150, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2709, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

sect("1/5 RUNNER PROBE + SEED (engine's own parse code)")
spec = importlib.util.spec_from_file_location(
    "ici_eng", "aws/lambdas/justhodl-ici-flows/source/lambda_function.py")
eng = importlib.util.module_from_spec(spec)
spec.loader.exec_module(eng)
mmf_raw = eng._live("mmf")
ltf_raw = eng._live("ltf")
R["probe"] = {"mmf_rows": len(mmf_raw), "ltf_rows": len(ltf_raw)}
assert len(mmf_raw) >= 8, "MMF parse thin (%d) — do not deploy dead pillar" % len(mmf_raw)
assert len(ltf_raw) >= 8, "LTF parse thin (%d)" % len(ltf_raw)
mmf_n = eng._norm_mmf(mmf_raw)
md, mrec = sorted(mmf_n.items())[-1]
ld, lrec = sorted(ltf_raw.items())[-1]
R["probe"]["mmf_latest"] = {"date": md, **{k: mrec.get(k) for k in ("total", "govt", "retail", "inst")}}
R["probe"]["ltf_latest"] = {"date": ld, **{k: lrec.get(k) for k in ("eq_dom", "eq_world", "bond", "total")}}
print("  MMF latest:", json.dumps(R["probe"]["mmf_latest"]))
print("  LTF latest:", json.dumps(R["probe"]["ltf_latest"]))
today = datetime.now(timezone.utc).date()
assert (today - datetime.fromisoformat(md).date()).days <= 21, "MMF stale: %s" % md
assert 4000 <= (mrec.get("total") or 0) <= 10000, "MMF total implausible: %s" % mrec
assert sum(1 for k in ("eq_dom", "eq_world", "hybrid", "bond") if lrec.get(k) is not None) >= 3, \
    "LTF classes thin: %s" % lrec
for key, raw in ((eng.H_MMF, mmf_raw), (eng.H_LTF, ltf_raw)):
    try:
        stored = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        stored = {}
    stored.update(raw)
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(dict(sorted(stored.items())[-800:]), separators=(",", ":")).encode(),
                  ContentType="application/json")
    print("  SEEDED %s: %d weeks" % (key, len(stored)))

sect("2/5 CREATE FUNCTION + RULE")
print("  settling 25s…"); time.sleep(25)
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
cfg = json.load(open("aws/lambdas/justhodl-ici-flows/config.json"))
zb = zip_fn("justhodl-ici-flows")
try:
    lam.get_function(FunctionName="justhodl-ici-flows")
    wait_ok("justhodl-ici-flows")
    lam.update_function_code(FunctionName="justhodl-ici-flows", ZipFile=zb)
    print("  existed -> updated")
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName="justhodl-ici-flows", Runtime=cfg["runtime"], Role=cfg["role"],
                        Handler=cfg["handler"], Code={"ZipFile": zb}, Timeout=cfg["timeout"],
                        MemorySize=cfg["memory"], Architectures=cfg["architectures"],
                        Description=cfg["description"])
    print("  created")
wait_ok("justhodl-ici-flows")
ev = boto3.client("events", region_name=REGION)
arn = "arn:aws:lambda:%s:857687956942:function:justhodl-ici-flows" % REGION
ra = ev.put_rule(Name=cfg["schedule"]["name"], ScheduleExpression=cfg["schedule"]["expression"],
                 State="ENABLED", Description=cfg["schedule"]["description"])["RuleArn"]
try:
    lam.add_permission(FunctionName="justhodl-ici-flows", StatementId="evt-ici",
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=ra)
except lam.exceptions.ResourceConflictException:
    pass
ev.put_targets(Rule=cfg["schedule"]["name"], Targets=[{"Id": "1", "Arn": arn}])
print("  rule:", cfg["schedule"]["expression"])

sect("3/5 RUN + PROVE")
r = lam.invoke(FunctionName="justhodl-ici-flows", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:220])
assert not r.get("FunctionError"), pay
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ici-flows.json")["Body"].read())
M, L = doc["mmf"], doc["long_term"]
R["after"] = {"version": doc.get("version"), "regime": doc.get("regime"),
              "signal": doc.get("signal"), "provisional": doc.get("provisional"),
              "mmf": {k: M.get(k) for k in ("date", "total_b", "govt_share_pct", "wow_b",
                                            "chg_13w_b", "z_13w", "yoy_pct", "weeks_n")},
              "equity_4w_m": L.get("equity_sum_4w_m"), "equity_z": L.get("equity_z_4w"),
              "rotation_z": L.get("eq_minus_bond_4w_z"),
              "classes": {k: v.get("sum_4w_m") for k, v in (L.get("classes") or {}).items()}}
print(json.dumps(R["after"], indent=1, default=str)[:800])
assert doc.get("version") == "1.0.0"
assert 4000 <= (M.get("total_b") or 0) <= 10000
assert doc.get("regime") in ("CAPITULATION_HOARD", "DEFENSIVE_ROTATION", "NEUTRAL", "CHASE", "FULL_DEPLOYMENT")
assert isinstance(doc.get("signal"), int) and -2 <= doc["signal"] <= 2
assert len(L.get("classes") or {}) >= 4
assert len(M.get("history") or []) >= 8

sect("4/5 BOARD FUSION")
for _try in range(6):
    try:
        wait_ok("justhodl-signal-board")
        lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zip_fn("justhodl-signal-board"))
        break
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
            time.sleep(18)
        else:
            raise
wait_ok("justhodl-signal-board")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "Fund Flows (ICI)" in sb
import re as _re
mm = _re.search(r'\{[^{}]*Fund Flows \(ICI\)[^{}]*\}', sb)
R["board_row"] = mm.group(0)[:240] if mm else "present"
print("  board:", R["board_row"])

sect("5/5 NAV + REPORT")
def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh/1"}), timeout=20) as r:
        return r.read().decode("utf-8", "ignore")
assert "/ici-flows.html" in get("https://raw.githubusercontent.com/ElMooro/si/main/nav-manifest.json")
R["nav_repo"] = "OK"
time.sleep(75)
for url, key in (("https://justhodl.ai/ici-flows.html", "page_live"),
                 ("https://justhodl.ai/nav-manifest.json", "nav_live")):
    try:
        R[key] = "LIVE" if "ici-flows" in get(url).lower() or "DRY POWDER" in get(url) else "200_no_marker"
    except Exception as e:
        R[key] = "propagating: " + str(e)[:50]
    print(" ", key, R[key])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2709_ici_flows.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2709 COMPLETE — slow-money pillar live")
