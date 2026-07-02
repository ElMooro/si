"""ops 2710 — REAL ACM term premium (NY Fed), replacing the fleet's proxy.

New justhodl-term-premium: parses the official ACMTermPremium.xls in-Lambda
(vendored pure-python xlrd — self-updating daily, no runner dependency), full
daily series 1961-> archived to S3 with graceful fallback. yield-curve now
publishes the REAL tp10 into term_premium_proxy_bps (source-flagged, proxy
auto-fallback) so cycle-clock + all readers inherit it untouched. Board row
"Term Premium (ACM)" + term-premium.html + nav/directory.
Report: aws/ops/reports/2710_acm_term_premium.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=170, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2710, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) jh/1"}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=45, binary=False):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "ignore")
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            for f in files:
                if "__pycache__" in root:
                    continue
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
                time.sleep(18)
            else:
                raise
    raise RuntimeError(what)
def ensure_rule(fn, name, expr, desc=""):
    arn = "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, fn)
    ra = ev.put_rule(Name=name, ScheduleExpression=expr, State="ENABLED", Description=desc)["RuleArn"]
    try:
        lam.add_permission(FunctionName=fn, StatementId="evt-" + name, Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=ra)
    except lam.exceptions.ResourceConflictException:
        pass
    ev.put_targets(Rule=name, Targets=[{"Id": "1", "Arn": arn}])

sect("1/5 RUNNER PROBE — NY Fed ACM file reachable")
ok_url = None
for u in ("https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls",
          "https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACM_TermPremium.xls"):
    try:
        b = get(u, binary=True)
        print("  %s -> %d bytes, magic %s" % (u.rsplit("/", 1)[-1], len(b), b[:4].hex()))
        if len(b) > 200_000:
            ok_url = u
            break
    except Exception as e:
        print("  %s: %s" % (u.rsplit("/", 1)[-1], str(e)[:70]))
R["probe"] = ok_url or "none"
assert ok_url, "ACM file unreachable from runner — abort before creating a dead pillar"

sect("2/5 CREATE + RUN justhodl-term-premium")
print("  settling 30s…"); time.sleep(30)
cfg = json.load(open("aws/lambdas/justhodl-term-premium/config.json"))
zb = zip_fn("justhodl-term-premium")
print("  zip size %.1f MB (xlrd vendored)" % (len(zb) / 1e6))
try:
    lam.get_function(FunctionName="justhodl-term-premium")
    wait_ok("justhodl-term-premium")
    retry(lambda: lam.update_function_code(FunctionName="justhodl-term-premium", ZipFile=zb), "tp code")
except lam.exceptions.ResourceNotFoundException:
    retry(lambda: lam.create_function(FunctionName="justhodl-term-premium", Runtime=cfg["runtime"],
          Role=cfg["role"], Handler=cfg["handler"], Code={"ZipFile": zb}, Timeout=cfg["timeout"],
          MemorySize=cfg["memory"], Architectures=cfg["architectures"], Description=cfg["description"]),
          "tp create")
wait_ok("justhodl-term-premium")
ensure_rule("justhodl-term-premium", cfg["schedule"]["name"], cfg["schedule"]["expression"],
            cfg["schedule"]["description"])
r = lam.invoke(FunctionName="justhodl-term-premium", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:180])
assert not r.get("FunctionError"), pay
tp = json.loads(s3.get_object(Bucket=BUCKET, Key="data/term-premium.json")["Body"].read())
L, X = tp["latest"], tp["decomposition"]
R["acm"] = {"date": L["date"], "tp10": L["tp10"], "tp5": L["tp5"], "tp2": L["tp2"],
            "z_10y": tp["z_10y"], "pctile": tp["pctile_full_history"],
            "deltas": tp["deltas_bps"], "regime": tp["regime"],
            "series_n": tp["series_n"], "first": tp["first_date"], "source": tp["source"],
            "decomp": X["read"], "identity": X["identity_check_pct"]}
print(" ", json.dumps(R["acm"])[:460])
assert tp["source"] == "live", "should self-parse live on first run: %s" % tp["source"]
assert tp["series_n"] >= 12000 and tp["first_date"] <= "1965-01-01"
assert -2.0 <= L["tp10"] <= 3.5, "tp10 implausible"
assert L["date"] >= (datetime.now(timezone.utc) - timedelta(days=12)).strftime("%Y-%m-%d"), "ACM stale"
assert abs(X["identity_check_pct"]) <= 0.03, "decomposition identity broken"
assert len(tp["history_chart"]) >= 300

sect("3/5 YIELD-CURVE — real value into the proxy field")
for fn in ("justhodl-yield-curve", "justhodl-signal-board"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
r = lam.invoke(FunctionName="justhodl-yield-curve", InvocationType="RequestResponse")
assert not r.get("FunctionError"), (r["Payload"].read() or b"")[:200]
yc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/yield-curve.json")["Body"].read())
R["yield_curve"] = {"source": yc.get("term_premium_source"),
                    "proxy_field_bps": yc.get("term_premium_proxy_bps"),
                    "acm_block": yc.get("term_premium_acm")}
print(" ", json.dumps(R["yield_curve"], default=str)[:320])
assert yc.get("term_premium_source") == "ACM", "yield-curve not sourcing ACM"
assert abs(yc.get("term_premium_proxy_bps") - L["tp10"] * 100) <= 2, "proxy field != ACM tp10"

sect("4/5 BOARD ROW")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "Term Premium (ACM)" in sb, "board row absent"
m = re.search(r'\{[^{}]*Term Premium[^{}]*\}', sb)
R["board_row"] = m.group(0)[:220] if m else "present"
print("  board:", R["board_row"])

sect("5/5 PAGE + REPORT")
time.sleep(70)
try:
    R["page"] = "LIVE" if "ACM TERM PREMIUM" in get("https://justhodl.ai/term-premium.html", timeout=20) else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2710_acm_term_premium.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2710 COMPLETE — the fleet now runs on the real ACM series")

# rev v2: weekly-downsample fix rerun

# rev v3: block relocated above out dict
