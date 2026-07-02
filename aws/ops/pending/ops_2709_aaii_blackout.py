"""ops 2709 — the last two real gap indicators, institutional style.

AAII (individual-investor half of the positioning triad): engine existed but
had schedule:None — NEVER RAN despite 5 consumers incl cycle-clock. Fixed:
weekly rule, runner-seeded 26w history from the legacy sentiment.xls (full
series stashed to data/history/aaii-full.json), fused as the sentiment
composite's 7th weekly flipped component + board row.

BLACKOUT (true gap, zero prior coverage): new justhodl-earnings-blackout —
S&P-cap-weighted buyback-blackout share now + 42-day forward curve + reporting
intensity, street proxy [T-30,T+2]. Board row + blackout.html + nav.
Report: aws/ops/reports/2709_aaii_blackout.json.
"""
import os, io, json, time, zipfile, re, subprocess, sys, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=170, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2709, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) jh/1"}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=30, binary=False):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "ignore")
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

sect("1/6 RUNNER SEED — AAII full history via sentiment.xls (xlrd)")
seed_rows = []
try:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd==2.0.1"], check=True)
    import xlrd
    blob = get("https://www.aaii.com/files/surveys/sentiment.xls", timeout=45, binary=True)
    bk = xlrd.open_workbook(file_contents=blob)
    sh = bk.sheet_by_index(0)
    for r_ in range(sh.nrows):
        row = sh.row_values(r_)
        d = None
        c0 = row[0]
        if isinstance(c0, (int, float)) and 30000 < c0 < 60000:
            d = (datetime(1899, 12, 30) + timedelta(days=c0)).date().isoformat()
        elif isinstance(c0, str) and re.match(r"\d{1,2}[-/]\d{1,2}[-/]\d{2,4}", c0.strip()):
            for fmt in ("%m-%d-%y", "%m/%d/%Y", "%m/%d/%y"):
                try:
                    d = datetime.strptime(c0.strip(), fmt).date().isoformat(); break
                except Exception:
                    pass
        if not d:
            continue
        try:
            b_, n_, br = float(row[1]), float(row[2]), float(row[3])
        except Exception:
            continue
        if b_ > 1.5:
            b_, n_, br = b_ / 100, n_ / 100, br / 100
        if 0 <= b_ <= 1 and 0 <= br <= 1 and 0.8 <= b_ + n_ + br <= 1.2:
            seed_rows.append({"week_ending": d, "bullish": round(b_, 4), "neutral": round(n_, 4),
                              "bearish": round(br, 4), "bull_bear_spread": round(b_ - br, 4)})
    seed_rows.sort(key=lambda x: x["week_ending"])
    print("  parsed %d weekly rows (%s -> %s)" % (len(seed_rows),
          seed_rows[0]["week_ending"] if seed_rows else "-", seed_rows[-1]["week_ending"] if seed_rows else "-"))
except Exception as e:
    print("  xls seed unavailable:", str(e)[:100])
R["aaii_seed_n"] = len(seed_rows)
if len(seed_rows) >= 26:
    s3.put_object(Bucket=BUCKET, Key="data/history/aaii-full.json",
                  Body=json.dumps(seed_rows, separators=(",", ":")).encode(), ContentType="application/json")
    try:
        cur = json.loads(s3.get_object(Bucket=BUCKET, Key="data/aaii-sentiment.json")["Body"].read())
    except Exception:
        cur = {}
    cur["history_26w"] = seed_rows[-26:]
    s3.put_object(Bucket=BUCKET, Key="data/aaii-sentiment.json",
                  Body=json.dumps(cur, separators=(",", ":")).encode(), ContentType="application/json")
    print("  SEEDED feed history_26w=26 + full series stashed")

sect("2/6 DEPLOY aaii + composite + board; ensure AAII rule")
print("  settling 30s…"); time.sleep(30)
for fn in ("justhodl-aaii-sentiment", "justhodl-put-call-extreme", "justhodl-signal-board"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
ensure_rule("justhodl-aaii-sentiment", "justhodl-aaii-weekly", "cron(0 18 ? * THU,FRI *)",
            "AAII survey Thu release; Fri retry")
print("  rule justhodl-aaii-weekly ensured")

sect("3/6 RUN AAII + PROVE")
r = lam.invoke(FunctionName="justhodl-aaii-sentiment", InvocationType="RequestResponse")
assert not r.get("FunctionError"), (r["Payload"].read() or b"")[:250]
aj = json.loads(s3.get_object(Bucket=BUCKET, Key="data/aaii-sentiment.json")["Body"].read())
L = aj.get("latest") or {}
R["aaii"] = {"week_ending": L.get("week_ending"), "bullish": L.get("bullish"),
             "bearish": L.get("bearish"), "spread": L.get("bull_bear_spread"),
             "history_n": len(aj.get("history_26w") or []), "interp": (aj.get("interpretation") or "")[:140]}
print(" ", json.dumps(R["aaii"]))
R["aaii"]["source"] = aj.get("source"); R["aaii"]["backfilled"] = aj.get("backfilled_rows")
assert isinstance(L.get("bullish"), (int, float)) and 0.15 <= L["bullish"] <= 0.62, "bull outside truth band: %s" % L
assert isinstance(L.get("bearish"), (int, float)) and 0.15 <= L["bearish"] <= 0.62, "bear outside truth band: %s" % L
assert (L.get("neutral") or 0) >= 0.05
assert abs((L.get("bullish") or 0) + (L.get("neutral") or 0) + (L.get("bearish") or 0) - 1) <= 0.06, "fracs don't sum"
assert not any((h.get("bullish") or 0) >= 0.95 or (h.get("bearish") or 0) >= 0.95
               for h in aj.get("history_26w") or []), "poison rows persist in history"
we = L.get("week_ending") or "1970-01-01"
assert (datetime.now(timezone.utc) - timedelta(days=12)).strftime("%Y-%m-%d") <= we <= (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d"), "AAII week out of range: %s" % we
assert R["aaii"]["history_n"] >= 8, "history too thin for composite: %d" % R["aaii"]["history_n"]

sect("4/6 FUSION PROOF — composite 7th + two board rows")
r = lam.invoke(FunctionName="justhodl-put-call-extreme", InvocationType="RequestResponse")
assert not r.get("FunctionError")
pce = json.loads(s3.get_object(Bucket=BUCKET, Key="data/put-call-extreme.json")["Body"].read())
comp = next((c for c in pce.get("signals", []) if c.get("id") == "AAII_SPREAD"), None)
assert comp and comp.get("ok"), "AAII component not ok: %s" % comp
R["composite"] = {"aaii": comp, "n_total": pce.get("n_total_signals"),
                  "composite_z": pce.get("composite_z"), "state": pce.get("state")}
print("  composite:", json.dumps(R["composite"], default=str)[:420])
assert pce.get("n_total_signals") == 7, "expected 7 components"

sect("5/6 CREATE + RUN earnings-blackout")
cfg = json.load(open("aws/lambdas/justhodl-earnings-blackout/config.json"))
zb = zip_fn("justhodl-earnings-blackout")
try:
    lam.get_function(FunctionName="justhodl-earnings-blackout")
    wait_ok("justhodl-earnings-blackout")
    retry(lambda: lam.update_function_code(FunctionName="justhodl-earnings-blackout", ZipFile=zb), "bo code")
except lam.exceptions.ResourceNotFoundException:
    retry(lambda: lam.create_function(FunctionName="justhodl-earnings-blackout", Runtime=cfg["runtime"],
          Role=cfg["role"], Handler=cfg["handler"], Code={"ZipFile": zb}, Timeout=cfg["timeout"],
          MemorySize=cfg["memory"], Architectures=cfg["architectures"], Description=cfg["description"]),
          "bo create")
wait_ok("justhodl-earnings-blackout")
ensure_rule("justhodl-earnings-blackout", cfg["schedule"]["name"], cfg["schedule"]["expression"],
            cfg["schedule"]["description"])
r = lam.invoke(FunctionName="justhodl-earnings-blackout", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:160])
assert not r.get("FunctionError"), pay
bd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/earnings-blackout.json")["Body"].read())
N = bd["now"]
R["blackout"] = {"now_pct": N["blackout_mktcap_pct"], "state": N["state"],
                 "caps_matched": N["n_caps_matched"], "with_dates": N["n_with_dates"],
                 "rep14": bd["next_14d"]["reporting_mktcap_pct"],
                 "peak": bd["peak"], "trough": bd["trough"], "curve_n": len(bd["curve"])}
print(" ", json.dumps(R["blackout"])[:420])
assert N["n_caps_matched"] >= 440, "SP500 cap coverage thin: %s" % N
assert N["n_with_dates"] >= 350, "earnings-date coverage thin: %s" % N
assert 3 <= N["blackout_mktcap_pct"] <= 97 and len(bd["curve"]) == 42
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
for marker in ("AAII", "Buyback Blackout"):
    assert marker in sb, "board row missing: " + marker
for marker in ("AAII Retail Sentiment", "Buyback Blackout"):
    m = re.search(r'\{[^{}]*' + marker + r'[^{}]*\}', sb)
    R.setdefault("board", {})[marker] = m.group(0)[:200] if m else "present"
    print("  board:", R["board"][marker])

sect("6/6 PAGE + INFORMATIONAL PROBES + REPORT")
time.sleep(70)
try:
    R["page"] = "LIVE" if "BUYBACK BLACKOUT CURVE" in get("https://justhodl.ai/blackout.html", timeout=20) else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  blackout.html:", R["page"])
try:
    j = json.loads(get("https://api.stlouisfed.org/fred/series/observations?series_id=SKEW&api_key=2f057499936072679d8843d7fce99989&file_type=json&sort_order=desc&limit=2"))
    ob = [o for o in j.get("observations", []) if o.get("value") not in (".", None)]
    R["skew_probe"] = ob[0] if ob else "no data"
except Exception as e:
    R["skew_probe"] = "unavailable: " + str(e)[:60]
print("  FRED SKEW probe:", R["skew_probe"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2709_aaii_blackout.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2709 COMPLETE — positioning triad closed + corporate-bid switch live")
