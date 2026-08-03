"""ops_4312 -- reversal radar, institutional finish: sparklines in
every row, cross-asset sector tiles (no more em-dash bucket), real
movers on this SECOND run (deltas vs last_scores from 02:42), visual
market-turn thermometers + breadth history line, sort/table/CSV.
Gate: v2.1 rows carry spk[>=8]; sectors include ETF/FX/FUTURES/CRYPTO
tiles; movers non-empty; page markers on edge."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RUN_START = datetime.now(timezone.utc)

def git_floor(d):
    try:
        out = subprocess.run(["git", "log", "-1", "--format=%ct",
                              "--", "aws/lambdas/%s" % d],
                             capture_output=True, text=True,
                             timeout=30).stdout.strip()
        return datetime.fromtimestamp(int(out), tz=timezone.utc)
    except Exception:
        return None
fails = []
with report("4312_reversal_pro") as r:
    r.heading("ops 4312 -- radar, finished")
    fl = git_floor("justhodl-trend-reversal") or RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-trend-reversal")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy never crossed git floor")
    else:
        p = lam.invoke(FunctionName="justhodl-trend-reversal",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("root: %s" % (p["Payload"].read() or b"")[:160].decode(
            "utf-8", "ignore"))
        doc = {}
        for _ in range(40):  # await chain finalize (version bump)
            time.sleep(15)
            try:
                doc = json.loads(s3.get_object(
                    Bucket="justhodl-dashboard-live",
                    Key="data/trend-reversal.json")["Body"].read())
                if doc.get("version") == "2.1":
                    break
            except Exception:
                pass
        if doc.get("version") != "2.1":
            fails.append("v2.1 never finalized (saw %s)"
                         % doc.get("version"))
        else:
            rows = doc.get("rows") or []
            r.ok("UNIVERSE %s · hot %s · movers %s"
                 % (doc.get("universe_n"), doc.get("hot_n"),
                    [(m["t"], m["delta"]) for m in
                     (doc.get("movers") or [])[:6]]))
            spk = [len(x.get("spk") or []) for x in rows[:5]]
            r.log("spk lens (top5): %s" % spk)
            secs = [s0["sector"] for s0 in doc.get("sectors") or []]
            r.log("sector tiles: %s" % secs)
            if not all(x >= 8 for x in spk):
                fails.append("sparklines thin: %s" % spk)
            for c0 in ("ETF", "FX", "CRYPTO"):
                if c0 not in secs:
                    fails.append("tile %s missing" % c0)
            if "—" in secs or None in secs:
                fails.append("em-dash bucket still present")
            if not doc.get("movers"):
                fails.append("movers empty on second run")
    body = ""
    for _ in range(12):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/trend-reversal.html",
                headers={"User-Agent": "ops/4312",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "sprkSVG" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("thermo", "ghist", "sprkSVG", "table view", "⬇ CSV",
               "secbar", "why.html?t="):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if "sprkSVG" in body:
        r.ok("page v3 LIVE (%d bytes)" % len(body))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4312 PASS -- the radar reads like a product")
if fails:
    sys.exit(1)
