"""ops_4316 -- reversal + rehypo fan-out: why.html Trend&Reversal
panel (client join, every ticker), best-setups rows annotated with
reversal standing + verdict-conflict flags, risk-gate gains the
collateral advisory leg (brain-cited weights untouched; composite
adjusts only at STRAINED/SEIZING, disclosed). MI deferred -- its
helpers matched no known pattern; guessing is how bugs are born."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RUN_START = datetime.now(timezone.utc)
def floor_ok(fn):
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct", "--",
                             "aws/lambdas/%s" % fn],
                            capture_output=True, text=True,
                            timeout=30).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    for _ in range(50):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                return True
        except Exception:
            pass
        time.sleep(9)
    return False
fails = []
with report("4316_fanout") as r:
    r.heading("ops 4316 -- the two engines, everywhere they belong")
    r.section("1. best-setups carries the radar")
    if not floor_ok("justhodl-best-setups"):
        fails.append("best-setups deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-best-setups",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/best-setups.json")["Body"].read())
        jn = d.get("reversal_join") or {}
        r.ok("annotated %s/50 setups · radar universe %s"
             % (jn.get("n"), jn.get("radar_universe")))
        r.log("CONFLICTS (bullish verdict vs top-forming): %s"
              % jn.get("conflicts"))
        samp = next((x for x in d.get("top_setups") or []
                     if x.get("reversal")), {})
        r.kv(ticker=samp.get("ticker"),
             rv=json.dumps(samp.get("reversal"))[:100],
             conflict=samp.get("reversal_conflict"))
        if (jn.get("n") or 0) < 8:
            fails.append("join thin: %s" % jn)
    r.section("2. risk-gate collateral leg")
    if not floor_ok("justhodl-risk-gate"):
        fails.append("risk-gate deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-risk-gate",
                   InvocationType="RequestResponse", Payload=b"{}")
        g = json.loads(s3.get_object(
            Bucket="justhodl-dashboard-live",
            Key="data/risk-gate.json")["Body"].read())
        col = (g.get("legs") or {}).get("collateral") or {}
        r.ok("collateral leg: score=%s applied=%s"
             % (col.get("score"), col.get("applied", "none "
                "(band < STRAINED -- weights untouched)")))
        r.log("why: %s" % (col.get("why") or [""])[0][:110])
        r.log("posture=%s composite=%s"
              % (g.get("posture"), g.get("composite")))
        if col.get("score") is None and "unreadable" in str(
                col.get("why")):
            fails.append("collateral leg unreadable")
        if not col:
            fails.append("collateral leg absent")
    r.section("3. why.html panel on edge")
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/why.html",
                headers={"User-Agent": "ops/4316",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "composeReversal" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("jh-reversal", "composeReversal",
               "Trend &amp; Reversal", "open radar"):
        if mk not in body:
            fails.append("why.html missing %s" % mk)
    if "composeReversal" in body:
        r.ok("why.html panel LIVE (%d bytes)" % len(body))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4316 PASS -- research a name, see its trend; "
             "gate risk, see collateral")
if fails:
    sys.exit(1)

# retrigger: risk-gate bucket/client identifiers corrected
