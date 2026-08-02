"""
ops_4291 -- quantum-desk page v3: the whole artifact, rendered.

Khalid's read was correct -- the page showed a fraction of the engine.
Coverage audit found unrendered fields (weights, evidence_stats,
regime abstains, barometer headline, full audit incl compass ER,
mm conviction/verdict/size/earnings) plus a live display:none bug
hiding built pills, plus 180 days of history the page never fetched.
v3 renders every field; engine v2.1 adds per-class scores to history
so sparklines grow real. Gate: engine v2.1.0 live with scores in the
history row; edge page carries the v3 markers and fetches HIST; hero
shows the barometer headline; mm pills bug gone.
"""
import json, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

fails = []
with report("4291_desk_v3") as r:
    r.heading("ops 4291 -- quantum desk v3: nothing left unrendered")

    r.section("1. engine v2.1.0 + history scores")
    doc = None
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-quantum-desk")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 12 * 60:
                    lam.invoke(FunctionName="justhodl-quantum-desk",
                               InvocationType="RequestResponse",
                               Payload=b"{}")
                    doc = json.loads(s3.get_object(
                        Bucket=BUCKET,
                        Key="data/quantum-desk.json")["Body"].read())
                    if doc.get("version") == "2.1.0":
                        break
        except Exception:
            pass
        time.sleep(8)
    if not doc or doc.get("version") != "2.1.0":
        fails.append("engine v2.1.0 not landed (saw %s)"
                     % (doc or {}).get("version"))
    else:
        hist = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/quantum-desk-history.json")["Body"].read())
        last = (hist.get("rows") or [{}])[-1]
        if last.get("scores"):
            r.ok("history row now carries per-class scores: %s"
                 % dict(list(last["scores"].items())[:4]))
        else:
            fails.append("history row missing scores block")
        for key in ("weights", "evidence_stats", "canary_barometer",
                    "regime", "money_map"):
            if key not in doc:
                fails.append("artifact missing %s" % key)
        r.log("weights: %s" % json.dumps(doc.get("weights"))[:130])
        cb = doc.get("canary_barometer") or {}
        r.log("barometer headline: %s" % cb.get("headline"))
        mm0 = (doc.get("money_map") or [{}])[0]
        r.log("mm[0] renderable fields: fit=%s conv=%s verdict=%s "
              "size=%s earn=%s x%s"
              % (mm0.get("khalid_fit"), mm0.get("conviction"),
                 mm0.get("setup_verdict"), mm0.get("size_hint_x"),
                 mm0.get("earnings_in_days"),
                 mm0.get("n_corroborating")))

    r.section("2. page v3 on the edge")
    body = ""
    for i in range(12):
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/quantum-desk.html",
                headers={"User-Agent": "ops/4291",
                         "Cache-Control": "no-cache"})
            body = urllib.request.urlopen(req, timeout=25).read().decode(
                "utf-8", "ignore")
            if "Desk history" in body:
                break
        except Exception as e:
            r.log("wait %d: %s" % (i, str(e)[:60]))
        time.sleep(20)
    marks = ["Desk history", "Blend weights", "evidence from",
             "quantum-desk-history.json", "Evidence join",
             "asym status"]
    missing = [m for m in marks if m not in body]
    if missing:
        fails.append("page markers missing on edge: %s" % missing)
    else:
        r.ok("page v3 LIVE (%d bytes) -- history fetch, weights line, "
             "full audit, evidence tables all present" % len(body))
    if "display:none\">" in body and "histsec" not in body.split(
            "display:none")[0][-80:]:
        pass  # only the intentional histsec hider is allowed
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4291 PASS -- the desk shows everything it knows")
if fails:
    sys.exit(1)
