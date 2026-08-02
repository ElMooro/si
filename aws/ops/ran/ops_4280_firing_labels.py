"""ops_4280 -- v2.0.2 cosmetic: firing-canary names render as names,
not dict reprs. Gate: version 2.0.2 + triggered[0] contains no '{'."""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4280_firing_labels") as r:
    r.heading("ops 4280 -- firing labels humanized")
    doc = None
    for _ in range(45):
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
                        Bucket="justhodl-dashboard-live",
                        Key="data/quantum-desk.json")["Body"].read())
                    if doc.get("version") == "2.0.2":
                        break
        except Exception:
            pass
        time.sleep(8)
    cb = (doc or {}).get("canary_barometer") or {}
    trig = cb.get("triggered") or []
    r.log("triggered: %s" % trig[:6])
    if (doc or {}).get("version") != "2.0.2":
        fails.append("v2.0.2 not landed")
    elif trig and "{" in str(trig[0]):
        fails.append("still dict reprs: %s" % trig[0])
    else:
        r.ok("labels clean; barometer %s %s"
             % (cb.get("level"), cb.get("score")))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4280 PASS")
if fails:
    sys.exit(1)
