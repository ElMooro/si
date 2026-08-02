"""
ops_4267 -- party map, finally live: the 404 told the whole story.
The unitedstates/congress-legislators repo is YAML-first on main; the
JSON build ships on gh-pages (what theunitedstates.io serves). Branch
fixed. Gate: map < 20 min old, >= 500 members, source shows gh-pages.
"""
import json, sys, time
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
with report("4267_party_map_live") as r:
    r.heading("ops 4267 -- congress party map: gh-pages source")
    ok_deploy = False
    for _ in range(45):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-political-stocks")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm_dt = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm_dt).total_seconds() < 12 * 60:
                    ok_deploy = True
                    break
        except Exception:
            pass
        time.sleep(8)
    if not ok_deploy:
        fails.append("deploy never settled inside 12-min window")
    else:
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:130].decode("utf-8",
                                                          "ignore"))
        try:
            h = s3.head_object(Bucket=BUCKET,
                               Key="data/congress-party-map.json")
            a = (datetime.now(timezone.utc)
                 - h["LastModified"]).total_seconds() / 60.0
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/congress-party-map.json")["Body"].read())
            n = doc.get("n") or len(doc.get("party_map") or {})
            src2 = str(doc.get("source", ""))
            if a < 20 and n >= 500 and "gh-pages" in src2:
                from collections import Counter
                parties = Counter(
                    (doc.get("party_map") or {}).values())
                r.ok("party map LIVE after 62 days: %.1f min old, "
                     "%d members, composition %s, source gh-pages"
                     % (a, n, dict(parties.most_common(4))))
            else:
                fails.append("still not live: age=%.0f min n=%s "
                             "source=%s" % (a, n, src2[:70]))
        except Exception as e:
            fails.append("verify: %s" % str(e)[:110])
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4267 PASS -- self-refreshing party map on the "
             "canonical dataset; wave 4 truly closed")
if fails:
    sys.exit(1)
