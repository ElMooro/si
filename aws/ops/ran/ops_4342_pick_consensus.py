"""ops_4342 -- enfranchise PICK, surface avg-win + conf/age: the
council's first true multi-engine consensus should ignite."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=400,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4342_pick_consensus") as r:
    r.heading("ops 4342 -- five seats, one voice")
    try:
        ts = subprocess.run(
            ["git", "log", "-1", "--format=%ct", "--",
             "aws/lambdas/justhodl-alpha-council"],
            capture_output=True, text=True, timeout=30
        ).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-alpha-council")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") in (None, "Active") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("deploy floor")
    else:
        lam.invoke(FunctionName="justhodl-alpha-council",
                   InvocationType="RequestResponse", Payload=b"{}")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/alpha-council.json"
        )["Body"].read())
        cc = d.get("consensus_calls") or []
        r.ok("consensus=%d · self_logged=%s"
             % (len(cc), d.get("self_logged")))
        for x in cc[:5]:
            names = [e0["engine"] for e0 in x["engines"]]
            r.log("%s %s · n=%s · score=%s · %s"
                  % (x["symbol"], x["direction"],
                     x["council_n"], x["weighted_score"], names))
            if len(set(names)) != x["council_n"]:
                fails.append("dup engines on %s" % x["symbol"])
        pick = next((x for x in cc if x["symbol"] == "PICK"),
                    None)
        if not pick:
            fails.append("PICK consensus did not form")
        elif pick["council_n"] < 4:
            fails.append("PICK thin: n=%s" % pick["council_n"])
        else:
            r.ok("PICK consensus: %d distinct proven engines, "
                 "unanimous %s" % (pick["council_n"],
                                   pick["direction"]))
        if not d.get("self_logged"):
            fails.append("consensus not self-logged")
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/alpha-council.html",
                headers={"User-Agent": "ops/4342"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "avg win" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ("avg win", "conf ${tc.confidence"):
        if mk not in body:
            fails.append("edge missing %s" % mk)
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4342 PASS -- the council speaks, and signs its name")
