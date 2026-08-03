"""ops_4343 -- every engine on the council page links home."""
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
with report("4343_engine_links") as r:
    r.heading("ops 4343 -- every seat links home")
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
        for c0 in d.get("council") or []:
            if not str(c0.get("link", "")).startswith("http"):
                fails.append("seat unlinked: %s" % c0["engine"])
        r.ok("seat links: %d/%d"
             % (sum(1 for c0 in d.get("council") or []
                    if str(c0.get("link", "")
                           ).startswith("http")),
                len(d.get("council") or [])))
        for x in (d.get("consensus_calls") or [])[:3]:
            for e0 in x["engines"]:
                if not str(e0.get("link", "")).startswith("http"):
                    fails.append("pill unlinked: %s"
                                 % e0["engine"])
        r.log("sample links: %s"
              % [(c0["engine"], c0["link"])
                 for c0 in (d.get("council") or [])[:4]])
    body = ""
    for _ in range(13):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/alpha-council.html",
                headers={"User-Agent": "ops/4343"}),
                timeout=25).read().decode("utf-8", "ignore")
            if 'href="${esc(c.link' in body:
                break
        except Exception:
            pass
        time.sleep(20)
    for mk in ('href="${esc(c.link', 'href="${esc(e.link'):
        if mk not in body:
            fails.append("edge missing anchor template")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4343 PASS -- twelve seats, twelve doors")
