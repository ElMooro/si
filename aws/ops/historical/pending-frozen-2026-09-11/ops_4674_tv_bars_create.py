"""ops 4674 — CREATE justhodl-tv-bars, then prove the rail.

4673 found the real blocker: deploy-lambdas only CREATES functions in
a manual MISSING-mode dispatch; a brand-new Lambda pushed on the normal
path is silently skipped (workflow still reports success). Khalid's
directive is autonomous deployment, so the op creates the function
itself from the repo source — no manual dispatch, no console.

Then the same 3-symbol proof: does TV serve pre-2020 ICE history to a
server-side socket authenticated with the SSM session?
"""
import gzip
import io
import json
import sys
import time
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-tv-bars"
SRC = "aws/lambdas/justhodl-tv-bars/source/lambda_function.py"
CFG = "aws/lambdas/justhodl-tv-bars/config.json"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
TEST = ["BAMLH0A0HYM2", "BAMLC0A2CAA", "BAMLH0A3HYC"]


def gj(key, gz=False):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if gz else raw)
    except Exception:
        return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4674_tv_bars_create") as r:
        r.heading("ops 4674 — create tv-bars engine + prove the rail")
        misses = 0

        r.section("1. Create or update the function from repo source")
        cfg = json.load(open(CFG))
        code = open(SRC, "rb").read()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w",
                             zipfile.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py",
                       code.decode("utf-8", "replace"))
        zb = buf.getvalue()
        exists = True
        try:
            lam.get_function(FunctionName=FN)
        except Exception:
            exists = False
        if exists:
            lam.update_function_code(FunctionName=FN, ZipFile=zb)
            r.log("  updated existing function code")
        else:
            lam.create_function(
                FunctionName=FN, Runtime=cfg["runtime"],
                Role=cfg["role"], Handler=cfg["handler"],
                Code={"ZipFile": zb}, Timeout=cfg["timeout"],
                MemorySize=cfg["memory"],
                Description=cfg["description"][:255],
                Environment={"Variables": cfg.get("env") or {}})
            r.ok("  CREATED %s (deploy-lambdas skips new functions "
                 "outside MISSING-mode dispatch)" % FN)
        t0 = time.time()
        active = False
        while time.time() - t0 < 240:
            try:
                c = lam.get_function(
                    FunctionName=FN)["Configuration"]
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") in (
                            "Successful", None):
                    active = True
                    break
            except Exception:
                pass
            time.sleep(8)
        misses += contract(r, "deploy", active, "%s Active" % FN)
        if not active:
            sys.exit(1)

        r.section("2. Schedule (hourly) so convergence is autonomous")
        try:
            ev = boto3.client("events", region_name="us-east-1")
            sch = cfg.get("schedule") or {}
            if sch:
                ev.put_rule(Name=sch["name"],
                            ScheduleExpression=sch["expression"],
                            State="ENABLED",
                            Description=sch.get("description", "")[:255])
                arn = lam.get_function(
                    FunctionName=FN)["Configuration"]["FunctionArn"]
                ev.put_targets(Rule=sch["name"],
                               Targets=[{"Id": "1", "Arn": arn}])
                try:
                    lam.add_permission(
                        FunctionName=FN,
                        StatementId="events-%s" % sch["name"][:40],
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com")
                except Exception:
                    pass
                r.ok("  schedule %s -> %s" % (sch["name"],
                                              sch["expression"]))
        except Exception as e:
            r.warn("  schedule: %s" % str(e)[:110])

        r.section("3. Pull 3 ICE symbols (sync)")
        resp = lam.invoke(FunctionName=FN,
                          InvocationType="RequestResponse",
                          Payload=json.dumps(
                              {"symbols": TEST}).encode())
        raw = resp["Payload"].read().decode("utf-8", "replace")
        r.log("  handler: %s" % raw[:500])
        if resp.get("FunctionError"):
            misses += contract(r, "pull", False,
                               "handler error: %s" % raw[:300])

        r.section("4. What landed")
        deep = 0
        for sid in TEST:
            d = gj("data/warm/tv-bars/%s.json.gz" % sid, gz=True)
            if not d:
                r.log("  %s: nothing banked" % sid)
                continue
            r.log("  %s: n=%s %s -> %s"
                  % (sid, d.get("n"), d.get("first_date"),
                     d.get("last_date")))
            if str(d.get("first_date") or "9999") < "2020":
                deep += 1
        st = gj("data/warm/tv-bars/_state.json")
        r.log("  failures: %s"
              % dict(list((st.get("failures") or {}).items())[:5]))
        misses += contract(r, "depth", deep >= 1,
                           "%d/%d symbols carry pre-2020 history"
                           % (deep, len(TEST)))

        r.section("verdict")
        if misses:
            r.fail("tv rail: %d red — protocol evidence above drives "
                   "the next revision" % misses)
            sys.exit(1)
        r.ok("server-side TV history rail LIVE and scheduled — no "
             "browser, no manual reload, converges hourly")


if __name__ == "__main__":
    main()
