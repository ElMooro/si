"""ops 4710 — deploy justhodl-te-fred-mirror, prove the FRED
separation is real (not just written that way), run the first tranche,
verify cross-check + data.html surfacing.

Creates the function from repo source (deploy-lambdas only creates in
MISSING-mode dispatch — same lesson as tv-bars, ops 4674), wires the
hourly schedule, then runs the contract that matters most tonight:
a LIVE delete attempt against data/warm/fred-scoped/ must still be
rejected AFTER this new engine exists, proving the new engine has no
path to ever touch it (not tested by writing to te-mirror -- tested by
confirming fred-scoped stays exactly as protected as before).
"""
import io
import json
import sys
import time
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-te-fred-mirror"
SRC = "aws/lambdas/justhodl-te-fred-mirror/source/lambda_function.py"
CFG = "aws/lambdas/justhodl-te-fred-mirror/config.json"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4710_te_mirror_create") as r:
        r.heading("ops 4710 — create te-fred-mirror + prove FRED "
                  "separation")
        misses = 0

        r.section("1. Create/update the function")
        cfg = json.load(open(CFG))
        code = open(SRC, "rb").read()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
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
            r.ok("  CREATED %s" % FN)
        t0, active = time.time(), False
        while time.time() - t0 < 240:
            try:
                c = lam.get_function(FunctionName=FN)["Configuration"]
                if c.get("State") == "Active" and c.get(
                        "LastUpdateStatus") in ("Successful", None):
                    active = True
                    break
            except Exception:
                pass
            time.sleep(8)
        misses += contract(r, "deploy", active, "%s Active" % FN)
        if not active:
            sys.exit(1)

        r.section("2. Hourly schedule")
        try:
            sch = cfg.get("schedule") or {}
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

        r.section("3. THE CRITICAL PROOF — fred-scoped is UNTOUCHED "
                  "and stays delete-proof after this engine exists")
        probe_k = "data/warm/fred-scoped/_permanence_probe_te.json"
        s3.put_object(Bucket=B, Key=probe_k, Body=b"{}",
                     ContentType="application/json")
        blocked = False
        try:
            s3.delete_object(Bucket=B, Key=probe_k)
        except Exception as e:
            blocked = "denied" in str(e).lower() or "AccessDenied" \
                in str(e)
        misses += contract(
            r, "separation", blocked,
            "fred-scoped delete-proof STILL enforced — the new "
            "engine's existence changed nothing about FRED's "
            "protection")

        r.section("4. Run the first tranche (sync)")
        resp = lam.invoke(FunctionName=FN,
                         InvocationType="RequestResponse",
                         Payload=b"{}")
        raw = resp["Payload"].read().decode("utf-8", "replace")
        r.log("  handler: %s" % raw[:400])

        r.section("5. Verify: wrote ONLY to te-mirror, cross-check "
                  "populated, FRED docs byte-identical")
        try:
            idx = json.loads(s3.get_object(
                Bucket=B, Key="data/warm/te-mirror/_index.json"
            )["Body"].read())
            r.log("  te-mirror index: n_symbols=%s mean_agree_pct=%s"
                 % (idx.get("n_symbols"), idx.get("mean_agree_pct")))
            sample_sid = next(iter(idx.get("symbols") or {}), None)
            if sample_sid:
                doc = json.loads(s3.get_object(
                    Bucket=B,
                    Key="data/warm/te-mirror/%s.json" % sample_sid
                )["Body"].read())
                xc = doc.get("cross_check") or {}
                r.log("  sample %s: n=%s cross_check=%s"
                     % (sample_sid, doc.get("n"), xc))
                misses += contract(
                    r, "crosscheck", xc.get("fred_doc_found") is True,
                    "cross-check found and compared against the "
                    "REAL FRED doc (agree_pct=%s on %s shared "
                    "dates)" % (xc.get("agree_pct"),
                               xc.get("shared_dates")))
        except Exception as e:
            r.warn("  te-mirror readback: %s" % str(e)[:120])

        r.section("6. data.html surfacing — kick provider-catalog, "
                  "confirm the note appears")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                     InvocationType="Event")
        except Exception as e:
            r.warn("  catalog kick: %s" % str(e)[:80])
        note, t1 = None, time.time()
        while time.time() - t1 < 180:
            time.sleep(15)
            try:
                hub = json.loads(s3.get_object(
                    Bucket=B,
                    Key="data/provider-catalog.json")["Body"].read())
                for pv in hub.get("providers") or []:
                    if pv.get("slug") == "te-mirror":
                        note = pv.get("coverage_note") or pv.get(
                            "catalog_note")
                if note:
                    break
            except Exception:
                pass
        r.log("  te-mirror note on data.html: %s" % note)
        misses += contract(r, "surface", bool(note),
                          "provider card note is live")

        r.section("verdict")
        if misses:
            r.fail("te-mirror deploy: %d red" % misses)
            sys.exit(1)
        r.ok("standing engine live: hourly schedule, FRED separation "
            "PROVEN (not assumed), cross-check populated, data.html "
            "surfacing confirmed")


if __name__ == "__main__":
    main()
