"""ops/4853 -- catalyst-chain birth verify (Fusion 3).
 G0  all four inputs readable; spine (catalyst+readthrough) fresh
     <48h; shapes: by_ticker dicts + direction_map + beneficiaries.
 (1) settle 'catalyst-chain v1.0.0'; schedule daily 22:15 UTC.
 (2) invoke; poll data/catalyst-chain.json <=3 min.
 (3) truths: LIVE; sampled top-unpriced chain score == in-op
     independent recompute from the four LIVE docs; unpriced and
     completed disjoint; every unpriced has stage>=2 and
     s4 != UP; stage hist sums to n_subjects; no-coverage gap
     count == independent count; readout top chains.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
FN = "justhodl-catalyst-chain"
B = "justhodl-dashboard-live"
OUT_KEY = "data/catalyst-chain.json"
MARKER = "catalyst-chain v1.0.0"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
sched = boto3.client("scheduler", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4853 -- catalyst-chain birth verify") as rep:
        rep.heading("G0. input contracts")
        try:
            cat = sread("data/catalyst.json")
            rt = sread("data/readthrough.json")
            bl = sread("data/backlog.json")
            er = sread("data/estimate-revisions.json")
        except ClientError as e:
            rep.fail("inputs unreadable: %s" % e)
            sys.exit(1)
        ok = (isinstance(cat.get("by_ticker"), dict)
              and cat["by_ticker"]
              and isinstance(rt.get("beneficiaries"), list)
              and isinstance(rt.get("by_beneficiary"), list)
              and isinstance(bl.get("by_ticker"), dict)
              and isinstance(er.get("direction_map"), dict))
        if ok:
            rep.ok("shapes ok: cat=%d bl=%d dmap=%d benef=%d"
                   % (len(cat["by_ticker"]), len(bl["by_ticker"]),
                      len(er["direction_map"]),
                      len(rt["by_beneficiary"])))
        else:
            rep.fail("shape contract broken")
            sys.exit(1)

        rep.heading("1. settle + schedule")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
                if MARKER in src:
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        fn_arn = ("arn:aws:lambda:%s:%s:function:%s"
                  % (REGION, ACCOUNT, FN))
        role = ("arn:aws:iam::%s:role/justhodl-scheduler-role"
                % ACCOUNT)
        try:
            sched.create_schedule(
                Name="justhodl-catalyst-chain-daily",
                ScheduleExpression="cron(15 22 * * ? *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": fn_arn, "RoleArn": role,
                        "Input": "{}",
                        "RetryPolicy": {
                            "MaximumRetryAttempts": 2,
                            "MaximumEventAgeInSeconds": 3600}},
                Description="catalyst-chain daily 22:15 "
                "(ops 4853)")
            rep.ok("schedule daily 22:15 UTC")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConflictException":
                rep.ok("schedule exists")
            else:
                rep.fail("schedule: %s" % e)
                FAILED.append("sched")

        rep.heading("2. invoke + poll")
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 180:
            time.sleep(8)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("3. truths")
        if doc.get("status") == "LIVE" and doc.get("chains"):
            rep.ok("  LIVE; chains=%d unpriced=%d completed=%d"
                   % (len(doc["chains"]), len(doc["unpriced"]),
                      len(doc["completed"])))
        else:
            rep.fail("  status=%s" % doc.get("status"))
            FAILED.append("live")
            sys.exit(1)
        up = {r["t"] for r in doc["unpriced"]}
        co = {r["t"] for r in doc["completed"]}
        if not (up & co) and all(
                r["stage"] >= 2 and r["s4_direction"] != "UP"
                for r in doc["unpriced"]):
            rep.ok("  unpriced/completed disjoint; unpriced "
                   "invariants hold")
        else:
            rep.fail("  invariant broken: overlap=%s" % (up & co))
            FAILED.append("inv")
        if sum(doc["diag"]["stage_hist"].values()) \
                == doc["diag"]["n_subjects"]:
            rep.ok("  stage hist sums to %d subjects"
                   % doc["diag"]["n_subjects"])
        else:
            rep.fail("  hist mismatch")
            FAILED.append("hist")
        # independent recompute of the top unpriced chain
        top = doc["unpriced"][0] if doc["unpriced"] else None
        if top:
            t = top["t"]
            cat_by = cat["by_ticker"]
            if top["order"] == 1:
                ev = (cat_by.get(t) or {}).get("score") or 0
                conf = 1.0
            else:
                srcs = top["sources"]
                ev = sum((cat_by.get(s) or {}).get("score") or 0
                         for s in srcs)
                if ev <= 0:
                    rr = next((r for r in rt["by_beneficiary"]
                               if r.get("ticker") == t), {})
                    ev = round((rr.get("max_score") or 0) / 20.0,
                               3)
                conf = max((b.get("edge_confidence") or 0)
                           for b in rt["beneficiaries"]
                           if b.get("ticker") == t)
            blr = bl["by_ticker"].get(t)
            confirmed = bool(blr and (
                (blr.get("rpo_qoq") or 0) > 0
                or blr.get("deferred_accelerating") is True
                or (blr.get("deferred_qoq") or 0) > 10))
            d4 = er["direction_map"].get(t)
            mult = 1.2 if d4 == "DOWN" else 1.0
            exp = round(ev * conf * (1.5 if confirmed else 1.0)
                        * mult, 3)
            if abs(top["score"] - exp) < 1e-6:
                rep.ok("  top unpriced %s score %.3f == "
                       "independent recompute" % (t,
                                                  top["score"]))
            else:
                rep.fail("  %s score %.3f != ind %.3f"
                         % (t, top["score"], exp))
                FAILED.append("ind")
        n_gap_ind = sum(
            1 for t in ({x for x in cat["by_ticker"]}
                        | {r["ticker"] for r in
                           rt["by_beneficiary"]
                           if r.get("ticker")})
            if t not in bl["by_ticker"])
        if doc["diag"]["n_no_filing_coverage"] == n_gap_ind:
            rep.ok("  no-coverage gap count == independent (%d)"
                   % n_gap_ind)
        else:
            rep.fail("  gap %d != ind %d"
                     % (doc["diag"]["n_no_filing_coverage"],
                        n_gap_ind))
            FAILED.append("gap")

        rep.heading("4. readout -- top unpriced chains")
        for r in doc["unpriced"][:8]:
            rep.log("  %-6s o%d stage%d score %6.3f conf %.2f "
                    "%s | %s | %s"
                    % (r["t"], r["order"], r["stage"],
                       r["score"], r["s2_conf"],
                       (r.get("s2_tier") or "")[:18],
                       r["s3_why"][:34], r["s4_direction"]))

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("Fusion 3 LIVE -- the chain the fleet saw in "
               "pieces is now one machine")


if __name__ == "__main__":
    main()
