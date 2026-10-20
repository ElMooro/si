"""ops 6572 - piece 3 of the bucket design: the governor's storage fuse (v1.5.0) - rights, selftest, live dry run.

1. Governor role: + cloudwatch:ListMetrics (storage types of the bucket), s3:GetBucketVersioning / s3:PutBucketVersioning
   on justhodl-dashboard-live only (the second-strike lever; {"action":"storage_restore"} re-enables).
2. {"action":"storage_selftest"}: the six scenarios (quiet / warn / trip / objects / two warns / second) through the
   pure logic, plus the live read path (daily S3 metrics of the bucket, versioning status).
3. {"action":"storage_check"} dry: today's real assessment of the bucket - growth over the last complete day, level,
   yesterday's S3 cost line - reported, nothing recorded, nothing held.
4. {"action":"status"}: version 1.5.0.
"""
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
ROLE, POLICY = "justhodl-spend-governor-role", "justhodl-spend-governor"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def gov(payload):
    lam = c("lambda")
    for attempt in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:400])
            return body
        except lam.exceptions.TooManyRequestsException:
            time.sleep(20)
    raise RuntimeError("governor busy")


def role_rights(r):
    iam = c("iam")
    doc = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY)["PolicyDocument"]
    stmts = [s for s in doc["Statement"] if s.get("Sid") not in ("StorageFuseMetrics", "StorageFuseVersioning")]
    stmts.append({"Sid": "StorageFuseMetrics", "Effect": "Allow", "Action": ["cloudwatch:ListMetrics"], "Resource": "*"})
    stmts.append({"Sid": "StorageFuseVersioning", "Effect": "Allow", "Action": ["s3:GetBucketVersioning", "s3:PutBucketVersioning"],
                  "Resource": "arn:aws:s3:::%s" % LIVE})
    doc["Statement"] = stmts
    iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(doc))
    r.ok("governor role: + ListMetrics, Get/PutBucketVersioning on %s only (%d statements)" % (LIVE, len(stmts)))


def main():
    with report("ops_6572_storage_fuse_verify") as r:
        r.heading("ops 6572 - storage fuse v1.5.0: rights, selftest, live dry run")
        role_rights(r)
        time.sleep(20)   # IAM propagation
        st = gov({"action": "status"})
        r.log("governor version %s, stage %s" % (st.get("version"), st.get("stage")))
        if st.get("version") != "1.5.0":
            r.fail("governor is %s, not 1.5.0 - deploy first" % st.get("version"))
            raise SystemExit(1)
        res = None
        for attempt in range(4):
            res = gov({"action": "storage_selftest"})
            if res.get("ok"):
                break
            r.log("selftest attempt %d: %s" % (attempt + 1, json.dumps(res)[:300]))
            time.sleep(20)
        r.log("storage_selftest: ok=%s scenarios=%s live=%s" % (res.get("ok"), json.dumps(res.get("scenarios")), json.dumps(res.get("live"))[:600]))
        dry = gov({"action": "storage_check", "dry": True})
        a = dry.get("assessment") or {}
        r.log("storage_check (dry): ran=%s level=%s %s; bucket %s GB / %s objects; S3 yesterday $%s" % (
            dry.get("ran"), a.get("level"), a.get("reason"), a.get("gb"), a.get("objects"), dry.get("s3_usd_yesterday")))
        r.kv(version=st.get("version"), selftest_ok=bool(res.get("ok")), dry_level=a.get("level"), delta_gb=a.get("delta_gb"), s3_usd_yesterday=dry.get("s3_usd_yesterday"))
        if not res.get("ok") or not dry.get("ran"):
            r.fail("storage fuse not proven live")
            raise SystemExit(1)
        r.ok("storage fuse live: rights in place, logic verified, bucket read end to end; nothing held")


if __name__ == "__main__":
    main()
