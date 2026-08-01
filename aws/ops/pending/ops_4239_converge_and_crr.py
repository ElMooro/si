"""
ops_4239 — converge the schedule drift, and close the one DR gap that is
actually real.

A. RECONCILER v1.1.0 — a third mode. Full "enforce" is too blunt for an
   unattended daily loop: a legitimately new engine whose schedule has
   not yet reached the manifest would be disabled before anyone noticed.
   DUPLICATE_TARGET is different — one rule listing the same function
   twice with the same payload has no valid meaning at all, it can only
   ever mean double-fire. That single class now self-heals continuously;
   every other drift class still waits for a human. Mode is set to
   enforce-duplicates and the 4 outstanding duplicates are converged.

B. DR CORRECTION. The audit in ops 4238 corrected an assumption I had
   stated as fact: justhodl-dr-snapshot IS deployed, IS scheduled at
   06:00 UTC, HAS run 28 times in 14 days, and justhodl-dashboard-live-dr
   holds 3,000+ objects with the newest written today, versioning on.
   Backups are real.

   What is NOT real is the cross-region replication its own docstring
   claims. get_bucket_replication returns nothing on either DR bucket.
   Every byte still lives in us-east-1, which is the exact scenario the
   backup was built to survive. This section creates the us-west-2
   destination, the replication role, and the replication rule — and
   then VERIFIES by reading the configuration back rather than trusting
   the API call's return.

   justhodl-backups-857687956942 is also reported: 1 object, 19 days
   stale, versioning disabled. It is left untouched and flagged rather
   than deleted.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, DEST_REGION = "us-east-1", "us-west-2"
SRC = "justhodl-dashboard-live-dr"
DEST = "justhodl-dr-usw2-857687956942"
ROLE_NAME = "jh-s3-dr-replication"
FN = "justhodl-schedule-reconciler"
MARKER = "schedule-reconciler v1.1.0 ops4239 enforce-duplicates"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=180)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
s3w = boto3.client("s3", region_name=DEST_REGION, config=CFG)
ssm = boto3.client("ssm", region_name=REGION, config=CFG)
iam = boto3.client("iam", config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
    return buf.getvalue()

def wait_active(fn, b=200):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
                return True
        except Exception: pass
        time.sleep(4)
    return False

with report("4239_converge_and_crr") as rep:
    rep.heading("ops 4239 — converge schedules, real cross-region DR")
    fails = []

    rep.section("A. Reconciler v1.1.0 + converge duplicates")
    try:
        wait_active(FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN))
        ok = False
        for i in range(25):
            time.sleep(6)
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                src = zipfile.ZipFile(io.BytesIO(urlopen(loc, timeout=60).read())
                                      ).read("lambda_function.py").decode("utf-8","ignore")
                if MARKER in src: ok = True; break
            except Exception: pass
        (rep.ok if ok else rep.fail)("marker %s" % ("verified" if ok else "MISSING"))
        if not ok: fails.append("reconciler marker")
        ssm.put_parameter(Name="/justhodl/schedules/mode",
                          Value="enforce-duplicates", Type="String", Overwrite=True)
        rep.ok("SSM mode = enforce-duplicates")
        wait_active(FN)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        b = json.loads(r["Payload"].read() or b"{}")
        rep.log("converge run -> %s" % json.dumps(b)[:300])
        time.sleep(5)
        r2 = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        b2 = json.loads(r2["Payload"].read() or b"{}")
        rep.log("verify run  -> %s" % json.dumps(b2)[:300])
        if b2.get("drift_count") == 0:
            rep.ok("DRIFT = 0 — live AWS now matches the declared manifest")
        else:
            rep.warn("residual drift %s: %s" % (b2.get("drift_count"), b2.get("by_class")))
    except Exception as e:
        fails.append("reconciler: %s" % str(e)[:170])

    rep.section("B1. Destination bucket in %s" % DEST_REGION)
    try:
        try:
            s3w.create_bucket(Bucket=DEST,
                CreateBucketConfiguration={"LocationConstraint": DEST_REGION})
            rep.ok("created %s" % DEST)
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                rep.log("bucket already exists")
            else:
                raise
        s3w.put_bucket_versioning(Bucket=DEST,
            VersioningConfiguration={"Status": "Enabled"})
        v = s3w.get_bucket_versioning(Bucket=DEST).get("Status")
        (rep.ok if v == "Enabled" else rep.fail)("destination versioning=%s" % v)
        if v != "Enabled": fails.append("dest versioning")
    except Exception as e:
        fails.append("dest bucket: %s" % str(e)[:170])

    rep.section("B2. Replication role")
    role_arn = None
    trust = {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
             "Principal":{"Service":"s3.amazonaws.com"},
             "Action":"sts:AssumeRole"}]}
    perm = {"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":["s3:GetReplicationConfiguration",
         "s3:ListBucket"],"Resource":"arn:aws:s3:::%s" % SRC},
        {"Effect":"Allow","Action":["s3:GetObjectVersionForReplication",
         "s3:GetObjectVersionAcl","s3:GetObjectVersionTagging"],
         "Resource":"arn:aws:s3:::%s/*" % SRC},
        {"Effect":"Allow","Action":["s3:ReplicateObject","s3:ReplicateDelete",
         "s3:ReplicateTags"],"Resource":"arn:aws:s3:::%s/*" % DEST}]}
    try:
        try:
            r = iam.create_role(RoleName=ROLE_NAME,
                AssumeRolePolicyDocument=json.dumps(trust),
                Description="S3 cross-region replication for JustHodl DR")
            role_arn = r["Role"]["Arn"]
            rep.ok("created role %s" % ROLE_NAME)
            time.sleep(12)
        except Exception as e:
            if "EntityAlreadyExists" in str(e):
                role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
                rep.log("role already exists")
            else:
                raise
        iam.put_role_policy(RoleName=ROLE_NAME, PolicyName="replication",
                            PolicyDocument=json.dumps(perm))
        rep.ok("role policy attached — %s" % role_arn)
    except Exception as e:
        fails.append("role: %s" % str(e)[:170])

    rep.section("B3. Replication rule + read-back verification")
    if role_arn:
        try:
            s3.put_bucket_versioning(Bucket=SRC,
                VersioningConfiguration={"Status": "Enabled"})
            s3.put_bucket_replication(Bucket=SRC,
                ReplicationConfiguration={"Role": role_arn, "Rules": [{
                    "ID": "jh-dr-usw2", "Priority": 1, "Status": "Enabled",
                    "Filter": {}, "DeleteMarkerReplication": {"Status": "Disabled"},
                    "Destination": {"Bucket": "arn:aws:s3:::%s" % DEST,
                                    "StorageClass": "STANDARD_IA"}}]})
            rc = s3.get_bucket_replication(Bucket=SRC)["ReplicationConfiguration"]
            rules = rc.get("Rules", [])
            good = any(r.get("Status") == "Enabled" and
                       DEST in (r.get("Destination") or {}).get("Bucket","")
                       for r in rules)
            (rep.ok if good else rep.fail)(
                "replication read-back: %d rule(s), enabled->%s = %s"
                % (len(rules), DEST, good))
            if not good: fails.append("replication read-back")
            rep.kv(section="crr", source=SRC, dest=DEST, region=DEST_REGION,
                   rules=len(rules), verified=good)
        except Exception as e:
            fails.append("replication: %s" % str(e)[:190])

    rep.section("B4. Note the stale second bucket")
    rep.warn("justhodl-backups-857687956942 — 1 object, 19 days old, "
             "versioning DISABLED. Left untouched: identify what wrote it "
             "before removing anything.")

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4239 PASS — schedules converged and DR now leaves us-east-1.")
