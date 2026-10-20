"""ops 6548 - stop the Lightpanda crawler pulling ~46 GB/day (~$4/day) straight from justhodl-dashboard-live.

Evidence (ops 6547, S3 server access logs, Oct 16-20): the bucket served 44 -> 133 GB/day of anonymous GETs while
every engine was frozen.  46 GB/day of that is the self-declared crawler "Lightpanda/1.0" (20,559 requests, ~2.2 MB
each, concentrated 02-06 UTC) fetching the big data/*.json files directly from the public S3 endpoint with
Referer justhodl.ai.  It is not a customer and not one of our engines.

STAGED (direct lane).  Mutates ONE thing: the bucket policy of justhodl-dashboard-live, by adding (or refreshing)
one explicit-Deny statement keyed on the aws:UserAgent header.  Every other statement is left byte-for-byte as is.
  1. read the live policy, refuse to continue if it is not what ops 6547 saw (34 statements, PublicReadDataDir)
  2. back the raw policy up to the private ops bucket (the rollback copy)
  3. add statement DenyAbusiveScrapersByUserAgent (idempotent: replaced in place when it already exists)
  4. put, read back, prove every other statement is unchanged
  5. prove it with real HTTP: HEAD of a public object -> 403 for a Lightpanda UA, 200 for a browser UA
Rollback: put the backup back (printed in the report), or delete the one statement by Sid.

Cost: one PutBucketPolicy + 4 HEAD requests.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
SID = "DenyAbusiveScrapersByUserAgent"
PROBE_KEY = "data/master-ranker.json"          # public via PublicReadDataDir; HEAD only, no body is transferred
MAX_POLICY_BYTES = 19500                       # S3 limit is 20,480


def deny_statement():
    return {
        "Sid": SID,
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::%s/*" % BUCKET,
        "Condition": {"StringLike": {"aws:UserAgent": ["*Lightpanda*", "*lightpanda*", "*LIGHTPANDA*"]}},
    }


def merge(policy, stmt):
    """-> (new policy, other statements unchanged?).  Replaces the statement with the same Sid, else appends."""
    out = json.loads(json.dumps(policy))
    stmts = out["Statement"] if isinstance(out["Statement"], list) else [out["Statement"]]
    kept = [s for s in stmts if s.get("Sid") != SID]
    out["Statement"] = kept + [stmt]
    return out, kept


def head(url, ua):
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": ua})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def selftest():
    base = {"Version": "2012-10-17", "Statement": [{"Sid": "A", "Effect": "Allow", "Principal": "*",
                                                    "Action": "s3:GetObject", "Resource": "arn:aws:s3:::b/*"}]}
    once, kept1 = merge(base, deny_statement())
    twice, kept2 = merge(once, deny_statement())
    assert [s["Sid"] for s in once["Statement"]] == ["A", SID], once
    assert [s["Sid"] for s in twice["Statement"]] == ["A", SID], "idempotent"
    assert kept1 == base["Statement"] and kept2 == base["Statement"]
    single = {"Version": "2012-10-17", "Statement": base["Statement"][0]}
    assert [s["Sid"] for s in merge(single, deny_statement())[0]["Statement"]] == ["A", SID]
    st = deny_statement()
    assert st["Effect"] == "Deny" and st["Action"] == "s3:GetObject" and "Lightpanda" in json.dumps(st)
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6548_deny_scraper_user_agents") as r:
        r.heading("ops 6548 - deny the Lightpanda crawler at the bucket")
        raw = s3.get_bucket_policy(Bucket=BUCKET)["Policy"]
        policy = json.loads(raw)
        stmts = policy["Statement"] if isinstance(policy["Statement"], list) else [policy["Statement"]]
        sids = [s.get("Sid") for s in stmts]
        r.log("live policy: %d statements, %d bytes" % (len(stmts), len(raw)))
        if "PublicReadDataDir" not in sids or len(stmts) < 30:
            r.fail("policy is not the one ops 6547 inspected (PublicReadDataDir missing or < 30 statements); refusing to change it")
            raise SystemExit(1)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bkey = "backups/bucket-policy/%s-%s.json" % (BUCKET, stamp)
        s3.put_object(Bucket=PRIVATE, Key=bkey, Body=raw.encode(), ContentType="application/json", ServerSideEncryption="AES256")
        r.ok("rollback copy: s3://%s/%s" % (PRIVATE, bkey))

        new, kept = merge(policy, deny_statement())
        body = json.dumps(new, separators=(",", ":"))
        if len(body) > MAX_POLICY_BYTES:
            r.fail("new policy would be %d bytes (limit ~20 KB); not applying" % len(body))
            raise SystemExit(1)
        s3.put_bucket_policy(Bucket=BUCKET, Policy=body)
        r.log("put_bucket_policy: %d statements, %d bytes" % (len(new["Statement"]), len(body)))

        back = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        bstm = back["Statement"] if isinstance(back["Statement"], list) else [back["Statement"]]
        mine = [s for s in bstm if s.get("Sid") == SID]
        others = [s for s in bstm if s.get("Sid") != SID]
        assert len(mine) == 1 and mine[0]["Effect"] == "Deny", "deny statement missing after put"
        assert others == kept, "another statement changed"
        r.ok("read-back: %d statements, the %d others are identical to before" % (len(bstm), len(others)))

        url = "https://%s.s3.%s.amazonaws.com/%s" % (BUCKET, REGION, PROBE_KEY)
        want = {"Lightpanda/1.0": 403, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15": 200}
        got = {}
        for _ in range(12):                       # a bucket-policy change takes a few seconds to propagate
            got = {ua: head(url, ua) for ua in want}
            if got == want:
                break
            time.sleep(5)
        for ua, code in got.items():
            r.log("HEAD %s as %r -> %s (want %s)" % (PROBE_KEY, ua[:24], code, want[ua]))
        if got != want:
            r.fail("probe did not return the expected codes: %s" % got)
            raise SystemExit(1)
        r.ok("Lightpanda is refused (403); a normal browser still gets the file (200)")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
