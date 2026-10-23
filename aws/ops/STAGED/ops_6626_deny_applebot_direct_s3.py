"""ops 6626 - refuse Applebot's direct S3 reads of the multi-MB data files (Khalid 2026-10-23: "supposed to be $200").

Evidence (ops 6623-6625, S3 server access logs): the billed S3 egress of the live bucket was 18.1 GB on Oct 22
(CE DataTransfer-Out-Bytes) and is running at ~45 GB/day on Oct 23. The largest single part - 10.2 GB on Oct 22 and a
30.4 GB/day pace on Oct 23 - is Apple's crawler: user agent "... Safari/605.1.15 (Applebot/0.1;
+http://www.apple.com/go/applebot)", addresses 17.166.x that reverse-resolve to *.applebot.apple.com, round the
clock. Its renderer executes why.html's fleet-forensic block, which fetches eleven 0.1-4.5 MB files
(best-setups, share-flows, estimate-revisions, buyback-engine, forensic-screen, ...) straight from
https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com with a ?_=<now> cache-buster, ~6,000 renders a day.
At $0.09/GB that is $0.9-2.7/day ($27-82/month) for a crawler, the same failure class ops 6549 closed for
Lightpanda on 2026-10-20.

STAGED (direct lane). Mutates ONE thing: the existing bucket-policy statement DenyAbusiveScrapersByUserAgent gets
"*Applebot*" / "*applebot*" added to its aws:UserAgent list. Every other statement is left byte-for-byte as it is.
  1. read the live policy; back the raw JSON up to the private ops bucket (the rollback copy)
  2. build the new policy and PROVE that every other statement is identical and that the one statement differs
     only by the added user-agent patterns (refuse otherwise); size stays under the S3 limit
  3. before: anonymous HEAD with a normal Safari user agent of one real object per public Allow pattern, plus the
     eleven why.html files; HEAD with the observed Applebot user agent of data/best-setups.json
  4. put, read back, compare
  5. after (with propagation retries): every Safari-UA probe answers exactly as before, the Applebot UA gets 403,
     Lightpanda still gets 403, a Googlebot UA is untouched. Any mismatch -> the original policy goes straight back
Applebot still crawls the HTML pages (they are served by GitHub Pages / Cloudflare, not this bucket); only its
renderer's direct downloads of the data files are refused. Rollback by hand: put the backup object named in the report.
Cost: 1 PutBucketPolicy, a few ListObjectsV2, ~120 HEAD requests (< $0.001).
"""
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
import fnmatch
import json
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
ADD = ["*Applebot*", "*applebot*"]
SAFARI_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
APPLEBOT_UA = SAFARI_UA + " (Applebot/0.1; +http://www.apple.com/go/applebot)"
GOOGLEBOT_UA = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
LIGHTPANDA_UA = "Lightpanda/1.0"
MAX_POLICY_BYTES = 20000                       # S3 limit is 20,480
ARN_PREFIX = "arn:aws:s3:::%s/" % BUCKET
WHY_FILES = ["data/forensic-screen.json", "data/share-flows.json", "data/buyback-engine.json", "data/industry-rotation.json",
             "data/master-ranker.json", "data/accumulation-radar.json", "data/insider-radar.json", "data/best-setups.json",
             "data/finra-short.json", "data/ai-rerating-radar.json", "data/estimate-revisions.json"]


def as_list(x):
    return list(x) if isinstance(x, list) else ([] if x is None else [x])


def extended(stmts):
    """(new statements, changed?) - only the SID statement's aws:UserAgent list grows by ADD."""
    out, changed, found = [], False, False
    for s in stmts:
        if s.get("Sid") != SID:
            out.append(s)
            continue
        found = True
        s2 = json.loads(json.dumps(s))
        like = s2.setdefault("Condition", {}).setdefault("StringLike", {})
        uas = as_list(like.get("aws:UserAgent"))
        for u in ADD:
            if u not in uas:
                uas.append(u)
                changed = True
        like["aws:UserAgent"] = uas
        out.append(s2)
    if not found:
        raise ValueError("statement %s not found - this op only extends the existing scraper deny" % SID)
    return out, changed


def proof(old, new):
    """None when the only difference is ADD inside the SID statement's user-agent list; else the reason."""
    if len(old) != len(new):
        return "statement count changed"
    for a, b in zip(old, new):
        if a.get("Sid") != b.get("Sid"):
            return "statement order changed"
        if a.get("Sid") != SID:
            if json.dumps(a, sort_keys=True) != json.dumps(b, sort_keys=True):
                return "statement %s changed" % a.get("Sid")
            continue
        a2, b2 = json.loads(json.dumps(a)), json.loads(json.dumps(b))
        ua_a = as_list(a2["Condition"]["StringLike"].pop("aws:UserAgent"))
        ua_b = as_list(b2["Condition"]["StringLike"].pop("aws:UserAgent"))
        if json.dumps(a2, sort_keys=True) != json.dumps(b2, sort_keys=True):
            return "deny statement changed beyond its user-agent list"
        if ua_b[:len(ua_a)] != ua_a or set(ua_b) - set(ua_a) - set(ADD):
            return "user-agent list changed beyond the added patterns"
    return None


def head(url, ua):
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": ua, "Referer": "https://justhodl.ai/"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001 - a network blip is a result too
        return "ERR:%s" % type(e).__name__


def sample_key(s3, pattern):
    path = pattern[len(ARN_PREFIX):] if pattern.startswith(ARN_PREFIX) else None
    if not path:
        return None
    if "*" not in path and "?" not in path:
        return path
    prefix = path.split("*")[0].split("?")[0]
    token = None
    for _ in range(3):
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 200}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            if fnmatch.fnmatchcase(o["Key"], path) and o["Size"] > 0:
                return o["Key"]
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return None


def selftest():
    deny = {"Sid": SID, "Effect": "Deny", "Principal": "*", "Action": "s3:GetObject", "Resource": ARN_PREFIX + "*",
            "Condition": {"StringLike": {"aws:UserAgent": ["*Lightpanda*", "*lightpanda*"]}}}
    allow = {"Sid": "PublicReadMergedV1", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": [ARN_PREFIX + "data/*"]}
    old = [allow, deny]
    new, changed = extended(old)
    assert changed and proof(old, new) is None
    assert new[1]["Condition"]["StringLike"]["aws:UserAgent"] == ["*Lightpanda*", "*lightpanda*"] + ADD
    again, changed2 = extended(new)
    assert not changed2 and again == new, "idempotent"
    bad = json.loads(json.dumps(new))
    bad[0]["Resource"].append(ARN_PREFIX + "x/*")
    assert proof(old, bad) == "statement PublicReadMergedV1 changed"
    bad2 = json.loads(json.dumps(new))
    bad2[1]["Condition"]["StringLike"]["aws:UserAgent"].append("*Mozilla*")
    assert proof(old, bad2) is not None
    try:
        extended([allow])
        raise AssertionError("must refuse without the deny statement")
    except ValueError:
        pass
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6626_deny_applebot_direct_s3") as r:
        r.heading("ops 6626 - refuse Applebot's direct S3 downloads of the data files")
        selftest()
        raw = s3.get_bucket_policy(Bucket=BUCKET)["Policy"]
        policy = json.loads(raw)
        old = as_list(policy["Statement"])
        r.log("live policy: %d statements, %d bytes (S3 limit 20,480)" % (len(old), len(raw)))
        new, changed = extended(old)
        why = proof(old, new)
        if why:
            r.fail("refusing: %s" % why)
            raise SystemExit(1)
        body = json.dumps(dict(policy, Statement=new), separators=(",", ":"))
        if len(body) > MAX_POLICY_BYTES:
            r.fail("new policy %d bytes is over the %d safety limit" % (len(body), MAX_POLICY_BYTES))
            raise SystemExit(1)
        base = "https://%s.s3.%s.amazonaws.com/" % (BUCKET, REGION)
        patterns = []
        for s in old:
            if s.get("Effect") == "Allow":
                for rsrc in as_list(s.get("Resource")):
                    if rsrc not in patterns:
                        patterns.append(rsrc)
        keys = OrderedDict((k, None) for k in WHY_FILES)
        for pat in patterns:
            k = sample_key(s3, pat)
            if k:
                keys[k] = pat
        urls = {k: base + k for k in keys}
        probe = base + "data/best-setups.json"
        before = {k: head(u, SAFARI_UA) for k, u in urls.items()}
        ab_before = head(probe, APPLEBOT_UA)
        r.log("probe set: %d objects (%d Allow patterns + the why.html files); Safari before: %s; Applebot before: %s" % (
            len(before), len(patterns), dict((c, sum(1 for v in before.values() if v == c)) for c in set(before.values())), ab_before))
        if not changed:
            r.ok("Applebot patterns already in %s; nothing to put" % SID)
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            bkey = "backups/bucket-policy/%s-%s.json" % (BUCKET, stamp)
            s3.put_object(Bucket=PRIVATE, Key=bkey, Body=raw.encode(), ContentType="application/json", ServerSideEncryption="AES256")
            r.ok("rollback copy: s3://%s/%s" % (PRIVATE, bkey))
            s3.put_bucket_policy(Bucket=BUCKET, Policy=body)
            back = as_list(json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])["Statement"])
            if proof(old, back) or json.dumps(back, sort_keys=True) != json.dumps(new, sort_keys=True):
                s3.put_bucket_policy(Bucket=BUCKET, Policy=raw)
                r.fail("read-back differs from what was put; original policy restored")
                raise SystemExit(1)
            r.ok("put + read-back: %d statements, %d bytes" % (len(back), len(body)))
        want = {APPLEBOT_UA: 403, LIGHTPANDA_UA: 403, GOOGLEBOT_UA: before.get("data/best-setups.json"), SAFARI_UA: before.get("data/best-setups.json")}
        ok, after, codes = False, {}, {}
        for _ in range(16):                      # a bucket-policy change takes a few seconds to propagate
            after = {k: head(u, SAFARI_UA) for k, u in urls.items()}
            codes = {ua: head(probe, ua) for ua in want}
            if after == before and codes == want:
                ok = True
                break
            time.sleep(5)
        diffs = {k: (before[k], after.get(k)) for k in before if before[k] != after.get(k)}
        r.log("Safari probes after: %d of %d unchanged; differing: %s" % (len(before) - len(diffs), len(before), diffs or "none"))
        r.log("data/best-setups.json -> Applebot %s (want 403), Lightpanda %s (want 403), Googlebot %s, Safari %s" % (
            codes.get(APPLEBOT_UA), codes.get(LIGHTPANDA_UA), codes.get(GOOGLEBOT_UA), codes.get(SAFARI_UA)))
        if not ok:
            if changed:
                s3.put_bucket_policy(Bucket=BUCKET, Policy=raw)
                r.fail("probe mismatch; the original policy has been put back")
            else:
                r.fail("probe mismatch with the policy already in place")
            raise SystemExit(1)
        r.kv(changed=changed, probes=len(before), applebot=codes.get(APPLEBOT_UA), safari=codes.get(SAFARI_UA))
        r.ok("Applebot's direct downloads are refused (403); every public object still answers a browser exactly as before")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
