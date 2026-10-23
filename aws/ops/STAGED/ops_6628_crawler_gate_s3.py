"""ops 6628 - the S3 half of the crawler gate (Khalid 2026-10-23: "block crawlers from my data ... as long as it
doesn't affect my system operations and functionality").

The Cloudflare half shipped in commit 14261948 (justhodl-data-proxy refuses GET/HEAD from self-declared crawlers).
Pages that still read the live bucket directly (why.html's fleet-forensic block, master-rank, jh-fund-chips,
jh-ticker-research ...) send a crawler's renderer straight to S3, past the Worker: Applebot did ~10-30 GB/day that way
(denied by ops 6626), Googlebot ~0.5 GB/day. This op gives the bucket the same list.

STAGED (direct lane). Mutates ONE thing: the existing statement DenyAbusiveScrapersByUserAgent on justhodl-dashboard-live
gets "*<token>*" for every config/crawler-user-agents.json s3_tokens entry it does not already carry (crawlers that render
JavaScript, the only ones that can reach those S3 URLs). Every other statement stays byte-for-byte as it is.
  1. read the live policy; back the raw JSON up to the private ops bucket
  2. build the new policy; PROVE every other statement is identical and the one statement only gained patterns;
     refuse if the policy would exceed 20,000 bytes (S3 limit 20,480)
  3. before: Safari-UA HEAD of one real object per public Allow pattern + the why.html files
  4. put, read back, compare
  5. after (propagation retries): every Safari probe answers exactly as before; one sample user agent per added token
     gets 403; every config must_pass user agent that is a real HTTP client (browsers, HeadlessChrome, python, boto3,
     curl, the engines' own agents, ChatGPT-User, Claude-User, Google Sheets, Stripe, monitors) still gets 200.
     Any mismatch -> the original policy goes straight back.
Cost: 1 PutBucketPolicy, a few ListObjectsV2, ~200 HEAD requests (< $0.001).
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
MAX_POLICY_BYTES = 20000
ARN_PREFIX = "arn:aws:s3:::%s/" % BUCKET
CONFIG = json.loads((ROOT / "config" / "crawler-user-agents.json").read_text(encoding="utf-8"))
SAFARI_UA = CONFIG["must_pass"][0]
WHY_FILES = ["data/forensic-screen.json", "data/share-flows.json", "data/buyback-engine.json", "data/industry-rotation.json",
             "data/master-ranker.json", "data/accumulation-radar.json", "data/insider-radar.json", "data/best-setups.json",
             "data/finra-short.json", "data/ai-rerating-radar.json", "data/estimate-revisions.json"]
PROBE_KEY = "data/best-setups.json"


def as_list(x):
    return list(x) if isinstance(x, list) else ([] if x is None else [x])


def wanted_patterns():
    return ["*%s*" % t for t in CONFIG["s3_tokens"]]


def sample_ua(token):
    """A realistic user agent carrying the token (what the crawler sends)."""
    return "Mozilla/5.0 (compatible; %s/2.1; +https://example.invalid/bot)" % token


def extended(stmts, add):
    out, changed, found = [], [], False
    for s in stmts:
        if s.get("Sid") != SID:
            out.append(s)
            continue
        found = True
        s2 = json.loads(json.dumps(s))
        like = s2.setdefault("Condition", {}).setdefault("StringLike", {})
        uas = as_list(like.get("aws:UserAgent"))
        for u in add:
            if u not in uas:
                uas.append(u)
                changed.append(u)
        like["aws:UserAgent"] = uas
        out.append(s2)
    if not found:
        raise ValueError("statement %s not found - this op only extends the existing scraper deny" % SID)
    return out, changed


def proof(old, new, add):
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
        if ua_b[:len(ua_a)] != ua_a or set(ua_b) - set(ua_a) - set(add):
            return "user-agent list changed beyond the added patterns"
    return None


def head(url, ua):
    headers = {"Referer": "https://justhodl.ai/"}
    if ua:
        headers["User-Agent"] = ua
    req = urllib.request.Request(url, method="HEAD", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001
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
            "Condition": {"StringLike": {"aws:UserAgent": ["*Lightpanda*", "*lightpanda*", "*Applebot*", "*applebot*"]}}}
    allow = {"Sid": "PublicReadMergedV1", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": [ARN_PREFIX + "data/*"]}
    add = wanted_patterns()
    old = [allow, deny]
    new, changed = extended(old, add)
    assert proof(old, new, add) is None
    assert "*Applebot*" not in changed and "*Lightpanda*" not in changed and "*Googlebot*" in changed
    again, changed2 = extended(new, add)
    assert not changed2 and again == new, "idempotent"
    bad = json.loads(json.dumps(new))
    bad[0]["Resource"].append(ARN_PREFIX + "x/*")
    assert proof(old, bad, add)
    for ua in CONFIG["must_pass"]:
        for pat in add:
            assert not fnmatch.fnmatchcase(ua, pat), "must_pass agent %r would be denied by %s" % (ua, pat)
    for t in CONFIG["s3_tokens"]:
        assert any(fnmatch.fnmatchcase(sample_ua(t), p) for p in add), t
        assert t in CONFIG["worker_tokens"], t
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6628_crawler_gate_s3") as r:
        r.heading("ops 6628 - the crawler list at the bucket (S3 half of the crawler gate)")
        selftest()
        add = wanted_patterns()
        raw = s3.get_bucket_policy(Bucket=BUCKET)["Policy"]
        policy = json.loads(raw)
        old = as_list(policy["Statement"])
        new, changed = extended(old, add)
        why = proof(old, new, add)
        if why:
            r.fail("refusing: %s" % why)
            raise SystemExit(1)
        body = json.dumps(dict(policy, Statement=new), separators=(",", ":"))
        r.log("live policy %d bytes; adding %d patterns -> %d bytes (limit %d)" % (len(raw), len(changed), len(body), MAX_POLICY_BYTES))
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
        probe = base + PROBE_KEY
        before = {k: head(u, SAFARI_UA) for k, u in urls.items()}
        pass_before = {ua: head(probe, ua) for ua in CONFIG["must_pass"]}
        r.log("probe set %d objects; Safari before: %s; must_pass before: %s" % (
            len(before), dict((c, sum(1 for v in before.values() if v == c)) for c in set(before.values())),
            dict((c, sum(1 for v in pass_before.values() if v == c)) for c in set(pass_before.values()))))
        if not changed:
            r.ok("all %d patterns already present; nothing to put" % len(add))
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            bkey = "backups/bucket-policy/%s-%s.json" % (BUCKET, stamp)
            s3.put_object(Bucket=PRIVATE, Key=bkey, Body=raw.encode(), ContentType="application/json", ServerSideEncryption="AES256")
            r.ok("rollback copy: s3://%s/%s" % (PRIVATE, bkey))
            s3.put_bucket_policy(Bucket=BUCKET, Policy=body)
            back = as_list(json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])["Statement"])
            if json.dumps(back, sort_keys=True) != json.dumps(new, sort_keys=True):
                s3.put_bucket_policy(Bucket=BUCKET, Policy=raw)
                r.fail("read-back differs from what was put; original policy restored")
                raise SystemExit(1)
            r.ok("put + read-back: %d statements, %d bytes, +%d patterns: %s" % (len(back), len(body), len(changed), ", ".join(changed)))
        crawler_uas = {t: sample_ua(t) for t in CONFIG["s3_tokens"]}
        ok, after, crawl, passing = False, {}, {}, {}
        for _ in range(16):
            after = {k: head(u, SAFARI_UA) for k, u in urls.items()}
            crawl = {t: head(probe, ua) for t, ua in crawler_uas.items()}
            passing = {ua: head(probe, ua) for ua in CONFIG["must_pass"]}
            if after == before and all(v == 403 for v in crawl.values()) and passing == pass_before:
                ok = True
                break
            time.sleep(5)
        diffs = {k: (before[k], after.get(k)) for k in before if before[k] != after.get(k)}
        r.log("Safari probes after: %d of %d unchanged; differing: %s" % (len(before) - len(diffs), len(before), diffs or "none"))
        r.log("crawler sample agents -> %s" % json.dumps(crawl))
        r.log("must_pass agents unchanged: %s; statuses %s" % (passing == pass_before,
              dict((c, sum(1 for v in passing.values() if v == c)) for c in set(passing.values()))))
        if not ok:
            if changed:
                s3.put_bucket_policy(Bucket=BUCKET, Policy=raw)
                r.fail("probe mismatch; the original policy has been put back")
            else:
                r.fail("probe mismatch with the policy already in place")
            raise SystemExit(1)
        r.kv(added=len(changed), crawler_tokens=len(crawl), crawlers_refused=sum(1 for v in crawl.values() if v == 403),
             must_pass=len(passing), policy_bytes=len(body))
        r.ok("every listed crawler is refused at the bucket; every browser, probe, script and engine agent answers exactly as before")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
