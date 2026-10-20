"""ops 6549 - stop the Lightpanda crawler pulling ~46 GB/day (~$4/day) straight from justhodl-dashboard-live.

(ops 6548 was this script's first attempt: it stopped before changing anything because the live bucket policy is
20,366 of the 20,480 bytes S3 allows - there was no room for one more statement.  This version first makes room.)

Evidence (ops 6547, S3 server access logs, Oct 16-20): the bucket served 44 -> 133 GB/day of anonymous GETs while
every engine was frozen.  46 GB/day of that is the self-declared crawler "Lightpanda/1.0" (20,559 requests, ~2.2 MB
each, concentrated 02-06 UTC) fetching the big data/*.json files directly from the public S3 endpoint with
Referer justhodl.ai.  It is not a customer and not one of our engines.

STAGED (direct lane).  Mutates ONE thing: the bucket policy of justhodl-dashboard-live.
  1. read the live policy; back the raw JSON up to the private ops bucket (the rollback copy)
  2. make room: the ~24 "PublicRead*" Allow statements that share Effect / Principal / Action / no Condition are
     merged into one statement whose Resource list is their union.  A merge of identical-permission Allow
     statements cannot change what is allowed; the script PROVES it by exploding old and new policy into
     (effect, principal, action, resource, condition) tuples and refusing to continue unless the sets are equal
  3. add statement DenyAbusiveScrapersByUserAgent (aws:UserAgent like *Lightpanda*; idempotent)
  4. before putting: anonymous HEAD of one existing object per original Allow pattern -> status codes recorded
  5. put, read back, prove again that nothing but the one new Deny statement differs (as tuples)
  6. after putting: the same anonymous HEADs must return the same status codes, and a Lightpanda UA must get 403
     while a browser UA still gets 200.  Any mismatch -> the original policy is put straight back (automatic
     rollback) and the run fails
Rollback by hand: put the backup object named in the report.

Cost: 1 PutBucketPolicy, a few ListObjectsV2, ~60 HEAD requests (<$0.001).
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
MERGED_SID = "PublicReadMergedV1"
BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
MAX_POLICY_BYTES = 19800                       # S3 limit is 20,480
ARN_PREFIX = "arn:aws:s3:::%s/" % BUCKET


def as_list(x):
    return list(x) if isinstance(x, list) else ([] if x is None else [x])


def deny_statement():
    return {
        "Sid": SID,
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:GetObject",
        "Resource": ARN_PREFIX + "*",
        "Condition": {"StringLike": {"aws:UserAgent": ["*Lightpanda*", "*lightpanda*"]}},
    }


def statements(policy):
    return as_list(policy["Statement"])


def exploded(stmts):
    """Every statement as a set of (effect, principal, action, resource, condition) tuples.  Statements with a
    Not* element are carried whole (they are never touched)."""
    out = set()
    for s in stmts:
        if any(k in s for k in ("NotAction", "NotPrincipal", "NotResource")):
            out.add(("__whole__", json.dumps(s, sort_keys=True)))
            continue
        pr = json.dumps(s.get("Principal"), sort_keys=True)
        cond = json.dumps(s.get("Condition", {}), sort_keys=True)
        for a in as_list(s["Action"]):
            for rsrc in as_list(s.get("Resource")):
                out.add((s["Effect"], pr, a, rsrc, cond))
    return out


def compact(stmts, min_group=3):
    """Merge Allow statements that differ only by Sid and Resource.  Returns (new statements, merged count)."""
    groups = OrderedDict()
    order = []
    for s in stmts:
        mergeable = (s.get("Effect") == "Allow" and not any(k in s for k in ("NotAction", "NotPrincipal", "NotResource", "Condition")))
        if mergeable:
            key = (json.dumps(s.get("Principal"), sort_keys=True), json.dumps(sorted(as_list(s["Action"]))))
            groups.setdefault(key, []).append(s)
        order.append((mergeable, s))
    merge_keys = {k for k, v in groups.items() if len(v) >= min_group}
    out, done, merged_n, idx = [], set(), 0, 0
    for mergeable, s in order:
        if not mergeable:
            out.append(s)
            continue
        key = (json.dumps(s.get("Principal"), sort_keys=True), json.dumps(sorted(as_list(s["Action"]))))
        if key not in merge_keys:
            out.append(s)
            continue
        if key in done:
            continue
        done.add(key)
        members = groups[key]
        resources = []
        for m in members:
            for rsrc in as_list(m.get("Resource")):
                if rsrc not in resources:
                    resources.append(rsrc)
        idx += 1
        out.append({"Sid": MERGED_SID if idx == 1 else "%s%d" % (MERGED_SID, idx), "Effect": "Allow",
                    "Principal": members[0].get("Principal"), "Action": members[0]["Action"], "Resource": resources})
        merged_n += len(members)
    return out, merged_n


def with_deny(stmts):
    return [s for s in stmts if s.get("Sid") != SID] + [deny_statement()]


def head(url, ua):
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": ua})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001 - a network blip is a result too
        return "ERR:%s" % type(e).__name__


def sample_key(s3, pattern):
    """One existing object that an Allow resource pattern covers (None when the bucket has none)."""
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
    def st(sid, res, eff="Allow", **kw):
        d = {"Sid": sid, "Effect": eff, "Principal": "*", "Action": "s3:GetObject", "Resource": res}
        d.update(kw)
        return d
    p = {"Version": "2012-10-17", "Statement": [
        st("A", ARN_PREFIX + "data/*"), st("B", ARN_PREFIX + "x/*"), st("C", [ARN_PREFIX + "y/*", ARN_PREFIX + "x/*"]),
        st("D", ARN_PREFIX + "z/*", eff="Deny"), st("E", ARN_PREFIX + "w/*", Condition={"Null": {"k": "true"}}),
        st("F", ARN_PREFIX + "v.json", Action="s3:PutObject")]}
    new, n = compact(statements(p))
    assert n == 3 and [s["Sid"] for s in new] == [MERGED_SID, "D", "E", "F"], new
    assert exploded(statements(p)) == exploded(new), "merge must not change the permission set"
    again, n2 = compact(new)
    assert n2 == 0 and again == new, "idempotent"
    final = with_deny(new)
    assert [s["Sid"] for s in final][-1] == SID and len(with_deny(final)) == len(final)
    assert exploded(final) - exploded(new) == {("Deny", '"*"', "s3:GetObject", ARN_PREFIX + "*",
                                                json.dumps(deny_statement()["Condition"], sort_keys=True))}
    assert exploded(new) <= exploded(final)
    # a wrong merge must be caught by the proof
    bad = [dict(s) for s in new]
    bad[0] = dict(bad[0], Resource=bad[0]["Resource"][:-1])
    assert exploded(statements(p)) != exploded(bad)
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    with report("ops_6549_deny_scraper_user_agents") as r:
        r.heading("ops 6549 - deny the Lightpanda crawler at the bucket (making room in the policy first)")
        raw = s3.get_bucket_policy(Bucket=BUCKET)["Policy"]
        policy = json.loads(raw)
        old = statements(policy)
        sids = [s.get("Sid") for s in old]
        r.log("live policy: %d statements, %d bytes (S3 limit 20,480)" % (len(old), len(raw)))
        has_deny = SID in sids
        if not has_deny and ("PublicReadDataDir" not in sids and MERGED_SID not in sids):
            r.fail("policy is not the one ops 6547 inspected (no PublicReadDataDir / merged statement); refusing to change it")
            raise SystemExit(1)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bkey = "backups/bucket-policy/%s-%s.json" % (BUCKET, stamp)
        s3.put_object(Bucket=PRIVATE, Key=bkey, Body=raw.encode(), ContentType="application/json", ServerSideEncryption="AES256")
        r.ok("rollback copy: s3://%s/%s" % (PRIVATE, bkey))

        # 2 + 3: the new statement list and the proof that only the Deny differs
        merged, merged_n = compact(old)
        proof_a = exploded(merged) == exploded(old)
        r.log("merged %d Allow statements into %d; permission tuples identical to before: %s" %
              (merged_n, len([s for s in merged if str(s.get("Sid", "")).startswith(MERGED_SID)]), proof_a))
        if not proof_a:
            r.fail("merge changed the permission set; not applying")
            raise SystemExit(1)
        final = with_deny(merged)
        added = exploded(final) - exploded(old)
        removed = exploded(old) - exploded(final)
        want_added = exploded([deny_statement()]) - exploded(old)
        if removed or added != want_added:
            r.fail("final policy differs from the old one by more than the Deny (removed=%d, extra=%d)" %
                   (len(removed), len(added - want_added)))
            raise SystemExit(1)
        new_policy = dict(policy, Statement=final)
        body = json.dumps(new_policy, separators=(",", ":"))
        r.log("new policy: %d statements, %d bytes; only difference: +1 Deny tuple" % (len(final), len(body)))
        if len(body) > MAX_POLICY_BYTES:
            r.fail("new policy is %d bytes, over the %d safety limit; not applying" % (len(body), MAX_POLICY_BYTES))
            raise SystemExit(1)

        # 4: anonymous status of one real object per original Allow pattern, before
        patterns = []
        for s in old:
            if s.get("Effect") == "Allow" and "Resource" in s:
                for rsrc in as_list(s["Resource"]):
                    if rsrc not in patterns:
                        patterns.append(rsrc)
        keys = OrderedDict()
        for pat in patterns:
            k = sample_key(s3, pat)
            if k:
                keys[k] = pat
        base = "https://%s.s3.%s.amazonaws.com/" % (BUCKET, REGION)
        probe_urls = {k: base + k for k in keys}
        probe_urls["data/master-ranker.json"] = base + "data/master-ranker.json"
        before = {k: head(u, BROWSER_UA) for k, u in probe_urls.items()}
        r.log("probe set: %d objects covering %d Allow patterns; before: %s" %
              (len(before), len(patterns), dict((s_, sum(1 for v in before.values() if v == s_)) for s_ in set(before.values()))))

        # 5: put + read back
        s3.put_bucket_policy(Bucket=BUCKET, Policy=body)
        back = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        if exploded(statements(back)) != exploded(final):
            s3.put_bucket_policy(Bucket=BUCKET, Policy=raw)
            r.fail("read-back differs from what was put; original policy restored")
            raise SystemExit(1)
        r.ok("put + read-back: %d statements" % len(statements(back)))

        # 6: after
        ok, after, deny_codes = False, {}, {}
        want_ua = {"Lightpanda/1.0": 403, BROWSER_UA: 200}
        for _ in range(14):                       # a bucket-policy change takes a few seconds to propagate
            after = {k: head(u, BROWSER_UA) for k, u in probe_urls.items()}
            deny_codes = {ua: head(probe_urls["data/master-ranker.json"], ua) for ua in want_ua}
            if after == before and deny_codes == want_ua:
                ok = True
                break
            time.sleep(5)
        diffs = {k: (before[k], after.get(k)) for k in before if before[k] != after.get(k)}
        r.log("after: %d of %d objects unchanged; differing: %s" % (len(before) - len(diffs), len(before), diffs or "none"))
        r.log("Lightpanda UA -> %s (want 403); browser UA -> %s (want 200)" % (deny_codes.get("Lightpanda/1.0"), deny_codes.get(BROWSER_UA)))
        if not ok:
            s3.put_bucket_policy(Bucket=BUCKET, Policy=raw)
            r.fail("probe mismatch; the original policy has been put back (backup %s)" % bkey)
            raise SystemExit(1)
        r.ok("Lightpanda is refused (403); every public object still answers exactly as before")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
