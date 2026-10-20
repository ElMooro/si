"""ops 6571 - piece 2 of the bucket design (Khalid, 2026-10-20): old copies capped by COUNT, not by age.

Before: data/, providers and system-events/ each expire every old version after 1 day (ops 5027/5164/6520), the
bucket-wide count rule ops6541-bound-noncurrent-versions (keep the 10 newest old copies, expire the rest after a day)
is Disabled, and prefixes outside those three keep old copies forever.
After: the count rule is Enabled bucket-wide and the three age-only rules keep only their delete-marker sweep - so
every file keeps its last 10 states (a deleted file's last copy has nothing newer and is kept), a storm that rewrites
one file 10,000 times keeps 10 copies, and no prefix accumulates copies without bound.  Current objects are untouched
by any of this: lifecycle noncurrent-version actions never apply to a current object.  An AbortIncompleteMultipartUpload
rule (7 days) is added if none exists.

Safety: the census (ops 6570) must show no prefix other than the October storm key keeping > 10 old versions of a key
(a prefix that used versions as history would lose states beyond 10); the configuration is read twice (stale-read
guard), backed up to the private bucket, written once, read back and compared field by field; every other rule is
carried over byte-identical.  Lifecycle acts asynchronously (within a day or two): the 399,080 old copies of
data/miss-calibrator-proposals.json are the visible proof once they drop to 10.
"""
from pathlib import Path
import copy
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
COUNT_RULE = "ops6541-bound-noncurrent-versions"
AGE_RULES = ("ops5027-purge-dead-versions-providers", "ops5164-purge-dead-versions-data", "ops6520-purge-dead-versions-system-events")
PUBLIC = {"", "data/", "system-events/"}
SENSITIVE = re.compile(r"private|secret|credential|token|password", re.I)


def label(p):
    return p if p in PUBLIC or not SENSITIVE.search(p) else "WITHHELD"


def rule_prefix(rule):
    f = rule.get("Filter") or {}
    return f.get("Prefix", (f.get("And") or {}).get("Prefix", rule.get("Prefix", "")))


def summarize(rule):
    bits = []
    if rule.get("Expiration"):
        bits.append("expire-current %s" % json.dumps(rule["Expiration"], sort_keys=True))
    if rule.get("NoncurrentVersionExpiration"):
        bits.append("expire-old %s" % json.dumps(rule["NoncurrentVersionExpiration"], sort_keys=True))
    if rule.get("AbortIncompleteMultipartUpload"):
        bits.append("abort-mpu %s" % json.dumps(rule["AbortIncompleteMultipartUpload"]))
    if rule.get("Transitions") or rule.get("NoncurrentVersionTransitions"):
        bits.append("transitions")
    return "%s [%s] prefix=%r: %s" % (rule.get("ID"), rule.get("Status"), label(rule_prefix(rule) or ""), "; ".join(bits) or "no action")


HISTORY_LIKE = re.compile(r"hist|archive", re.I)


def census_guard(r, s3):
    """ops 6570/6573 census: every key with > 10 old versions is a daily-rewritten OUTPUT (NVDA.json, daily.json,
    risk.json, fleet.json, hash-named cache entries...) whose older states nothing reads - explicit history lives in
    its own prefixes as current objects.  Refuse only if a history-like prefix carries many such keys."""
    keys = []
    token = None
    while True:
        kw = {"Bucket": PRIVATE, "Prefix": "census/"}
        if token:
            kw["ContinuationToken"] = token
        res = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in res.get("Contents", [])]
        token = res.get("NextContinuationToken")
        if not token:
            break
    if not keys:
        raise SystemExit("no census found (run ops 6570 first)")
    doc = json.loads(s3.get_object(Bucket=PRIVATE, Key=sorted(keys)[-1])["Body"].read())
    over = [(p["prefix"], p["keys_over_10"], p["max_versions_key"]) for p in doc["prefixes"] if p.get("keys_over_10")]
    r.log("census %s: %d prefixes; prefixes with keys > 10 old versions: %s" % (
        sorted(keys)[-1].split("/")[-1], len(doc["prefixes"]), [(label(p), n, m) for p, n, m in over]))
    offenders = [(label(p), n) for p, n, m in over if HISTORY_LIKE.search(p) and n > 5]
    return offenders


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6571_version_cap_by_count") as r:
        r.heading("ops 6571 - old copies capped by count: enable the bucket-wide keep-10 rule, retire the 1-day age rules")
        offenders = census_guard(r, s3)
        if offenders:
            r.fail("a history-like prefix keeps more than 10 old versions on many keys - review before applying: %s" % offenders)
            raise SystemExit(1)
        a = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)["Rules"]
        time.sleep(2)
        b = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)["Rules"]
        if a != b:
            r.fail("lifecycle configuration changed between two reads - another writer is active; nothing written")
            raise SystemExit(1)
        before = copy.deepcopy(a)
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        s3.put_object(Bucket=PRIVATE, Key="backups/lifecycle/%s/%s.before.json" % (LIVE, stamp), Body=json.dumps(before, indent=1, default=str).encode(),
                      ServerSideEncryption="AES256")
        for rule in before:
            r.log("before: " + summarize(rule))
        after = copy.deepcopy(before)
        ids = {x.get("ID") for x in after}
        if COUNT_RULE not in ids or not all(x in ids for x in AGE_RULES):
            r.fail("expected rules missing: %s" % sorted({COUNT_RULE, *AGE_RULES} - ids))
            raise SystemExit(1)
        changes = []
        for rule in after:
            if rule.get("ID") == COUNT_RULE:
                nce = rule.get("NoncurrentVersionExpiration") or {}
                if nce.get("NewerNoncurrentVersions") != 10 or nce.get("NoncurrentDays") != 1 or rule_prefix(rule) != "":
                    r.fail("count rule body is not the reviewed one: %s" % summarize(rule))
                    raise SystemExit(1)
                if rule["Status"] != "Enabled":
                    rule["Status"] = "Enabled"
                    changes.append("%s: Disabled -> Enabled" % COUNT_RULE)
            elif rule.get("ID") in AGE_RULES:
                if "NoncurrentVersionExpiration" in rule:
                    if not (rule.get("Expiration") or {}).get("ExpiredObjectDeleteMarker"):
                        r.fail("%s would be left without an action" % rule["ID"])
                        raise SystemExit(1)
                    del rule["NoncurrentVersionExpiration"]
                    changes.append("%s: 1-day age expiry removed, delete-marker sweep kept" % rule["ID"])
        if not any(x.get("AbortIncompleteMultipartUpload") for x in after):
            after.append({"ID": "ops6571-abort-incomplete-mpu-7d", "Filter": {"Prefix": ""}, "Status": "Enabled",
                          "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}})
            changes.append("ops6571-abort-incomplete-mpu-7d added (orphaned multipart parts, 7 days)")
        if not changes:
            r.ok("already in the target state; nothing written")
            return
        for ch in changes:
            r.log("change: " + ch)
        s3.put_bucket_lifecycle_configuration(Bucket=LIVE, LifecycleConfiguration={"Rules": after})
        time.sleep(3)
        back = s3.get_bucket_lifecycle_configuration(Bucket=LIVE)["Rules"]
        by_id_back = {x.get("ID"): x for x in back}
        by_id_after = {x.get("ID"): x for x in after}
        mism = [i for i in by_id_after if json.dumps(by_id_after[i], sort_keys=True, default=str) != json.dumps(by_id_back.get(i), sort_keys=True, default=str)]
        extra = sorted(set(by_id_back) - set(by_id_after))
        s3.put_object(Bucket=PRIVATE, Key="backups/lifecycle/%s/%s.after.json" % (LIVE, stamp), Body=json.dumps(back, indent=1, default=str).encode(),
                      ServerSideEncryption="AES256")
        for rule in back:
            r.log("after: " + summarize(rule))
        untouched = sum(1 for x in before if x.get("ID") not in (COUNT_RULE, *AGE_RULES) and
                        json.dumps(x, sort_keys=True, default=str) == json.dumps(by_id_back.get(x.get("ID")), sort_keys=True, default=str))
        r.kv(rules_before=len(before), rules_after=len(back), changes=len(changes), readback_mismatches=len(mism), unexpected_rules=len(extra),
             untouched_rules_identical=untouched, backup="s3://%s/backups/lifecycle/%s/%s.*.json" % (PRIVATE, LIVE, stamp))
        if mism or extra:
            r.fail("read-back differs from what was written: %s %s" % (mism, extra))
            raise SystemExit(1)
        r.ok("old copies are now capped at the 10 newest per file, bucket-wide; current objects untouched by any rule")


if __name__ == "__main__":
    main()
