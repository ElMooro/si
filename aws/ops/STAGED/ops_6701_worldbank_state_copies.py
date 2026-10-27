"""ops 6701 - Khalid, 2026-10-27 19:36 ET: "if it's just multiple copies of the same thing yes I want them gone, I only need
one copy" - the old copies of justhodl-worldbank-full's progress file left by v1.0.0 (ops 6699: 56,690 versions /
143 GB of data/warm/worldbank-full/_state/state.json in the live bucket, every one replicated to the sealed archive).

  1. evidence that they are copies of one bookkeeping file: every version is the same key; the newest and a sample of
     old versions decoded side by side (phase, queue, have counts) - progress snapshots, no World Bank data (the data is
     src/<indicator>.zip, untouched);
  2. LIVE bucket: delete every old version of _state/state.json and _state/inflight.json, keep the current one;
     a version-id delete is never replicated, so the archive is not touched by this step; read back: one version each;
  3. ARCHIVE bucket (sealed - no principal but root may delete): its own lifecycle rule (the seal's "3 newest old
     copies per file") is what removes old copies there; this reads the rule and counts the archive's copies now, so
     the report says what is left and when it clears.
No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
ARCHIVE = "justhodl-warehouse-archive-857687956942"
KEYS = ["data/warm/worldbank-full/_state/state.json", "data/warm/worldbank-full/_state/inflight.json"]
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, read_timeout=120, max_pool_connections=32)


def versions(s3, bucket, key):
    out, tok = [], {}
    while True:
        resp = s3.list_object_versions(Bucket=bucket, Prefix=key, MaxKeys=1000, **tok)
        out += [v for v in resp.get("Versions", []) if v["Key"] == key]
        if not resp.get("IsTruncated"):
            break
        tok = {"KeyMarker": resp["NextKeyMarker"], "VersionIdMarker": resp["NextVersionIdMarker"]}
    return sorted(out, key=lambda v: v["LastModified"], reverse=True)


def summary(s3, bucket, vid, key):
    raw = s3.get_object(Bucket=bucket, Key=key, VersionId=vid)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    st = json.loads(raw)
    have = st.get("have") or {}
    return "phase %s, queue %d, have %d, failures %d, as_of %s, top-level keys %s" % (
        st.get("phase"), len(st.get("queue") or []), len(have), len(st.get("failures") or {}), st.get("as_of"), sorted(st)[:12])


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    with report("ops_6701_worldbank_state_copies") as r:
        r.heading("ops 6701 - the old copies of worldbank-full's progress file: live deleted down to one, archive trimmed by its own rule")
        r.section("1. what the copies are")
        vs = versions(s3, LIVE, KEYS[0])
        r.log("live %s: %d versions, %.1f GB, %s .. %s" % (KEYS[0], len(vs), sum(v["Size"] for v in vs) / 1e9,
                                                         vs[-1]["LastModified"].isoformat()[:19], vs[0]["LastModified"].isoformat()[:19]))
        for v in [vs[0]] + [vs[i] for i in (len(vs) // 4, len(vs) // 2, len(vs) - 1) if 0 < i < len(vs)]:
            r.log("   %s %s %8d B: %s" % ("CURRENT" if v.get("IsLatest") else "old    ", v["LastModified"].isoformat()[:19], v["Size"],
                                         summary(s3, LIVE, v["VersionId"], KEYS[0])))
        r.log("the indicator data itself lives in src/<indicator>.zip and is not touched")

        r.section("2. live bucket: delete the old versions, keep the current one")
        for key in KEYS:
            vs = versions(s3, LIVE, key)
            cur = [v for v in vs if v.get("IsLatest")]
            old = [v for v in vs if not v.get("IsLatest")]
            freed, errors = 0, []
            for i in range(0, len(old), 1000):
                batch = old[i:i + 1000]
                resp = s3.delete_objects(Bucket=LIVE, Delete={"Objects": [{"Key": key, "VersionId": v["VersionId"]} for v in batch], "Quiet": True})
                errs = resp.get("Errors", [])
                errors += errs
                bad = {e.get("VersionId") for e in errs}
                freed += sum(v["Size"] for v in batch if v["VersionId"] not in bad)
            after = versions(s3, LIVE, key)
            gone = {v["VersionId"] for v in old}
            leftover = [v for v in after if v["VersionId"] in gone]
            # the engine is mid-drain and checkpoints every 120 s: a version written during this op is the new current,
            # and the one that was current at listing time is then its only old copy
            (r.ok if not leftover and not errors and after and after[0].get("IsLatest") else r.fail)(
                "%s: %d old versions deleted (%.1f GB), %d errors %s; now %d version(s) (%d written while this ran), current %s %d B" % (
                    key, len(old) - len(errors), freed / 1e9, len(errors), errors[:2], len(after),
                    sum(1 for v in after if cur and v["LastModified"] > cur[0]["LastModified"]),
                    after[0]["LastModified"].isoformat()[:19] if after else None, after[0]["Size"] if after else 0))

        r.section("3. archive bucket (sealed): its own lifecycle rule")
        try:
            for rule in s3.get_bucket_lifecycle_configuration(Bucket=ARCHIVE).get("Rules", []):
                r.log("rule %s [%s] filter %s | noncurrent %s | expiration %s" % (rule.get("ID"), rule.get("Status"), rule.get("Filter"),
                                                                              rule.get("NoncurrentVersionExpiration"), rule.get("Expiration")))
        except Exception as e:  # noqa: BLE001
            r.warn("archive lifecycle: %s" % type(e).__name__)
        now = datetime.now(timezone.utc)
        for key in KEYS:
            vs = versions(s3, ARCHIVE, key)
            old = [v for v in vs if not v.get("IsLatest")]
            r.log("archive %s: %d versions (%d old), %.1f GB; oldest %s, newest %s; old copies beyond the 3 newest: %d / %.1f GB" % (
                key, len(vs), len(old), sum(v["Size"] for v in vs) / 1e9, vs[-1]["LastModified"].isoformat()[:19] if vs else None,
                vs[0]["LastModified"].isoformat()[:19] if vs else None, max(0, len(old) - 3), sum(v["Size"] for v in old[3:]) / 1e9))
            if old:
                r.log("   oldest old copy is %.1f days old" % ((now - old[-1]["LastModified"]).total_seconds() / 86400))
        r.ok("ops 6701 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
