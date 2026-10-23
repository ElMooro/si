"""ops 6633 - prove on a canary prefix that a Disabled, higher-priority replication rule keeps its prefix out of the
archive while the whole-bucket rule keeps copying everything else (archive scope B, Khalid 2026-10-23: "2archive lets do
B"). No real folder is touched here.

AWS documents both halves - "if there are two or more rules with the same destination bucket, then objects will be
replicated according to the rule with the highest priority" and "if a rule is disabled, Amazon S3 doesn't perform the
actions specified in the rule" - but not the combination, so it is tested before any real folder relies on it:
  1. read the live bucket's replication configuration: it must be ops 6574's (rule warehouse-archive-all, priority 1,
     prefix '', Enabled) plus, at most, this op's own self-test rule from an earlier run; back it up to the private
     bucket (backups/replication/);
  2. put it back with rule archive-scope-self-test added: priority 100, Status Disabled, prefix
     _health/archive-scope-excluded/, same destination; read it back;
  3. every 30 s for up to 9 minutes write a canary pair - _health/archive-scope-excluded/... and
     _health/archive-scope-included/... - and HEAD both: S3 stamps x-amz-replication-status (PENDING) on an object at
     write time when a rule will copy it, and leaves it absent when none applies;
  4. PASS = 3 consecutive pairs written at least 60 s after the change where the excluded canary carries no replication
     status and the included one does; then the included canary must reach the archive and no excluded canary may be
     there;
  5. anything else - or any error after step 2 - puts the backed-up configuration back exactly and reads it back.
On PASS the self-test rule stays: it carries nothing but canaries and lets later checks re-prove the mechanism.
Canaries are ~200-byte files; copies that reach the archive stay there (no delete path but the root user).
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
SCOPE = json.loads((ROOT / "config" / "archive-scope.json").read_text(encoding="utf-8"))
BASE_ID = SCOPE["base_rule"]["id"]
ST = SCOPE["self_test"]
EXCL, INCL = ST["excluded_prefix"], ST["included_prefix"]
PAIRS, GAP_S, SETTLE_S, NEED, GIVE_UP_S = 18, 30, 60, 3, 240
REPORT_NAME = "ops_6633_archive_scope_canary"


def prefix_of(rule):
    f = rule.get("Filter")
    if f is None:
        return rule.get("Prefix")
    if "Prefix" in f:
        return f["Prefix"]
    if "And" in f:
        return f["And"].get("Prefix")
    return "" if f == {} else None


def self_test_rule(base):
    return {"ID": ST["rule_id"], "Priority": ST["priority"], "Status": "Disabled", "Filter": {"Prefix": EXCL},
            "DeleteMarkerReplication": {"Status": "Disabled"}, "Destination": dict(base["Destination"])}


def check_base(cfg):
    """The configuration this op may change: ops 6574's base rule plus, at most, an earlier self-test rule."""
    rules = cfg.get("Rules", [])
    base = [x for x in rules if x.get("ID") == BASE_ID]
    others = [x.get("ID") for x in rules if x.get("ID") not in (BASE_ID, ST["rule_id"])]
    if len(base) != 1:
        return None, "base rule %s missing" % BASE_ID
    b = base[0]
    if others:
        return None, "unexpected rules present: %s" % others
    if b.get("Status") != "Enabled" or prefix_of(b) != "" or b.get("Priority") != SCOPE["base_rule"]["priority"]:
        return None, "base rule differs: %s" % json.dumps(b, default=str)
    if b["Destination"].get("Bucket") != "arn:aws:s3:::%s" % ARCHIVE:
        return None, "base rule destination differs: %s" % b["Destination"]
    if (b.get("DeleteMarkerReplication") or {}).get("Status") != "Disabled":
        return None, "base rule replicates delete markers"
    return b, None


def selftest():
    base = {"ID": BASE_ID, "Priority": 1, "Status": "Enabled", "Filter": {"Prefix": ""}, "DeleteMarkerReplication": {"Status": "Disabled"},
            "Destination": {"Bucket": "arn:aws:s3:::%s" % ARCHIVE, "StorageClass": "STANDARD_IA"}}
    b, err = check_base({"Role": "r", "Rules": [base]})
    assert b is base and err is None
    st = self_test_rule(base)
    assert st["Status"] == "Disabled" and st["Priority"] > 1 and prefix_of(st) == EXCL and st["Destination"] == base["Destination"]
    assert check_base({"Role": "r", "Rules": [base, st]})[1] is None
    assert check_base({"Role": "r", "Rules": [base, dict(st, ID="other")]})[1].startswith("unexpected")
    assert check_base({"Role": "r", "Rules": [dict(base, Status="Disabled")]})[1].startswith("base rule differs")
    assert not EXCL.startswith(INCL) and not INCL.startswith(EXCL) and EXCL.startswith("_health/")
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def status(key):
        try:
            return s3.head_object(Bucket=LIVE, Key=key).get("ReplicationStatus")
        except ClientError as ex:
            return "ERR:%s" % ex.response["Error"]["Code"]

    def in_archive(key):
        try:
            h = s3.head_object(Bucket=ARCHIVE, Key=key)
            return "present:%s" % (h.get("ReplicationStatus") or "?")
        except ClientError as ex:
            return "absent:%s" % ex.response["Error"]["Code"]

    with report(REPORT_NAME) as r:
        r.heading("ops 6633 - canary proof: a Disabled higher-priority rule keeps its prefix out of the archive")
        r.section("1. current replication configuration")
        current = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
        base, err = check_base(current)
        r.log("rules now: %s" % [(x.get("ID"), x.get("Priority"), x.get("Status"), prefix_of(x)) for x in current.get("Rules", [])])
        if err:
            r.fail("refusing to change an unexpected configuration: %s" % err)
            sys.exit(1)
        original = {"Role": current["Role"], "Rules": [x for x in current["Rules"] if x.get("ID") == BASE_ID]}
        bkey = "backups/replication/%s-%s.json" % (LIVE, run)
        s3.put_object(Bucket=PRIVATE, Key=bkey, Body=json.dumps(current, default=str, indent=1).encode(), ContentType="application/json",
                      ServerSideEncryption="AES256")
        r.ok("backup: s3://%s/%s (restore = put_bucket_replication with that document)" % (PRIVATE, bkey))

        changed = False
        verdict = False
        try:
            r.section("2. add the disabled self-test rule")
            want = {"Role": current["Role"], "Rules": [base, self_test_rule(base)]}
            s3.put_bucket_replication(Bucket=LIVE, ReplicationConfiguration=want)
            changed = True
            t_change = time.time()
            back = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
            got = sorted((x.get("ID"), x.get("Priority"), x.get("Status"), prefix_of(x)) for x in back["Rules"])
            r.log("read back: %s" % got)
            if got != sorted([(BASE_ID, base["Priority"], "Enabled", ""), (ST["rule_id"], ST["priority"], "Disabled", EXCL)]):
                raise RuntimeError("read-back differs: %s" % got)

            r.section("3. canary pairs")
            pairs, streak, marked = [], 0, 0
            for i in range(PAIRS):
                body = json.dumps({"purpose": "archive scope canary (ops 6633)", "run": run, "pair": i,
                                   "written_at": datetime.now(timezone.utc).isoformat()}).encode()
                ek, ik = "%scanary-%s-%02d.json" % (EXCL, run, i), "%scanary-%s-%02d.json" % (INCL, run, i)
                for k in (ek, ik):
                    s3.put_object(Bucket=LIVE, Key=k, Body=body, ContentType="application/json", CacheControl="no-store")
                time.sleep(2)
                es, is_ = status(ek), status(ik)
                if is_ is None:                       # one more look before calling the archive stopped
                    time.sleep(5)
                    es, is_ = status(ek), status(ik)
                age = time.time() - t_change
                pairs.append({"pair": i, "seconds_after_change": round(age), "excluded": es, "included": is_, "excluded_key": ek, "included_key": ik})
                r.log("pair %02d at +%3ds: excluded -> %s | included -> %s" % (i, age, es, is_))
                if is_ is None or str(is_).startswith("ERR"):
                    raise RuntimeError("included canary not marked for replication (%s) - the new configuration stopped the archive" % is_)
                if age >= SETTLE_S and es is None and is_ in ("PENDING", "COMPLETED"):
                    streak, marked = streak + 1, 0
                    if streak >= NEED:
                        break
                elif age >= SETTLE_S:
                    streak, marked = 0, marked + 1
                    if marked >= NEED + 2 and age >= GIVE_UP_S:
                        break                          # settled and still copied: the rule does not exclude
                time.sleep(GAP_S)
            if streak < NEED:
                raise RuntimeError("excluded canaries kept being marked for replication: %s" % [p["excluded"] for p in pairs])

            r.section("4. arrival in the archive")
            last_in = pairs[-1]["included_key"]
            deadline = time.time() + 840            # replication usually takes seconds; AWS allows up to 15 min
            src = status(last_in)
            while src != "COMPLETED" and time.time() < deadline:
                time.sleep(20)
                src = status(last_in)
            arch = in_archive(last_in)
            r.log("included canary %s: source %s, archive %s" % (last_in, src, arch))
            if src != "COMPLETED" or not arch.startswith("present"):
                raise RuntimeError("included canary did not reach the archive in 14 min (source %s, archive %s)" % (src, arch))
            leaked = []
            for p in pairs:
                a = in_archive(p["excluded_key"])
                if a.startswith("present"):
                    leaked.append((p["pair"], p["seconds_after_change"]))
            settled_leaks = [x for x in leaked if x[1] >= SETTLE_S]
            r.log("excluded canaries present in the archive: %s (only pairs written before the change settled may appear)" % leaked)
            if settled_leaks:
                raise RuntimeError("excluded canaries written after the change reached the archive: %s" % settled_leaks)
            verdict = True
            r.kv(mechanism="proven", pairs=len(pairs), first_excluded_unmarked_at_s=next((p["seconds_after_change"] for p in pairs if p["excluded"] is None), None),
                 backup=bkey, self_test_rule=ST["rule_id"])
            r.ok("a Disabled rule with a higher priority keeps its prefix out of the archive; everything else is still copied")
        except Exception as ex:  # noqa: BLE001
            r.warn("%s: %s" % (type(ex).__name__, str(ex)[:400]))
        finally:
            if changed and not verdict:
                r.section("5. restore")
                try:
                    s3.put_bucket_replication(Bucket=LIVE, ReplicationConfiguration=original)
                    back = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
                    ok_ = [(x.get("ID"), x.get("Status"), prefix_of(x)) for x in back["Rules"]] == [(BASE_ID, "Enabled", "")]
                    (r.ok if ok_ else r.warn)("configuration restored to the single whole-bucket rule: %s" % ok_)
                except Exception as ex2:  # noqa: BLE001
                    r.fail("RESTORE FAILED (%s %s) - put back s3://%s/%s by hand" % (type(ex2).__name__, str(ex2)[:200], PRIVATE, bkey))
        if not verdict:
            r.fail("mechanism not proven - the archive copies everything as before; scope B needs the include-list fallback")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
