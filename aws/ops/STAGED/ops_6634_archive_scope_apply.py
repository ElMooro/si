"""ops 6634 - apply archive scope B (Khalid 2026-10-23: "2archive lets do B" - protect only data that can't be
re-downloaded, like the provider history on data.html, and skip files the engines rebuild every day).

Runs only on top of the two checks before it:
  - ops 6632 measured exactly these prefixes from the inventory (its data file must list the same prefix set, with no
    problem found);
  - ops 6633 proved on a canary prefix that a Disabled, higher-priority rule keeps its prefix out of the archive while
    the whole-bucket rule keeps copying (its self-test rule is left in the configuration only on success).
Then:
  1. back up the live bucket's replication configuration to the private bucket (backups/replication/);
  2. put: the whole-bucket rule warehouse-archive-all (unchanged), the self-test rule (unchanged) and one Disabled rule
     per excluded prefix of config/archive-scope.json (priority 200+, same destination); read it back exactly;
  3. canary pair again on the self-test prefixes - with the full rule set, the excluded canary must carry no replication
     status and the included one must (S3 stamps it at write time when a rule will copy the object);
  4. add ReplicationStatus to the live bucket's daily versions inventory (versions-daily), so tomorrow's inventory
     shows, for every single write after the change, whether it went to the archive - the complete check (ops 6635);
  5. any failure after step 2 puts the backed-up configuration back exactly (the archive copies everything again).
Not touched: anything already in the archive (only the root user can delete there), the archive's own rules and seal,
every folder not listed. Undo = put back the backup (or delete the archive-skip-* rules).
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
SCOPE = json.loads((ROOT / "config" / "archive-scope.json").read_text(encoding="utf-8"))
MEASURED = ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6632_archive_scope_savings_data.md"
BASE_ID = SCOPE["base_rule"]["id"]
ST = SCOPE["self_test"]
SKIP_PRIORITY = 200
INV_ID = "versions-daily"
BROAD = {"", "data/", "audit-private/", "audit-private/20260909-originals/", "data/warm/", "data/providers/", "_health/"}


def rule_id(prefix):
    return ("archive-skip-" + re.sub(r"[^A-Za-z0-9]+", "-", prefix).strip("-"))[:255]


def prefix_of(rule):
    f = rule.get("Filter")
    if f is None:
        return rule.get("Prefix")
    if "Prefix" in f:
        return f["Prefix"]
    if "And" in f:
        return f["And"].get("Prefix")
    return "" if f == {} else None


def overlaps(a, b):
    return a.startswith(b) or b.startswith(a)


def validate(scope):
    problems = []
    ex = [e["prefix"] for e in scope["exclude"]]
    keep = [k["prefix"] for k in scope["must_keep"]]
    for p in ex:
        if "/" not in p or p in BROAD:
            problems.append("excluded prefix too broad: %r" % p)
    for i, a in enumerate(ex):
        for b in ex[i + 1:]:
            if overlaps(a, b):
                problems.append("excluded prefixes overlap: %r / %r" % (a, b))
        for k in keep + [scope["self_test"]["excluded_prefix"], scope["self_test"]["included_prefix"]]:
            if overlaps(a, k):
                problems.append("excluded %r overlaps %r" % (a, k))
    ids = [rule_id(p) for p in ex]
    if len(set(ids)) != len(ids):
        problems.append("rule ids collide")
    if len(ex) + 2 > 1000:
        problems.append("more than 1,000 replication rules")
    return problems


def desired_rules(base, scope):
    dest = dict(base["Destination"])
    rules = [base, {"ID": ST["rule_id"], "Priority": ST["priority"], "Status": "Disabled", "Filter": {"Prefix": ST["excluded_prefix"]},
                    "DeleteMarkerReplication": {"Status": "Disabled"}, "Destination": dict(dest)}]
    for i, e in enumerate(scope["exclude"]):
        rules.append({"ID": rule_id(e["prefix"]), "Priority": SKIP_PRIORITY + i, "Status": "Disabled", "Filter": {"Prefix": e["prefix"]},
                      "DeleteMarkerReplication": {"Status": "Disabled"}, "Destination": dict(dest)})
    return rules


def shape(rules):
    return sorted((x.get("ID"), x.get("Priority"), x.get("Status"), prefix_of(x), (x.get("Destination") or {}).get("Bucket")) for x in rules)


def measured_prefixes():
    txt = MEASURED.read_text(encoding="utf-8")
    d = json.loads(re.search(r"```json\n(.*)\n```", txt, re.S).group(1))
    return d, {x["prefix"] for x in d["excluded"]}


def selftest():
    assert validate(SCOPE) == [], validate(SCOPE)
    base = {"ID": BASE_ID, "Priority": 1, "Status": "Enabled", "Filter": {"Prefix": ""}, "DeleteMarkerReplication": {"Status": "Disabled"},
            "Destination": {"Bucket": "arn:aws:s3:::%s" % ARCHIVE, "StorageClass": "STANDARD_IA"}}
    rules = desired_rules(base, SCOPE)
    assert rules[0] is base and len(rules) == len(SCOPE["exclude"]) + 2
    assert len({x["Priority"] for x in rules}) == len(rules) and len({x["ID"] for x in rules}) == len(rules)
    assert all(x["Status"] == "Disabled" and x["Priority"] > 1 for x in rules[1:])
    assert all(prefix_of(x) for x in rules[1:]) and prefix_of(rules[0]) == ""
    assert rule_id("data/providers/fred/page-") == "archive-skip-data-providers-fred-page"
    print("selftest ok (%d rules)" % len(rules))


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

    with report("ops_6634_archive_scope_apply") as r:
        r.heading("ops 6634 - archive scope B: the archive stops copying re-downloadable and rebuilt folders")
        r.section("0. preconditions")
        problems = validate(SCOPE)
        try:
            measured, mset = measured_prefixes()
            if measured.get("problems"):
                problems.append("ops 6632 found problems: %s" % measured["problems"])
            want_set = {e["prefix"] for e in SCOPE["exclude"]}
            if mset != want_set:
                problems.append("ops 6632 measured a different prefix set: missing %s, extra %s" % (sorted(want_set - mset), sorted(mset - want_set)))
            tot = measured["totals"]["excluded"]
            r.log("ops 6632 (inventory %s): saving $%.2f/month now, $%.2f 12-month average; %d writes/day leave the archive" % (
                measured["inventory"], tot["ia_first_month"], tot["ia_12m_avg"], tot["writes_day"]))
        except Exception as ex:  # noqa: BLE001
            problems.append("ops 6632 data unreadable: %s %s" % (type(ex).__name__, str(ex)[:160]))
        current = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
        r.log("rules now (%d): %s" % (len(current["Rules"]), [(x.get("ID"), x.get("Priority"), x.get("Status"), prefix_of(x)) for x in current["Rules"]][:8]))
        base = [x for x in current["Rules"] if x.get("ID") == BASE_ID]
        selft = [x for x in current["Rules"] if x.get("ID") == ST["rule_id"]]
        others = [x.get("ID") for x in current["Rules"] if x.get("ID") not in (BASE_ID, ST["rule_id"]) and not str(x.get("ID")).startswith("archive-skip-")]
        if len(base) != 1 or base[0].get("Status") != "Enabled" or prefix_of(base[0]) != "" or base[0]["Destination"].get("Bucket") != "arn:aws:s3:::%s" % ARCHIVE:
            problems.append("base rule %s missing or changed" % BASE_ID)
        if len(selft) != 1 or selft[0].get("Status") != "Disabled":
            problems.append("self-test rule absent - ops 6633 did not prove the mechanism")
        if others:
            problems.append("unexpected rules: %s" % others)
        if problems:
            for p in problems:
                r.warn(p)
            r.fail("preconditions not met - nothing changed")
            sys.exit(1)
        r.ok("scope validated, measured by ops 6632, mechanism proven by ops 6633")

        bkey = "backups/replication/%s-%s.json" % (LIVE, run)
        s3.put_object(Bucket=PRIVATE, Key=bkey, Body=json.dumps(current, default=str, indent=1).encode(), ContentType="application/json",
                      ServerSideEncryption="AES256")
        r.ok("backup: s3://%s/%s" % (PRIVATE, bkey))
        restore_to = {"Role": current["Role"], "Rules": current["Rules"]}

        changed, done = False, False
        try:
            r.section("1. replication rules")
            want = {"Role": current["Role"], "Rules": desired_rules(base[0], SCOPE)}
            s3.put_bucket_replication(Bucket=LIVE, ReplicationConfiguration=want)
            changed = True
            t_change = time.time()
            applied_at = datetime.now(timezone.utc).isoformat()
            back = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
            if shape(back["Rules"]) != shape(want["Rules"]):
                raise RuntimeError("read-back differs: %s" % shape(back["Rules"])[:5])
            r.ok("%d rules live: the whole-bucket rule, the self-test rule and %d disabled archive-skip rules (applied %s)" % (
                len(back["Rules"]), len(SCOPE["exclude"]), applied_at[:19]))
            for e in SCOPE["exclude"]:
                r.log("  skip %-66s (%s)" % (e["prefix"], e["kind"]))

            r.section("2. canary pair with the full rule set")
            time.sleep(60)
            body = json.dumps({"purpose": "archive scope canary (ops 6634)", "run": run, "written_at": datetime.now(timezone.utc).isoformat()}).encode()
            ek = "%scanary-%s-apply.json" % (ST["excluded_prefix"], run)
            ik = "%scanary-%s-apply.json" % (ST["included_prefix"], run)
            for attempt in range(4):
                for k in (ek, ik):
                    s3.put_object(Bucket=LIVE, Key=k, Body=body, ContentType="application/json", CacheControl="no-store")
                time.sleep(3)
                es, is_ = status(ek), status(ik)
                r.log("attempt %d at +%ds: excluded -> %s | included -> %s" % (attempt, time.time() - t_change, es, is_))
                if es is None and is_ in ("PENDING", "COMPLETED"):
                    break
                time.sleep(30)
            if not (es is None and is_ in ("PENDING", "COMPLETED")):
                raise RuntimeError("canary pair wrong with the full rule set (excluded %s, included %s)" % (es, is_))
            r.ok("excluded canary not marked for the archive, included canary marked (%s)" % is_)
            done = True
        except Exception as ex:  # noqa: BLE001
            r.warn("%s: %s" % (type(ex).__name__, str(ex)[:400]))
        finally:
            if changed and not done:
                r.section("restore")
                try:
                    s3.put_bucket_replication(Bucket=LIVE, ReplicationConfiguration=restore_to)
                    back = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
                    (r.ok if shape(back["Rules"]) == shape(restore_to["Rules"]) else r.warn)("configuration restored from the backup")
                except Exception as ex2:  # noqa: BLE001
                    r.fail("RESTORE FAILED (%s %s) - put back s3://%s/%s by hand" % (type(ex2).__name__, str(ex2)[:200], PRIVATE, bkey))
        if not done:
            r.fail("scope B not applied - the archive copies everything as before")
            sys.exit(1)

        r.section("3. inventory field for the complete check")
        try:
            inv = s3.get_bucket_inventory_configuration(Bucket=LIVE, Id=INV_ID)["InventoryConfiguration"]
            fields = list(inv.get("OptionalFields") or [])
            if "ReplicationStatus" in fields:
                r.log("versions-daily already lists ReplicationStatus")
            else:
                inv["OptionalFields"] = fields + ["ReplicationStatus"]
                s3.put_bucket_inventory_configuration(Bucket=LIVE, Id=INV_ID, InventoryConfiguration=inv)
                back = s3.get_bucket_inventory_configuration(Bucket=LIVE, Id=INV_ID)["InventoryConfiguration"]
                (r.ok if "ReplicationStatus" in (back.get("OptionalFields") or []) else r.warn)(
                    "versions-daily now lists ReplicationStatus (fields %s)" % back.get("OptionalFields"))
        except Exception as ex:  # noqa: BLE001
            r.warn("inventory field not added (%s %s) - the next-day check falls back to sampling" % (type(ex).__name__, str(ex)[:200]))

        r.kv(applied_at=applied_at, skip_rules=len(SCOPE["exclude"]), backup=bkey,
             saving_first_month_usd=measured["totals"]["excluded"]["ia_first_month"],
             saving_12m_avg_usd=measured["totals"]["excluded"]["ia_12m_avg"])
        r.ok("archive scope B live; tomorrow's inventory proves every write (ops 6635)")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
