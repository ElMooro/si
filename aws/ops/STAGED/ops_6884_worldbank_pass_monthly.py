"""ops 6884 - the World Bank full pass: weekly -> monthly.  DRY RUN (ops 6885 applies the same plan).

Khalid 2026-11-21, answering "World Bank pass monthly? Saves about $9 a month; World Bank series would be up to a month
behind instead of a week": yes.

Measured (ops 6874 / 6876): justhodl-worldbank-full ran 240 times x 677 s in a week - one whole re-drain of the ~29k
indicator catalog - $11.60 a month of Lambda plus about $2.50 of writes.  The pass is started by the Scheduler schedule
justhodl-worldbank-full-weekly (rate(7 days), payload {"redrain": true}); the daily tick justhodl-worldbank-full-2h
(03:20 UTC) only carries a started pass forward and is NOT touched.  World Bank data is annual and quarterly: the
catalog changes a few times a year.

Plan: the weekly schedule becomes cron(0 1 1 * ? *) - 01:00 UTC on the 1st of each month - same target, same payload,
same state.  A fixed cron, not rate(30 days): changing a rate() re-anchors it at the moment of the change and could start
a pass now.  The change goes through scripts/cadence_apply (drift check against the expected "before", backup of the live
schedule to the private bucket, read-back), and the declaration in config/schedule-manifest.json (repo and live bucket)
is brought in step for this one entry only, previous copy backed up.

    python3 ops_6884_worldbank_pass_monthly.py --selftest
"""
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN = "justhodl-worldbank-full"
NAME, GROUP = "justhodl-worldbank-full-weekly", "default"
TICK = "justhodl-worldbank-full-2h"
BEFORE = {"expr": "rate(7 days)", "state": "ENABLED"}
AFTER = {"expr": "cron(0 1 1 * ? *)", "state": "ENABLED"}
ACTION = {"kind": "schedule", "op": "retime", "name": NAME, "group": GROUP, "before": BEFORE, "after": AFTER}


def patch_manifest(text, name, before_expr, after_expr):
    """Change the expr of ONE declared entry, byte for byte everywhere else.  -> (new text, status)."""
    doc = json.loads(text)
    hits = [e for sec in ("rules", "schedules") for e in doc.get(sec, []) if e.get("name") == name]
    if len(hits) != 1:
        return text, "entry count %d" % len(hits)
    if hits[0].get("expr") == after_expr:
        return text, "already"
    if hits[0].get("expr") != before_expr:
        return text, "drift: declared %s" % hits[0].get("expr")
    marker = '"name": "%s"' % name
    at = text.find(marker)
    if at < 0 or text.find(marker, at + 1) >= 0:
        return text, "entry text not unique"
    start = text.rfind("{", 0, at)
    old = '"expr": "%s"' % before_expr
    pos = text.find(old, start, at)
    if pos < 0:
        return text, "expr text not found in the entry"
    new_text = text[:pos] + '"expr": "%s"' % after_expr + text[pos + len(old):]
    back = json.loads(new_text)
    same = [e for sec in ("rules", "schedules") for e in back.get(sec, []) if e.get("name") != name] == [e for sec in ("rules", "schedules") for e in doc.get(sec, []) if e.get("name") != name]
    changed = [e for sec in ("rules", "schedules") for e in back.get(sec, []) if e.get("name") == name]
    if not same or len(changed) != 1 or changed[0].get("expr") != after_expr or {k: v for k, v in changed[0].items() if k != "expr"} != {k: v for k, v in hits[0].items() if k != "expr"}:
        return text, "patch would change more than the one expr"
    return new_text, "patched"


def selftest():
    doc = {"version": 1, "rules": [{"expr": "rate(7 days)", "kind": "events", "name": "other-weekly", "state": "ENABLED"}],
           "schedules": [{"expr": "cron(20 3 * * ? *)", "group": "default", "kind": "scheduler", "name": TICK, "state": "ENABLED"},
                         {"expr": "rate(7 days)", "group": "default", "kind": "scheduler", "name": NAME, "state": "ENABLED", "targets": [{"input": "{\"redrain\": true}"}]}]}
    text = json.dumps(doc, indent=1, sort_keys=True)
    new, status = patch_manifest(text, NAME, BEFORE["expr"], AFTER["expr"])
    assert status == "patched" and new != text and len(new) - len(text) == len(AFTER["expr"]) - len(BEFORE["expr"])
    back = json.loads(new)
    assert back["schedules"][1]["expr"] == AFTER["expr"] and back["schedules"][0] == doc["schedules"][0] and back["rules"] == doc["rules"]       # the other rate(7 days) is untouched
    assert patch_manifest(new, NAME, BEFORE["expr"], AFTER["expr"]) == (new, "already")
    assert patch_manifest(text.replace("rate(7 days)", "rate(3 days)"), NAME, BEFORE["expr"], AFTER["expr"])[1].startswith("drift")
    assert patch_manifest(text, "no-such", BEFORE["expr"], AFTER["expr"])[1] == "entry count 0"
    assert ACTION["after"]["expr"].startswith("cron(") and ACTION["before"]["expr"] == "rate(7 days)"


def main(dry_run=True):
    selftest()
    import boto3
    from botocore.config import Config
    import cadence_apply as ca
    from ops_report import report
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60)
    sch, s3, lam = (boto3.client(n, region_name=REGION, config=cfg) for n in ("scheduler", "s3", "lambda"))
    name = "ops_6884_worldbank_pass_monthly" if dry_run else "ops_6885_worldbank_pass_monthly_apply"
    with report(name) as r:
        r.heading("%s - World Bank full pass weekly -> monthly (%s), %s" % (name.split("_")[1], "DRY RUN" if dry_run else "ACTING", time.strftime("%Y-%m-%dT%H:%MZ", time.gmtime())))
        r.section("1. Live state")
        live = sch.get_schedule(Name=NAME, GroupName=GROUP)
        tgt = live.get("Target") or {}
        r.log("%s: %s | %s | timezone %s | target %s | payload %s | window %s" % (NAME, live.get("ScheduleExpression"), live.get("State"), live.get("ScheduleExpressionTimezone"),
                                                                              ca.fn_name(tgt.get("Arn")), tgt.get("Input"), (live.get("FlexibleTimeWindow") or {}).get("Mode")))
        tick = sch.get_schedule(Name=TICK, GroupName=GROUP)
        r.log("%s (not touched): %s | %s | payload %s" % (TICK, tick.get("ScheduleExpression"), tick.get("State"), (tick.get("Target") or {}).get("Input")))
        cf = lam.get_function_configuration(FunctionName=FN)
        r.log("%s: timeout %s s, memory %s MB, last modified %s" % (FN, cf.get("Timeout"), cf.get("MemorySize"), (cf.get("LastModified") or "")[:19]))
        if ca.fn_name(tgt.get("Arn")) != FN or "redrain" not in str(tgt.get("Input")):
            r.fail("the schedule is not the re-drain trigger of %s - nothing done" % FN)
            raise SystemExit(1)

        r.section("2. The change")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        store = ca.Store(s3, PRIVATE, "backups/worldbank-monthly-2026-11-21/%s" % stamp)
        res = ca.apply_actions([ACTION], None, sch, store, dry_run=dry_run, log=r.log)
        r.log("schedule: %s -> %s : %s" % (BEFORE["expr"], AFTER["expr"], json.dumps(res)))
        status = res[0]["status"] if res else "none"
        if status not in ("would_apply", "applied", "already"):
            r.fail("schedule change did not go through: %s" % status)
            raise SystemExit(1)
        now = sch.get_schedule(Name=NAME, GroupName=GROUP)
        r.log("read-back: %s | %s | target %s | payload %s" % (now.get("ScheduleExpression"), now.get("State"), ca.fn_name((now.get("Target") or {}).get("Arn")), (now.get("Target") or {}).get("Input")))
        if not dry_run and ((now.get("Target") or {}).get("Input") != tgt.get("Input") or (now.get("Target") or {}).get("Arn") != tgt.get("Arn") or now.get("State") != live.get("State")):
            r.fail("target, payload or state changed with the expression - restore from s3://%s/backups/worldbank-monthly-2026-11-21/%s/" % (PRIVATE, stamp))
            raise SystemExit(1)

        r.section("3. The declaration (config/schedule-manifest.json), this one entry only")
        live_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        new_text, how = patch_manifest(live_text, NAME, BEFORE["expr"], AFTER["expr"])
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo_entry = [e for sec in ("rules", "schedules") for e in json.loads(repo_text).get(sec, []) if e.get("name") == NAME]
        r.log("live manifest: %s (%d bytes); repo copy declares %s" % (how, len(live_text), repo_entry[0].get("expr") if repo_entry else "nothing"))
        if how == "patched" and not dry_run:
            if not repo_entry or repo_entry[0].get("expr") != AFTER["expr"]:
                r.fail("the repo copy does not declare the new expression - commit it with this op")
                raise SystemExit(1)
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=live_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=new_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            again = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
            r.ok("live manifest declares %s for %s (backup backups/schedule-manifest/%s/); read-back %s" % (AFTER["expr"], NAME, stamp, "equal" if again == new_text else "DIFFERS"))
        elif how not in ("patched", "already"):
            r.warn("live manifest not changed: %s" % how)

        r.section("4. What this is worth")
        r.log("measured (ops 6876): 240 runs x 677 s at 1,024 MB in a week = one pass = about $2.77 of Lambda; $11.60 a month at one pass a week")
        r.log("at one pass a month: about $2.77 a month of Lambda - about $8.80 a month less.  The engine's writes (about $2.50 a month today) fall with it.")
        r.log("the price: World Bank series can be up to a month behind the source instead of a week (annual and quarterly data)")
        r.kv(dry_run=dry_run, status=status, manifest=how, expr_now=now.get("ScheduleExpression"))
        if dry_run:
            r.ok("dry run: nothing changed.  ops 6885 applies exactly this plan")
        else:
            r.ok("World Bank full pass now starts at 01:00 UTC on the 1st of each month; next pass 2026-12-01")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main(dry_run=True)
