"""cadence_rollout - run the committed config/cadence-plan.json against the account (ops 6553 dry run / 6554 apply).

Order, and why:
  1. fresh snapshot of every scheduled rule / schedule (payloads hashed)            -> what is live right now
  2. retime first, disable second, one item at a time precondition-checked           -> no job is ever without a trigger
     (cadence_apply.apply_actions: an item that moved since the plan is "drift" and left alone; full definition backed
     up to the private bucket before the first write; read-back after)
  3. the S3 copies of config/fanout-manifest.json (what justhodl-scheduler reads every tick) and
     config/schedule-manifest.json (what the reconciler would enforce) get the same edits as the repo, previous
     versions saved to the private bucket first
  4. second snapshot: zero enabled triggers able to fire more than once a day (exempt excluded), or the run fails
Dry run does 1, computes 2 and 3 without writing, and reports.  Nothing is deleted in either mode.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "aws/lambdas/justhodl-spend-governor/source"))
import cadence_apply as ca  # noqa: E402
import cadence_core as cc  # noqa: E402
import cadence_plan as cp  # noqa: E402

REGION = "us-east-1"
PRIVATE = "justhodl-ops-private-857687956942"
LIVE = "justhodl-dashboard-live"
MANIFESTS = ("config/fanout-manifest.json", "config/schedule-manifest.json")


def final_state(snapshot, actions):
    final = {(t["kind"], t["name"]): (t.get("expr") or "", t.get("state")) for t in snapshot
             if t.get("name") and not t.get("err")}
    for a in actions:
        if a["op"] in ("retime", "disable"):
            final[(a["kind"], a["name"])] = (a["after"]["expr"], a["after"]["state"])
    return final


def manifest_edits(s3, plan, final, log):
    """-> {key: (old_text, new_text, note)} for the two S3 manifests (new_text None = nothing to change)."""
    out = {}
    for key in MANIFESTS:
        old = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
        repo = (ROOT / key).read_text(encoding="utf-8")
        if key.endswith("fanout-manifest.json"):
            fan = json.loads(old)
            if json.dumps(fan, indent=1, ensure_ascii=False) + "\n" != old:
                out[key] = (old, None, "S3 copy does not round-trip; left alone - fix by hand")
                continue
            removed = cp.apply_fanout(fan, plan["fanout_removals"])
            new = json.dumps(fan, indent=1, ensure_ascii=False) + "\n"
            note = "%d function(s) leave a tick" % len(removed)
        else:
            new, n = cp.apply_schedule_manifest(old, plan["actions"], final)
            note = "%d entries changed" % n
        same_as_repo = new == repo
        note += "; result %s the committed repo file" % ("equals" if same_as_repo else "DIFFERS from")
        out[key] = (old, new if new != old else None, note)
        log("%s: %s" % (key, note))
    return out


def run(r, dry_run, workers=4):
    import boto3
    from botocore.config import Config
    cfg = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=30)
    events = boto3.client("events", region_name=REGION, config=cfg)
    scheduler = boto3.client("scheduler", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    stamp = ca.stamp()
    store = ca.Store(s3, PRIVATE, "backups/cadence-floor/%s" % stamp)
    plan = json.loads((ROOT / "config/cadence-plan.json").read_text(encoding="utf-8"))
    floor = json.loads((ROOT / "config/cadence-floor.json").read_text(encoding="utf-8"))
    exempt = floor.get("exempt_triggers", [])
    r.heading("ops %s - once-a-day cadence: %s" % ("6554 apply" if not dry_run else "6553 DRY RUN", plan["rule"][:90]))
    r.kv(actions=len(plan["actions"]), ops=json.dumps(plan["summary"]["ops"]), dry_run=dry_run,
         backups="s3://%s/backups/cadence-floor/%s/" % (PRIVATE, stamp))

    r.section("1. live snapshot")
    snap = ca.snapshot_triggers(events, scheduler, log=r.log)
    before_bad = ca.violations(snap, exempt)
    r.log("enabled triggers able to fire more than once a day: %d" % len(before_bad))
    if len(snap) < 400:
        r.fail("snapshot incomplete (%d records)" % len(snap))
        raise SystemExit(1)

    r.section("2. triggers (%s)" % ("would change" if dry_run else "changing"))
    results = ca.apply_actions(plan["actions"], events, scheduler, store, dry_run=dry_run, workers=workers,
                               canary=not dry_run, log=r.log)
    summary = ca.summarise(results)
    r.kv(**{k: v for k, v in summary.items()})
    for res in results:
        if res["status"] in ("drift", "missing", "failed", "skipped_managed"):
            r.warn("%s %s %s: %s %s" % (res["kind"], res["name"], res["op"], res["status"],
                                         res.get("live") or res.get("error") or ""))
    removed = sum(res.get("targets_removed", 0) for res in results)
    r.log("shared-rule targets %s: %d" % ("to remove" if dry_run else "removed", removed))
    if any(res.get("aborted") for res in results):
        r.fail("canary failed; nothing else was touched")
        raise SystemExit(1)

    r.section("3. S3 manifests")
    final = final_state(snap, plan["actions"])
    edits = manifest_edits(s3, plan, final, r.log)
    for key, (old, new, note) in edits.items():
        if new is None:
            r.log("%s: no change needed" % key)
            continue
        if not dry_run:
            store.put(ca.safe_key("manifest", key.split("/")[-1]), {"text": old})
            s3.put_object(Bucket=LIVE, Key=key, Body=new.encode("utf-8"), ContentType="application/json",
                          CacheControl="no-cache")
            back = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
            if back != new:
                r.fail("%s read-back differs" % key)
                raise SystemExit(1)
            r.ok("%s uploaded (previous copy backed up)" % key)

    r.section("4. verification")
    if dry_run:
        bad = [v for v in ca.violations(snap, exempt) if (v["kind"], v["name"]) not in
               {(a["kind"], a["name"]) for a in plan["actions"]}]
        r.log("enabled violators the plan does not cover: %d %s" % (len(bad), [(b["kind"], b["name"], b["expr"]) for b in bad[:10]]))
        would = summary.get("would_apply", 0) + summary.get("already", 0)
        r.kv(would_apply=summary.get("would_apply", 0), already=summary.get("already", 0),
             drift=summary.get("drift", 0), missing=summary.get("missing", 0))
        if bad or summary.get("drift") or summary.get("missing") or summary.get("failed"):
            r.warn("plan needs attention before apply (see above)")
        else:
            r.ok("dry run clean: %d items ready, nothing drifted" % would)
        return
    after = ca.snapshot_triggers(events, scheduler, log=r.log)
    bad = ca.violations(after, exempt)
    r.kv(enabled_violators_after=len(bad), enabled_violators_before=len(before_bad),
         firings_per_day_after=round(sum(cc.fires_per_day(t["expr"]) for t in
                                        {(t["kind"], t["name"]): t for t in after if t.get("state") == "ENABLED" and t.get("expr")}.values()), 1))
    for b in bad[:20]:
        r.warn("still able to fire >1/day: %s %s %s (%s)" % (b["kind"], b["name"], b["expr"], b.get("fn")))
    store.put("after-snapshot.json", {"generated_at": ca.stamp(), "triggers": after})
    if bad or summary.get("failed"):
        r.fail("account not at the floor: %d violators, %d failed writes" % (len(bad), summary.get("failed", 0)))
        raise SystemExit(1)
    r.ok("every enabled trigger fires at most once a day (exempt: %s)" % ", ".join(e["name"] for e in exempt))
