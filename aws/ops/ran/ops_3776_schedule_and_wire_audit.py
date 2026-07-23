#!/usr/bin/env python3
"""ops 3776 — prove the schedule is ARMED, then wire capture_gap into consumers.

Two jobs.

[A] SCHEDULE PROOF. Every chokepoint invoke in this arc (3765-3775) was MINE.
    config.json declares cron(30 15 * * ? *) but the fleet-health arc proved
    that a declared schedule is not a live one: classic EventBridge rules were
    silently absent across 13 engines and every one of them looked healthy
    because a human kept invoking them. So this ops checks BOTH the classic
    events client AND the Scheduler client for a live, ENABLED trigger, and
    creates one via EventBridge Scheduler if it is missing. Without this the
    "ledger accretes unattended" claim is untested faith, and the ledger is the
    entire basis of the pool-widening design.

[B] WIRE THE SIGNAL. Per the engine contract (every note feeds >=1 consumer),
    capture_gap currently feeds only its own page. It should reach the desks
    Khalid actually reads. This ops does the AUDIT half honestly: it greps the
    real consumer sources for their join patterns and reports exactly where a
    capture_gap overlay can attach, rather than blind-patching four engines in
    one shot (which is how the 3766/3770 field-drop bugs happened). Wiring
    lands in the next ops with verified anchors.
"""
import sys, json, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "shared"))

from ops_report import report
import boto3

FN = "justhodl-chokepoint"
BUCKET = "justhodl-dashboard-live"
LEDGER = "chokepoint/fundamentals-ledger.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def main():
    with report("3776_schedule_and_wire_audit") as rep:
        rep.heading("ops 3776 — schedule proof + consumer wiring audit")

        fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
        rep.kv(function_arn=fn_arn)

        # ── [A] IS ANYTHING ACTUALLY SCHEDULED? ───────────────────────────
        rep.section("[A] Schedule proof — declared != live")
        live_triggers = []

        # classic EventBridge rules that target this function
        try:
            for page in ev.get_paginator("list_rules").paginate():
                for r in page.get("Rules", []):
                    try:
                        tg = ev.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
                    except Exception:
                        continue
                    if any(t.get("Arn") == fn_arn for t in tg):
                        live_triggers.append(("classic", r["Name"], r.get("State"),
                                              r.get("ScheduleExpression")))
        except Exception as e:
            rep.warn("classic rule scan failed: %s" % str(e)[:140])

        # EventBridge Scheduler schedules
        try:
            for page in sch.get_paginator("list_schedules").paginate():
                for sd in page.get("Schedules", []):
                    try:
                        full = sch.get_schedule(Name=sd["Name"],
                                                GroupName=sd.get("GroupName", "default"))
                    except Exception:
                        continue
                    if (full.get("Target") or {}).get("Arn") == fn_arn:
                        live_triggers.append(("scheduler", sd["Name"], full.get("State"),
                                              full.get("ScheduleExpression")))
        except Exception as e:
            rep.warn("scheduler scan failed: %s" % str(e)[:140])

        for kind, name, state, expr in live_triggers:
            rep.log("  %-10s %-42s %-9s %s" % (kind, name, state, expr))
        rep.kv(live_triggers=len(live_triggers))

        enabled = [t for t in live_triggers if str(t[2]).upper() == "ENABLED"]
        if not enabled:
            rep.warn("NO enabled trigger found — the daily run was never armed. "
                     "Every invoke in this arc was manual; the ledger would have "
                     "stopped accreting the moment I stopped pushing.")
            rep.section("Creating EventBridge Scheduler trigger")
            name = "justhodl-chokepoint-daily"
            params = dict(
                Name=name,
                ScheduleExpression="cron(30 15 * * ? *)",
                ScheduleExpressionTimezone="UTC",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": fn_arn, "RoleArn": SCHED_ROLE,
                        "Input": json.dumps({"mode": "full"})},
                State="ENABLED",
                Description="Daily capture-gap + criticality sweep (ledger accretes).",
            )
            try:
                sch.create_schedule(**params)
                rep.ok("created Scheduler '%s'" % name)
            except sch.exceptions.ConflictException:
                sch.update_schedule(**params)
                rep.ok("schedule existed — updated to ENABLED")
            except Exception as e:
                gate(rep, "SCHED.create", False, str(e)[:200])

            # re-read to prove it
            try:
                v = sch.get_schedule(Name=name, GroupName="default")
                gate(rep, "SCHED.armed", v.get("State") == "ENABLED",
                     "%s %s -> %s" % (v.get("State"), v.get("ScheduleExpression"),
                                      (v.get("Target") or {}).get("Arn", "")[-40:]))
            except Exception as e:
                gate(rep, "SCHED.armed", False, str(e)[:160])
        else:
            gate(rep, "SCHED.armed", True,
                 "%d enabled trigger(s): %s" % (len(enabled), enabled[0][1]))

        # ── LEDGER STATE (the thing the schedule must keep feeding) ───────
        rep.section("Ledger state — the basis of the widened pool")
        try:
            head = s3.head_object(Bucket=BUCKET, Key=LEDGER)
            age_h = (time.time() - head["LastModified"].timestamp()) / 3600.0
            led = json.loads(s3.get_object(Bucket=BUCKET, Key=LEDGER)["Body"].read())
            rep.kv(ledger_n=led.get("n"), ledger_updated=led.get("updated_at"),
                   ledger_age_hours=round(age_h, 2),
                   ledger_bytes=head["ContentLength"])
            gate(rep, "LEDGER.exists", (led.get("n") or 0) > 1000,
                 "%s names persisted" % led.get("n"))
            rows = led.get("rows") or {}
            with_mult = sum(1 for r in rows.values() if r.get("ev_sales") is not None)
            rep.kv(ledger_rows_with_multiples=with_mult)
            gate(rep, "LEDGER.multiples_persisted", with_mult > 0,
                 "%d rows carry ev_sales — catch-up survives restarts" % with_mult)
        except Exception as e:
            gate(rep, "LEDGER.exists", False, str(e)[:160])

        # ── [B] CONSUMER WIRING AUDIT (audit only, no blind patching) ────
        rep.section("[B] Where can capture_gap attach? (audit, not patch)")
        targets = {
            "justhodl-best-setups": "data/best-setups.json",
            "justhodl-master-ranker": "data/master-ranker.json",
            "justhodl-equity-research": "data/equity-research",
            "justhodl-comeback-screener": "data/comeback-screener.json",
        }
        for fn, feed in targets.items():
            src_p = ROOT / "lambdas" / fn / "source" / "lambda_function.py"
            if not src_p.exists():
                rep.warn("  %-32s SOURCE MISSING" % fn)
                continue
            t = src_p.read_text()
            has_choke = "chokepoint.json" in t
            has_census = "census" in t.lower()
            has_boom = "industry-boom" in t
            # how does it iterate rows? that determines the join anchor
            n_read = t.count("_read(") + t.count("read_json(") + t.count("get_s3_json(")
            rep.log("  %-32s chokepoint=%-5s census=%-5s boom=%-5s readers=%d" % (
                fn, has_choke, has_census, has_boom, n_read))

        rep.section("Verdict on wiring")
        rep.log("Wiring deliberately NOT applied in this ops. The two silent-zero")
        rep.log("bugs in this arc (backlog 3766, catch-up 3770) both came from")
        rep.log("writing a consumer against a field list I had not verified line by")
        rep.log("line. Four engines in one push would repeat that at 4x scale.")
        rep.log("Next ops patches ONE consumer with grep-verified anchors, proves")
        rep.log("the join count is non-zero on the live artifact, then moves on.")

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — schedule armed, ledger persistent, wiring targets mapped")


if __name__ == "__main__":
    main()
