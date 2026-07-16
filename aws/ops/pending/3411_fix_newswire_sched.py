"""ops 3411 — news-wire is a '1/day' engine but its EventBridge Scheduler entry 'news-wire-sched'
is set to rate(15 minutes) = 98 Haiku runs/day (~98x intended cost, ~$2.33/day wasted on
duplicate digests). FIX: reset the Scheduler to fire once daily at 11:00 UTC (matches config).
research-critique is NOT touched here — it's invoked 1-per-ticker by prewarm BY DESIGN; cost
options presented separately."""
import boto3, json
from ops_report import report
sched=boto3.client("scheduler",region_name="us-east-1")
cw=boto3.client("cloudwatch",region_name="us-east-1")
with report("3411_fix_newswire_sched") as r:
    name="news-wire-sched"
    r.section("Before")
    cur=sched.get_schedule(Name=name)
    r.log(f"  {name}: {cur['ScheduleExpression']} ({cur['State']})")
    r.section("Fix → once daily 11:00 UTC")
    # preserve the target + role, only change the expression
    sched.update_schedule(
        Name=name,
        ScheduleExpression="cron(0 11 * * ? *)",
        FlexibleTimeWindow=cur["FlexibleTimeWindow"],
        Target=cur["Target"],
        State="ENABLED",
        GroupName=cur.get("GroupName","default"),
    )
    aft=sched.get_schedule(Name=name)
    r.ok(f"  {name} now: {aft['ScheduleExpression']} ({aft['State']})")
    r.log("  → 98 runs/day → 1 run/day. Saves ~97 Haiku calls/day (~$2.33/day, ~$70/mo).")
    r.section("research-critique — for your decision (NOT changed)")
    r.log("  Invoked 1-per-ticker by equity-prewarm after each research succeeds (BY DESIGN).")
    r.log("  29 tickers/day × Sonnet 4.6 @ max_tokens 4000 = ~$1.91/day.")
    r.log("  Levers: (a) switch critic to Haiku or GLM-5.1 (~5-10x cheaper), (b) critique only")
    r.log("  high-conviction tickers, (c) lower max_tokens 4000→1500. Awaiting your pick.")
