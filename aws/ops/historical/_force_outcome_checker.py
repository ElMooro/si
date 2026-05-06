"""Force-run outcome-checker, then census what got newly scored.

This is the sweep we want post-multi-horizon-deployment: signals logged
2-30 days ago whose check_timestamps have now elapsed should be picked
up and scored.
"""
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
out_tbl = DDB.Table("justhodl-outcomes")


def census_outcomes():
    counts = defaultdict(int)
    last_key = None
    pages = 0
    while True:
        kw = {
            "Limit": 1000,
            "FilterExpression": (Attr("correct").eq(True) | Attr("correct").eq(False))
                                & Attr("is_legacy").ne(True),
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = out_tbl.scan(**kw)
        for item in resp.get("Items", []):
            counts[item.get("signal_type", "?")] += 1
        last_key = resp.get("LastEvaluatedKey")
        pages += 1
        if not last_key or pages > 30:
            break
    return counts, sum(counts.values())


def main():
    with report("force_outcome_checker") as r:
        # 1. Census BEFORE
        r.heading("1) Outcome census BEFORE force-run")
        before, total_before = census_outcomes()
        r.log(f"  total scored outcomes: {total_before}")
        r.log(f"  unique signal types:   {len(before)}")

        # 2. Force invoke outcome-checker
        r.heading("2) Force invoke outcome-checker")
        t0 = time.time()
        try:
            resp = LAM.invoke(
                FunctionName="justhodl-outcome-checker",
                InvocationType="RequestResponse",
            )
            body = resp["Payload"].read().decode()
            r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {body[:600]}")
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        # 3. Census AFTER
        r.heading("3) Outcome census AFTER force-run")
        time.sleep(2)
        after, total_after = census_outcomes()
        r.log(f"  total scored outcomes: {total_after}")
        r.log(f"  unique signal types:   {len(after)}")
        r.log(f"  new outcomes:          +{total_after - total_before}")

        # 4. Diff
        r.heading("4) Per-signal diff (only types with new outcomes)")
        any_lift = False
        for stype in sorted(set(before) | set(after)):
            b = before.get(stype, 0)
            a = after.get(stype, 0)
            if a > b:
                r.log(f"  ✓ {stype:35s}  {b} → {a}  (+{a - b})")
                any_lift = True
        if not any_lift:
            r.log("  no new outcomes scored this run (all signals' next windows still in future)")

        # 5. Newly-active types (had 0 outcomes before, now have ≥1)
        r.heading("5) Newly-activated signal types")
        any_new = False
        for stype in sorted(after):
            if stype not in before:
                r.log(f"  🟢 {stype:35s}  newly active with {after[stype]} outcomes")
                any_new = True
        if not any_new:
            r.log("  no newly-activated signal types this run")

        # 6. Signal types still dormant but currently in pending state
        r.heading("6) Run a 2nd time after delay to allow async score writes")
        time.sleep(20)
        re_after, total_re = census_outcomes()
        r.log(f"  total scored: {total_re}  (+{total_re - total_after} from 2nd census)")


if __name__ == "__main__":
    main()
