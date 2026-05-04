"""Invoke wave-signal-logger now and dump per-handler counts."""
import json
import time
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta
from collections import Counter
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")


def main():
    with report("smoke_wave_logger") as r:
        r.heading("Invoke justhodl-wave-signal-logger")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-wave-signal-logger", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  body: {body}")

        r.heading("DDB scan: signals just written by this Lambda")
        time.sleep(2)
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        tbl = ddb.Table("justhodl-signals")
        last_key = None
        items = []
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            items.extend(resp.get("Items", []))
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 6:
                break
        wave_items = [it for it in items if it.get("source") == "wave-signal-logger-v1"]
        types = Counter(it.get("signal_type", "?") for it in wave_items)
        r.log(f"  total wave-logger signals in last 2 min: {len(wave_items)}")
        for t, n in types.most_common():
            r.log(f"    {t:30s} n={n}")

        r.log("")
        r.log("  Sample (first 8):")
        for it in wave_items[:8]:
            bp = it.get("baseline_price")
            bp_str = f"${bp}" if bp else "—"
            r.log(f"    {it.get('signal_type'):20s} {it.get('measure_against', ''):8s} {it.get('predicted_direction'):8s} conf={it.get('confidence')}  bp={bp_str}  val={it.get('signal_value', '')[:30]}")


if __name__ == "__main__":
    main()
