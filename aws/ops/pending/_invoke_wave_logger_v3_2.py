"""Re-invoke wave-signal-logger after schema fixes for divergence + COT paths."""
import json
import time
import boto3
from collections import Counter
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Attr
from ops_report import report

lam = boto3.client("lambda", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")


def main():
    with report("invoke_wave_logger_v3_2") as r:
        # Wait for code update
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName="justhodl-wave-signal-logger")
            if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg["State"] == "Active":
                break
            time.sleep(2)
        r.log(f"  ready: state={cfg['State']} mod={cfg['LastModified']}")

        r.heading("Invoke v3.2")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-wave-signal-logger", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status={resp['StatusCode']} duration={time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        time.sleep(3)
        r.heading("Verify all v3 signal types in DDB (last 5 min)")
        tbl = ddb.Table("justhodl-signals")
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        types = Counter()
        last_key = None; pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key: kw["ExclusiveStartKey"] = last_key
            resp = tbl.scan(**kw)
            for it in resp.get("Items", []):
                types[it.get("signal_type", "?")] += 1
            last_key = resp.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 5: break

        v3_set = {"correlation_break", "divergence_extreme", "cot_extreme"}
        for t, n in types.most_common(30):
            new_flag = "★" if t in v3_set else " "
            r.log(f"  {new_flag} {t:30s} n={n}")


if __name__ == "__main__":
    main()
