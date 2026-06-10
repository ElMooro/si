# ops 1544 — CloudWatch diagnosis: why s3_deep_spx returns None despite valid base
import json, time, boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
out = {"ops": 1544}

# base shape sanity
doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/spx-history-deep.json")["Body"].read())
out["base_head"] = {"keys": sorted(doc.keys()), "n_points": doc.get("n_points"),
                    "p0": doc.get("points", [])[:2], "p_last": doc.get("points", [])[-1:],
                    "source": doc.get("source")}

start_ms = int(time.time() * 1000)
lam.invoke(FunctionName="justhodl-historical-analogs", InvocationType="RequestResponse", Payload=b"{}")
time.sleep(6)
r = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-historical-analogs",
                           startTime=start_ms, filterPattern='"[spx]"')
out["analogs_spx_lines"] = [e["message"].strip() for e in r.get("events", [])][:10]
r2 = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-historical-analogs",
                            startTime=start_ms, filterPattern='"[analogs]"')
out["analogs_lines"] = [e["message"].strip() for e in r2.get("events", [])][:14]

start2 = int(time.time() * 1000)
lam.invoke(FunctionName="justhodl-alert-backtester", InvocationType="RequestResponse", Payload=b"{}")
time.sleep(6)
r3 = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-alert-backtester",
                            startTime=start2, filterPattern='"[spx]"')
out["bt_spx_lines"] = [e["message"].strip() for e in r3.get("events", [])][:8]
bt = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/alert-backtests.json")["Body"].read())
out["bt_span_now"] = bt.get("spy_span") or bt.get("spx_span") or bt.get("price_span")
a = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/historical-analogs.json")["Body"].read())
out["pool_now"] = a.get("n_historical_dates_evaluated")

open("aws/ops/reports/1544_diag.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, default=str)[:1100])
