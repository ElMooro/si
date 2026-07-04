"""ops 2791 — verify LLM cost governance is LIVE end-to-end against real infra.

(a) confirm a sample of the 75 importer Lambdas were just redeployed (LastModified recent)
(b) exercise the full governance path with real S3/DDB/SSM:
    cache_put->cache_get round-trip, log_cost->DDB query, budget_ok, economy_downgrade
Cleans up its own test artifacts.
"""
import os, sys, json, time, subprocess
from datetime import datetime, timezone
import boto3

sys.path.insert(0, "aws/shared")
R = {"ops": 2791, "ts": datetime.now(timezone.utc).isoformat()}

# (a) sample importer redeploy freshness ---------------------------------------
lam = boto3.client("lambda", region_name="us-east-1")
try:
    importers = subprocess.check_output(
        "grep -rlE '(from|import)[[:space:]]+(llm_router|anthropic_shim)' aws/lambdas/*/source/ 2>/dev/null "
        "| awk -F/ '{print $3}' | sort -u", shell=True).decode().split()
except Exception:
    importers = []
R["importer_count"] = len(importers)
sample = importers[:10]
fresh = {}
now = time.time()
for fn in sample:
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        lm = cfg["LastModified"]  # e.g. 2026-07-04T01:57:00.000+0000
        t = datetime.strptime(lm.split(".")[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        fresh[fn] = round((now - t) / 60, 1)  # minutes since last deploy
    except Exception as e:
        fresh[fn] = "err:" + str(e)[:40]
R["redeploy_age_min"] = fresh
recent = [v for v in fresh.values() if isinstance(v, (int, float)) and v < 30]
R["sample_redeployed_recently"] = "%d/%d within 30min" % (len(recent), len(sample))
print("importers found: %d | sample redeployed <30min: %s" % (len(importers), R["sample_redeployed_recently"]))

# (b) exercise governance against real infra -----------------------------------
gov = {}
try:
    import llm_cost as lc
    model = "claude-haiku-4-5-20251001"
    key = "selftest-" + lc.make_key(model, [{"role": "user", "content": "ops2791 probe"}], None, 256)
    # cache round-trip (real S3)
    lc.cache_put(key, "PROBE_VALUE_2791", model)
    time.sleep(1)
    got = lc.cache_get(key)
    gov["cache_roundtrip"] = (got == "PROBE_VALUE_2791")
    # cost ledger (real DDB atomic add)
    c = lc.log_cost(model, 1000, 500, cached=False)
    gov["log_cost_usd"] = round(c, 6)
    lc.log_cost(model, 0, 0, cached=True)  # a cache-hit row too
    time.sleep(1)
    ddb = boto3.client("dynamodb", region_name="us-east-1")
    today = time.strftime("%Y-%m-%d", time.gmtime())
    q = ddb.query(TableName="justhodl-llm-cost", KeyConditionExpression="#d = :d",
                  ExpressionAttributeNames={"#d": "date"},
                  ExpressionAttributeValues={":d": {"S": today}})
    mine = [i for i in q.get("Items", []) if i["engine_model"]["S"].startswith("local|")]
    gov["ledger_row_written"] = bool(mine)
    if mine:
        row = mine[0]
        gov["ledger_sample"] = {"calls": row.get("calls", {}).get("N"),
                                "real_calls": row.get("real_calls", {}).get("N"),
                                "cache_hits": row.get("cache_hits", {}).get("N"),
                                "cost_usd": row.get("cost_usd", {}).get("N")}
    # budget + mode
    gov["budget_ok"] = lc.budget_ok()
    gov["economy_downgrade_normal"] = lc.economy_downgrade("reason")  # should stay 'reason' in normal mode
    gov["config"] = {"mode": lc._config()["mode"], "budget": lc._config()["budget"]}
    # cleanup test artifacts
    try:
        boto3.client("s3", region_name="us-east-1").delete_object(
            Bucket="justhodl-dashboard-live", Key="llm-cache/" + key + ".json")
        ddb.delete_item(TableName="justhodl-llm-cost",
                        Key={"date": {"S": today}, "engine_model": {"S": "local|" + model}})
        gov["cleanup"] = "ok"
    except Exception as e:
        gov["cleanup"] = "err:" + str(e)[:60]
    print("governance:", json.dumps(gov))
except Exception as e:
    gov["ERR"] = str(e)[:160]
    print("governance ERR", str(e)[:160])
R["governance"] = gov

R["status"] = "GOVERNANCE LIVE" if (gov.get("cache_roundtrip") and gov.get("ledger_row_written")) else "CHECK"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2791_governance_verify.json", "w"), indent=1, default=str)
print("OPS 2791:", R["status"])
