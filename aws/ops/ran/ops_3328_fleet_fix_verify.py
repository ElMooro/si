"""ops 3328 — verify the 6 FMP-migrated engines run clean + produce data
after redeploy. Force-invoke each (retry to let deploy roll), report
statusCode/FunctionError + a size/marker from each output where known."""
import json, time, sys
from pathlib import Path
import boto3
from ops_report import report

LAM=boto3.client("lambda","us-east-1")
S3=boto3.client("s3","us-east-1")
BUCKET="justhodl-dashboard-live"

# engine -> output S3 key (None = just check invoke success)
ENGINES={
  "justhodl-rating-change-cluster":"data/rating-change-cluster.json",
  "justhodl-sellside-views":"data/sellside-views.json",
  "justhodl-52wk-quality-breakout":"data/52wk-quality-breakout.json",
  "justhodl-starmine":"data/starmine.json",
  "justhodl-buyback-scanner":"data/buyback-scanner.json",
  "justhodl-insider-sell-cluster":"data/insider-sell-cluster.json",
}

def head(key):
    try:
        h=S3.head_object(Bucket=BUCKET,Key=key)
        return {"size":h["ContentLength"],"modified":h["LastModified"].isoformat()}
    except Exception as e:
        return {"err":type(e).__name__}

with report("3328_fleet_fix_verify") as rep:
    fails=[]
    for fn,key in ENGINES.items():
        before = head(key) if key else {}
        result=None
        for attempt in range(3):
            try:
                r=LAM.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
                err=r.get("FunctionError")
                body=r["Payload"].read().decode()[:200]
                time.sleep(3)
                after=head(key) if key else {}
                fresh = key and after.get("modified") and after.get("modified")!=before.get("modified")
                result={"fn_error":err,"body":body,"out":after,"fresh":bool(fresh)}
                if not err and (not key or fresh):
                    break
            except Exception as e:
                result={"err":str(e)[:120]}
            time.sleep(15)
        rep.kv(**{fn:result})
        if result.get("fn_error") or result.get("err"):
            fails.append(f"{fn}: {result.get('fn_error') or result.get('err')}")
        elif key and not result.get("fresh"):
            fails.append(f"{fn}: output not refreshed (may be slow/large; check body)")
    rep.section("VERDICT")
    if fails:
        for f in fails: rep.warn(f)
        rep.kv(RESULT="PARTIAL", issues=len(fails))
        # warn-only: some engines are large/slow; don't hard-fail the run
    else:
        rep.ok("all 6 FMP-migrated engines run clean + refreshed output")
        rep.kv(RESULT="ALL_GREEN")
