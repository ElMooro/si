# ops 1635 — sentinel v1.5 paper-watch deploy + closed-loop integrity for 4 new signal types
import json, zipfile, io, os, time
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.resource("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1635}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-alert-sentinel/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-alert-sentinel/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-alert-sentinel", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-alert-sentinel")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel"] = {"version": d.get("version"), "n_changes": d.get("n_changes"),
                    "state_saved": d.get("state_saved"),
                    "paper_msgs": [c for c in (d.get("changes") or []) if "research note" in c][:4],
                    "queue_total": len(d.get("changes") or [])}
T = ddb.Table("justhodl-signals")
integ = {}
for st_ in ("ps_value_momentum", "hp_score", "research_paper", "insider_decline_cluster"):
    try:
        r = T.scan(FilterExpression=Attr("signal_id").begins_with(st_ + "#"))
        items = r.get("Items") or []
        samp = items[0] if items else {}
        integ[st_] = {"count": len(items),
                       "statuses": sorted({i.get("status") for i in items}),
                       "sample_ok": all(k in samp for k in
                                          ("baseline_price", "check_windows",
                                           "measure_against", "horizon_days_primary"))
                                     if samp else None,
                       "sample_id": samp.get("signal_id"),
                       "sample_conf": str(samp.get("confidence"))}
    except Exception as e:
        integ[st_] = {"error": str(e)[:80]}
out["loop_integrity"] = integ
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1635_loop_integrity.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"sent_v": out["sentinel"]["version"], "queue": out["sentinel"]["queue_total"],
                   "types": {k: v.get("count") for k, v in integ.items()}}, default=str))
