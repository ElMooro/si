"""ops 5830 -- does it work? (Claude, 2026-09-19). READ-ONLY. The morning after: the 05:45 UTC read (owned voice, full
board?), the ledger, the endpoint (asleep? wake registration intact?), the wall schedules, the exams on the page."""
import json, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REGION, PRI, PUB, NAME = "us-east-1", "justhodl-ai-857687956942", "justhodl-dashboard-live", "jh-owned-coder-async"


def main():
    s3 = boto3.client("s3", region_name=REGION); sm = boto3.client("sagemaker", region_name=REGION)
    aas = boto3.client("application-autoscaling", region_name=REGION); cw = boto3.client("cloudwatch", region_name=REGION); sch = boto3.client("scheduler", region_name=REGION)
    with report("5830_saturday_check") as r:
        r.heading("ops 5830 -- Saturday check: does it work?")
        r.section("1. Today's read")
        doc = json.loads(s3.get_object(Bucket=PRI, Key="ai/market-read/latest.json")["Body"].read()); rd = doc.get("read") or {}; ov = rd.get("owned_voice") or {}
        r.kv(read_id=doc.get("read_id"), generated_at=doc.get("generated_at"), voice=rd.get("voice"), owned_state=ov.get("state"), latency_s=ov.get("latency_s"), repaired=ov.get("repaired"),
             stances={k: (rd.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}, opportunities=len(rd.get("best_opportunities") or []), calls=len(rd.get("calls") or []),
             decision=rd.get("decision_status"), prompt_budget_chars=None)
        r.log("overall: " + str(rd.get("overall"))[:400])
        ai = json.loads(s3.get_object(Bucket=PUB, Key="data/ai.json")["Body"].read()); sb = ai.get("scoreboard") or {}
        r.kv(page_generated_at=ai.get("generated_at"), page_voice=str(sb.get("voice"))[:90], calls_made=sb.get("calls_made"), calls_graded=sb.get("calls_graded"), hit_rate=sb.get("hit_rate"),
             exam_holdout=json.dumps((ai.get("market_exam") or {}).get("holdout", {}).get("model_scores"))[:120], coding=json.dumps({k: (ai.get("coding_exam") or {}).get(k) for k in ("base_score", "learning_pts", "n_candidates")}))
        r.section("2. Endpoint + wake registration")
        ep = sm.describe_endpoint(EndpointName=NAME); pv = (ep.get("ProductionVariants") or [{}])[0]
        tg = aas.describe_scalable_targets(ServiceNamespace="sagemaker", ResourceIds=["endpoint/%s/variant/owned" % NAME], ScalableDimension="sagemaker:variant:DesiredInstanceCount").get("ScalableTargets", [])
        pol = aas.describe_scaling_policies(ServiceNamespace="sagemaker", ResourceId="endpoint/%s/variant/owned" % NAME, ScalableDimension="sagemaker:variant:DesiredInstanceCount").get("ScalingPolicies", [])
        al = cw.describe_alarms(AlarmNames=["jh-owned-coder-has-backlog"]).get("MetricAlarms", [{}])[0]
        r.kv(status=ep["EndpointStatus"], config=ep["EndpointConfigName"][-22:], instances=pv.get("CurrentInstanceCount"), target=[(t.get("MinCapacity"), t.get("MaxCapacity")) for t in tg],
             policies=[p["PolicyName"] for p in pol], alarm_state=al.get("StateValue"), alarm_actions=len(al.get("AlarmActions") or []))
        now = datetime.now(timezone.utc)
        inv = cw.get_metric_statistics(Namespace="AWS/SageMaker", MetricName="Invocations", Dimensions=[{"Name": "EndpointName", "Value": NAME}, {"Name": "VariantName", "Value": "owned"}], StartTime=now - timedelta(hours=24), EndTime=now, Period=86400, Statistics=["Sum"]).get("Datapoints", [])
        r.kv(invocations_24h=int(sum(d["Sum"] for d in inv)) if inv else 0)
        r.section("3. Monday wall schedules")
        for name in ("justhodl-ai-wall-prepare", "justhodl-ai-wall-post"):
            try:
                d = sch.get_schedule(Name=name, GroupName="default")
                r.log("%s: %s %s state=%s target=%s" % (name, d.get("ScheduleExpression"), d.get("ScheduleExpressionTimezone"), d.get("State"), (d.get("Target") or {}).get("Input")))
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (name, str(e)[:120]))
        r.ok("check complete")


if __name__ == "__main__":
    main()
