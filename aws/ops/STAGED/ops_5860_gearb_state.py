"""ops 5860 -- is Gear B training on the new supply? (Claude, 2026-09-20). READ-ONLY: today's jh-gearb/jh-exam jobs,
the control (launch, caps), the newest dataset manifest (kept rows, families), and one dry tick's refusal text if any."""
import json, sys, re
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    sm = boto3.client("sagemaker", region_name="us-east-1"); s3 = boto3.client("s3", region_name="us-east-1"); lam = boto3.client("lambda", region_name="us-east-1")
    with report("5860_gearb_state") as r:
        r.heading("ops 5860 -- Gear B on the new supply")
        r.section("1. Jobs since 2026-09-19")
        jobs = sm.list_training_jobs(NameContains="jh-", CreationTimeAfter=datetime(2026, 9, 19, tzinfo=timezone.utc), MaxResults=40, SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries", [])
        for j in jobs:
            r.log("%s %s %s" % (j["CreationTime"].strftime("%m-%d %H:%M"), j["TrainingJobName"], j["TrainingJobStatus"]))
        r.section("2. Control + newest dataset manifest")
        ctl = json.loads(s3.get_object(Bucket=PRI, Key="factory/control/gearb.json")["Body"].read())
        r.kv(**{k: ctl.get(k) for k in ("launch", "max_jobs_per_day", "min_sft_rows", "max_family_share", "model_source", "epochs", "max_steps") if k in ctl})
        keys = []
        token = None
        while True:
            kw = {"Bucket": PRI, "Prefix": "factory/gearb/datasets/", "MaxKeys": 1000}
            if token: kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw); keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith("/manifest.json")]
            token = page.get("NextContinuationToken")
            if not token: break
        keys.sort(key=lambda k: int(re.search(r"gen-(\d+)/", k).group(1)))
        for k in keys[-3:]:
            m = json.loads(s3.get_object(Bucket=PRI, Key=k)["Body"].read())
            r.log("%s kept=%s families=%s seen=%s launched=%s at=%s" % (k.split("/")[-2], m.get("kept"), m.get("families"), json.dumps(m.get("seen"))[:160], m.get("launched") or m.get("launched_job"), m.get("at")))
        r.section("3. A tick, dry (launch=false) -- what would it do?")
        resp = lam.invoke(FunctionName="justhodl-ai", InvocationType="RequestResponse", Payload=json.dumps({"mode": "gearb", "body": {"launch": False}}).encode())
        out = json.loads(resp["Payload"].read())
        r.log(json.dumps(out, default=str)[:900])
        r.ok("done")


if __name__ == "__main__":
    main()
