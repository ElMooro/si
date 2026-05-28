"""ops 1104 — deploy justhodl-episode-reference + schedule + invoke + verify."""
import io, os, json, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"; ACCOUNT = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-episode-reference"
BUCKET = "justhodl-dashboard-live"


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(lam, fn, t=120):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed":
                return False
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        time.sleep(3)
    return False


def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    rpt = {"started": datetime.now(timezone.utc).isoformat()}

    cfg = json.load(open(os.path.join(REPO_ROOT, "aws/lambdas", FN, "config.json")))
    src = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")

    # create or update
    exists = False
    try:
        lam.get_function_configuration(FunctionName=FN); exists = True
    except ClientError:
        pass

    if not exists:
        try:
            lam.create_function(
                FunctionName=FN, Runtime=cfg["runtime"], Role=cfg["role"],
                Handler=cfg["handler"], Code={"ZipFile": zip_src(src)},
                Description=cfg["description"][:255], Timeout=cfg["timeout"],
                MemorySize=cfg["memory"], Architectures=cfg["architectures"],
                Environment={"Variables": cfg["env"]})
            rpt["create"] = "CREATED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                rpt["create"] = "RACED"; exists = True
            else:
                rpt["create_err"] = str(e)[:300]; return _save(rpt)
        wait_active(lam, FN)
    if exists:
        wait_active(lam, FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src), Publish=False)
        wait_active(lam, FN)
        rpt["update"] = "CODE_SYNCED"

    # schedule
    sch = cfg.get("schedule") or {}
    if sch:
        arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
        try:
            events.put_rule(Name=sch["rule_name"], ScheduleExpression=sch["cron"], State="ENABLED",
                            Description=sch.get("description", "")[:255])
            events.put_targets(Rule=sch["rule_name"], Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(FunctionName=FN, StatementId=f"EB-{sch['rule_name']}",
                                   Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                                   SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{sch['rule_name']}")
                rpt["schedule"] = "CREATED+PERM"
            except ClientError as e:
                rpt["schedule"] = "EXISTS_PERM" if e.response["Error"]["Code"] == "ResourceConflictException" else f"perm_err:{e}"
        except ClientError as e:
            rpt["schedule_err"] = str(e)[:200]

    # invoke
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
    rpt["invoke_status"] = inv["StatusCode"]
    rpt["fn_err"] = inv.get("FunctionError")
    rpt["log_tail"] = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", "replace")[-1500:]

    time.sleep(3)
    try:
        o = s3.get_object(Bucket=BUCKET, Key="data/episode-reference.json")
        d = json.loads(o["Body"].read())
        rpt["verify"] = {
            "size_kb": round(o["ContentLength"] / 1024, 1),
            "n_indicators": d.get("n_indicators"),
            "n_episodes": len(d.get("episodes", [])),
            "sample_2s10s": d.get("indicators", {}).get("T10Y2Y"),
        }
    except Exception as e:
        rpt["verify_err"] = str(e)[:200]

    return _save(rpt)


def _save(rpt):
    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1104.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(rpt, open(out, "w"), indent=2, default=str)
    print(json.dumps(rpt, indent=2, default=str)[:3000])


if __name__ == "__main__":
    main()
