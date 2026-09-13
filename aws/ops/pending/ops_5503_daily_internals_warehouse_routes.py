"""Runner-only: one daily schedule, internals receipt, warehouse routing proof.

No vendor requests, broad schedule enforcement, or watchlist writes. Requires
the reviewed Lambda, Worker and Pages deployments before this op is pushed.
"""
import copy
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws/ops"), str(ROOT / "aws/lambdas/justhodl-schedule-reconciler/source")]
from ops_report import report
from internals_schedule import NAME, request_from_manifest
from brief_schedules import FUNCTION_ARN, ROLE_ARN, SPECS, verify_schedule

BUCKET = "justhodl-dashboard-live"
MANIFEST = "config/schedule-manifest.json"
KEY = "data/jh-internals.json"
RECONCILER = "justhodl-schedule-reconciler"
COMPILER = "justhodl-brief-compiler"
ARN = "arn:aws:scheduler:us-east-1:857687956942:schedule/default/" + NAME
RECEIPT = ROOT / "aws/ops/reports/5503_daily_internals_warehouse_receipt.json"


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,default=str).encode()).hexdigest()


def public_bytes(url):
    if urllib.parse.urlsplit(url).hostname not in {"justhodl.ai","justhodl-data-proxy.raafouis.workers.dev"}:
        raise ValueError("Public verification host not allowed")
    result = subprocess.run(["curl","--fail","--silent","--show-error","--location","--compressed",
                             "--max-time","90","--user-agent","JustHodl-Ops/5503","--write-out","\n%{http_code}",url],capture_output=True)
    if result.returncode:
        raise RuntimeError("Public verification HTTP failed: "+result.stderr.decode()[:150])
    body, status = result.stdout.rsplit(b"\n",1)
    if status != b"200":
        raise RuntimeError("Public verification expected HTTP 200; observed "+status.decode())
    return body


class PageScripts(HTMLParser):
    def __init__(self):
        super().__init__()
        self.paths = []

    def handle_starttag(self,tag,attrs):
        if tag == "script":
            self.paths.append(urllib.parse.urlsplit(dict(attrs).get("src", "")).path)


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        sys.exit(1)
    import boto3
    from botocore.config import Config
    from botocore.httpsession import URLLib3Session
    s3 = boto3.client("s3",region_name="us-east-1")
    lam = boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=330,retries={"max_attempts":1}))
    iam = boto3.client("iam",region_name="us-east-1")
    sch = boto3.client("scheduler",region_name="us-east-1")
    proof = {"op":5503,"started_at":datetime.now(timezone.utc).isoformat(),"actions":[],"packages":{}}

    def read(key):
        obj = s3.get_object(Bucket=BUCKET,Key=key)
        return json.loads(obj["Body"].read()),obj

    def invoke(fn,payload,http=False):
        inv = lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=json.dumps(payload).encode())
        body = json.loads(inv["Payload"].read())
        if inv.get("FunctionError"):
            raise RuntimeError("Lambda error in "+fn+": "+str(body.get("errorType")))
        if http:
            if body.get("statusCode") != 200:
                raise RuntimeError("Warehouse-only Lambda route failed: "+str(body.get("statusCode")))
            return json.loads(body["body"])
        if body.get("ok") is not True:
            raise RuntimeError("Invocation not OK: "+fn)
        return body

    def package(fn,members):
        full = lam.get_function(FunctionName=fn)
        with urllib.request.urlopen(full["Code"]["Location"],timeout=60) as response:
            archive = zipfile.ZipFile(io.BytesIO(response.read()))
        files = {}
        for name in members:
            local = ROOT / "aws/lambdas" / fn / "source" / name
            if not local.exists(): local = ROOT / "aws/shared" / name
            raw = archive.read(name)
            if raw != local.read_bytes():
                raise ValueError("Deployed package differs from reviewed checkout: "+fn+"/"+name)
            files[name] = hashlib.sha256(raw).hexdigest()
        cfg = full["Configuration"]
        proof["packages"][fn] = {"files":files,"code_sha256":cfg["CodeSha256"],
                                  "last_modified":cfg["LastModified"],"role":cfg["Role"]}
        return archive

    def six_snapshot():
        out = {}
        for name in SPECS:
            row = sch.get_schedule(Name=name,GroupName="default")
            out[name] = {k:v for k,v in row.items() if k not in ("ResponseMetadata","CreationDate","LastModificationDate")}
        return out

    def others(doc):
        return {k:[row for row in doc.get(k,[]) if row.get("name") != NAME] for k in ("rules","schedules")}

    with report("ops_5503_daily_internals_warehouse_routes") as r:
        try:
            package(RECONCILER,("lambda_function.py","internals_schedule.py","brief_schedules.py"))
            compiler_zip = package(COMPILER,("lambda_function.py","compile_jh_internals.py","internals_warehouse.py","brief_compiler.py","brief_contract.py"))
            package("justhodl-symdir",("lambda_function.py","warehouse_routing.py"))
            # Check all production artifacts before scheduling or publishing.
            nonce = os.environ["GITHUB_RUN_ID"]
            for attempt in range(7):
                try:
                    query = "?ops=5503&run="+nonce+"&verify="+str(attempt)
                    deployed_js = public_bytes("https://justhodl.ai/jh-warehouse-routing.js"+query)
                    html = public_bytes("https://justhodl.ai/chart-pro.html"+query).decode()
                    tags = PageScripts()
                    tags.feed(html)
                    state = {"asset_sha256":hashlib.sha256(deployed_js).hexdigest(),
                             "expected_sha256":hashlib.sha256((ROOT / "jh-warehouse-routing.js").read_bytes()).hexdigest(),
                             "script_present":"/jh-warehouse-routing.js" in tags.paths,
                             "tail_query_present":"&days=2&tail=1" in html,
                             "tail_normalizer_present":"window.jhWarehouseDailyTail(" in html}
                    ready = state["asset_sha256"] == state["expected_sha256"] and all(state[k] for k in ("script_present","tail_query_present","tail_normalizer_present"))
                    proof["pages_readiness"] = state
                    print(json.dumps({"phase":"pages_readiness",**state}),flush=True)
                    if ready: break
                except Exception as exc:
                    proof["pages_readiness"] = {"error":str(exc)[:180]}
                    print(json.dumps({"phase":"pages_readiness",**proof["pages_readiness"]}),flush=True)
                if attempt == 6:
                    raise ValueError("Pages routing assets did not reach the edge within the readiness window")
                time.sleep(5)
            proof["pages"] = {"routing_js_sha256":hashlib.sha256(deployed_js).hexdigest(),"chart_hooks_present":True}
            bank = invoke("justhodl-symdir",{"mode":"warehouse-ohlc","symbol":"AAPL","span":"day"},http=True)
            if bank.get("warehouse_empty") is not False or not bank.get("bars") or bank.get("vendor_requests") != 0:
                raise ValueError("AAPL warehouse probe did not return banked bars")
            proof["symdir"] = {k:v for k,v in bank.items() if k != "bars"}
            proof["symdir"]["count"] = len(bank["bars"])
            url = "https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=AAPL&span=day&days=12000&ops=5503"
            worker = json.loads(public_bytes(url+"&run="+nonce))
            worker_status = 200
            if worker.get("source") != "warehouse" or worker.get("warehouse_key") != bank["warehouse_key"]:
                raise ValueError("Worker did not serve the verified AAPL warehouse bank")
            if len(worker.get("bars",[])) != len(bank["bars"]):
                raise ValueError("Worker changed the full banked daily row count")
            proof["worker"] = {k:v for k,v in worker.items() if k != "bars"}
            proof["worker"]["http_status"] = worker_status
            r.ok("Chart/SymDir: AAPL banked bars=%s key=%s" % (len(bank["bars"]),bank["warehouse_key"]))

            # Execute the actual deployed compiler builder against live S3
            # under a transport guard. Nothing in this verification can call
            # FRED, Finviz, Polygon or any other vendor HTTP endpoint.
            network = {"s3_requests":0,"other_http_attempts":0}
            original_send = URLLib3Session.send
            def send(session,request):
                host = urllib.parse.urlsplit(request.url).hostname or ""
                if host not in {BUCKET+".s3.us-east-1.amazonaws.com", "s3.us-east-1.amazonaws.com",
                                BUCKET+".s3.amazonaws.com", "s3.amazonaws.com"}:
                    network["other_http_attempts"] += 1
                    raise RuntimeError("Non-S3 HTTP forbidden in compiler verification")
                network["s3_requests"] += 1
                return original_send(session,request)
            def forbidden(*args,**kwargs):
                network["other_http_attempts"] += 1
                raise RuntimeError("Vendor HTTP forbidden in compiler verification")
            with tempfile.TemporaryDirectory() as temp:
                for name in ("compile_jh_internals.py","internals_warehouse.py"):
                    Path(temp,name).write_bytes(compiler_zip.read(name))
                sys.path.insert(0,temp)
                spec = importlib.util.spec_from_file_location("deployed_internals",Path(temp,"internals_warehouse.py"))
                module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
                with patch.object(URLLib3Session,"send",send), patch("urllib.request.urlopen",forbidden):
                    candidate_doc, _ = module.build(s3)
                sys.path.remove(temp)
            if candidate_doc["warehouse"]["fresh_fred_legs"] != 6 or network["other_http_attempts"]:
                raise ValueError("Six fresh FRED legs and zero vendor HTTP required")
            proof["deployed_build_network_guard"] = network
            proof["fred_sources"] = candidate_doc["warehouse"]["fred"]
            r.ok("Deployed compiler read six fresh warehouse FRED legs; vendor HTTP attempts=0")

            before, obj = read(MANIFEST)
            reviewed = json.loads((ROOT / MANIFEST).read_text())
            req = request_from_manifest(reviewed)
            desired = next(x for x in reviewed["schedules"] if x.get("name") == NAME)
            manifest = copy.deepcopy(before)
            existing = [x for k in ("rules","schedules") for x in manifest.get(k,[]) if x.get("name") == NAME]
            if existing:
                request_from_manifest(manifest)
            else:
                manifest.setdefault("schedules",[]).append(desired)
            request_from_manifest(manifest)
            if others(before) != others(manifest): raise ValueError("Unrelated manifest change")
            six_before = six_snapshot()
            execution_role = iam.get_role(RoleName=ROLE_ARN.rsplit("/",1)[1])["Role"]
            trust = {"Version":"2012-10-17","Statement":[{"Effect":"Allow",
                "Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole",
                "Condition":{"StringEquals":{"aws:SourceAccount":"857687956942"},
                             "ArnEquals":{"aws:SourceArn":"arn:aws:scheduler:us-east-1:857687956942:schedule-group/default"}}}]}
            if execution_role["AssumeRolePolicyDocument"] != trust:
                raise ValueError("Existing Scheduler execution-role trust differs")
            invoke_permission = iam.simulate_principal_policy(PolicySourceArn=ROLE_ARN,
                ActionNames=["lambda:InvokeFunction"],ResourceArns=[FUNCTION_ARN])["EvaluationResults"]
            if not all(x["EvalDecision"] == "allowed" for x in invoke_permission):
                raise ValueError("Scheduler execution role cannot invoke the compiler")
            proof["scheduler_invoke_permission"] = "allowed; default schedule-group trust verified"
            reconciler_role = proof["packages"][RECONCILER]["role"]
            decisions = iam.simulate_principal_policy(PolicySourceArn=reconciler_role,
                ActionNames=["scheduler:CreateSchedule","scheduler:GetSchedule"],ResourceArns=[ARN])["EvaluationResults"]
            decisions += iam.simulate_principal_policy(PolicySourceArn=reconciler_role,
                ActionNames=["iam:PassRole"],ResourceArns=[ROLE_ARN],ContextEntries=[{
                    "ContextKeyName":"iam:PassedToService","ContextKeyValues":["scheduler.amazonaws.com"],"ContextKeyType":"string"}])["EvaluationResults"]
            if any(x["EvalDecision"] == "explicitDeny" for x in decisions):
                raise ValueError("Explicit IAM denial; refusing permission change")
            if not all(x["EvalDecision"] == "allowed" for x in decisions):
                policy = {"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":["scheduler:CreateSchedule","scheduler:GetSchedule"],"Resource":[ARN]},
                    {"Effect":"Allow","Action":"iam:PassRole","Resource":ROLE_ARN,
                     "Condition":{"StringEquals":{"iam:PassedToService":"scheduler.amazonaws.com"}}}]}
                policy_name = "justhodl-reconcile-daily-internals"
                policy_arn = "arn:aws:iam::857687956942:policy/"+policy_name
                try:
                    old = iam.get_policy(PolicyArn=policy_arn)["Policy"]
                    current = iam.get_policy_version(PolicyArn=policy_arn,VersionId=old["DefaultVersionId"])["PolicyVersion"]["Document"]
                    if current != policy: raise ValueError("Existing scoped policy differs")
                except iam.exceptions.NoSuchEntityException:
                    iam.create_policy(PolicyName=policy_name,PolicyDocument=json.dumps(policy),Description="Create/get only the declared daily internals schedule")
                iam.attach_role_policy(RoleName=reconciler_role.rsplit("/",1)[1],PolicyArn=policy_arn)
                proof["actions"].append("attached exact one-schedule reconciler policy")
                time.sleep(12)
            if manifest != before:
                manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
                manifest["source"] = "ops 5503: append daily internals; other declarations preserved"
                s3.put_object(Bucket=BUCKET,Key=MANIFEST,Body=json.dumps(manifest).encode(),ContentType="application/json",IfMatch=obj["ETag"])
                proof["actions"].append("appended one daily internals manifest declaration")
            proof["reconciler"] = invoke(RECONCILER,{"mode":"attach-internals"})
            schedule = sch.get_schedule(Name=NAME,GroupName="default")
            verify_schedule(schedule,req)
            proof["schedule"] = {k:schedule[k] for k in ("Arn","State","ScheduleExpression","ScheduleExpressionTimezone","LastModificationDate","Target")}
            after, manifest_obj = read(MANIFEST)
            if others(before) != others(after) or six_before != six_snapshot():
                raise ValueError("Unrelated declaration or one of the original six schedules changed")
            drift, _ = read("data/schedule-drift.json")
            if drift.get("enforced") != []:
                raise ValueError("Broad enforcement must remain empty")
            proof["other_manifest_sha256"] = digest(others(after))
            proof["original_six_schedules_unchanged"] = True
            proof["manifest_last_modified"] = manifest_obj["LastModified"].isoformat()
            proof["drift"] = {k:drift.get(k) for k in ("drift_count","by_class","enforced","compiler_attachment")}
            r.ok("Daily schedule ENABLED: "+schedule["Arn"])

            started = datetime.now(timezone.utc).replace(microsecond=0)
            proof["compiler_invoke"] = invoke(COMPILER,{"mode":"internals","kicked_by":"ops_5503"})
            live, live_obj = read(KEY)
            if live_obj["LastModified"] < started or live.get("schema_version") != 1:
                raise ValueError("Internals did not publish schema 1 after invocation")
            if live["fields"] != candidate_doc["fields"] or live["warehouse"]["fresh_fred_legs"] != 6:
                raise ValueError("Live fields differ from guarded warehouse build")
            if live["warehouse"]["fred_http_requests"] != 0:
                raise ValueError("Compiler reported a FRED HTTP request")
            proof["live"] = {"key":KEY,"last_modified":live_obj["LastModified"].isoformat(),
                             "schema_version":live["schema_version"],"fields":live["fields"],"breadth":live["breadth"]}
            proof["actions"].append("invoked internals compiler once")
            proof["status"] = "PASS"
            r.ok("LastModified=%s fields=%s" % (proof["live"]["last_modified"],json.dumps(live["fields"])))
        except Exception as exc:
            proof["status"] = "FAIL"
            proof["error"] = {"type":type(exc).__name__,"message":str(exc)[:250]}
            r.fail(json.dumps(proof["error"]))
            RECEIPT.write_text(json.dumps(proof,indent=2,default=str)+"\n")
            sys.exit(1)
        proof["finished_at"] = datetime.now(timezone.utc).isoformat()
        RECEIPT.write_text(json.dumps(proof,indent=2,default=str)+"\n")


if __name__ == "__main__":
    main()
