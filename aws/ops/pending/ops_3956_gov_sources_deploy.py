"""
ops_3956 — deploy justhodl-gov-sources (Khalid: an engine + page for
everything we learned pulling from gov agencies). SELF-HEALING create if
deploy-lambdas hasn't made the function (role/runtime/env discovered from
existing fleet functions); EventBridge Scheduler gov-sources-daily
cron(50 11) after the vault's 11:35; settle by marker STRING in zip; async
invoke + poll artifact; gates: 13 agencies, >=8 LIVE, BOJ+MOF+BCRP LIVE
specifically, vault join non-zero for boj/mof, SERVED page check with edge
retries; nav-manifest served check soft-logged.
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
sched = boto3.client("scheduler", region_name="us-east-1")
FN, MARK = "justhodl-gov-sources", "gov-sources v1.0 REGISTRY"
SRC = ROOT / "lambdas" / "justhodl-gov-sources" / "source"
UA = {"User-Agent": "Mozilla/5.0"}


def zip_src():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in SRC.iterdir():
            z.write(f, f.name)
    return buf.getvalue()


def deployed_src():
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    blob = urllib.request.urlopen(loc, timeout=60).read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        return z.read("lambda_function.py").decode("utf-8", "ignore")


def main():
    with report("3956_gov_sources_deploy") as rep:
        rep.heading("ops 3956 — gov-sources registry deploy")
        checks = []

        rep.section("function: settle or self-heal create")
        exists = True
        try:
            lam.get_function(FunctionName=FN)
        except lam.exceptions.ResourceNotFoundException:
            exists = False
        settled = False
        deadline = time.time() + 300
        while time.time() < deadline:
            try:
                if MARK in deployed_src():
                    settled = True
                    break
            except Exception:
                pass
            time.sleep(15)
        if not settled:
            rep.log("  deploy-lambdas hasn't landed it — self-heal create/update")
            donor = lam.get_function_configuration(FunctionName="justhodl-tradingview")
            envd = lam.get_function_configuration(FunctionName="justhodl-signal-backtest")
            env = {"Variables": (envd.get("Environment") or {}).get("Variables") or {}}
            if exists:
                lam.update_function_code(FunctionName=FN, ZipFile=zip_src())
            else:
                lam.create_function(
                    FunctionName=FN, Runtime=donor["Runtime"], Role=donor["Role"],
                    Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_src()},
                    Timeout=300, MemorySize=512, Environment=env)
            for _ in range(20):
                try:
                    c = lam.get_function_configuration(FunctionName=FN)
                    if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                        break
                except Exception:
                    pass
                time.sleep(8)
            settled = MARK in deployed_src()
        checks.append(("engine deployed (marker in zip)", settled))
        if not settled:
            rep.fail("engine never settled")
            sys.exit(1)
        rep.ok("  engine settled")

        rep.section("schedule: gov-sources-daily cron(50 11)")
        try:
            sched.get_schedule(Name="gov-sources-daily")
            rep.ok("  schedule exists")
        except sched.exceptions.ResourceNotFoundException:
            donor_s = sched.get_schedule(Name="tradingview-vault-daily")
            fn_arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
            sched.create_schedule(
                Name="gov-sources-daily",
                ScheduleExpression="cron(50 11 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": fn_arn, "RoleArn": donor_s["Target"]["RoleArn"],
                        "Input": "{}"})
            rep.ok("  schedule created (role from vault schedule)")
        checks.append(("schedule armed", True))

        rep.section("invoke + poll artifact")
        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        doc = None
        for i in range(40):
            time.sleep(10)
            try:
                d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                             Key="data/gov-sources.json")["Body"].read())
                if d.get("generated_at", "") > t_mark:
                    doc = d
                    rep.ok(f"  artifact written ~{(i+1)*10}s")
                    break
            except Exception:
                pass
        checks.append(("artifact written", doc is not None))
        if not doc:
            rep.fail("never wrote")
            sys.exit(1)

        st = {a["id"]: a["status"] for a in doc.get("agencies") or []}
        vj = {a["id"]: a.get("vault_count", 0) for a in doc.get("agencies") or []}
        rep.kv(n_agencies=doc.get("n_agencies"), n_live=doc.get("n_live"),
               n_degraded=doc.get("n_degraded"), statuses=str(st))
        checks.append(("13 agencies", doc.get("n_agencies") == 13))
        checks.append(("n_live >= 8", (doc.get("n_live") or 0) >= 8))
        for key in ("boj", "mof_japan", "bcrp"):
            checks.append((f"{key} LIVE", st.get(key) == "LIVE"))
        checks.append(("vault join boj >= 1", vj.get("boj", 0) >= 1))
        checks.append(("vault join mof >= 1", vj.get("mof_japan", 0) >= 1))

        rep.section("served page check (edge retries)")
        served = False
        for a in range(9):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/gov-sources.html?cb=" + str(time.time()),
                    headers=UA)
                body = urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "ignore")
                if "GOVERNMENT DATA SOURCES" in body and "gov-sources.json" in body:
                    served = True
                    rep.ok(f"  served attempt {a+1} ({len(body)}b)")
                    break
                rep.log(f"  attempt {a+1}: {len(body)}b, markers missing")
            except Exception as e:
                rep.log(f"  attempt {a+1}: {str(e)[:80]}")
            time.sleep(20)
        checks.append(("page served with markers", served))
        try:
            mreq = urllib.request.Request(
                "https://justhodl.ai/nav-manifest.json?cb=" + str(time.time()), headers=UA)
            mm = urllib.request.urlopen(mreq, timeout=20).read().decode("utf-8", "ignore")
            rep.log(f"  nav-manifest served has entry: {'gov-sources' in mm} (soft)")
        except Exception as e:
            rep.log(f"  manifest check: {str(e)[:70]} (soft)")

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — gov-sources registry LIVE: {doc.get('n_live')}/13 agencies "
               f"probing green, page served, schedule armed cron(50 11)")


if __name__ == "__main__":
    main()
