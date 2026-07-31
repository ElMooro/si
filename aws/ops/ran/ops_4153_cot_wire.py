"""ops_4153 — cot-feed create/settle/invoke + vault v3.21.0 + exact spots
from discovery + schedule."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from collections import Counter
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=280,
                                 retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
VMARK = "tradingview-vault v3.21.0 ops4153 cot-feed"


def zipped(name):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
        if name == "justhodl-tradingview":
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
    return src, buf.getvalue()


def settle(rep, name, mark, zb, create=False):
    try:
        lam.get_function_configuration(FunctionName=name)
        exists = True
    except Exception:
        exists = False
    if not exists:
        donor = lam.get_function_configuration(
            FunctionName="justhodl-families-feed")
        lam.create_function(FunctionName=name, Runtime=donor["Runtime"],
                            Role=donor["Role"],
                            Handler="lambda_function.lambda_handler",
                            Code={"ZipFile": zb}, Timeout=300,
                            MemorySize=512, Publish=True)
        rep.ok(f"  {name} CREATED")
    else:
        for att in range(5):
            try:
                lam.update_function_code(FunctionName=name, ZipFile=zb,
                                         Publish=True)
                break
            except Exception:
                time.sleep(8)
    for i in range(35):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=name)["Code"]
                    ["Location"], timeout=60).read())).read(
                    "lambda_function.py").decode()
                if mark in dep:
                    rep.ok(f"  {name} settled at loop {i}")
                    return True
        except Exception:
            pass
        time.sleep(9)
    rep.fail(f"  {name} never settled")
    return False


def main():
    with report("4153_cot_wire") as rep:
        rep.heading("ops 4153 — COT feed + vault wire")
        checks = []
        _, fz = zipped("justhodl-cot-feed")
        checks.append(("cot-feed settled",
                       settle(rep, "justhodl-cot-feed",
                              "cot-feed v1.0 ops4153", fz)))
        r = lam.invoke(FunctionName="justhodl-cot-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(feed_err=r.get("FunctionError"),
               out=r["Payload"].read().decode()[:120])
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cot-feed.json")["Body"].read())
        rep.kv(wanted=fd.get("wanted"), resolved=fd.get("resolved"),
               elapsed=fd.get("elapsed_s"))
        rep.log("  miss sample: " + json.dumps(
            (fd.get("misses") or [])[:8])[:300])
        checks.append(("resolved >= 220",
                       (fd.get("resolved") or 0) >= 220))
        p = fd.get("prices") or {}
        for bare, want in (("099741_F_DP_L", 51686.0),
                           ("067651_F_MMP_L", 187469.0)):
            got = (p.get(bare) or {}).get("value")
            rep.log(f"  spot {bare}: got={got} want={want}")
            checks.append((f"spot {bare} exact",
                           got is not None
                           and abs(float(got) - want) < 1))

        src, vz = zipped("justhodl-tradingview")
        assert VMARK in src
        checks.append(("vault v3.21.0 settled",
                       settle(rep, "justhodl-tradingview", VMARK, vz)))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        v = None
        for i in range(46):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            if str(v.get("marker")) == VMARK:
                rep.ok(f"  artifact after ~{(i+1)*15}s")
                break
        else:
            rep.fail("artifact never moved")
            for l, k in checks:
                (rep.ok if k else rep.fail)(f"  {l}")
            sys.exit(1)
        nc = sum(1 for r2 in v.get("symbols") or []
                 if r2.get("adapter") == "feed:cot")
        live = sum(1 for r2 in v.get("symbols") or []
                   if r2.get("status") == "LIVE")
        rep.kv(cot_adapter=nc, total_live=live)
        checks += [("feed:cot >= 200", nc >= 200),
                   ("total LIVE >= 3500", live >= 3500)]

        arn = lam.get_function_configuration(
            FunctionName="justhodl-cot-feed")["FunctionArn"]
        role = sch.get_schedule(
            Name="families-feed-daily")["Target"]["RoleArn"]
        try:
            sch.create_schedule(Name="cot-feed-daily",
                                ScheduleExpression="cron(45 11 * * ? *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn, "RoleArn": role,
                                        "Input": "{}"})
            rep.ok("  schedule cot-feed-daily cron(45 11)")
        except sch.exceptions.ConflictException:
            rep.ok("  schedule exists")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"COT WIRED — resolved {fd.get('resolved')}/"
               f"{fd.get('wanted')}, vault feed:cot {nc}, LIVE {live}")


if __name__ == "__main__":
    main()
