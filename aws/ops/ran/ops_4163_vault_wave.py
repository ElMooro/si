"""ops_4163 — vault v3.22.0 fire + gates + source-map v3 + crawler retire."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"


def settle(rep, name, mark):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    assert mark in src
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
        if name == "justhodl-tradingview":
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
    for att in range(5):
        try:
            lam.update_function_code(FunctionName=name,
                                     ZipFile=buf.getvalue(), Publish=True)
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
    with report("4163_vault_wave") as rep:
        rep.heading("ops 4163 — vault wave + source-map + retire")
        checks = [("vault v3.22.0 settled",
                   settle(rep, "justhodl-tradingview",
                          "tradingview-vault v3.22.0 ops4162 wb-wave"))]
        t_op = datetime.now(timezone.utc)
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        v = None
        got = False
        for i in range(48):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            try:
                if datetime.fromisoformat(
                        str(v.get("generated_at"))) > t_op:
                    rep.ok(f"  artifact after ~{(i+1)*15}s")
                    got = True
                    break
            except Exception:
                pass
        if not got:
            rep.fail("artifact never refreshed post-invoke")
            sys.exit(1)
        live = 0
        srcmap = {}
        for r2 in v.get("symbols") or []:
            sy = r2.get("symbol")
            st2 = r2.get("status")
            if st2 == "LIVE":
                live += 1
            srcmap[sy] = {"source": r2.get("source"), "status": st2,
                          "asof": r2.get("asof"),
                          "origin": "fleet" if st2 == "LIVE"
                          else ("nfs" if st2 == "NO_FREE_SOURCE"
                                else "pending")}
        rep.kv(total_live=live)
        checks.append(("total LIVE >= 4300", live >= 4300))
        tv = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-sources.json")["Body"].read())
        trows = (tv.get("sources") or tv.get("symbols")
                 or tv.get("by_symbol") or {})
        n_tv = 0
        if isinstance(trows, dict):
            for sy, rec in trows.items():
                s2 = (rec.get("source") if isinstance(rec, dict)
                      else rec) or ""
                if s2:
                    n_tv += 1
                    e = srcmap.setdefault(sy, {})
                    e["tv_source"] = str(s2)[:60]
                    if e.get("origin") in (None, "pending", "nfs"):
                        e["origin"] = "tv"
        s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                      Body=json.dumps(
                          {"generated_at": datetime.now(
                              timezone.utc).isoformat(),
                           "marker": "source-map v3.0 ops4163",
                           "n": len(srcmap), "tv_attributed": n_tv,
                           "by_symbol": srcmap}).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=600")
        rep.kv(source_map_n=len(srcmap), tv_attributed=n_tv)
        checks.append(("source-map >= 10000", len(srcmap) >= 10000))
        retired = []
        try:
            sch = boto3.client("scheduler", region_name="us-east-1")
            for pg in sch.get_paginator("list_schedules").paginate():
                for s4 in pg.get("Schedules", []):
                    nm = s4.get("Name", "")
                    if "crawler" in nm:
                        full = sch.get_schedule(Name=nm)
                        sch.update_schedule(
                            Name=nm, State="DISABLED",
                            ScheduleExpression=full["ScheduleExpression"],
                            FlexibleTimeWindow={"Mode": "OFF"},
                            Target=full["Target"])
                        retired.append(nm)
        except Exception as e4:
            rep.log(f"  retire EXC {type(e4).__name__}")
        rep.kv(retired=json.dumps(retired)[:120])
        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"VAULT WAVE — LIVE {live}, source-map {len(srcmap)}, "
               f"retired {retired}")


if __name__ == "__main__":
    main()
