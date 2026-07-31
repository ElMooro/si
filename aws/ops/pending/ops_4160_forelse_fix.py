"""ops_4160 — coverage triple-round: symbol-feed v1.2 (indices, futures,
crypto), vault fire, source-map v3 merge, dead-crawler retirement."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from collections import Counter
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
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
VMARK = "tradingview-vault v3.21.1 ops4154 dict-thaw"


def settle(rep, name, mark, zb):
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
    with report("4160_forelse_fix") as rep:
        rep.heading("ops 4158 — indices/futures/crypto + source-map v3")
        checks = []

        s2src = (ROOT / "lambdas" / "justhodl-symbol-feed" / "source" /
                "lambda_function.py").read_text()
        b2 = io.BytesIO()
        with zf.ZipFile(b2, "w", zf.ZIP_DEFLATED) as z9:
            z9.writestr("lambda_function.py", s2src)
        checks.append(("feed v1.3 settled",
                       settle(rep, "justhodl-symbol-feed",
                              "symbol-feed v1.3 ops4160", b2.getvalue())))
        r = lam.invoke(FunctionName="justhodl-symbol-feed",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(sf_err=r.get("FunctionError"),
               sf_out=r["Payload"].read().decode()[:120])
        fd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/symbol-feed.json")["Body"].read())
        rep.kv(targets=fd.get("targets"), resolved=fd.get("resolved"))
        checks.append(("targets >= 1800",
                       (fd.get("targets") or 0) >= 1800))
        checks.append(("resolved >= 1080",
                       (fd.get("resolved") or 0) >= 1080))
        p = fd.get("prices") or {}
        for bare, lo in (("ES1!", 1000),):
            v2 = (p.get(bare) or {}).get("value")
            rep.log(f"  spot {bare}: {v2} "
                    f"ysym={(p.get(bare) or {}).get('ysym')}")
            checks.append((f"spot {bare} > {lo}",
                           isinstance(v2, (int, float)) and v2 > lo))

        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        gen0 = str(v.get("generated_at") or "")
        fresh0 = gen0 and (datetime.now(timezone.utc)
                           - datetime.fromisoformat(gen0)
                           ).total_seconds() < 1500
        rep.kv(artifact_gen=gen0[:19], already_fresh=fresh0)
        t_op = datetime.now(timezone.utc)
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        got = False
        for i in range(50):
            time.sleep(15)
            v = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
            gen = str(v.get("generated_at") or "")
            try:
                if gen and datetime.fromisoformat(gen) > t_op:
                    rep.ok(f"  post-invoke artifact after ~{(i+1)*15}s")
                    got = True
                    break
            except Exception:
                pass
        if not got:
            rep.fail("artifact never refreshed post-invoke")
            sys.exit(1)
        live = 0
        nsym = 0
        srcmap = {}
        for r2 in v.get("symbols") or []:
            sy = r2.get("symbol")
            st2 = r2.get("status")
            if st2 == "LIVE":
                live += 1
            if r2.get("adapter") == "feed:symbol":
                nsym += 1
            srcmap[sy] = {"source": r2.get("source"),
                          "status": st2, "asof": r2.get("asof"),
                          "origin": "fleet" if st2 == "LIVE" else
                          ("nfs" if st2 == "NO_FREE_SOURCE"
                           else "pending")}
        rep.kv(total_live=live, feed_symbol_rows=nsym)
        checks += [("feed:symbol >= 450", nsym >= 450),
                   ("total LIVE >= 3800", live >= 3800)]

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
        doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
               "marker": "source-map v3.0 ops4158 merged",
               "n": len(srcmap), "tv_attributed": n_tv,
               "by_symbol": srcmap}
        s3.put_object(Bucket=BUCKET, Key="data/source-map.json",
                      Body=json.dumps(doc).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=600")
        rep.kv(source_map_n=len(srcmap), tv_attributed=n_tv)
        checks.append(("source-map v3 written >= 10000",
                       len(srcmap) >= 10000))

        retired = []
        try:
            for pg in sch.get_paginator("list_schedules").paginate():
                for s2 in pg.get("Schedules", []):
                    nm = s2.get("Name", "")
                    if "crawler" in nm or "notes-crawl" in nm:
                        sch.update_schedule(
                            Name=nm, State="DISABLED",
                            ScheduleExpression=sch.get_schedule(
                                Name=nm)["ScheduleExpression"],
                            FlexibleTimeWindow={"Mode": "OFF"},
                            Target=sch.get_schedule(Name=nm)["Target"])
                        retired.append(nm)
        except Exception as e3:
            rep.log(f"  retire EXC {type(e3).__name__}: {str(e3)[:100]}")
        rep.kv(retired=json.dumps(retired)[:120])

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"TRIPLE ROUND — resolved {fd.get('resolved')}, "
               f"LIVE {live}, source-map {len(srcmap)}")


if __name__ == "__main__":
    main()
