"""ops_4085 — STEP 1: make the 765 FRED: tickers fetchable, + the step-2 pipe.

ops 4084: of 10,319 imported tickers only 43.6% had a fetch route. Largest
gap is macro. Step 1 closes the certain part of it — FRED: tickers, where
the series id IS the fetch key — with every id VERIFIED against the FRED
API before it becomes an alias.

Also ships the prerequisite step 2 needs: the harvester was discarding the
description whenever source was null, which is every macro symbol. That
description is the join key for mapping ECONOMICS: codes to FRED series,
so v1.8.1 captures it end-to-end (content -> background -> ingest ->
data/tv-descriptions.json) and it accretes while the matcher is built.
"""
import io, json, sys, time, urllib.request, zipfile as zf
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def deploy(rep, fn, marker, donor="justhodl-tv-workbench", envkeys=("FRED_KEY",)):
    d = ROOT / "lambdas" / fn
    src = (d / "source" / "lambda_function.py").read_text()
    cfg = json.loads((d / "config.json").read_text())
    assert marker in src, f"{fn}: marker missing from source"
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
    code = buf.getvalue()
    dc = lam.get_function_configuration(FunctionName=donor)
    env = dict((dc.get("Environment") or {}).get("Variables") or {})
    keep = {k: env[k] for k in envkeys if k in env}
    try:
        lam.get_function_configuration(FunctionName=fn); exists = True
    except lam.exceptions.ResourceNotFoundException:
        exists = False
    if not exists:
        lam.create_function(FunctionName=fn, Runtime=dc["Runtime"], Role=dc["Role"],
                            Handler=cfg["handler"], Code={"ZipFile": code},
                            Timeout=cfg["timeout"], MemorySize=cfg["memory"],
                            Description=cfg["description"],
                            Environment={"Variables": keep}, Publish=True)
        rep.log(f"  ✓ CREATED {fn} (env {list(keep)})")
    else:
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=fn, ZipFile=code, Publish=True); break
            except lam.exceptions.ResourceConflictException:
                time.sleep(12)
        rep.log(f"  ✓ updated {fn}")
    for a in range(24):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=60).read())
                                 ).read("lambda_function.py").decode()
                if marker in dep:
                    rep.log(f"  ✓ {fn} settled by marker (attempt {a+1})")
                    return True
        except Exception as e:
            rep.log(f"  settle {a+1}: {str(e)[:60]}")
        time.sleep(10)
    return False


def main():
    with report("4085_step1_fred_aliases") as rep:
        rep.heading("ops 4085 — STEP 1: FRED tickers fetchable + descs pipe")
        checks = []

        rep.section("A. deploy justhodl-symbol-resolver")
        ok = deploy(rep, "justhodl-symbol-resolver", "symbol-resolver v1.0 ops4085 step1-fred")
        checks.append(("resolver settled", ok))
        if not ok:
            rep.log("✗ stale artifact — refusing to invoke"); sys.exit(1)

        rep.section("B. invoke — verify FRED ids against the FRED API")
        r = lam.invoke(FunctionName="justhodl-symbol-resolver",
                       InvocationType="RequestResponse", Payload=b'{"source":"ops4085"}')
        rep.log(f"  status={r['StatusCode']} fnerr={r.get('FunctionError')}")
        rep.log(f"  {r['Payload'].read().decode()[:220]}")
        checks.append(("invoke clean", r.get("FunctionError") is None))

        sa = json.loads(s3.get_object(Bucket=BUCKET, Key="data/symbol-aliases.json")["Body"].read())
        rep.kv(fred_total=sa.get("fred_tickers_total"), verified=sa.get("fred_verified"),
               dead=sa.get("fred_dead"), coverage=sa.get("coverage_pct"),
               calls=sa.get("fred_calls_this_run"))
        rep.section("Sample verified aliases (real, pullable series)")
        for code, row in list((sa.get("detail") or {}).items())[:12]:
            rep.log(f"  {code:22} → {row['alias']:26} {str(row.get('title'))[:44]}")
        checks.append(("aliases produced", (sa.get("fred_verified") or 0) > 0))
        # HONESTY: nothing unverified may become an alias.
        det = sa.get("detail") or {}
        checks.append(("every alias was FRED-verified (confidence 1.0)",
                       all(v.get("route") == "fred-verified" and v.get("confidence") == 1.0
                           for v in det.values())))
        checks.append(("every alias carries a real series title",
                       all(v.get("title") for v in det.values())))
        checks.append(("no ECONOMICS guesswork shipped in step 1",
                       not any(str(v.get("tv_symbol","")).startswith("ECONOMICS:")
                               for v in det.values())))

        rep.section("C. vault v3.11.0 consumes the generated aliases")
        ok2 = deploy(rep, "justhodl-tradingview", "tradingview-vault v3.11.0 ops4085 generated-aliases",
                     envkeys=("FRED_KEY","FMP_KEY","POLYGON_KEY","POLY_KEY"))
        checks.append(("vault settled", ok2))
        r2 = lam.invoke(FunctionName="justhodl-tradingview",
                        InvocationType="RequestResponse", Payload=b'{"source":"ops4085"}')
        rep.log(f"  vault invoke fnerr={r2.get('FunctionError')}")
        rep.log(f"  {r2['Payload'].read().decode()[:200]}")
        checks.append(("vault invoke clean", r2.get("FunctionError") is None))
        v = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        rows = v.get("symbols") or []
        live = [x for x in rows if str(x.get("status")).upper() == "LIVE"]
        rep.kv(vault_rows=len(rows), vault_live=len(live))
        rep.log(f"  vault rows {len(rows)}  LIVE {len(live)}")
        checks.append(("vault still healthy after the alias layer", len(live) >= 400))

        rep.section("D. ingest v-descs (step 2 prerequisite)")
        ok3 = deploy(rep, "justhodl-tv-notes-ingest", "_save_descs", envkeys=())
        checks.append(("ingest settled with _save_descs", ok3))

        rep.section("E. extension v1.8.1 — descriptions captured")
        try:
            old = s3.get_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip")["Body"].read()
            rooted = not any(n.startswith("chrome-extension/") for n in zf.ZipFile(io.BytesIO(old)).namelist()[:4])
        except Exception:
            rooted = True
        buf = io.BytesIO(); srcd = REPO / "chrome-extension"
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            for f in sorted(srcd.rglob("*")):
                if f.is_file():
                    z.write(f, str(f.relative_to(srcd if rooted else REPO)))
        data = buf.getvalue()
        s3.put_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip", Body=data,
                      ContentType="application/zip", CacheControl="max-age=300")
        chk = zf.ZipFile(io.BytesIO(s3.get_object(Bucket=BUCKET, Key="tools/jh-tv-extension.zip")["Body"].read()))
        pre = "" if rooted else "chrome-extension/"
        man = json.loads(chk.read(pre + "manifest.json"))
        cjs = chk.read(pre + "content.js").decode()
        bjs = chk.read(pre + "background.js").decode()
        rep.kv(ext_version=man.get("version"), ext_bytes=len(data))
        checks.append(("extension v1.8.1", man.get("version") == "1.8.1"))
        checks.append(("descriptions captured when source is null",
                       "DESCS[sym] = dsc" in cjs and "if (!gotSrc)" in cjs))
        checks.append(("descs shipped in the sync payload", "descs: DESCS" in cjs))
        checks.append(("background forwards descs", "msg.descs" in bjs and "descs: descs || {}" in bjs))
        checks.append(("v1.8.0 AIMD backoff not regressed",
                       "function onOk" in cjs and "wall_events" in cjs
                       and "setTimeout(step, 240)" not in cjs))
        checks.append(("priority walk not regressed", "PRIORITY WALK" in cjs))

        rep.section("F. schedule the resolver")
        role = None
        for pg in sch.get_paginator("list_schedules").paginate():
            for s_ in pg.get("Schedules", []):
                d2 = sch.get_schedule(Name=s_["Name"])
                if d2.get("Target", {}).get("RoleArn"):
                    role = d2["Target"]["RoleArn"]; break
            if role: break
        arn = lam.get_function_configuration(FunctionName="justhodl-symbol-resolver")["FunctionArn"]
        spec = dict(Name="symbol-resolver-daily", ScheduleExpression="cron(50 11 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": arn, "RoleArn": role, "Input": json.dumps({"source": "schedule"})},
                    State="ENABLED", Description="TV ticker -> fetchable series aliases (ops 4085)")
        try:
            sch.create_schedule(**spec); rep.log("  ✓ created symbol-resolver-daily")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**spec); rep.log("  ✓ updated symbol-resolver-daily")
        got = sch.get_schedule(Name="symbol-resolver-daily")
        rep.log(f"  state={got.get('State')} expr={got.get('ScheduleExpression')} "
                f"(before the vault's 11:35 so aliases are fresh)")
        checks.append(("resolver schedule ENABLED", got.get("State") == "ENABLED"))

        rep.section("VERDICT")
        for n, o in checks: rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}"); sys.exit(1)
        rep.log(f"✅ PASS_ALL — {sa.get('fred_verified')} FRED tickers verified and "
                f"aliased; ledger accretes toward {sa.get('fred_tickers_total')}. "
                f"Descriptions now captured for step 2.")


if __name__ == "__main__":
    main()
