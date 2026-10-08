"""
ops_3978 — data-census v1.1: CloudWatch truth + resource fix + async close.

ops 3977 proved the lambda crashes server-side (12 min, no output). Before
retrying: read the ACTUAL error from CloudWatch. Then apply the near-certain
fix — the walk over ~800 artifacts incl. multi-MB parses under a 600s cap:
timeout 900 / memory 3008, engine v1.1 caps artifacts at 4MB and adds a
780s wall-clock budget that publishes partial-but-honest instead of dying.

ops 3975/3976 history, honestly: run 1 died at Configure-AWS-credentials
(zero lines executed). Run 2 created nothing new (Deploy Lambdas had in fact
created the function at 17:34:41 — the create path works when credentials
do), settled the marker in 10s, then died on the SYNCHRONOUS invoke with
ConnectionClosedError: the census walks ~800 S3 artifacts and the held
HTTPS connection outlived the runner's patience. The vault hit this exact
wall and ops 3972 set the pattern: InvocationType="Event", then poll the
OUTPUT KEY for a fresh generated_at.

This op: (a) reads data/data-census.json first — the crashed invoke may
have completed server-side; (b) if stale/absent, invokes async and polls up
to 12 min; (c) runs every verification gate; (d) creates the
data-census-daily schedule the crashed op never reached; (e) verifies
data-census.html at the Cloudflare edge.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-data-census"
OUT = "data/data-census.json"
PAGE = "https://justhodl.ai/data-census.html"
PAGE_MARKS = ["v2-ops3979", "Metric directory", "pulled from",
              "data-census.json", "JPEXPYY"]


def read_out():
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
    except Exception:
        return None


def main():
    with report("3979_census_v12_provenance") as rep:
        rep.heading("ops 3977 — data-census async close + schedule + page edge")
        checks = []
        now = datetime.now(timezone.utc)

        rep.section("A0. the truth from CloudWatch")
        logs = boto3.client("logs", region_name="us-east-1")
        try:
            sts = logs.describe_log_streams(logGroupName=f"/aws/lambda/{FN}",
                                            orderBy="LastEventTime", descending=True,
                                            limit=2)["logStreams"]
            for st_ in sts:
                ev = logs.get_log_events(logGroupName=f"/aws/lambda/{FN}",
                                         logStreamName=st_["logStreamName"],
                                         limit=60)["events"]
                for e in ev:
                    m = e["message"].rstrip()
                    if any(k in m for k in ("Error", "Task timed out", "Traceback",
                                            "MemoryError", "[census]", "REPORT")):
                        rep.log(f"  {m[:190]}")
        except Exception as e:
            rep.log(f"  logs: {type(e).__name__}: {str(e)[:100]}")

        rep.section("A1. push engine v1.2 + settle by marker")
        import io, zipfile as zf
        MARK = "data-census v1.2 ops3979 provenance"
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for a in range(8):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(),
                                         Publish=True)
                break
            except lam.exceptions.ResourceConflictException:
                time.sleep(15)
        settled = False
        for i2 in range(30):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                info = lam.get_function(FunctionName=FN)
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    info["Code"]["Location"], timeout=60).read()
                )).read("lambda_function.py").decode()
                if MARK in dep:
                    settled = True
                    break
            time.sleep(10)
        rep.kv(v12_settled=settled, timeout=c.get("Timeout"), memory=c.get("MemorySize"))
        checks.append(("v1.2 settled by marker", settled))
        if not settled:
            rep.fail("v1.2 never settled")
            sys.exit(1)

        rep.section("A. does the crashed invoke's output already exist?")
        doc = read_out()
        fresh = False
        if doc:
            try:
                gen = datetime.fromisoformat(doc.get("generated_at"))
                fresh = ((now - gen).total_seconds() < 3600
                         and doc.get("marker") == "data-census v1.2 ops3979 provenance")
            except Exception:
                pass
            rep.log(f"  found generated_at={doc.get('generated_at')} fresh={fresh}")
        else:
            rep.log("  no artifact yet")

        if not fresh:
            rep.section("B. async invoke + poll (the 3972 pattern)")
            prev_gen = (doc or {}).get("generated_at")
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"source": "ops3979"}).encode())
            for i in range(45):
                time.sleep(20)
                doc = read_out()
                if doc and doc.get("generated_at") != prev_gen:
                    rep.ok(f"  census wrote after ~{(i+1)*20}s")
                    fresh = True
                    break
                if i % 6 == 5:
                    rep.log(f"  [{i}] still waiting")
        checks.append(("census artifact fresh", fresh))
        if not fresh:
            rep.fail("census never wrote — check CloudWatch for the lambda error")
            sys.exit(1)

        rep.section("C. verify")
        t = doc.get("totals") or {}
        rep.kv(**t)
        rep.kv(marker=doc.get("marker"), elapsed_s=doc.get("elapsed_s"))
        mis = doc.get("mislabel_candidates") or []
        con = doc.get("measure_conflicts") or []
        gap = doc.get("gap_fill_candidates") or []
        rep.kv(mislabels=len(mis), conflicts=len(con), gap_candidates=len(gap))
        for m in mis[:6]:
            rep.log(f"  MISLABEL v={m['value']} {m['countries_claimed']} "
                    f"{[p['artifact'].split('/')[-1]+':'+p['path'] for p in m['paths'][:3]]}")
        for c in con[:5]:
            rep.log(f"  CONFLICT {c['country']} {c['measures']} spread={c['spread']} "
                    f"n={c['n_engines']}")
        for g in gap[:8]:
            rep.log(f"  GAP {g['symbol']:9s} notes={g['n_notes']} -> "
                    f"{[x['artifact'].split('/')[-1]+':'+x['path'] for x in g['candidates'][:2]]}")
        led_ok = False
        try:
            h = s3.head_object(Bucket=BUCKET, Key="data-census/paths-ledger.json")
            led_ok = h["ContentLength"] > 10000
            rep.kv(ledger_bytes=h["ContentLength"])
        except Exception as e:
            rep.log(f"  ledger: {type(e).__name__}")
        checks += [
            (">=100 artifacts walked", (t.get("artifacts") or 0) >= 100),
            (">=2000 scalar paths indexed", (t.get("scalar_paths") or 0) >= 2000),
            ("all three detectors present", all(k in doc for k in
             ("mislabel_candidates", "measure_conflicts", "gap_fill_candidates"))),
            ("full paths ledger written", led_ok),
            ("honesty clause published", "never an auto-fix" in
             str(doc.get("honesty", ""))),
        ]
        md = doc.get("metric_directory") or []
        rep.kv(metric_directory_n=len(md),
               truncated=t.get("artifacts_truncated_by_time_budget"))
        for m in md[:8]:
            rep.log(f"  DIR {str(m.get('name'))[:40]:40s} = {m.get('value')} "
                    f"live={m.get('live')} from={str(m.get('pulled_from'))[:30]} "
                    f"engine={str(m.get('engine'))[:34]}")
        full = [m for m in md if m.get("name") and m.get("pulled_from")
                and m.get("engine")]
        checks += [
            ("metric directory populated (>=50)", len(md) >= 50),
            (">=50 entries carry name+source+engine", len(full) >= 50),
            ("keyed-list walk landed (identifier-labelled paths present)",
             any("[" in (m.get("path") or "") and "[0]" not in (m.get("path") or "")
                 for m in md)),
        ]

        rep.section("D. schedule (crashed op never reached it)")
        try:
            role = sch.get_schedule(Name="tradingview-vault-daily")["Target"]["RoleArn"]
            kw = dict(Name="data-census-daily",
                      ScheduleExpression="cron(45 12 * * ? *)",
                      FlexibleTimeWindow={"Mode": "OFF"},
                      Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                              "RoleArn": role,
                              "Input": json.dumps({"source": "schedule"})},
                      State="ENABLED")
            try:
                sch.create_schedule(**kw)
                rep.ok("  schedule created")
            except sch.exceptions.ConflictException:
                sch.update_schedule(**kw)
                rep.ok("  schedule updated")
            st = sch.get_schedule(Name="data-census-daily")
            rep.kv(schedule_state=st.get("State"), cron=st.get("ScheduleExpression"))
            checks.append(("schedule ENABLED", st.get("State") == "ENABLED"))
        except Exception as e:
            rep.fail(f"  {type(e).__name__}: {str(e)[:120]}")
            checks.append(("schedule ENABLED", False))

        rep.section("E. page at the Cloudflare edge")
        html, got = "", 0
        for i in range(10):
            try:
                req = urllib.request.Request(PAGE + f"?cb={int(time.time())}",
                                             headers={"User-Agent": "Mozilla/5.0",
                                                      "Cache-Control": "no-cache"})
                html = urllib.request.urlopen(req, timeout=30).read().decode("utf8", "ignore")
                got = sum(1 for m in PAGE_MARKS if m in html)
                rep.log(f"  [{i}] {len(html)}B {got}/{len(PAGE_MARKS)}")
                if got == len(PAGE_MARKS):
                    break
            except Exception as e:
                rep.log(f"  [{i}] {type(e).__name__}")
            time.sleep(20)
        checks.append(("data-census.html live at edge", got == len(PAGE_MARKS)))

        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — census {t.get('artifacts')} artifacts / "
               f"{t.get('scalar_paths')} values; {len(mis)} mislabel candidates, "
               f"{len(con)} conflicts, {len(gap)} gap candidates; scheduled 12:45; "
               f"page {len(html)}B at edge")


if __name__ == "__main__":
    main()
