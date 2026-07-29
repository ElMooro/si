"""ops_4073 — promote source-map.json to a real, scheduled engine.

ops 4071 confirmed: data/source-map.json — the artifact harvest-monitor
renders — had NO Lambda producer anywhere in 756 functions and NO
schedule.  It existed only because an ops script wrote it by hand, so the
monitor froze the moment the session ended.

This op:
  A. creates/updates justhodl-source-map (self-heal: role, runtime and
     layer config discovered from a live donor rather than assumed)
  B. settles BY MARKER inside the deployed zip — State==Active returns
     instantly when the deploy has not started and would invoke the OLD
     artifact (that failure burned ops 3830 entirely)
  C. invokes and asserts the artifact is real: agency/venue split present,
     ECONOMICS map present, progress telemetry present
  D. arms an EventBridge schedule, borrowing the role from an existing
     schedule, and PROVES it is enabled rather than declaring it
  E. field-coverage audit: every key the engine writes must be rendered
     by harvest-monitor.html
  F. re-checks the extension zip at the edge — ops 4072 caught the Pages
     copy serving v1.4.0 from ops 3162 while S3 held v1.7.8
"""
import io
import json
import re
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name="us-east-1")

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-source-map"
DONOR = "justhodl-tv-workbench"
MARK = "source-map engine v2.0 ops4073"
SCHED = "source-map-daily"


def main():
    with report("4073_source_map_engine") as rep:
        rep.heading("ops 4073 — source-map promoted to a scheduled engine")
        checks = []

        # ═════════ A. deploy (self-heal from a live donor) ═════════
        rep.section("A. create/update the function")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        cfg = json.loads((ROOT / "lambdas" / FN / "config.json").read_text())
        assert MARK in src, "marker missing from source"

        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        code = buf.getvalue()

        try:
            d = lam.get_function_configuration(FunctionName=DONOR)
            role = d["Role"]
            runtime = d["Runtime"]
            rep.log(f"  donor {DONOR}: role/runtime discovered")
        except Exception as e:
            role, runtime = cfg["role"], cfg["runtime"]
            rep.log(f"  donor unreadable ({str(e)[:50]}) — config.json values")

        exists = True
        try:
            lam.get_function_configuration(FunctionName=FN)
        except lam.exceptions.ResourceNotFoundException:
            exists = False

        if not exists:
            lam.create_function(
                FunctionName=FN, Runtime=runtime, Role=role,
                Handler=cfg["handler"], Code={"ZipFile": code},
                Timeout=cfg["timeout"], MemorySize=cfg["memory"],
                Description=cfg["description"], Publish=True)
            rep.log(f"  ✓ CREATED {FN}")
        else:
            for _ in range(6):
                try:
                    lam.update_function_code(FunctionName=FN, ZipFile=code,
                                             Publish=True)
                    break
                except lam.exceptions.ResourceConflictException:
                    time.sleep(12)
            rep.log(f"  ✓ updated {FN}")
        rep.kv(created=not exists, bytes=len(code))

        # ═════════ B. ZIP-SETTLE BY MARKER (not State==Active) ═════════
        rep.section("B. settle by marker inside the deployed artifact")
        settled = False
        for a in range(24):
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if (c.get("State") == "Active"
                        and c.get("LastUpdateStatus") != "InProgress"):
                    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                    dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                        loc, timeout=60).read())).read(
                        "lambda_function.py").decode()
                    if MARK in dep:
                        settled = True
                        rep.log(f"  ✓ marker present in deployed zip "
                                f"(attempt {a + 1})")
                        break
            except Exception as e:
                rep.log(f"  settle attempt {a + 1}: {str(e)[:60]}")
            time.sleep(10)
        checks.append(("deployed artifact carries the marker", settled))
        if not settled:
            rep.log("✗ never settled — refusing to invoke a stale artifact")
            sys.exit(1)

        # ═════════ C. invoke + assert the artifact is real ═════════
        rep.section("C. invoke and read the live artifact")
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b'{"source":"ops4073"}')
        pay = r["Payload"].read().decode()
        rep.log(f"  invoke status={r['StatusCode']} fnerr={r.get('FunctionError')}")
        rep.log(f"  payload: {pay[:220]}")
        checks.append(("invoke clean", r.get("FunctionError") is None))

        art = json.loads(s3.get_object(Bucket=BUCKET,
                                       Key="data/source-map.json")["Body"].read())
        rep.kv(marker=art.get("marker"),
               sourced=art.get("symbols_with_source"),
               agency_rows=art.get("agency_rows"),
               venue_rows=art.get("venue_rows"),
               econ=art.get("economics_symbols"),
               junk=art.get("junk_purged"))
        checks.append(("artifact rewritten by the ENGINE, not an ops script",
                       art.get("marker") == MARK))

        prog = art.get("harvest_progress") or {}
        rep.log(f"  walk: {prog.get('walked')}/{prog.get('total')} "
                f"({prog.get('pct')}%) tier1={prog.get('tier1_done')} "
                f"rate={prog.get('rate_per_min')}/min eta={prog.get('eta_hours')}h")
        rep.section("Agency families attested so far")
        af = art.get("agency_families") or {}
        if af:
            for f, n in sorted(af.items(), key=lambda x: -x[1]):
                rep.log(f"  {n:5d}  {f}")
        else:
            rep.log("  (none yet — expected: the walk had not reached the "
                    "ECONOMICS tier when this ran. v1.7.8 changes that.)")
        rep.log(f"  venue rows (low-information): {art.get('venue_rows')}")

        # Structural, not value, assertions — a fresh walk legitimately
        # has zero agency rows, and a gate that demands otherwise would
        # be asserting a value the data has not earned yet.
        for k in ("generated_at", "known_families", "agency_families",
                  "economics_agencies", "harvest_progress", "new_sources"):
            checks.append((f"artifact carries `{k}`", k in art))

        # ═════════ D. arm the schedule and PROVE it ═════════
        rep.section("D. schedule")
        donor_role = None
        try:
            for pg in sch.get_paginator("list_schedules").paginate():
                for s_ in pg.get("Schedules", []):
                    d2 = sch.get_schedule(Name=s_["Name"])
                    if d2.get("Target", {}).get("RoleArn"):
                        donor_role = d2["Target"]["RoleArn"]
                        break
                if donor_role:
                    break
        except Exception as e:
            rep.log(f"  schedule donor scan: {str(e)[:70]}")

        armed = False
        if donor_role:
            arn = lam.get_function_configuration(
                FunctionName=FN)["FunctionArn"]
            spec = dict(
                Name=SCHED,
                ScheduleExpression="cron(20 12 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": donor_role,
                        "Input": json.dumps({"source": "schedule"})},
                State="ENABLED",
                Description="source-map attribution rollup (ops 4073)")
            try:
                sch.create_schedule(**spec)
                rep.log(f"  ✓ created schedule {SCHED}")
            except sch.exceptions.ConflictException:
                sch.update_schedule(**spec)
                rep.log(f"  ✓ updated schedule {SCHED}")
            got = sch.get_schedule(Name=SCHED)
            armed = got.get("State") == "ENABLED"
            rep.log(f"  state={got.get('State')} "
                    f"expr={got.get('ScheduleExpression')}")
            rep.kv(schedule=SCHED, state=got.get("State"))
        else:
            rep.log("  ✗ no donor role found — schedule NOT armed")
        checks.append(("schedule armed and ENABLED (verified, not declared)",
                       armed))

        # ═════════ E. field coverage ═════════
        rep.section("E. field-coverage audit against harvest-monitor.html")
        page = (REPO / "harvest-monitor.html").read_text()
        DYNAMIC = {"generated_at", "marker"}   # rendered via ago()/not shown
        missing = [k for k in art.keys()
                   if k not in DYNAMIC and k not in page]
        for k in sorted(art.keys()):
            mark = "·" if k in DYNAMIC else ("✓" if k in page else "✗")
            rep.log(f"  {mark} {k}")
        rep.kv(keys=len(art), unrendered=len(missing))
        checks.append((f"every shipped key is rendered "
                       f"({len(art) - len(missing)}/{len(art)})", not missing))
        if missing:
            rep.log(f"  ✗ unrendered: {missing}")

        # ═════════ F. edge parity on the extension zip ═════════
        rep.section("F. extension zip — S3 vs the Pages copy at the edge")
        s3zip = zf.ZipFile(io.BytesIO(s3.get_object(
            Bucket=BUCKET, Key="tools/jh-tv-extension.zip")["Body"].read()))
        s3v = json.loads(s3zip.read("manifest.json")).get("version")
        rep.log(f"  S3 zip version   : {s3v}")
        edgev = None
        for a in range(8):
            try:
                req = urllib.request.Request(
                    "https://justhodl.ai/tools/jh-tv-extension.zip",
                    headers={"User-Agent": f"justhodl-ops/4073-{a}",
                             "Cache-Control": "no-cache", "Pragma": "no-cache"})
                b = urllib.request.urlopen(req, timeout=40).read()
                edgev = json.loads(zf.ZipFile(io.BytesIO(b)).read(
                    "manifest.json")).get("version")
                if edgev == s3v:
                    break
            except Exception as e:
                rep.log(f"  edge attempt {a + 1}: {str(e)[:60]}")
            time.sleep(20)
        rep.log(f"  edge zip version : {edgev}")
        rep.kv(s3_zip=s3v, edge_zip=edgev)
        checks.append(("S3 zip is v1.7.8", s3v == "1.7.8"))
        checks.append(("edge (Pages) zip matches S3 — decoy closed",
                       edgev == s3v))

        # ═════════ verdict ═════════
        rep.section("VERDICT")
        for n, o in checks:
            rep.log(f"  {'✓' if o else '✗'} {n}")
        bad = [n for n, o in checks if not o]
        if bad:
            rep.log(f"✗ FAILED: {bad}")
            sys.exit(1)
        rep.log("✅ PASS_ALL — source-map.json now has a real producer on a "
                "daily schedule; the monitor no longer depends on me.")


if __name__ == "__main__":
    main()
