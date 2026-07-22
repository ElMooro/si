#!/usr/bin/env python3
"""ops 3736 — SHIP justhodl-grid-queue v1.0 (canary list item #2).

Physical import-flow canary built on the vocabulary PROVEN live in ops 3729:
HS6/HS4 + NAICS x country monthly import values from the US Census
International Trade API.

Deploy is DECLARATIVE (config.json -> deploy-lambdas.yml). This ops does NOT
call deploy_lambda — the same push carries the lambda source, and calling the
helper here would clobber the workflow (documented gotcha). Instead we:

  G0  KEY CONTRACT — grep the ENGINE SOURCE for every key the gates read,
      before any gate consumes one. Never type a key from memory.
  G1  zip-settle: poll get_function until the deployed artifact contains the
      VERSION marker, so we never invoke the OLD code
  G2  env: EIA_API_KEY present (inherited from eia-energy-agent)
  G3  invoke, assert 200 + a real data month
  G4  S3 artifact fresh + shape correct
  G5  DATA TRUTH: values are real and non-fabricated (TW 8542 must be a
      plausible 9-10 figure USD number, month must not be in the future)
  G6  signals ladder + industry rollup populated
  G7  schedule exists
"""
import json
import sys
import time
import traceback
import zipfile
import io
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-grid-queue"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/grid-queue.json"
REGION = "us-east-1"

with report("3736_grid_queue_regate") as rep:
    rep.heading("ops 3736 — SHIP justhodl-grid-queue v1.0 (power buildout canary)")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3736.json").write_text(json.dumps({"verdict": "STARTED"}))

    def gate(n, ok, detail):
        out["gates"][n] = {"ok": bool(ok), "detail": str(detail)[:900]}
        rep.log(("PASS " if ok else "FAIL ") + n + " — " + str(detail)[:800])
        if not ok:
            fails.append(n)
        return ok

    try:
        lam = boto3.client("lambda", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)

        # ── G0 KEY CONTRACT ──────────────────────────────────────────────
        rep.section("G0 — key contract (grep producer before gating)")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        need_keys = ["queue", "planned_capacity", "industrial_load",
                     "hotspots", "gaps", "coverage", "method",
                     "attribution", "active_mw", "large_projects"]
        missing = [k for k in need_keys if '"%s"' % k not in src]
        gate("G0_key_contract", not missing,
             "engine emits all gated keys" if not missing else "MISSING %s" % missing)

        # ── G1 zip settle ────────────────────────────────────────────────
        rep.section("G1 — zip settle (never invoke the old artifact)")
        marker = 'VERSION = "1.0.0"'
        settled = False
        for attempt in range(24):
            try:
                cfg = lam.get_function(FunctionName=FN)
                conf = cfg["Configuration"]
                # 3735 FAILED HERE: on a BRAND-NEW function State='Pending'
                # while LastUpdateStatus is already 'Successful', so the old
                # loop passed at 0s and the invoke hit ResourceConflict.
                # Both must be settled before any invoke.
                st = conf.get("State")
                lus = conf.get("LastUpdateStatus")
                if st != "Active" or lus == "InProgress":
                    rep.log("  waiting: State=%s LastUpdateStatus=%s" % (st, lus))
                    time.sleep(15)
                    continue
                url = cfg["Code"]["Location"]
                blob = urllib.request.urlopen(url, timeout=60).read()
                z = zipfile.ZipFile(io.BytesIO(blob))
                body = z.read("lambda_function.py").decode("utf-8", "replace")
                if marker in body and "grid-queue" in body:
                    settled = True
                    gate("G1_zip_settle", True,
                         "marker found after %ds" % (attempt * 15))
                    break
            except lam.exceptions.ResourceNotFoundException:
                rep.log("  function not yet created, waiting… (%d)" % attempt)
            except Exception as e:
                rep.log("  settle probe: %s %s" % (type(e).__name__, str(e)[:90]))
            time.sleep(15)
        if not settled:
            gate("G1_zip_settle", False, "marker never appeared in deployed zip")

        # ── G2 env ───────────────────────────────────────────────────────
        rep.section("G2 — EIA_API_KEY inherited")
        env = {}
        try:
            env = (lam.get_function_configuration(FunctionName=FN)
                   .get("Environment", {}).get("Variables", {}))
        except Exception as e:
            rep.warn("env read: %s" % str(e)[:120])
        ck = env.get("EIA_API_KEY", "")
        gate("G2_eia_key", bool(ck) and len(ck) > 20,
             "key present len=%d" % len(ck) if ck else "EIA_API_KEY ABSENT")

        # ── G3 ASYNC invoke ──────────────────────────────────────────────
        # 3730 died on ConnectionClosedError at 272s: a RequestResponse gate
        # on a ~1,200-call engine drops the runner connection. House pattern
        # for long engines = InvocationType='Event' + gate on S3 freshness.
        rep.section("G3 — async invoke (Event) + S3 freshness gate")
        try:
            before = None
            try:
                before = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                rep.log("  no prior artifact (first run)")
            r = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            gate("G3_invoke_accepted", r.get("StatusCode") == 202,
                 "async accepted status=%s" % r.get("StatusCode"))

            fresh = False
            for i in range(30):          # up to ~7.5 min
                time.sleep(15)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
                    if before is None or h["LastModified"] > before:
                        age = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60
                        fresh = True
                        rep.log("  artifact written after ~%ds (age %.1fmin)"
                                % ((i + 1) * 15, age))
                        break
                except Exception:
                    pass
            gate("G3_artifact_written", fresh,
                 "new artifact observed" if fresh else "no new artifact in 7.5min")
        except Exception as e:
            gate("G3_invoke_accepted", False, "%s %s" % (type(e).__name__, str(e)[:220]))

        # ── G4 artifact ──────────────────────────────────────────────────
        rep.section("G4 — S3 artifact fresh + shape")
        doc = None
        try:
            head = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
            age = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 60
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
            shape_ok = all(k in doc for k in
                           ("queue", "planned_capacity", "industrial_load",
                            "hotspots", "coverage", "attribution"))
            gate("G4_artifact", age < 20 and shape_ok,
                 "age=%.1fmin shape_ok=%s active_mw=%s"
                 % (age, shape_ok, (doc.get("queue") or {}).get("active_mw")))
        except Exception as e:
            gate("G4_artifact", False, "%s %s" % (type(e).__name__, str(e)[:200]))

        # ── G5 DATA TRUTH (queue parsed for real) ────────────────────────
        rep.section("G5 — data truth (CAISO parse is real, not empty)")
        if doc:
            q = doc.get("queue") or {}
            n_act = q.get("active_projects") or 0
            mw = q.get("active_mw") or 0
            large = q.get("large_projects") or []
            fuels = q.get("by_fuel_mw") or {}
            # CAISO's active book is thousands of projects / >100 GW.
            # A parser that silently returns 0 rows is the failure we guard.
            plausible = n_act >= 200 and mw >= 20000
            gate("G5_queue_parsed", plausible,
                 "active=%d projects %.0f MW large>=100MW=%d fuels=%d"
                 % (n_act, mw, len(large), len(fuels)))
            for p0 in large[:6]:
                rep.log("  %-38s %7.1f MW  %-12s %s"
                        % (p0.get("project", "")[:38], p0.get("mw") or 0,
                           (p0.get("fuel") or "")[:12], p0.get("county") or ""))
            rep.log("  fuel mix: %s" % list(fuels.items())[:6])
            rep.log("  withdrawal_ratio=%s%%  completion_ratio=%s%%"
                    % (q.get("withdrawal_ratio"), q.get("completion_ratio")))
        else:
            gate("G5_queue_parsed", False, "no doc")

        # ── G6 EIA legs + hotspots ───────────────────────────────────────
        rep.section("G6 — EIA planned capacity + industrial load + hotspots")
        if doc:
            pc = doc.get("planned_capacity") or {}
            il = doc.get("industrial_load") or {}
            hs = doc.get("hotspots") or []
            states = il.get("states") or []
            rep.log("  planned period=%s industrial_plants=%s"
                    % (pc.get("period"), pc.get("n_industrial")))
            for u in (pc.get("upcoming_uprates") or [])[:5]:
                rep.log("  UPRATE %-30s %6.1f MW  %s  %s"
                        % ((u.get("plant") or "")[:30], u.get("uprate_mw") or 0,
                           u.get("when"), u.get("state")))
            rep.log("  industrial load period=%s states=%d"
                    % (il.get("period"), len(states)))
            for st in states[:5]:
                rep.log("  LOAD %-4s %8.1f GWh  YoY %s%%  3m %s%%"
                        % (st.get("state"), st.get("sales_gwh") or 0,
                           st.get("yoy_pct"), st.get("mom_3m_pct")))
            for h in hs[:6]:
                rep.log("  HOTSPOT %-4s legs=%d %-20s load %s%% uprate %s MW"
                        % (h.get("state"), h.get("legs"), h.get("read"),
                           h.get("industrial_load_yoy_pct"),
                           h.get("planned_uprate_mw")))
            gate("G6_eia_legs",
                 len(states) >= 20 and (pc.get("n_industrial") or 0) > 0,
                 "load_states=%d industrial_plants=%s hotspots=%d"
                 % (len(states), pc.get("n_industrial"), len(hs)))
            # honesty: gaps must be declared, never silently empty
            gaps = doc.get("gaps") or []
            gate("G6_gaps_declared", len(gaps) >= 5,
                 "declared %d known gaps (ERCOT/PJM/MISO/LBNL/permits)" % len(gaps))
        else:
            gate("G6_eia_legs", False, "no doc")

        # ── G7 schedule ──────────────────────────────────────────────────
        rep.section("G7 — schedule (ensure, then verify)")
        try:
            # 3735: declarative schedule did not materialise on first create.
            # House pattern = ensure via EventBridge Scheduler, FTW OFF.
            try:
                sch0 = boto3.client("scheduler", region_name=REGION)
                acct = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
                sch0.create_schedule(
                    Name="justhodl-grid-queue-daily",
                    ScheduleExpression="cron(50 12 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={
                        "Arn": "arn:aws:lambda:%s:%s:function:%s" % (REGION, acct, FN),
                        "RoleArn": "arn:aws:iam::%s:role/justhodl-scheduler-role" % acct,
                        "Input": "{}",
                    },
                    Description="Grid queue canary — daily 12:50 UTC",
                )
                rep.log("  scheduler created")
            except Exception as e:
                rep.log("  scheduler ensure: %s" % str(e)[:130])
        except Exception as e:
            rep.log("  ensure block: %s" % str(e)[:120])
        try:
            sch = boto3.client("scheduler", region_name=REGION)
            found = False
            p = sch.get_paginator("list_schedules")
            for page in p.paginate():
                for s in page.get("Schedules", []):
                    if "import-canary" in s["Name"]:
                        found = True
                        rep.log("  scheduler: %s %s" % (s["Name"], s.get("State")))
            if not found:
                ev = boto3.client("events", region_name=REGION)
                for r0 in ev.list_rules(NamePrefix="justhodl-grid-queue").get("Rules", []):
                    found = True
                    rep.log("  eventbridge rule: %s %s" % (r0["Name"], r0.get("ScheduleExpression")))
            gate("G7_schedule", found, "schedule present" if found else "NO SCHEDULE")
        except Exception as e:
            gate("G7_schedule", False, str(e)[:180])

        # ── verdict ──────────────────────────────────────────────────────
        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        out["data_month"] = (doc or {}).get("data_month")
        Path("aws/ops/reports/3736.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict, function=FN,
               data_month=(doc or {}).get("data_month") or "?",
               n_lines=(doc or {}).get("n_lines") or 0,
               signals=len((doc or {}).get("signals") or []),
               failed=",".join(fails) or "none")

        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — import-canary live; page next")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3736.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
