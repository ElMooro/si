#!/usr/bin/env python3
"""ops 3731 — SHIP justhodl-import-canary v1.0 (canary list item #1).

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
  G2  env: CENSUS_API_KEY present (inherited from census-economic-agent)
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

FN = "justhodl-import-canary"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/import-canary.json"
REGION = "us-east-1"

with report("3731_import_canary_regate") as rep:
    rep.heading("ops 3731 — SHIP justhodl-import-canary v1.0 (import flow canary)")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3731.json").write_text(json.dumps({"verdict": "STARTED"}))

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
        need_keys = ["data_month", "lines", "naics_lines", "signals",
                     "industry_rollup", "coverage", "degraded",
                     "attribution", "scope_note", "n_lines"]
        missing = [k for k in need_keys if '"%s"' % k not in src]
        gate("G0_key_contract", not missing,
             "engine emits all gated keys" if not missing else "MISSING %s" % missing)

        # ── G1 zip settle ────────────────────────────────────────────────
        rep.section("G1 — zip settle (never invoke the old artifact)")
        marker = 'VERSION = "1.0.1"'
        settled = False
        for attempt in range(20):
            try:
                cfg = lam.get_function(FunctionName=FN)
                if cfg["Configuration"].get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                url = cfg["Code"]["Location"]
                blob = urllib.request.urlopen(url, timeout=60).read()
                z = zipfile.ZipFile(io.BytesIO(blob))
                body = z.read("lambda_function.py").decode("utf-8", "replace")
                if marker in body and "import-canary" in body:
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
        rep.section("G2 — CENSUS_API_KEY inherited")
        env = {}
        try:
            env = (lam.get_function_configuration(FunctionName=FN)
                   .get("Environment", {}).get("Variables", {}))
        except Exception as e:
            rep.warn("env read: %s" % str(e)[:120])
        ck = env.get("CENSUS_API_KEY", "")
        gate("G2_census_key", bool(ck) and len(ck) > 20,
             "key present len=%d" % len(ck) if ck else "CENSUS_API_KEY ABSENT")

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
                           ("data_month", "lines", "signals", "industry_rollup",
                            "coverage", "attribution"))
            gate("G4_artifact", age < 20 and shape_ok,
                 "age=%.1fmin shape_ok=%s n_lines=%s"
                 % (age, shape_ok, doc.get("n_lines")))
        except Exception as e:
            gate("G4_artifact", False, "%s %s" % (type(e).__name__, str(e)[:200]))

        # ── G5 DATA TRUTH ────────────────────────────────────────────────
        rep.section("G5 — data truth (real numbers, no fabrication)")
        if doc:
            dm = doc.get("data_month", "")
            now = datetime.now(timezone.utc)
            future = dm > now.strftime("%Y-%m")
            lines = doc.get("lines") or []
            ic = next((l for l in lines if l.get("code") == "8542"), None)
            lvl = (ic or {}).get("level")
            plausible = bool(lvl and 1e8 < lvl < 1e11)
            nonzero = sum(1 for l in lines if l.get("level"))
            gate("G5_data_truth",
                 (not future) and plausible and nonzero >= 10,
                 "month=%s future=%s HS4_8542_level=%s plausible=%s lines_with_level=%d"
                 % (dm, future, lvl, plausible, nonzero))
            conc = (ic or {}).get("concentration") or {}
            rep.log("  8542 top source: %s %.1f%% (HHI %s)"
                    % (conc.get("top_source"), conc.get("top_share_pct") or 0,
                       conc.get("hhi")))
            sh = (ic or {}).get("source_shift") or {}
            rep.log("  8542 share shift: +%s %s / %s %s"
                    % (sh.get("gainer"), sh.get("gainer_pp"),
                       sh.get("loser"), sh.get("loser_pp")))
        else:
            gate("G5_data_truth", False, "no doc")

        # ── G6 signals + rollup ──────────────────────────────────────────
        rep.section("G6 — signal ladder + industry rollup")
        if doc:
            sigs = doc.get("signals") or []
            roll = doc.get("industry_rollup") or []
            for s in sigs[:8]:
                rep.log("  %-24s %-9s YoY %+.1f%%  accel %s  src %s"
                        % (s.get("label", "")[:24], s.get("tier"),
                           s.get("yoy_pct") or 0, s.get("accel_pp"),
                           s.get("top_source")))
            for r0 in roll[:6]:
                rep.log("  ROLLUP %-26s %+.1f%%  $%.2fB/mo (%d lines)"
                        % (r0["industry"][:26], r0["import_yoy_pct"],
                           (r0["import_usd_mo"] or 0) / 1e9, r0["n_lines"]))
            gate("G6_signals", len(roll) >= 5,
                 "signals=%d rollup_industries=%d" % (len(sigs), len(roll)))
        else:
            gate("G6_signals", False, "no doc")

        # ── G7 schedule ──────────────────────────────────────────────────
        rep.section("G7 — schedule")
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
                for r0 in ev.list_rules(NamePrefix="justhodl-import-canary").get("Rules", []):
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
        Path("aws/ops/reports/3731.json").write_text(json.dumps(out, indent=2))
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
        Path("aws/ops/reports/3731.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
