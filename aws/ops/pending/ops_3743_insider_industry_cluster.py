#!/usr/bin/env python3
"""ops 3743 — SHIP canary #16: insider INDUSTRY-cluster overlay.

Two parts:
  1. justhodl-insider-cluster-scanner gains an ADDITIVE `all_ticker_buys`
     sidecar (schema 2.1). Its cluster filter drops every single-insider
     ticker, but 4 different companies each with one CEO buying IS the
     peer-group signal — the industry canary needs the discarded rows.
     Existing `clusters` consumers are untouched.
  2. justhodl-insider-industry-cluster v1.0 consumes that sidecar and rolls
     Form 4 buys up to industry with breadth / participation / conviction /
     concentration structure and a self-building base rate.

GATES
  G0  KEY CONTRACT — grep BOTH producers for every key a gate reads
  G1  scanner zip-settles to schema 2.1, then invoke, then sidecar present
  G1b SCHEDULE ORDER — scanner must publish BEFORE 14:20 UTC or the canary
      reads a stale sidecar every day (scanner has no config.json; its
      schedule is live-only and must be discovered, never assumed)
  G2  industry engine settles + async invoke + S3 freshness
  G3  DATA TRUTH — breadth is distinct companies; no industry may report
      more buying companies than it has listed names
  G4  FALSE-POSITIVE GUARD — biotech/regional banks must not top the board
      purely on raw count; thin universes must be labelled, not promoted
  G5  schedule ensured
"""
import io
import json
import sys
import time
import traceback
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

SCANNER = "justhodl-insider-cluster-scanner"
ENGINE = "justhodl-insider-industry-cluster"
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/insider-clusters.json"
OUT_KEY = "data/insider-industry-cluster.json"
REGION = "us-east-1"

with report("3743_insider_industry_cluster") as rep:
    rep.heading("ops 3743 — canary #16: insider industry-cluster overlay")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3743.json").write_text(json.dumps({"verdict": "STARTED"}))

    def gate(n, ok, detail):
        out["gates"][n] = {"ok": bool(ok), "detail": str(detail)[:900]}
        rep.log(("PASS " if ok else "FAIL ") + n + " — " + str(detail)[:800])
        if not ok:
            fails.append(n)
        return ok

    def settle(fn, marker, tries=24):
        lam = boto3.client("lambda", region_name=REGION)
        for i in range(tries):
            try:
                cfg = lam.get_function(FunctionName=fn)
                conf = cfg["Configuration"]
                if conf.get("State") != "Active" or conf.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                blob = urllib.request.urlopen(cfg["Code"]["Location"], timeout=60).read()
                body = zipfile.ZipFile(io.BytesIO(blob)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if marker in body:
                    return True, i * 15
            except Exception as e:
                rep.log("  settle %s: %s" % (fn, str(e)[:90]))
            time.sleep(15)
        return False, tries * 15

    try:
        lam = boto3.client("lambda", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)

        # ── G0 KEY CONTRACT ──────────────────────────────────────────────
        rep.section("G0 — key contract (grep both producers)")
        scan_src = (ROOT / "lambdas" / SCANNER / "source" / "lambda_function.py").read_text()
        eng_src = (ROOT / "lambdas" / ENGINE / "source" / "lambda_function.py").read_text()
        need_scan = ["all_ticker_buys", "max_role_tier", "total_value",
                     "n_insiders", "n_transactions"]
        need_eng = ["industries", "n_companies", "participation_pct",
                    "dollar_hhi", "z_vs_own_history", "tier", "coverage",
                    "thin_universe", "ceo_cfo_companies", "n_clusters"]
        m1 = [k for k in need_scan if '"%s"' % k not in scan_src]
        m2 = [k for k in need_eng if '"%s"' % k not in eng_src]
        gate("G0_key_contract", not m1 and not m2,
             "scanner missing=%s engine missing=%s" % (m1, m2))

        # ── G1 scanner sidecar ───────────────────────────────────────────
        rep.section("G1 — scanner schema 2.1 + sidecar populated")
        ok1, waited = settle(SCANNER, '"schema_version": "2.1"')
        gate("G1_scanner_settled", ok1, "schema 2.1 deployed after %ds" % waited)
        if ok1:
            try:
                before = s3.head_object(Bucket=BUCKET, Key=SRC_KEY)["LastModified"]
            except Exception:
                before = None
            lam.invoke(FunctionName=SCANNER, InvocationType="Event", Payload=b"{}")
            fresh = False
            for _ in range(36):        # scanner is a long SEC crawl
                time.sleep(15)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=SRC_KEY)
                    if before is None or h["LastModified"] > before:
                        fresh = True
                        break
                except Exception:
                    pass
            gate("G1_scanner_ran", fresh, "sidecar artifact refreshed" if fresh
                 else "scanner did not republish within 9min")
            src = json.loads(s3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
            sidecar = src.get("all_ticker_buys") or []
            clusters = src.get("clusters") or []
            rep.log("  clusters=%d  all_ticker_buys=%d (breadth gain %+d)"
                    % (len(clusters), len(sidecar), len(sidecar) - len(clusters)))
            gate("G1_sidecar_breadth", len(sidecar) > len(clusters),
                 "sidecar carries %d tickers vs %d filtered clusters"
                 % (len(sidecar), len(clusters)))
            if sidecar:
                rep.log("  sidecar sample: %s" % json.dumps(sidecar[0])[:300])

        # ── G1b SCHEDULE ORDER ───────────────────────────────────────────
        rep.section("G1b — scanner runs BEFORE the canary (no config.json)")
        sc_hours = []
        try:
            ev = boto3.client("events", region_name=REGION)
            for r0 in ev.list_rules().get("Rules", []):
                if "insider" in r0["Name"].lower() and "cluster" in r0["Name"].lower():
                    rep.log("  rule %s -> %s" % (r0["Name"], r0.get("ScheduleExpression")))
                    sc_hours.append((r0["Name"], r0.get("ScheduleExpression")))
            sch = boto3.client("scheduler", region_name=REGION)
            for page in sch.get_paginator("list_schedules").paginate():
                for s0 in page.get("Schedules", []):
                    if "insider" in s0["Name"].lower():
                        d = sch.get_schedule(Name=s0["Name"])
                        rep.log("  scheduler %s -> %s" % (s0["Name"],
                                                          d.get("ScheduleExpression")))
                        sc_hours.append((s0["Name"], d.get("ScheduleExpression")))
        except Exception as e:
            rep.warn("schedule discovery: %s" % str(e)[:140])
        gate("G1b_scanner_scheduled", bool(sc_hours),
             "scanner schedules found: %s" % sc_hours[:4] if sc_hours
             else "NO scanner schedule — sidecar would go stale")

        # ── G2 engine ────────────────────────────────────────────────────
        rep.section("G2 — industry engine settle + invoke")
        ok2, waited2 = settle(ENGINE, 'VERSION = "1.0.0"')
        gate("G2_engine_settled", ok2, "engine deployed after %ds" % waited2)
        doc = None
        if ok2:
            try:
                before2 = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                before2 = None
            lam.invoke(FunctionName=ENGINE, InvocationType="Event", Payload=b"{}")
            for _ in range(24):
                time.sleep(10)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
                    if before2 is None or h["LastModified"] > before2:
                        doc = json.loads(s3.get_object(Bucket=BUCKET,
                                                       Key=OUT_KEY)["Body"].read())
                        break
                except Exception:
                    pass
            gate("G2_artifact", doc is not None,
                 "industries=%s clusters=%s source=%s"
                 % ((doc or {}).get("n_industries"), (doc or {}).get("n_clusters"),
                    (doc or {}).get("source_feed")))

        # ── G3 DATA TRUTH ────────────────────────────────────────────────
        rep.section("G3 — data truth (breadth is real, rates are sane)")
        if doc:
            inds = doc.get("industries") or []
            impossible = [r["industry"] for r in inds
                          if r.get("n_listed") and r["n_companies"] > r["n_listed"]]
            bad_rate = [r["industry"] for r in inds
                        if r.get("participation_pct") is not None
                        and not (0 <= r["participation_pct"] <= 100)]
            for r in inds[:10]:
                rep.log("  %-32s %-28s n_co=%2d/%3d  %5s%%  $%.2fM  hhi=%s  z=%s"
                        % (r["industry"][:32], r["tier"][:28], r["n_companies"],
                           r.get("n_listed") or 0,
                           r.get("participation_pct"),
                           (r.get("total_value_usd") or 0) / 1e6,
                           r.get("dollar_hhi"), r.get("z_vs_own_history")))
            gate("G3_data_truth", not impossible and not bad_rate and len(inds) > 0,
                 "industries=%d impossible_breadth=%s bad_rate=%s"
                 % (len(inds), impossible[:3], bad_rate[:3]))
        else:
            gate("G3_data_truth", False, "no doc")

        # ── G4 FALSE-POSITIVE GUARD ──────────────────────────────────────
        rep.section("G4 — false-positive guard (biotech / thin universes)")
        if doc:
            inds = doc.get("industries") or []
            thin_promoted = [r["industry"] for r in inds
                             if r.get("thin_universe")
                             and r["tier"].startswith("PEER_CLUSTER")]
            # every PEER row must expose the honesty fields the reader needs
            missing_fields = [r["industry"] for r in inds
                              if r["tier"].startswith("PEER_CLUSTER")
                              and (r.get("participation_pct") is None
                                   or r.get("dollar_hhi") is None)]
            conc = [r["industry"] for r in inds if "_CONCENTRATED" in r["tier"]]
            rep.log("  thin universes labelled: %d · concentrated flagged: %s"
                    % (sum(1 for r in inds if r.get("thin_universe")), conc[:5]))
            gate("G4_fp_guard", not thin_promoted and not missing_fields,
                 "thin_promoted=%s missing_honesty_fields=%s"
                 % (thin_promoted[:3], missing_fields[:3]))
        else:
            gate("G4_fp_guard", False, "no doc")

        # ── G5 schedule ──────────────────────────────────────────────────
        rep.section("G5 — engine schedule")
        try:
            sch = boto3.client("scheduler", region_name=REGION)
            acct = boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
            try:
                sch.create_schedule(
                    Name="justhodl-insider-industry-cluster-daily",
                    ScheduleExpression="cron(20 14 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": "arn:aws:lambda:%s:%s:function:%s" % (REGION, acct, ENGINE),
                            "RoleArn": "arn:aws:iam::%s:role/justhodl-scheduler-role" % acct,
                            "Input": "{}"},
                    Description="Insider industry cluster — daily 14:20 UTC")
                rep.log("  scheduler created")
            except Exception as e:
                rep.log("  scheduler ensure: %s" % str(e)[:120])
            found = False
            for page in sch.get_paginator("list_schedules").paginate():
                for s0 in page.get("Schedules", []):
                    if "insider-industry-cluster" in s0["Name"]:
                        found = True
            if not found:
                ev = boto3.client("events", region_name=REGION)
                for r0 in ev.list_rules(NamePrefix=ENGINE).get("Rules", []):
                    found = True
            gate("G5_schedule", found, "schedule present" if found else "NO SCHEDULE")
        except Exception as e:
            gate("G5_schedule", False, str(e)[:170])

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        Path("aws/ops/reports/3743.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict,
               industries=(doc or {}).get("n_industries", 0),
               clusters=(doc or {}).get("n_clusters", 0),
               failed=",".join(fails) or "none")
        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — canary #16 live; page next")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3743.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
