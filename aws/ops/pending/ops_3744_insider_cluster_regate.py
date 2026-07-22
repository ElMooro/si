#!/usr/bin/env python3
"""ops 3744 — diagnose the scanner sidecar, then regate canary #16 v1.1.

3743 outcome: engine/guards/schedule all PASSED, but the scanner did not
republish inside 9 minutes so `all_ticker_buys` stayed empty and the canary
fell back to the 19-row filtered feed — which produced exactly the false
positive the design anticipated: Biotechnology, 6 buyers of 590 listed
(1.02% participation), tiered CONFIRMED.

TWO FIXES SHIPPED HERE
  1. LADDER (real logic defect): z=None was treated as PERMISSION to confirm.
     With no base rate the engine must be MORE conservative. v1.1 adds a
     participation FLOOR (4%) and requires z>=1.0 to reach CONFIRMED; broad
     but shallow industries now land in DIFFUSE.
  2. SCANNER: diagnose rather than just wait longer — read its timeout,
     last CloudWatch outcome, and whether the invoke errors, before deciding
     if 9 minutes was simply too short or the run is failing.

GATES
  G1  scanner config forensics (timeout/memory) + last CW log outcome
  G2  scanner invoked, sidecar populated (longer, evidence-based budget)
  G3  engine v1.1 settled + rerun
  G4  LADDER PROOF — a sub-floor industry must NOT be CONFIRMED; biotech is
      the specific regression test
  G5  honesty fields present on every surviving PEER row
"""
import io
import json
import sys
import time
import traceback
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
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

with report("3744_insider_cluster_regate") as rep:
    rep.heading("ops 3744 — scanner forensics + canary #16 v1.1 ladder fix")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3744.json").write_text(json.dumps({"verdict": "STARTED"}))

    def gate(n, ok, detail):
        out["gates"][n] = {"ok": bool(ok), "detail": str(detail)[:900]}
        rep.log(("PASS " if ok else "FAIL ") + n + " — " + str(detail)[:800])
        if not ok:
            fails.append(n)
        return ok

    try:
        lam = boto3.client("lambda", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)
        logs = boto3.client("logs", region_name=REGION)

        # ── G1 scanner forensics ─────────────────────────────────────────
        rep.section("G1 — scanner config + last run outcome")
        conf = lam.get_function_configuration(FunctionName=SCANNER)
        rep.log("  timeout=%ss memory=%sMB state=%s lastUpdate=%s"
                % (conf.get("Timeout"), conf.get("MemorySize"),
                   conf.get("State"), conf.get("LastUpdateStatus")))
        env = (conf.get("Environment") or {}).get("Variables") or {}
        rep.log("  env keys: %s" % sorted(env.keys()))
        try:
            lg = "/aws/lambda/%s" % SCANNER
            streams = logs.describe_log_streams(
                logGroupName=lg, orderBy="LastEventTime", descending=True,
                limit=2).get("logStreams", [])
            for st in streams[:1]:
                ev = logs.get_log_events(
                    logGroupName=lg, logStreamName=st["logStreamName"],
                    limit=40, startFromHead=False).get("events", [])
                for e in ev[-18:]:
                    msg = e["message"].strip()[:200]
                    if msg:
                        rep.log("    LOG %s" % msg)
        except Exception as e:
            rep.warn("  CW logs: %s" % str(e)[:140])
        gate("G1_scanner_config", (conf.get("Timeout") or 0) >= 300,
             "timeout %ss (a 7-day SEC crawl needs headroom)" % conf.get("Timeout"))

        # ── G2 scanner run with an evidence-based budget ─────────────────
        rep.section("G2 — invoke scanner, wait its full timeout + margin")
        try:
            before = s3.head_object(Bucket=BUCKET, Key=SRC_KEY)["LastModified"]
        except Exception:
            before = None
        r = lam.invoke(FunctionName=SCANNER, InvocationType="Event", Payload=b"{}")
        rep.log("  async accepted status=%s" % r.get("StatusCode"))
        budget = int(conf.get("Timeout") or 600) + 180
        rep.log("  waiting up to %ss" % budget)
        sidecar, clusters, fresh = [], [], False
        waited = 0
        while waited < budget:
            time.sleep(20)
            waited += 20
            try:
                h = s3.head_object(Bucket=BUCKET, Key=SRC_KEY)
                if before is None or h["LastModified"] > before:
                    fresh = True
                    break
            except Exception:
                pass
        src = json.loads(s3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
        sidecar = src.get("all_ticker_buys") or []
        clusters = src.get("clusters") or []
        rep.log("  schema=%s clusters=%d all_ticker_buys=%d (waited %ss, fresh=%s)"
                % (src.get("schema_version"), len(clusters), len(sidecar),
                   waited, fresh))
        stats = src.get("stats") or {}
        rep.log("  scanner stats: %s" % json.dumps(stats)[:300])
        gate("G2_sidecar", len(sidecar) > len(clusters),
             "sidecar=%d vs clusters=%d" % (len(sidecar), len(clusters)))

        # ── G3 engine v1.1 ───────────────────────────────────────────────
        rep.section("G3 — engine v1.1 settle + rerun")
        settled = False
        for i in range(24):
            try:
                cfg = lam.get_function(FunctionName=ENGINE)
                c2 = cfg["Configuration"]
                if c2.get("State") != "Active" or c2.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                blob = urllib.request.urlopen(cfg["Code"]["Location"], timeout=60).read()
                body = zipfile.ZipFile(io.BytesIO(blob)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if 'VERSION = "1.1.0"' in body:
                    settled = True
                    break
            except Exception as e:
                rep.log("  settle: %s" % str(e)[:90])
            time.sleep(15)
        gate("G3_engine_settled", settled, "v1.1.0 deployed")
        doc = None
        if settled:
            try:
                b2 = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                b2 = None
            lam.invoke(FunctionName=ENGINE, InvocationType="Event", Payload=b"{}")
            for _ in range(24):
                time.sleep(10)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
                    if b2 is None or h["LastModified"] > b2:
                        doc = json.loads(s3.get_object(
                            Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
                        break
                except Exception:
                    pass
            gate("G3_artifact", doc is not None,
                 "industries=%s clusters=%s diffuse=%s source=%s"
                 % ((doc or {}).get("n_industries"), (doc or {}).get("n_clusters"),
                    (doc or {}).get("n_diffuse"), (doc or {}).get("source_feed")))

        # ── G4 LADDER PROOF ──────────────────────────────────────────────
        rep.section("G4 — ladder proof (sub-floor industries cannot CONFIRM)")
        if doc:
            inds = doc.get("industries") or []
            floor = (doc.get("coverage") or {}).get("min_participation_pct")
            rep.log("  participation floor = %s%%" % floor)
            for r0 in inds[:12]:
                rep.log("  %-30s %-26s n_co=%2d/%4d  part=%5s%%  z=%-5s  awaiting_br=%s"
                        % (r0["industry"][:30], r0["tier"][:26], r0["n_companies"],
                           r0.get("n_listed") or 0, r0.get("participation_pct"),
                           r0.get("z_vs_own_history"), r0.get("awaiting_base_rate")))
            violations = [
                r0["industry"] for r0 in inds
                if r0["tier"].startswith("PEER_CLUSTER")
                and (r0.get("participation_pct") is None
                     or r0["participation_pct"] < (floor or 0))
            ]
            # the specific regression: no CONFIRMED without a base rate
            confirmed_no_z = [r0["industry"] for r0 in inds
                              if "CONFIRMED" in r0["tier"]
                              and r0.get("z_vs_own_history") is None]
            gate("G4_ladder", not violations and not confirmed_no_z,
                 "sub_floor_promoted=%s confirmed_without_base_rate=%s"
                 % (violations[:3], confirmed_no_z[:3]))
        else:
            gate("G4_ladder", False, "no doc")

        # ── G5 honesty fields ────────────────────────────────────────────
        rep.section("G5 — honesty fields on every PEER row")
        if doc:
            inds = doc.get("industries") or []
            peers = [r0 for r0 in inds if r0["tier"].startswith("PEER_CLUSTER")]
            missing = [r0["industry"] for r0 in peers
                       if r0.get("participation_pct") is None
                       or r0.get("dollar_hhi") is None
                       or r0.get("n_listed") in (None, 0)]
            rep.log("  peer rows=%d diffuse=%d emerging=%d"
                    % (len(peers),
                       sum(1 for r0 in inds if r0["tier"].startswith("DIFFUSE")),
                       sum(1 for r0 in inds if r0["tier"] == "EMERGING")))
            gate("G5_honesty", not missing, "missing honesty fields: %s" % missing[:4])
        else:
            gate("G5_honesty", False, "no doc")

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        Path("aws/ops/reports/3744.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict,
               sidecar=len(sidecar), clusters=len(clusters),
               industries=(doc or {}).get("n_industries", 0),
               peer_clusters=(doc or {}).get("n_clusters", 0),
               failed=",".join(fails) or "none")
        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — canary #16 v1.1 honest; page next")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3744.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
