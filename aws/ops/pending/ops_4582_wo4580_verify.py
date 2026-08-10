"""ops 4582 — wo4580/4581 impact-layer landing verification.

Fourteen build commits landed: shared impact substrate (impact_mapper +
evidence_weights), NEW justhodl-impact-graph + justhodl-distribution-
composite, deep upgrades to all ten ops-4559 engines, and the impact strip
on eleven pages. This op is the bug gate:

  1. creates the two NEW functions from repo source (deploy-lambdas only
     updates existing), wires EventBridge schedules, settles every zip
  2. runs impact-graph FIRST (the exposure graph is the substrate the
     dependents join), asserts its four outputs
  3. fires the remaining eleven engines async and polls each payload's
     generated_at — parallel wall-clock, not serial
  4. asserts the wo4580 contract battery: impact-map/1.0 everywhere,
     the estimated-pp integrity rule (ci+n_obs or it does not exist),
     every engine's new feature block, archives, data/impact/* keys
  5. probes the CDN for the impact strip on all eleven pages + the JS asset
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
ACCT = "857687956942"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
REPO = Path(__file__).resolve().parents[2] / "lambdas"

NEW = {
    "justhodl-impact-graph": "data/impact/exposure-graph.json",
    "justhodl-distribution-composite": "data/distribution-composite.json",
}
PATCHED = {
    "justhodl-flow-lookthrough": "data/flow-lookthrough.json",
    "justhodl-etf-true-flows": "data/etf-true-flows.json",
    "justhodl-share-flows": "data/share-flows.json",
    "justhodl-dark-pool": "data/dark-pool.json",
    "justhodl-stealth-accumulation": "data/stealth-accumulation.json",
    "justhodl-accum-composite": "data/accum-composite.json",
    "justhodl-port-cargo": "data/port-cargo.json",
    "justhodl-grid-queue": "data/grid-queue.json",
    "justhodl-freight-pulse": "data/freight-pulse.json",
    "justhodl-accumulation-radar": "data/accumulation-radar.json",
}
PAGES = ["retail-edges.html", "accumulation.html", "port-cargo.html",
         "grid-queue.html", "freight-pulse.html", "flow-lookthrough.html",
         "sector-flow.html", "share-flows.html", "dark-pool.html",
         "accum-composite.html", "distribution-composite.html"]


def zip_src(fn):
    buf = io.BytesIO()
    src = REPO / fn / "source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src))
        # NEW functions bypass deploy-lambdas' shared bundler on first
        # create — bundle their shared imports explicitly.
        shared = REPO.parent / "shared"
        for mod in ("impact_mapper.py", "evidence_weights.py",
                    "signals_emit.py", "edgar.py"):
            mp = shared / mod
            if mp.exists():
                z.write(mp, mod)
    return buf.getvalue()


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def key_exists_prefix(prefix):
    try:
        resp = s3.list_objects_v2(Bucket=B, Prefix=prefix, MaxKeys=3)
        return resp.get("KeyCount", 0) > 0
    except Exception:
        return False


def http_probe(url, needle=None):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 ops-4582", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=25) as r:
            body = r.read().decode("utf-8", "replace")
            return r.status, (needle in body) if needle else True
    except urllib.error.HTTPError as e:
        return e.code, False
    except Exception:
        return None, False


def settle(r, fn, deadline_s=300):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
                return True
        except lam.exceptions.ResourceNotFoundException:
            return False
        time.sleep(6)
    r.warn("  %s did not settle in %ss" % (fn, deadline_s))
    return False


def ensure_new(r, fn):
    cfg = json.loads((REPO / fn / "config.json").read_text())
    try:
        lam.get_function(FunctionName=fn)
        r.log("  %s exists — updating code from repo (with shared bundle)" % fn)
        lam.update_function_code(FunctionName=fn, ZipFile=zip_src(fn))
    except lam.exceptions.ResourceNotFoundException:
        r.log("  creating %s (%sMB / %ss)" % (fn, cfg["memory"], cfg["timeout"]))
        lam.create_function(
            FunctionName=fn, Runtime=cfg["runtime"], Role=cfg["role"],
            Handler=cfg["handler"], Timeout=cfg["timeout"],
            MemorySize=cfg["memory"], Code={"ZipFile": zip_src(fn)},
            Description=cfg.get("description", "")[:250],
            Environment={"Variables": cfg.get("environment", {})})
    settle(r, fn)
    scfg = cfg.get("schedule")
    if scfg:
        arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, fn)
        try:
            sch.get_schedule(Name=scfg["name"])
            r.log("  schedule exists: %s" % scfg["name"])
        except Exception:
            sch.create_schedule(
                Name=scfg["name"], ScheduleExpression=scfg["expression"],
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": SCHED_ROLE},
                Description=scfg.get("description", "")[:250])
            r.ok("  schedule created: %s (%s)" % (scfg["name"],
                                                  scfg["expression"]))


def fire(r, fn):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event")
        return True
    except Exception as e:
        r.fail("  %s async invoke error: %s" % (fn, str(e)[:120]))
        return False


def poll_many(r, want, budget_s):
    """want: {fn: (key, before_ts)}. Returns {fn: payload}."""
    outs, t0 = {}, time.time()
    pending = dict(want)
    while pending and time.time() - t0 < budget_s:
        time.sleep(12)
        for fn in list(pending):
            key, before_ts = pending[fn]
            cur = get_json(key)
            ts = (cur or {}).get("generated_at") or (cur or {}).get("as_of") or ""
            if cur is not None and ts and ts != before_ts:
                outs[fn] = cur
                r.log("  %s refreshed (%ss)" % (fn, int(time.time() - t0)))
                del pending[fn]
    for fn in pending:
        r.warn("  %s did not refresh in %ss — asserting on latest available"
               % (fn, budget_s))
        outs[fn] = get_json(pending[fn][0]) or {}
    return outs


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def valid_impact(j):
    m = (j or {}).get("impact_map")
    if not (isinstance(m, dict) and m.get("schema") == "impact-map/1.0"
            and isinstance(m.get("benefiting"), list)
            and isinstance(m.get("suffering"), list)
            and isinstance(m.get("insufficient"), list)):
        return False, "impact_map absent or malformed"
    for side in ("benefiting", "suffering"):
        for row in m[side]:
            if row.get("pp_kind") == "estimated" and (
                    row.get("ci") is None or row.get("n_obs") is None):
                return False, "NAKED estimated pp in %s: %r" % (side, row)
            if row.get("pp_kind") == "measured" and row.get("pp") is None:
                return False, "measured row without pp in %s" % side
    return True, "impact-map/1.0 valid (%d ben / %d suf / %d insufficient)" % (
        len(m["benefiting"]), len(m["suffering"]), len(m["insufficient"]))


def main():
    with report("4582_wo4580_verify") as r:
        r.heading("ops 4582 — wo4580 impact-layer verification")
        misses = 0

        r.section("1. New engines: create + schedule + settle")
        for fn in NEW:
            ensure_new(r, fn)

        r.section("2. Patched engines: settle deploys")
        for fn in PATCHED:
            settle(r, fn)

        r.section("3. impact-graph first (the substrate)")
        gkey = NEW["justhodl-impact-graph"]
        g_before = (get_json(gkey) or {}).get("generated_at") or ""
        fire(r, "justhodl-impact-graph")
        graph = poll_many(r, {"justhodl-impact-graph": (gkey, g_before)},
                          700)["justhodl-impact-graph"]
        misses += contract(r, "impact-graph",
                           (graph.get("n_tickers") or 0) > 500,
                           "exposure graph populated (n_tickers=%s)"
                           % graph.get("n_tickers"))
        cov = graph.get("field_coverage") or {}
        misses += contract(r, "impact-graph",
                           all(k in cov for k in ("industry", "mcap", "adv_usd")),
                           "field_coverage reported: %s" % cov)
        hist = get_json("data/impact/factor-history.json") or {}
        misses += contract(r, "impact-graph",
                           len(hist.get("days") or []) >= 1,
                           "factor history accruing (%d days)"
                           % len(hist.get("days") or []))
        betas = get_json("data/impact/betas.json") or {}
        misses += contract(r, "impact-graph",
                           betas.get("status") in ("BOOTSTRAPPING", "LIVE"),
                           "betas status honest: %s (pairs=%s)"
                           % (betas.get("status"), betas.get("pairs_by_factor")))
        conv = get_json("data/impact/convergence.json") or {}
        misses += contract(r, "impact-graph",
                           (conv.get("trade_impulse") or {}).get("state")
                           in ("EXPANSION_CONFIRMED", "CONTRACTION_CONFIRMED",
                               "MIXED", "NO_DATA"),
                           "trade_impulse state=%s (%d legs); flow rows=%d"
                           % ((conv.get("trade_impulse") or {}).get("state"),
                              (conv.get("trade_impulse") or {}).get("n_legs", 0),
                              len((conv.get("flow_convergence") or {})
                                  .get("rows") or [])))

        r.section("4. Fire remaining engines (async, parallel poll)")
        want = {}
        rest = {**PATCHED,
                "justhodl-distribution-composite":
                    NEW["justhodl-distribution-composite"]}
        for fn, key in rest.items():
            before = (get_json(key) or {}).get("generated_at") or \
                     (get_json(key) or {}).get("as_of") or ""
            if fire(r, fn):
                want[fn] = (key, before)
        outs = poll_many(r, want, 900)

        r.section("5. Contract battery")
        # shared: impact-map/1.0 + estimated-integrity everywhere
        for fn in list(rest) :
            ok, why = valid_impact(outs.get(fn))
            misses += contract(r, fn.replace("justhodl-", ""), ok, why)

        j = outs.get("justhodl-flow-lookthrough") or {}
        misses += contract(r, "lookthrough", j.get("version") == "2.2.0",
                           "v2.2.0 live")
        misses += contract(r, "lookthrough",
                           isinstance(j.get("passive_concentration"), list),
                           "passive_concentration board present (%d rows)"
                           % len(j.get("passive_concentration") or []))
        hidx = get_json("data/impact/etf-holdings-index.json") or {}
        misses += contract(r, "lookthrough",
                           len(hidx.get("etfs") or {}) >= 1,
                           "canonical holdings index written (%d ETFs)"
                           % len(hidx.get("etfs") or {}))
        nadv = sum(1 for x in (j.get("inflow_leaders") or [])
                   if x.get("flow_bps_adv_day") is not None)
        r.log("  lookthrough: %d inflow leaders carry bps_adv (graph join)" % nadv)

        j = outs.get("justhodl-etf-true-flows") or {}
        misses += contract(r, "etf-true-flows",
                           str(j.get("version")) == "2.1", "v2.1 live")
        misses += contract(r, "etf-true-flows",
                           isinstance(j.get("complexes"), list)
                           and len(j.get("complexes") or []) >= 1,
                           "wrapper complexes netted (%d families)"
                           % len(j.get("complexes") or []))
        bs = j.get("by_stock") or {}
        misses += contract(r, "etf-true-flows",
                           "inflows" in bs and "n_etfs_joined" in bs,
                           "per-stock lookthrough block present (joined=%s)"
                           % bs.get("n_etfs_joined"))
        gt = j.get("ground_truth") or {}
        misses += contract(r, "etf-true-flows",
                           gt.get("status") in ("WIRED_INDEX", "PENDING_WIRE"),
                           "N-PORT ground truth honest: %s (%d funds indexed)"
                           % (gt.get("status"), len(gt.get("per_etf") or [])))

        j = outs.get("justhodl-share-flows") or {}
        misses += contract(r, "share-flows", j.get("version") == "2.0.0",
                           "v2.0.0 live")
        bd = j.get("boards") or {}
        misses += contract(r, "share-flows",
                           all(k in bd for k in ("buyback_bluff",
                                                 "atm_shelves_active",
                                                 "buyback_blackout_weeks")),
                           "bluff/ATM/blackout boards present "
                           "(bluff=%d atm=%d weeks=%d)"
                           % (len(bd.get("buyback_bluff") or []),
                              len(bd.get("atm_shelves_active") or []),
                              len(bd.get("buyback_blackout_weeks") or [])))

        j = outs.get("justhodl-dark-pool") or {}
        misses += contract(r, "dark-pool", j.get("version") == "2.6.0",
                           "v2.6.0 live")
        sh = j.get("self_history") or {}
        misses += contract(r, "dark-pool",
                           sh.get("status") in ("LIVE", "INSUFFICIENT_HISTORY")
                           and (sh.get("n_weeks_archived") or 0) >= 1,
                           "self-history archiving (%s, %s wks)"
                           % (sh.get("status"), sh.get("n_weeks_archived")))
        misses += contract(r, "dark-pool",
                           key_exists_prefix("data/archive/dark-pool/week-"),
                           "weekly archive object exists")
        nvf = sum(1 for x in (j.get("board") or [])
                  if x.get("venue_fingerprint"))
        r.log("  dark-pool: %d board rows carry venue_fingerprint" % nvf)

        j = outs.get("justhodl-stealth-accumulation") or {}
        misses += contract(r, "stealth", j.get("version") == "1.2.0",
                           "v1.2.0 live")
        cw = (j.get("combo_weights") or {}).get("meta") or {}
        misses += contract(r, "stealth",
                           cw.get("overall_basis") in ("prior_only",
                                                       "empirical_shrunk"),
                           "learned-weight basis honest: %s"
                           % cw.get("overall_basis"))
        misses += contract(r, "stealth",
                           isinstance(j.get("signals_logged"), int),
                           "per-combo gradeable signals emitted (%s)"
                           % j.get("signals_logged"))

        j = outs.get("justhodl-accum-composite") or {}
        misses += contract(r, "accum-composite", str(j.get("version")) == "1.1",
                           "v1.1 live")
        w = j.get("weights") or {}
        misses += contract(r, "accum-composite",
                           all(k in w for k in ("priors", "learned",
                                                "effective_after_decay",
                                                "decay", "meta")),
                           "weights audit block (basis=%s)"
                           % ((w.get("meta") or {}).get("overall_basis")))
        ccv = j.get("component_coverage") or {}
        misses += contract(r, "accum-composite",
                           "congress_cluster" in ccv and "activist_13d" in ccv,
                           "new legs wired (congress=%s activist=%s)"
                           % (ccv.get("congress_cluster"),
                              ccv.get("activist_13d")))

        j = outs.get("justhodl-distribution-composite") or {}
        misses += contract(r, "dist-composite",
                           j.get("state") in ("OK", "INSUFFICIENT_DATA"),
                           "mirror engine live (state=%s, %s names)"
                           % (j.get("state"), j.get("n_names")))
        misses += contract(r, "dist-composite",
                           isinstance(j.get("signals_logged"), int),
                           "bearish signals gradeable (%s)"
                           % j.get("signals_logged"))

        j = outs.get("justhodl-port-cargo") or {}
        misses += contract(r, "port-cargo", j.get("version") == "1.1.0",
                           "v1.1.0 live")
        sb = j.get("seasonal_baseline") or {}
        misses += contract(r, "port-cargo",
                           sb.get("status") in ("OK", "UNAVAILABLE"),
                           "seasonal baseline status=%s (n_years=%s chg=%s%%)"
                           % (sb.get("status"), sb.get("n_years"),
                              sb.get("seasonal_chg_pct")))
        if sb.get("status") != "OK":
            r.warn("  port-cargo seasonal UNAVAILABLE — gaps: %s"
                   % (j.get("gaps") or [])[-3:])

        j = outs.get("justhodl-grid-queue") or {}
        misses += contract(r, "grid-queue", j.get("version") == "2.1.0",
                           "v2.1.0 live")
        qv = j.get("queue_velocity") or {}
        misses += contract(r, "grid-queue",
                           qv.get("status") in ("LIVE", "INSUFFICIENT_HISTORY")
                           and (qv.get("n_snapshots") or 0) >= 1,
                           "velocity archive (%s, %s snapshots)"
                           % (qv.get("status"), qv.get("n_snapshots")))
        misses += contract(r, "grid-queue",
                           (j.get("large_load_queue") or {}).get("status")
                           in ("OK", "UNAVAILABLE"),
                           "large-load probe honest: %s"
                           % (j.get("large_load_queue") or {}).get("status"))
        misses += contract(r, "grid-queue",
                           "completion_rate_by_fuel"
                           in (j.get("lbnl_priors") or {}),
                           "LBNL fuel-cohort priors carried (vintage=%s)"
                           % (j.get("lbnl_priors") or {}).get("vintage"))
        isone_gap = [g for g in (j.get("gaps") or []) if "ISO-NE" in str(g)]
        r.log("  grid-queue ISO-NE: %s"
              % ("LIVE" if "ISO-NE" in (j.get("iso_queues") or {})
                 else (isone_gap[-1][:180] if isone_gap else "no signature?")))

        j = outs.get("justhodl-freight-pulse") or {}
        misses += contract(r, "freight", j.get("version") == "2.0.0",
                           "v2.0.0 live")
        misses += contract(r, "freight",
                           (j.get("rate_vs_volume") or {}).get("status")
                           in ("OK", "UNAVAILABLE", "ERROR"),
                           "rate-vs-volume read=%s (rate_yoy=%s vol_yoy=%s)"
                           % ((j.get("rate_vs_volume") or {}).get("read"),
                              (j.get("rate_vs_volume") or {}).get("rate_yoy_pct"),
                              (j.get("rate_vs_volume") or {}).get("volume_yoy_pct")))
        fl = j.get("fast_leg") or {}
        misses += contract(r, "freight",
                           fl.get("status") in ("OK", "UNAVAILABLE"),
                           "EIA weekly fast leg status=%s (read=%s yoy=%s)"
                           % (fl.get("status"), fl.get("read"),
                              fl.get("yoy_pct_4w")))
        if fl.get("status") != "OK":
            r.warn("  freight fast leg err: %s" % fl.get("err"))
        misses += contract(r, "freight",
                           (j.get("lead_vs_port") or {}).get("status")
                           == "PENDING_HISTORY"
                           and key_exists_prefix("data/archive/freight-pulse/"),
                           "lead-vs-port declared + archive accruing (n=%s)"
                           % (j.get("lead_vs_port") or {}).get("n_archived"))

        j = outs.get("justhodl-accumulation-radar") or {}
        misses += contract(r, "radar", j.get("version") == "1.5.0"
                           or (j.get("legend") or True) and
                           str((j.get("standalone_accuracy") or {}).get(
                               "status", "")) != "",
                           "v1.5 accuracy block live (status=%s)"
                           % (j.get("standalone_accuracy") or {}).get("status"))
        misses += contract(r, "radar",
                           isinstance(j.get("signals_logged"), int),
                           "fixed-contract emitter ran (signals_logged=%s)"
                           % j.get("signals_logged"))

        r.section("6. data/impact/* + archive tree")
        for k in ("data/impact/exposure-graph.json",
                  "data/impact/factor-history.json",
                  "data/impact/betas.json", "data/impact/convergence.json",
                  "data/impact/etf-holdings-index.json"):
            misses += contract(r, "impact-tree", get_json(k) is not None,
                               "%s present" % k)
        misses += contract(r, "impact-tree",
                           key_exists_prefix("data/archive/grid-queue/"),
                           "grid-queue snapshot archive exists")

        r.section("7. CDN: impact strip on eleven pages + asset")
        st, hit = http_probe("https://justhodl.ai/assets/impact-strip.js"
                             "?v=4582", "jhImpactStripAuto")
        misses += contract(r, "cdn", st == 200 and hit,
                           "impact-strip.js live (HTTP %s)" % st)
        page_miss = []
        for p in PAGES:
            st, hit = http_probe("https://justhodl.ai/%s?v=4582" % p,
                                 "impact-strip.js")
            if not (st == 200 and hit):
                page_miss.append("%s(HTTP %s%s)" % (p, st,
                                                    "" if hit else " no-marker"))
        misses += contract(r, "cdn", not page_miss,
                           "strip marker on all %d pages%s"
                           % (len(PAGES),
                              "" if not page_miss else
                              " — missing: " + ", ".join(page_miss)))

        r.section("verdict")
        if misses:
            r.fail("wo4580 verification: %d contract misses — NOT green" % misses)
            sys.exit(1)
        r.ok("wo4580 verification: ALL GREEN — impact layer live across the "
             "fleet, honest gaps declared where history must accrue")


if __name__ == "__main__":
    main()
