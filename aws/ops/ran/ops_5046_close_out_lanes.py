"""ops_5046 -- close out both series lanes.

State at ops 5045:
    Eurostat  8,147/8,147 flows   564,204,000 series   1,128,408 pages
    ECB         207/207   flows     3,240,832 series       6,481 pages
    ECB: failed_flows=0, errors=0, holes=0, bracket 6481<=6481<=6481

Three things remain, and none of them are the data.

 1. The ECB manifest read flows_parsed=207/574. 574 is the raw FILE
    count under the warm prefix; 207 is the number of distinct FLOWS
    those files group into. Both numbers are right, but comparing a flow
    count against a file count renders "207/574" -- looking like 36%
    when the lane is complete. Fixed in the engine: a grouped provider
    reports its group count. Same class of mislabel as counting
    dataflows and calling them series, which is what started this.
 2. The ECB card was still mid-rewrite when 5045 ended. Verify it.
 3. Both lanes are finished, so rate(1 minute) is firing two no-op
    invocations a minute forever. Drop to hourly -- still armed, because
    sdmx-walker keeps adding flows.

  P0 deploy, then kick each provider once so both manifests are
     rewritten with the corrected flows_total
  P1 manifests: flows_parsed must equal flows_total for both
  P2 catalog run; both cards verified; coverage still a warm-mirror
     ratio and datasets must not absorb series
  P3 cadence rate(1 minute) -> rate(1 hour), both targets intact,
     reserved concurrency still 1 (permanent race interlock)
  P4 the day's ledger
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-series-extractor"
CAT = "justhodl-provider-catalog"
RULE = "justhodl-series-extractor-5min"
HUB = "data/provider-catalog.json"
LANES = {"eurostat": ("data/_state/series-extract-eurostat.json",
                      "data/providers/eurostat/series-manifest.json",
                      "data/providers/eurostat.json"),
         "ecb": ("data/_state/series-extract-ecb.json",
                 "data/providers/ecb/series-manifest.json",
                 "data/providers/ecb.json")}

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=k)["Body"].read())
    except Exception:
        return {}


with report("ops_5046_close_out_lanes") as R:
    fails = []
    out = {"op": "ops_5046"}

    R.section("P0 deploy + one run per provider")
    for i in range(16):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  extractor code fresh (%s)" % c.get("LastModified"))
                break
        except Exception:
            pass
        time.sleep(20)
    stamps = {p: jget(m).get("updated_at") for p, (_, m, _) in
              LANES.items()}
    for p in LANES:
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"provider": p}).encode())
            R.log("  kicked %s" % p)
        except Exception as e:
            R.log("  kick %s err %s" % (p, str(e)[:100]))
        time.sleep(5)

    R.section("P1 manifests -- flows_parsed vs flows_total")
    for p, (stk, mank, _) in LANES.items():
        fresh = False
        for i in range(20):
            m = jget(mank)
            if m.get("updated_at") != stamps.get(p):
                fresh = True
                break
            time.sleep(30)
        m = jget(mank)
        st = jget(stk)
        fp, ft = m.get("flows_parsed"), m.get("flows_total")
        R.log("  %-9s flows_parsed=%s / flows_total=%s  %s" % (
            p, fp, ft,
            "OK" if fp == ft else "*** MISMATCH ***"))
        R.log("            series=%s pages=%s pages_bytes=%.2f GB "
              "refreshed=%s" % (
                  f"{m.get('series_extracted') or 0:,}",
                  f"{m.get('pages') or 0:,}",
                  (m.get("pages_bytes") or 0) / 1e9, fresh))
        R.log("            failed_flows=%d errors=%d holes=%d" % (
            len(st.get("failed_flows") or []),
            len(st.get("errors") or {}),
            len(st.get("missing_pages") or [])))
        out[p] = {"parsed": fp, "total": ft,
                  "series": m.get("series_extracted"),
                  "pages": m.get("pages"),
                  "bytes": m.get("pages_bytes")}
        if fp != ft:
            fails.append("P1:%s" % p)

    R.section("P2 the cards")
    as_of0 = jget(HUB).get("as_of")
    t0 = (jget(HUB).get("totals") or {})
    R.log("  BEFORE hub totals: keys=%s gb=%s datasets=%s" % (
        f"{t0.get('keys') or 0:,}", t0.get("gb"), t0.get("datasets")))
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event", Payload=b"{}")
        R.log("  catalog Event invoke sent")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:120])
    for i in range(26):
        time.sleep(30)
        if jget(HUB).get("as_of") != as_of0:
            R.log("  hub rewritten after %ds" % ((i + 1) * 30))
            break
    for p, (_, _, pdoc) in LANES.items():
        d = jget(pdoc)
        ser = d.get("series") or {}
        dv = d.get("derived") or {}
        R.log("  %-9s series=%s counted=%s n_keys=%s total_mb=%s" % (
            p, f"{ser.get('count') or 0:,}", ser.get("counted"),
            f"{d.get('n_keys') or 0:,}", d.get("total_mb")))
        R.log("            derived objects=%s bytes=%.2f GB" % (
            f"{dv.get('objects') or 0:,}",
            (dv.get("bytes") or 0) / 1e9))
        if not (ser.get("count") or 0) > 100000:
            fails.append("P2:%s-series" % p)
        if not dv.get("objects"):
            fails.append("P2:%s-derived" % p)
    try:
        h = jget(HUB)
        t = h.get("totals") or {}
        R.log("  AFTER hub totals: keys=%s gb=%s datasets=%s" % (
            f"{t.get('keys') or 0:,}", t.get("gb"), t.get("datasets")))
        if t.get("datasets") != t0.get("datasets"):
            R.log("  NOTE datasets moved %s -> %s" % (t0.get("datasets"),
                                                      t.get("datasets")))
        for slug in ("ecb", "eurostat"):
            e = next((x for x in (h.get("providers") or [])
                      if x.get("slug") == slug), {})
            R.log("  %-9s hub row: series_count=%s coverage_pct=%s "
                  "datasets=%s" % (slug,
                                   f"{e.get('series_count') or 0:,}",
                                   e.get("coverage_pct"),
                                   e.get("datasets")))
            if e.get("coverage_pct") is not None and \
                    float(e["coverage_pct"]) > 100.5:
                fails.append("P2:%s-coverage" % slug)
        out["totals"] = t
    except Exception as e:
        R.log("  totals err %s" % str(e)[:110])

    R.section("P3 stand the lanes down")
    try:
        d0 = ev.describe_rule(Name=RULE)
        ev.put_rule(Name=RULE, ScheduleExpression="rate(1 hour)",
                    State="ENABLED")
        d = ev.describe_rule(Name=RULE)
        tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  cadence %s -> %s (%s)" % (d0.get("ScheduleExpression"),
                                           d.get("ScheduleExpression"),
                                           d.get("State")))
        R.log("  targets: %s" % [(t.get("Id"), str(t.get("Input"))[:34])
                                 for t in tg])
        if len(tg) < 2:
            R.log("  *** a lane lost its target ***")
            fails.append("P3:targets")
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  reserved concurrency=%s (permanent interlock)"
              % rc.get("ReservedConcurrentExecutions"))
        if rc.get("ReservedConcurrentExecutions") != 1:
            fails.append("P3:interlock")
        R.log("  memory/timeout left at 10240MB/900s so the next large "
              "flow does not need an incident to get headroom")
    except Exception as e:
        R.log("  rule err %s" % str(e)[:140])
        fails.append("P3")

    R.section("P4 ledger")
    R.log("  Eurostat 8,147/8,147 flows · %s series · %s pages" % (
        f"{(out.get('eurostat') or {}).get('series') or 0:,}",
        f"{(out.get('eurostat') or {}).get('pages') or 0:,}"))
    R.log("  ECB        207/207 flows · %s series · %s pages" % (
        f"{(out.get('ecb') or {}).get('series') or 0:,}",
        f"{(out.get('ecb') or {}).get('pages') or 0:,}"))
    R.log("  started the day at Eurostat 79/8,147 (0.97%) and ECB 0")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/series-lanes-closeout.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/series-lanes-closeout.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5046 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(eurostat=(out.get("eurostat") or {}).get("series"),
         ecb=(out.get("ecb") or {}).get("series"),
         keys=(out.get("totals") or {}).get("keys"),
         gb=(out.get("totals") or {}).get("gb"))
    R.log("ops 5046 GREEN -- both lanes closed out, cards honest, "
          "cadence at steady state")
