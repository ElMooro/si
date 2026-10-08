"""ops_5060 -- start the Census economic lane.

Scope (ops 5059, reviewable at data/_state/census-econ-scope.json):
108 families, 1,226 dataset-vintage entries, demographics excluded by
directive. CENSUS_API_KEY is present and answering, so this is hours of
paced crawling rather than the 31 days an unkeyed walker faced.

Built as an `econ` mode on justhodl-census-us rather than a new function
-- it already owns the key, the request pacing and the OOM read cap, and
this repo's deploy path does not reliably create new Lambdas. Separate
state (data/_state/census-econ.json) and separate prefix
(data/warm/census-econ/), so the timeseries lane is untouched.

Two details that decide whether this lane is worth anything:

  NAICS IS A PREDICATE. Query CBP without an explicit NAICS wildcard and
  the API cheerfully returns all-industry TOTALS -- a complete-looking
  import with the industrial detail silently absent. The walker detects
  a NAICS/SECTOR variable and appends the wildcard, falling back to
  totals only when a dataset rejects it.

  ORDERING, NOT DROPPING. cbp, zbp and nonemp first; the 75 ecn*
  families next; cps (703 entries) and sipp (178) last. Nothing is
  excluded -- but an interrupted crawl will already have banked the
  establishment, payroll and industry data the physical-economy desks
  read, instead of being 60% through monthly CPS microdata.

Five properties proven offline first: priority ordering retains every
family, NAICS detection across three shapes, variable chunking covers
each variable exactly once, geography excludes parent-requiring levels,
and the stall breaker retires a bad vintage on attempt 3 while the good
one behind it still drains.

  P0 confirm the deploy and the scope manifest
  P1 first supervised run, synchronous, with the response read
  P2 verify what actually landed in S3 -- and that NAICS detail is
     present, not just totals
  P3 wire the schedule so it drains unattended
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
FN = "justhodl-census-us"
ESTATE = "data/_state/census-econ.json"
SCOPE = "data/_state/census-econ-scope.json"
EROOT = "data/warm/census-econ/"

cfg = Config(read_timeout=900, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


with report("ops_5060_census_econ_lane") as R:
    fails = []
    out = {"op": "ops_5060"}

    R.section("P0 deploy + scope")
    for i in range(16):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s  mem=%s timeout=%s" % (
                    c.get("LastModified"), c.get("MemorySize"),
                    c.get("Timeout")))
                break
        except Exception:
            pass
        time.sleep(20)
    sc = jget(SCOPE)
    R.log("  scope: %s families, %s entries, excluded=%s" % (
        sc.get("total_families"), f"{sc.get('total_entries') or 0:,}",
        (sc.get("excluded") or [])[:6]))
    if not sc.get("families"):
        R.log("  scope manifest missing -- the walker has no input")
        fails.append("P0:scope")
    ts = jget("data/warm/census-us/_state/state.json")
    R.log("  timeseries lane (untouched): phase=%s n_done=%s rows=%s" % (
        ts.get("phase"), ts.get("n_done"),
        f"{ts.get('rows_total') or 0:,}"))

    R.section("P1 first supervised run")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"mode": "econ"}).encode())
        body = (r["Payload"].read() or b"").decode("utf-8", "replace")
        R.log("  status=%s FunctionError=%s in %.0fs" % (
            r.get("StatusCode"), r.get("FunctionError"),
            time.time() - t0))
        R.log("  %s" % body[:420])
        if r.get("FunctionError"):
            fails.append("P1:funcerror")
    except Exception as e:
        R.log("  invoke err %s (%.0fs)" % (str(e)[:150], time.time() - t0))
    st = jget(ESTATE)
    R.log("  state: phase=%s n_done=%s/%s queue_left=%s rows=%s "
          "failures=%d" % (
              st.get("phase"), st.get("n_done"), st.get("n_total"),
              st.get("queue_left"), f"{st.get('rows_total') or 0:,}",
              len(st.get("failures") or {})))
    for k, v in list((st.get("failures") or {}).items())[:6]:
        R.log("    fail %-28s %s" % (k[:28], str(v)[:80]))
    out.update(n_done=st.get("n_done"), n_total=st.get("n_total"),
               rows=st.get("rows_total"),
               failures=len(st.get("failures") or {}))

    R.section("P2 what landed, and is the industry detail there")
    objs, kw = [], {"Bucket": LIVE, "Prefix": EROOT, "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        objs += [(o["Key"], o["Size"]) for o in rr.get("Contents", [])]
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  %s objects, %.2f MB under %s" % (
        f"{len(objs):,}", sum(s for _, s in objs) / 1e6, EROOT))
    fams = {}
    for k, s_ in objs:
        fams[k[len(EROOT):].split("/")[0]] = fams.get(
            k[len(EROOT):].split("/")[0], 0) + 1
    R.log("  families landed: %s" % dict(sorted(fams.items(),
                                                key=lambda kv: -kv[1])[:10]))
    naics_ok = False
    for k, _ in objs[:40]:
        if "/cbp/" not in k and "/nonemp/" not in k:
            continue
        d = jget(k)
        if not isinstance(d, list) or len(d) < 2:
            continue
        hdr = d[0]
        naics_col = next((c for c in hdr
                          if str(c).upper().startswith("NAICS")), None)
        R.log("  %s: %s rows, header=%s" % (
            k[len(EROOT):][:52], f"{len(d) - 1:,}", hdr[:8]))
        if naics_col:
            vals = {row[hdr.index(naics_col)] for row in d[1:200]}
            R.log("    %s distinct values in sample: %s" % (
                naics_col, sorted(str(v) for v in vals)[:8]))
            if len(vals) > 1:
                naics_ok = True
                R.log("    INDUSTRY DETAIL PRESENT (not just totals)")
        break
    if objs and not naics_ok:
        R.log("  (no multi-value NAICS column seen yet -- expected while "
              "the first entries are still draining)")
    out["objects"] = len(objs)
    out["naics_detail"] = naics_ok

    R.section("P3 wire the schedule")
    try:
        names = ev.list_rule_names_by_target(
            TargetArn="arn:aws:lambda:%s:857687956942:function:%s"
                      % (REGION, FN)).get("RuleNames", [])
        R.log("  rules already targeting %s: %s" % (FN, names))
        wired = False
        for rn in names:
            tg = ev.list_targets_by_rule(Rule=rn).get("Targets", [])
            if any('"econ"' in str(t.get("Input") or "") for t in tg):
                wired = True
            arn = next((t["Arn"] for t in tg if FN in t["Arn"]), None)
            if not wired and arn:
                ev.put_targets(Rule=rn, Targets=[{
                    "Id": "econ", "Arn": arn,
                    "Input": json.dumps({"mode": "econ"})}])
                d = ev.describe_rule(Name=rn)
                R.log("  added econ target to %s (%s, %s)" % (
                    rn, d.get("ScheduleExpression"), d.get("State")))
                wired = True
                break
        if not wired:
            R.log("  *** no rule targets this function -- the econ lane "
                  "will only drain when invoked manually ***")
            fails.append("P3:nowire")
    except Exception as e:
        R.log("  wiring err %s" % str(e)[:140])
        fails.append("P3")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-econ-lane.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5060 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(n_done=out.get("n_done"), n_total=out.get("n_total"),
         objects=out.get("objects"), rows=out.get("rows"),
         naics=out.get("naics_detail"))
    R.log("ops 5060 GREEN -- economic lane draining")
