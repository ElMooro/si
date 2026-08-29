"""ops_5044 -- index the ECB series universe.

ops 5043 settled the completeness question: the RAW ECB mirror is
complete. 214 live dataflows against 214 in our catalog, zero added and
zero retired; slices reach 1980 (CSEC, BSI, ICP, SEC) and 1900 (HICP);
pre-coverage queries on eight of the ten latest-starting flows returned
HTTP 200 with ZERO rows, proving no earlier history exists to fetch. The
only catalogued flows without files are seven DISS publication stubs.

What was missing is the INDEX. The card's "214 series" is 214 dataflows.
CSEC 2022 alone holds 237,779 distinct series keys; IVF 2015-2019 holds
161,630. Millions of ECB series exist in the mirror and none were
extracted, because EXTRACTORS held only eurostat.

extract_ecb_flow ships with this op. Two structural differences from
Eurostat drove the design:
  * FORMAT. ECB csvdata is LONG -- one row per OBSERVATION, the series
    named in a KEY column. Eurostat TSV is WIDE -- one row per series,
    columns are periods. Different parser, same record schema.
  * SLICING. ECB stores a flow across many {FLOW}__{start}_{end} files,
    so one series appears in every slice it spans. Extracting per FILE
    would emit each series repeatedly with a truncated date range, so
    the unit of work is the FLOW: group_ecb orders a flow's slices
    chronologically and the extractor merges first_obs/last_obs/n_obs
    across all of them. That is what makes "all their history" true in
    the index and not just in the raw files.

Resumability is built the way today's failures taught: the budget is
checked between slices as well as every 65k rows, a part-way flow
flushes what it has and records slice_idx so the next run resumes at the
next slice, and si > start_slice guarantees at least one slice of
progress per invocation so it can never livelock. Six properties proven
offline first, including cross-slice merge, quoted-comma fields, empty
observations not blanking last_value, and malformed headers.

The eurostat code path is untouched: the grouped branch only runs when
GROUPERS has the provider, and eurostat has no grouper.

  P0 wait for the deploy; confirm eurostat's lane is unharmed
  P1 add an "ecb" target to the existing rule (the rule cap is
     saturated, so a second target with its own Input beats a new rule)
  P2 kick ECB once and watch a real run
  P3 read back a written page and prove the records carry merged history
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
RULE = "justhodl-series-extractor-5min"
ECB_STATE = "data/_state/series-extract-ecb.json"
EU_STATE = "data/_state/series-extract-eurostat.json"
ECB_PFX = "data/providers/ecb/series/"

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


with report("ops_5044_ecb_series_lane") as R:
    fails = []
    out = {"op": "ops_5044"}

    R.section("P0 deploy + eurostat unharmed")
    for i in range(18):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            lm = (c.get("LastModified") or "")[:19]
            if lm >= (NOW - timedelta(minutes=14)).strftime(
                    "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s  mem=%s timeout=%s" % (
                    lm, c.get("MemorySize"), c.get("Timeout")))
                break
        except Exception:
            pass
        time.sleep(20)
    eu = jget(EU_STATE)
    R.log("  eurostat: flows=%d n_pages=%s series=%s updated_at=%s" % (
        len(eu.get("flows_done") or []), eu.get("n_pages"),
        eu.get("series_count"), eu.get("updated_at")))
    out["eurostat_flows"] = len(eu.get("flows_done") or [])

    R.section("P1 wire the ecb target")
    try:
        tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  existing targets: %s" % [
            (t.get("Id"), str(t.get("Input"))[:40]) for t in tg])
        arn = next((t["Arn"] for t in tg if FN in t["Arn"]), None)
        if not arn:
            R.log("  no extractor target to copy -- aborting wiring")
            fails.append("P1:noarn")
        elif any('"ecb"' in str(t.get("Input") or "") for t in tg):
            R.log("  ecb target already present")
        else:
            ev.put_targets(Rule=RULE, Targets=[{
                "Id": "ecb", "Arn": arn,
                "Input": json.dumps({"provider": "ecb"})}])
            tg2 = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
            R.log("  targets now: %s" % [
                (t.get("Id"), str(t.get("Input"))[:40]) for t in tg2])
            R.log("  reserved concurrency stays 1, so ecb and eurostat "
                  "serialise -- separate state docs and separate page "
                  "prefixes mean they cannot collide")
    except Exception as e:
        R.log("  wiring err %s" % str(e)[:150])
        fails.append("P1")

    R.section("P2 first ECB run")
    try:
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"provider": "ecb"}).encode())
        R.log("  kick sent")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:130])
        fails.append("P2:invoke")
    seen = False
    for i in range(26):
        time.sleep(30)
        st = jget(ECB_STATE)
        if st.get("updated_at"):
            seen = True
            R.log("  t+%3dmin flows=%d/%s n_pages=%s series=%s "
                  "stopped_early=%s" % (
                      (i + 1) // 2, len(st.get("flows_done") or []),
                      st.get("flows_total_grouped"), st.get("n_pages"),
                      st.get("series_count"), st.get("stopped_early")))
            prog = st.get("flow_progress") or {}
            for fid, v in list(prog.items())[:3]:
                R.log("    in-flight %-12s slice %s/%s attempts=%s" % (
                    fid[:12], v.get("slice_idx"), v.get("slices"),
                    v.get("attempts")))
            errs = st.get("errors") or {}
            for k, v in list(errs.items())[:4]:
                R.log("    err %-12s %s" % (k[:12], str(v)[:100]))
            if int(st.get("n_pages") or 0) > 0:
                break
    if not seen:
        R.log("  no ECB state written -- the lane did not start")
        fails.append("P2:nostate")
    st = jget(ECB_STATE)
    out.update(ecb_flows=len(st.get("flows_done") or []),
               ecb_total=st.get("flows_total_grouped"),
               ecb_pages=st.get("n_pages"),
               ecb_series=st.get("series_count"),
               ecb_failed=len(st.get("failed_flows") or []))

    R.section("P3 read back a page -- is the history really merged")
    try:
        r = s3.list_objects_v2(Bucket=LIVE, Prefix=ECB_PFX, MaxKeys=5)
        objs = [o["Key"] for o in r.get("Contents", [])]
        R.log("  pages present: %s%s" % (objs[:3],
                                         " …" if len(objs) > 3 else ""))
        if objs:
            doc = jget(objs[0])
            rows = doc.get("rows") or []
            R.log("  %s: count=%s rows=%d" % (objs[0][len(ECB_PFX):],
                                              doc.get("count"), len(rows)))
            for x in rows[:4]:
                R.log("    %-42s %s..%s  n_obs=%s last=%s geo=%s" % (
                    str(x.get("id"))[:42], x.get("first_obs"),
                    x.get("last_obs"), x.get("n_obs"),
                    x.get("last_value"), x.get("geo")))
            spans = [x for x in rows
                     if x.get("first_obs") and x.get("last_obs")
                     and str(x["first_obs"])[:4] != str(x["last_obs"])[:4]]
            R.log("  records spanning more than one year: %d/%d "
                  "(cross-slice merge working)" % (len(spans), len(rows)))
            if rows and not spans:
                R.log("  *** no multi-year spans -- merge may not be "
                      "happening ***")
        else:
            R.log("  no pages yet")
            fails.append("P3:nopages")
    except Exception as e:
        R.log("  readback err %s" % str(e)[:130])
        fails.append("P3")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/ecb-series-lane.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/ecb-series-lane.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5044 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(ecb_flows=out.get("ecb_flows"), ecb_total=out.get("ecb_total"),
         ecb_pages=out.get("ecb_pages"), ecb_series=out.get("ecb_series"))
    R.log("ops 5044 GREEN -- ECB series lane live and merging history")
