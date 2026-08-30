"""ops_5050 -- start the Tier-1 build and prove a real range read.

ops 5049 settled the scope with measurements instead of assumptions.
Median Eurostat flow: 8 pages, ~2.2MB -- Tier 0 already serves it. But
1,516 flows (18.6%) span >100 pages and CENS_21COBHS_R3 is 7,913,000
series across 15,827 pages, ~4.4GB. Those are the flows Tier 1 exists
for, and only those: threshold 100 pages, so ~1,516 Eurostat + 17 ECB
flows, not all 8,354.

Shape per flow (the SSTable pattern):
    {FLOW}.jsonl        entries sorted by series id, one per line
    {FLOW}.blocks.json  first id + byte offset + length per 4,096 rows
A client loads the small block map, binary-searches it, and pulls ONE
block by HTTP Range. Immutable objects, edge-cacheable, no database and
no always-on service to pay for or page on.

Runs in-region as a mode on the existing extractor rather than a new
Lambda: the same pass from a CI runner is 313GB of egress (~$28) versus
GET requests (~$0.45) and Lambda time (~$2) here, and this repo's deploy
path does not reliably create new functions anyway. The mode branches
before any extraction state is touched, so the completed series lanes
cannot be disturbed by it.

  P0 deploy, confirm both series lanes still intact
  P1 wire t1 targets and raise cadence for the build window
  P2 run and watch flows land
  P3 THE PROOF: take a real built flow, fetch only its blocks.json,
     binary-search it, issue a genuine HTTP Range read for one block,
     and confirm the series is inside -- and report how few bytes that
     took versus fetching the flow whole
"""
import bisect
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


with report("ops_5050_tier1_build") as R:
    fails = []
    out = {"op": "ops_5050"}

    R.section("P0 deploy + lanes intact")
    for i in range(16):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s" % c.get("LastModified"))
                break
        except Exception:
            pass
        time.sleep(20)
    for p in ("eurostat", "ecb"):
        st = jget("data/_state/series-extract-%s.json" % p)
        R.log("  %-9s flows_done=%d n_pages=%s series=%s (must be "
              "unchanged)" % (p, len(st.get("flows_done") or []),
                              st.get("n_pages"), st.get("series_count")))

    R.section("P1 wire the t1 targets")
    try:
        tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        arn = next((t["Arn"] for t in tg if FN in t["Arn"]), None)
        have = {str(t.get("Input") or "") for t in tg}
        add = []
        for p in ("eurostat", "ecb"):
            pay = json.dumps({"provider": p, "mode": "t1"})
            if pay not in have:
                add.append({"Id": "t1%s" % p[:6], "Arn": arn,
                            "Input": pay})
        if add and arn:
            ev.put_targets(Rule=RULE, Targets=add)
        ev.put_rule(Name=RULE, ScheduleExpression="rate(2 minutes)",
                    State="ENABLED")
        tg2 = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  targets: %s" % [(t.get("Id"), str(t.get("Input"))[:38])
                                 for t in tg2])
        R.log("  cadence -> rate(2 minutes) for the build window "
              "(restore to hourly when candidates_left hits 0)")
        if len(tg2) < 4:
            fails.append("P1:targets")
    except Exception as e:
        R.log("  wiring err %s" % str(e)[:140])
        fails.append("P1")

    R.section("P2 build")
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"provider": "ecb",
                                           "mode": "t1"}).encode())
        body = (r["Payload"].read() or b"").decode("utf-8", "replace")
        R.log("  ecb t1 status=%s err=%s" % (r.get("StatusCode"),
                                             r.get("FunctionError")))
        R.log("  %s" % body[:400])
        if r.get("FunctionError"):
            fails.append("P2:ecb")
    except Exception as e:
        R.log("  ecb invoke err %s" % str(e)[:150])
        fails.append("P2:ecb-invoke")
    try:
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"provider": "eurostat",
                                       "mode": "t1"}).encode())
        R.log("  eurostat t1 kicked (Event)")
    except Exception as e:
        R.log("  eurostat kick err %s" % str(e)[:120])
    for i in range(14):
        time.sleep(60)
        for p in ("ecb", "eurostat"):
            t = jget("data/_state/t1-%s.json" % p)
            if t:
                R.log("  t+%2dmin %-9s flows=%d left=%s entries=%s "
                      "blocks=%s %.2f GB last=%s" % (
                          i + 1, p, len(t.get("flows_done") or []),
                          t.get("candidates_left"),
                          f"{t.get('entries') or 0:,}",
                          f"{t.get('blocks') or 0:,}",
                          (t.get("bytes") or 0) / 1e9, t.get("last_flow")))
    for p in ("ecb", "eurostat"):
        t = jget("data/_state/t1-%s.json" % p)
        out[p] = {"flows": len(t.get("flows_done") or []),
                  "left": t.get("candidates_left"),
                  "entries": t.get("entries"), "blocks": t.get("blocks"),
                  "bytes": t.get("bytes")}

    R.section("P3 real range read against a built flow")
    proved = False
    for p in ("ecb", "eurostat"):
        t = jget("data/_state/t1-%s.json" % p)
        for f in (t.get("flows_done") or [])[:1]:
            bkey = "index/%s/t1/%s.blocks.json" % (p, f)
            dkey = "index/%s/t1/%s.jsonl" % (p, f)
            try:
                bm = jget(bkey)
                blocks = bm.get("blocks") or []
                hb = s3.head_object(Bucket=LIVE, Key=bkey)
                hd = s3.head_object(Bucket=LIVE, Key=dkey)
                R.log("  %s/%s: %s entries, %d blocks, map %.0f KB, "
                      "data %.1f MB" % (p, f, f"{bm.get('n') or 0:,}",
                                        len(blocks),
                                        hb["ContentLength"] / 1024,
                                        hd["ContentLength"] / 1e6))
                mid = blocks[len(blocks) // 2]
                target = mid["k"]
                keys = [b["k"] for b in blocks]
                j = bisect.bisect_right(keys, target) - 1
                b = blocks[j]
                seg = s3.get_object(
                    Bucket=LIVE, Key=dkey,
                    Range="bytes=%d-%d" % (b["o"], b["o"] + b["c"] - 1)
                )["Body"].read()
                hit = any(json.loads(ln).get("id") == target
                          for ln in seg.splitlines() if ln.strip())
                R.log("  binary-searched %d blocks -> block %d, Range "
                      "read %.0f KB, found=%s" % (
                          len(blocks), j, len(seg) / 1024, hit))
                R.log("  read amplification: %.0f KB fetched vs %.1f MB "
                      "for the whole flow (%.0fx less)" % (
                          (len(seg) + hb["ContentLength"]) / 1024,
                          hd["ContentLength"] / 1e6,
                          hd["ContentLength"] /
                          max(1, len(seg) + hb["ContentLength"])))
                if not hit:
                    fails.append("P3:%s" % f)
                else:
                    proved = True
            except Exception as e:
                R.log("  probe err %s" % str(e)[:150])
    if not proved:
        fails.append("P3:unproven")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-tier1.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/index-tier1.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5050 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(ecb=out.get("ecb", {}).get("flows"),
         eurostat=out.get("eurostat", {}).get("flows"),
         left=out.get("eurostat", {}).get("left"))
    R.log("ops 5050 GREEN -- Tier 1 building, range read proven end to end")
