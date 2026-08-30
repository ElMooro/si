"""ops_5067 -- the real backlog, and expedite the biggest lane.

Three scans failed to find the fleet's gaps because they guessed at
paths. The engines name their own directories: BOJ state lives at
data/warm/boj-full/_state/api_{db}.json, not data/warm/boj/. Everything
"missing" in ops 5064-5066 was my scanner looking in the wrong place and
reporting a clean fleet. The page's prose was right the whole time --
and that prose is generated FROM these documents, so it was always the
better source than my scanner.

The real backlog, from the page's own numbers:
    BOJ        55,306 / 120,394 series   -> 65,088 missing   (46%)
    OECD          ~1,058 / 1,546          ->    488 denied at source
    GDELT      gaps 7,381 · v1 archive 4,983/4,986
    FRED       277,453 banked / 282,141 discovered -> 4,688
    census-econ  ~139 / 1,226            ->  1,087 (in flight)
    IMF 218/222 · World Bank 29,468/29,490 · FINRA 8/9 (no secret)

BOJ is the largest by an order of magnitude, so it gets the attention.
Its engine already discovered the full universe (22 dbs, 120,394 series,
ops 4987) and drains per-db into api_{db}.json part documents -- so this
is an unfinished crawl, not a blocked one. The question is whether it is
still advancing or has quietly stopped, which is exactly what went wrong
with census-us: complete-looking, untriggered, stale for weeks.

  P0 per-db truth: done/parts/rows for all 22 dbs, and which are short
  P1 is it running at all -- schedule, invocations, recent state mtimes
  P2 expedite: drive it and measure the real drain rate
  P3 the other lanes, read at their CORRECT paths this time
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
ACCT = "857687956942"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-boj-full"
BROOT = "data/warm/boj-full/_state/"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return None


def listing(prefix, cap=900):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while len(out) < cap:
        r = s3.list_objects_v2(**kw)
        out += [(o["Key"], o["LastModified"]) for o in
                r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


def boj_totals():
    tot = {"dbs": 0, "done": 0, "parts": 0, "rows": 0, "short": []}
    newest = None
    for k, lm in listing(BROOT):
        if "api_" not in k:
            continue
        d = jget(k)
        if not isinstance(d, dict):
            continue
        tot["dbs"] += 1
        dn = int(d.get("done") or 0)
        pt = int(d.get("parts") or 0)
        tot["done"] += dn
        tot["parts"] += pt
        tot["rows"] += int(d.get("rows") or 0)
        if pt and dn < pt:
            tot["short"].append((k.split("api_")[-1][:-5], dn, pt))
        newest = lm if not newest or lm > newest else newest
    tot["newest"] = newest
    return tot


with report("ops_5067_boj_expedite") as R:
    fails = []
    out = {"op": "ops_5067"}

    R.section("P0 BOJ per-db truth")
    t = boj_totals()
    R.log("  dbs=%d  parts done=%s/%s  rows=%s" % (
        t["dbs"], f"{t['done']:,}", f"{t['parts']:,}",
        f"{t['rows']:,}"))
    if t["parts"]:
        R.log("  part coverage %.1f%%  -> %s parts outstanding" % (
            100.0 * t["done"] / t["parts"],
            f"{t['parts'] - t['done']:,}"))
    R.log("  newest state write: %s" % t["newest"])
    R.log("  dbs still short (%d):" % len(t["short"]))
    for db, dn, pt in sorted(t["short"], key=lambda x: -(x[2] - x[1]))[:12]:
        R.log("    %-14s %s/%s  (%s outstanding)" % (
            db[:14], f"{dn:,}", f"{pt:,}", f"{pt - dn:,}"))
    out["boj_before"] = {"done": t["done"], "parts": t["parts"],
                         "rows": t["rows"], "short": len(t["short"])}

    R.section("P1 is BOJ actually running")
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": FN}],
            StartTime=NOW - timedelta(days=3), EndTime=NOW,
            Period=86400, Statistics=["Sum"])
        pts = sorted((p["Timestamp"].date().isoformat(), int(p["Sum"]))
                     for p in r.get("Datapoints", []))
        R.log("  invocations/day (3d): %s" % (
            " ".join("%s=%d" % (d[5:], v) for d, v in pts) or "NONE"))
        out["invocations"] = sum(v for _, v in pts)
    except Exception as e:
        R.log("  metric err %s" % str(e)[:100])
    try:
        rules = ev.list_rule_names_by_target(
            TargetArn="arn:aws:lambda:%s:%s:function:%s"
                      % (REGION, ACCT, FN)).get("RuleNames", [])
        info = []
        for rn in rules:
            d = ev.describe_rule(Name=rn)
            info.append((rn, d.get("State"), d.get("ScheduleExpression")))
        R.log("  rules: %s" % (info or "NONE -- untriggered, exactly "
                               "like census-us was"))
        out["rules"] = len(rules)
    except Exception as e:
        R.log("  rule err %s" % str(e)[:100])

    R.section("P2 expedite")
    t0 = time.time()
    for cycle in range(4):
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"mode": "api",
                                           "kicked_by": "ops_5067"}
                                          ).encode())
        except Exception as e:
            R.log("  invoke err %s" % str(e)[:100])
        time.sleep(600)
        n = boj_totals()
        el = (time.time() - t0) / 60.0
        R.log("  t+%2.0fmin  parts %s/%s (+%s)  rows %s (+%s)  short=%d"
              % (el, f"{n['done']:,}", f"{n['parts']:,}",
                 f"{n['done'] - t['done']:,}", f"{n['rows']:,}",
                 f"{n['rows'] - t['rows']:,}", len(n["short"])))
    n = boj_totals()
    el = (time.time() - t0) / 60.0
    gained = n["done"] - t["done"]
    R.log("  drained %s parts in %.0f min (%.1f/min); rows +%s" % (
        f"{gained:,}", el, gained / max(1, el),
        f"{n['rows'] - t['rows']:,}"))
    if gained > 0:
        left = n["parts"] - n["done"]
        R.log("  %s parts left -> ~%.1f h at this rate" % (
            f"{left:,}", left / (gained / max(1, el)) / 60.0))
    else:
        R.log("  NO MOVEMENT -- the drain is not advancing; the gap is "
              "not a slow crawl, it is a stopped one")
        fails.append("P2:stalled")
    out["boj_after"] = {"done": n["done"], "parts": n["parts"],
                        "rows": n["rows"]}

    R.section("P3 the other lanes, correct paths")
    o = jget("data/_state/sdmx-walk-oecd.json") or {}
    if o:
        dn = len(o.get("done") or []) if isinstance(o.get("done"),
                                                    list) else o.get("done")
        R.log("  oecd     done=%s n_total=%s failures=%s" % (
            dn, o.get("n_total"),
            len(o.get("failures") or {}) if isinstance(
                o.get("failures"), dict) else o.get("failures")))
    g = jget("data/warm/gdelt-full/_state/state.json") or {}
    if g:
        R.log("  gdelt    files=%s gaps=%s cursor=%s" % (
            g.get("files"), g.get("gaps"), g.get("cursor")))
    for p, lbl in (("data/warm/worldbank-full/_state/state.json", "wb"),
                   ("data/warm/finra-full/_state/state.json", "finra"),
                   ("data/warm/census-us/_state/state.json", "census-ts")):
        d = jget(p) or {}
        if d:
            nums = {k: v for k, v in d.items()
                    if isinstance(v, (int, float))
                    and not isinstance(v, bool)}
            R.log("  %-8s %s" % (lbl, json.dumps(nums)[:110]))
    tot = sum(len(jget("data/_state/census-econ-s%d.json" % k
                       ).get("done") or [])
              for k in range(12)
              if jget("data/_state/census-econ-s%d.json" % k))
    R.log("  census-econ shards done=%s/1226" % tot)
    out["census_econ_done"] = tot
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/fleet-gaps.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/fleet-gaps.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5067 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(boj_parts="%s/%s" % (n["done"], n["parts"]),
         boj_rows=n["rows"], census_econ=out.get("census_econ_done"))
    R.log("ops 5067 GREEN -- biggest lane expedited")
