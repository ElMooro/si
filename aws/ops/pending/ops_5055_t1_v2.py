"""ops_5055 -- rebuild Tier 1 with displayable entries.

ops 5054 proved the browser path works, and in doing so exposed what the
component tests could not: the two branches disagree about what a row
IS. Opening a small flow rendered

    id=eurostat:AACT_ALI01:A.AM400000.THS_AWU.AT  geo=AT
    last_obs=2025  last_value=114.47

while opening a large one rendered

    id=eurostat:MIGR_ASYRESDA:A.PER.POS_OTH.AD.M.UNK.LV  n=None

-- because v1 Tier-1 entries stored only id/first/last. Two of the four
columns were dashes on exactly the flows big enough to need an index,
and with no page pointer there was no way back to the full record. An
index whose entries cannot be displayed is half an index; that is a
composition defect, invisible to every test that checks one piece.

v2 entries carry geo, last_value, observation count and the page number
they came from. Cost: ~278M entries growing from ~80 to ~110 bytes, so
roughly 22GB -> 30GB, about $0.20/month more, and one more in-region
pass (~$2 of Lambda). The state carries entry_schema, so a stale v1 index
rebuilds itself rather than leaving blank columns in place forever.

  P0 confirm v1 finished, then let the schema gate reset it
  P1 watch the v2 rebuild
  P2 verify a v2 block over HTTP: 206, exact bytes, and every field the
     page renders actually present
  P3 restore hourly cadence when the rebuild completes
"""
import bisect
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
SITE = "https://justhodl.ai"
FN = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)


def jget(k):
    import gzip
    b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
    if k.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def http(url, rng=None, cap=2000000):
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 justhodl-ops-5055"})
    if rng:
        req.add_header("Range", rng)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, dict(r.headers), r.read(cap)
    except Exception as e:
        return getattr(e, "code", -1), {}, str(e)[:140].encode()


with report("ops_5055_t1_v2") as R:
    fails = []
    out = {"op": "ops_5055"}

    R.section("P0 state before the rebuild")
    for p in ("eurostat", "ecb"):
        t = jget("data/_state/t1-%s.json" % p)
        R.log("  %-9s v1 finished: flows=%d left=%s entries=%s %.2f GB "
              "entry_schema=%s" % (
                  p, len(t.get("flows_done") or []),
                  t.get("candidates_left"),
                  f"{t.get('entries') or 0:,}",
                  (t.get("bytes") or 0) / 1e9, t.get("entry_schema")))
        out.setdefault("v1", {})[p] = len(t.get("flows_done") or [])
    for i in range(14):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            R.log("  extractor LastModified=%s" % c.get("LastModified"))
            break
        except Exception:
            time.sleep(20)
    try:
        ev.put_rule(Name=RULE, ScheduleExpression="rate(2 minutes)",
                    State="ENABLED")
        R.log("  cadence held at rate(2 minutes) for the rebuild window")
    except Exception as e:
        R.log("  rule err %s" % str(e)[:110])
    for p in ("ecb", "eurostat"):
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps({"provider": p,
                                           "mode": "t1"}).encode())
            R.log("  kicked %s t1" % p)
        except Exception as e:
            R.log("  kick %s err %s" % (p, str(e)[:90]))
        time.sleep(3)

    R.section("P1 watch the v2 rebuild")
    for i in range(11):
        time.sleep(240)
        line = []
        for p in ("ecb", "eurostat"):
            t = jget("data/_state/t1-%s.json" % p)
            line.append("%s %d flows/%s left=%s v%s" % (
                p, len(t.get("flows_done") or []),
                f"{t.get('entries') or 0:,}", t.get("candidates_left"),
                t.get("entry_schema")))
        R.log("  t+%2dmin  %s" % ((i + 1) * 4, "  |  ".join(line)))
        te = jget("data/_state/t1-eurostat.json")
        if te.get("candidates_left") == 0 and \
                int(te.get("entry_schema") or 1) >= 2:
            R.log("  v2 REBUILD COMPLETE")
            break

    R.section("P2 verify a v2 block over HTTP")
    for prov in ("ecb", "eurostat"):
        t = jget("data/_state/t1-%s.json" % prov)
        if int(t.get("entry_schema") or 1) < 2 or not t.get("flows_done"):
            R.log("  %s not on v2 yet -- skipping" % prov)
            continue
        f = (t.get("flows_done") or [])[0]
        base = "%s/data/index/%s/t1/%s" % (SITE, prov, f)
        st, _, b = http(base + ".blocks.json")
        bm = json.loads(b)
        R.log("  %s/%s: %s entries, %d blocks, entry_schema=%s" % (
            prov, f, f"{bm.get('n') or 0:,}", len(bm.get("blocks") or []),
            bm.get("entry_schema")))
        blocks = bm["blocks"]
        keys = [x["k"] for x in blocks]
        probe = blocks[len(blocks) // 2]["k"]
        j = bisect.bisect_right(keys, probe) - 1
        blk = blocks[j]
        st, hd, seg = http(base + ".jsonl",
                           rng="bytes=%d-%d" % (blk["o"],
                                                blk["o"] + blk["c"] - 1))
        rows = [json.loads(l) for l in seg.splitlines() if l.strip()]
        R.log("  block %d -> HTTP %s, %s bytes (want %s), %d rows" % (
            j, st, f"{len(seg):,}", f"{blk['c']:,}", len(rows)))
        if st != 206 or len(seg) != blk["c"]:
            fails.append("P2:%s-range" % prov)
        r0 = rows[0] if rows else {}
        R.log("  sample: id=%s g=%s v=%s f=%s l=%s n=%s p=%s" % (
            str(r0.get("id"))[:44], r0.get("g"), r0.get("v"),
            r0.get("f"), r0.get("l"), r0.get("n"), r0.get("p")))
        have = {k: sum(1 for r in rows if r.get(k) is not None)
                for k in ("g", "v", "n", "p", "f", "l")}
        R.log("  field coverage over %d rows: %s" % (len(rows), have))
        if r0.get("p") is None:
            R.log("  *** no page pointer -- v2 did not take ***")
            fails.append("P2:%s-schema" % prov)
        if have.get("g", 0) == 0 and have.get("v", 0) == 0:
            R.log("  *** geo and value both empty for every row ***")
            fails.append("P2:%s-blank" % prov)
        out.setdefault("v2", {})[prov] = {
            "flow": f, "entries": bm.get("n"), "coverage": have}

    R.section("P3 stand down")
    te = jget("data/_state/t1-eurostat.json")
    done = (te.get("candidates_left") == 0
            and int(te.get("entry_schema") or 1) >= 2)
    try:
        if done:
            ev.put_rule(Name=RULE, ScheduleExpression="rate(1 hour)",
                        State="ENABLED")
            R.log("  cadence -> rate(1 hour); t1 targets kept so new "
                  "flows index unattended")
        else:
            R.log("  rebuild still running (left=%s) -- leaving "
                  "rate(2 minutes)" % te.get("candidates_left"))
        d = ev.describe_rule(Name=RULE)
        R.log("  rule=%s targets=%d" % (
            d.get("ScheduleExpression"),
            len(ev.list_targets_by_rule(Rule=RULE).get("Targets", []))))
    except Exception as e:
        R.log("  standdown err %s" % str(e)[:120])
    out["complete"] = done
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/index-serving.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5055 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(complete=done,
         ecb=(out.get("v2", {}).get("ecb", {}) or {}).get("entries"),
         eurostat=(out.get("v2", {}).get("eurostat", {}) or {}).get(
             "entries"))
    R.log("ops 5055 GREEN -- Tier-1 entries are displayable")
