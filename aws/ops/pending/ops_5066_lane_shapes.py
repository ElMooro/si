"""ops_5066 -- read each lane in its OWN vocabulary.

ops 5065 fixed discovery and found every state document -- boj, gdelt,
fred, imf, worldbank, oecd, finra, nyfed, frbddp, tic all turned up. It
still reported only census-econ gaps, because it then tried to read them
all through one fixed vocabulary of done/total pairs, and no two of
these engines name things the same way:

    oecd       done, n_total, progress_pct, failures
    boj        codes, db, done, fail, parts, rows
    gdelt      cursor, files, gaps, gaps_sample, failures
    worldbank  have, n_banked, bytes_total, failures
    finra      have, invalid, drain_src, last_discover
    fred       cats, n_categories, frontier, status

"done/n_total" is not in my pair list, which is the entire reason OECD's
488 denied datasets did not appear. Two scans in a row have now reported
a clean fleet because the measurement could not see, and the page's own
prose says otherwise -- BOJ 55306/120394, GDELT gaps 7381, FRED 277,453
of 282,141.

So this op stops imposing a schema. For every provider it prints EVERY
numeric field and the length of every list, which is ground truth about
that lane whatever it calls things, and only then applies per-lane rules
to compute the gap. Slower to read, impossible to be quietly wrong.

  P0 dump the numeric shape of every provider state document
  P1 BOJ specifically -- its coverage lives across many api_*.json part
     files, so one document was never going to show it
  P2 per-lane gaps computed from the fields that actually exist
  P3 ranked backlog and the expedite order
"""
import json
import sys
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return None


def keys(prefix, cap=800):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while len(out) < cap:
        r = s3.list_objects_v2(**kw)
        out += [o["Key"] for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


def shape(d, prefix=""):
    """Every number, and the size of every collection."""
    out = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        p = prefix + k
        if isinstance(v, bool):
            out[p] = v
        elif isinstance(v, (int, float)):
            out[p] = v
        elif isinstance(v, list):
            out[p + "[]"] = len(v)
        elif isinstance(v, dict) and len(prefix) < 12:
            sub = shape(v, p + ".")
            if len(sub) <= 12:
                out.update(sub)
            else:
                out[p + "{}"] = len(v)
        elif isinstance(v, str) and len(v) < 24:
            out[p] = v
    return out


with report("ops_5066_lane_shapes") as R:
    fails = []
    out = {"op": "ops_5066", "lanes": {}}

    R.section("P0 numeric shape of every provider state doc")
    provs, kw = [], {"Bucket": LIVE, "Prefix": "data/warm/",
                     "Delimiter": "/"}
    while True:
        r = s3.list_objects_v2(**kw)
        provs += [p["Prefix"] for p in r.get("CommonPrefixes", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    R.log("  %d providers" % len(provs))
    for p in provs:
        nm = p.split("/")[-2]
        docs = [k for k in keys(p + "_state/", cap=40)
                if k.endswith(".json")]
        docs += [k for k in keys(p, cap=40)
                 if k.endswith(("state.json", "coverage.json",
                                "progress.json"))]
        if not docs:
            continue
        main = sorted(docs, key=lambda k: (len(k), k))[0]
        d = jget(main)
        if not isinstance(d, dict):
            continue
        sh = shape(d)
        nums = {k: v for k, v in sh.items()
                if isinstance(v, (int, float))
                and not isinstance(v, bool)}
        R.log("  %-14s %-30s %s" % (
            nm[:14], main.split("/")[-1][:30],
            json.dumps(dict(sorted(nums.items(),
                                   key=lambda kv: -abs(kv[1]))[:7]))[:96]))
        out["lanes"][nm] = {"doc": main, "nums": nums,
                            "parts": len(docs)}

    R.section("P1 BOJ -- coverage spread across api_*.json parts")
    bparts = [k for k in keys("data/warm/boj/_state/", cap=400)
              if "api_" in k]
    R.log("  boj api part documents: %d" % len(bparts))
    tot_done = tot_parts = tot_rows = 0
    dbs = 0
    for k in bparts[:200]:
        d = jget(k)
        if not isinstance(d, dict):
            continue
        dbs += 1
        for a, b in (("done", "parts"),):
            va, vb = d.get(a), d.get(b)
            if isinstance(va, (int, float)):
                tot_done += va
            if isinstance(vb, (int, float)):
                tot_parts += vb
        if isinstance(d.get("rows"), (int, float)):
            tot_rows += d["rows"]
    R.log("  across %d dbs: done=%s parts=%s rows=%s" % (
        dbs, f"{tot_done:,}", f"{tot_parts:,}", f"{tot_rows:,}"))
    if tot_parts:
        R.log("  BOJ part coverage: %.1f%%  (%s parts outstanding)" % (
            100.0 * tot_done / tot_parts,
            f"{max(0, tot_parts - tot_done):,}"))
    out["boj"] = {"dbs": dbs, "done": tot_done, "parts": tot_parts,
                  "rows": tot_rows}

    R.section("P2 per-lane gaps, from fields that exist")
    gaps = []

    def add(name, have, total, note=""):
        if total and total > have:
            gaps.append({"lane": name, "have": have, "total": total,
                         "missing": total - have, "note": note})
            R.log("  %-12s %s / %s  (%.1f%%)  %s missing  %s" % (
                name, f"{have:,}", f"{total:,}", 100.0 * have / total,
                f"{total - have:,}", note))
    o = out["lanes"].get("oecd", {}).get("nums", {})
    add("oecd", int(o.get("done", 0)), int(o.get("n_total", 0)),
        "done/n_total -- the pair 5065 could not see")
    g = out["lanes"].get("gdelt", {}).get("nums", {})
    if g.get("gaps"):
        R.log("  %-12s gaps=%s files=%s  (gap list, not a ratio)" % (
            "gdelt", f"{int(g['gaps']):,}",
            f"{int(g.get('files', 0)):,}"))
        gaps.append({"lane": "gdelt", "have": int(g.get("files", 0)),
                     "total": int(g.get("files", 0)) + int(g["gaps"]),
                     "missing": int(g["gaps"]), "note": "date gaps"})
    if out["boj"]["parts"]:
        add("boj", out["boj"]["done"], out["boj"]["parts"],
            "summed across api_* part docs")
    w = out["lanes"].get("worldbank", {}).get("nums", {})
    add("worldbank", int(w.get("n_banked", 0)),
        int(w.get("n_total", w.get("have", 0))), "indicators")
    for nm in ("finra", "imf", "fred", "nyfed", "frbddp", "tic",
               "statcan", "bls"):
        n = out["lanes"].get(nm, {}).get("nums", {})
        if n:
            R.log("  %-12s %s" % (nm, json.dumps(
                dict(sorted(n.items(), key=lambda kv: -abs(kv[1]))[:6]))
                [:110]))

    R.section("P3 ranked backlog")
    gaps.sort(key=lambda x: -x["missing"])
    R.log("  %-12s %12s %12s %8s" % ("lane", "missing", "of", "have%"))
    for x in gaps:
        R.log("  %-12s %12s %12s %7.1f%%" % (
            x["lane"], f"{x['missing']:,}", f"{x['total']:,}",
            100.0 * x["have"] / max(1, x["total"])))
    R.log("  TOTAL MISSING: %s items across %d lanes" % (
        f"{sum(x['missing'] for x in gaps):,}", len(gaps)))
    out["gaps"] = gaps
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/fleet-gaps.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/fleet-gaps.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5066 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(lanes=len(out["lanes"]), gaps=len(gaps),
         missing=sum(x["missing"] for x in gaps),
         top=(gaps[0]["lane"] if gaps else "-"))
    R.log("ops 5066 GREEN -- lanes read in their own vocabulary")
