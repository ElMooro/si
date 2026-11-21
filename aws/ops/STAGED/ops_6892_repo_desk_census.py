"""ops 6892 - READ-ONLY census before the chart gets a Repo icon (Khalid 2026-11-21: "display an icon for it in the
chart.html make sure it cover all the different aspect of the repo market and repo rate including their spikes in crisis").

Two repo engines exist: justhodl-repo (the master board, data/repo.json + data/repo-history/<sid>.json, page repo.html)
and justhodl-repo-market (the overnight-funding desk, data/repo-market.json, read by no page of its own).  Before a desk
is drawn this reads what the two really publish today:

  A. the desk feed: score, regime, components, rates, distribution, spreads, facilities, reserves, calendar, episodes;
  B. the master board: groups, counts, tier-1 dials, the row fields;
  C. every history file: first and last date, observations, the largest one-step move and when - per group, and the
     full list of series that reach back before 2010 (the ones that can show 2008);
  D. the sibling feeds a repo desk could cite (funding plumbing, USD funding, settlement fails, NY Fed operations).
Nothing is written, nothing is invoked.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
SIBLINGS = ("data/funding-plumbing.json", "data/usd-funding.json", "data/settlement-fails.json", "data/repo-lending.json",
            "data/eurodollar-stress.json", "data/warm/nyfed-markets/repo-deep-summary.json", "data/repo-master-inventory.json",
            "data/repo-market-history.json")


def shape(v, depth=0):
    """A short description of a JSON value: scalars as they are, containers by size and first keys."""
    if isinstance(v, dict):
        if depth >= 2:
            return "{%d}" % len(v)
        return "{" + ", ".join("%s: %s" % (k, shape(x, depth + 1)) for k, x in list(v.items())[:14]) + (", ..." if len(v) > 14 else "") + "}"
    if isinstance(v, list):
        return "[%d%s]" % (len(v), (" of " + shape(v[0], depth + 1)) if v and depth < 2 else "")
    s = json.dumps(v, default=str)
    return s if len(s) <= 60 else s[:57] + '..."'


def biggest_step(dates, values):
    """-> (date, before, after) of the largest absolute one-step move, or None."""
    best = None
    for i in range(1, min(len(dates), len(values))):
        a, b = values[i - 1], values[i]
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            step = abs(b - a)
            if best is None or step > best[0]:
                best = (step, dates[i], a, b)
    return best[1:] if best else None


def selftest():
    assert biggest_step(["a", "b", "c", "d"], [1.0, 1.1, 5.25, 2.0]) == ("c", 1.1, 5.25)
    assert biggest_step(["a"], [1.0]) is None and biggest_step([], []) is None
    assert shape({"a": 1, "b": [1, 2], "c": {"d": {"e": {"f": 1}}}}) == "{a: 1, b: [2 of 1], c: {d: {1}}}"
    assert shape("x" * 80).endswith('..."')


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=48))

    def jget(key):
        try:
            o = s3.get_object(Bucket=LIVE, Key=key)
            raw = o["Body"].read()
            if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return json.loads(raw), o["LastModified"], len(raw)
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}, None, 0

    with report("ops_6892_repo_desk_census") as r:
        r.heading("ops 6892 - the two repo engines' publications, read whole (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("A. The desk feed: data/repo-market.json")
        desk, when, size = jget("data/repo-market.json")
        r.log("written %s, %d bytes | generated_at %s | as_of %s | score %s | regime %s (was %s) | source %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", size, desk.get("generated_at"), desk.get("as_of"), desk.get("repo_stress_score"),
            desk.get("regime"), desk.get("prev_regime"), desk.get("source_primary")))
        r.log("headline: %s" % desk.get("headline"))
        for c in desk.get("components") or []:
            r.log("  component %-12s weight %.2f subscore %5s | %-12s | %s" % (c.get("id"), c.get("weight") or 0, c.get("subscore"), c.get("reading"), c.get("label")))
        for block in ("rates", "distribution", "spreads", "facilities", "reserves", "calendar", "episodes"):
            v = desk.get(block)
            if isinstance(v, dict):
                for k, x in v.items():
                    if isinstance(x, dict):
                        x = {kk: vv for kk, vv in x.items() if not isinstance(vv, list) or len(vv) <= 6}
                    r.log("  %s.%s = %s" % (block, k, json.dumps(x, default=str)[:420]))
            else:
                r.log("  %s = %s" % (block, json.dumps(v, default=str)[:300]))

        r.section("B. The master board: data/repo.json")
        board, when, size = jget("data/repo.json")
        groups = board.get("groups") or []
        rows = [dict(s, group=g.get("name")) for g in groups for s in (g.get("series") or [])]
        r.log("written %s, %d bytes | as_of %s | v %s | %d groups, %d series | counts %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", size, board.get("as_of"), board.get("v") or board.get("version"), len(groups), len(rows),
            json.dumps(board.get("counts"))[:300]))
        r.log("top-level keys: %s" % shape({k: v for k, v in board.items() if k != "groups"}))
        if rows:
            r.log("row fields: %s" % shape(rows[0]))
        baro = board.get("barometer") or {}
        r.log("barometer: score %s | %s" % (baro.get("score"), "; ".join("%s=%s%s z %s" % (c.get("name"), c.get("value"), c.get("unit") or "", c.get("z")) for c in baro.get("components") or [])[:900]))

        r.section("C. Every history file")
        sids = sorted({s.get("sid") for s in rows if s.get("sid")})

        def read_hist(sid):
            doc, _, n = jget("data/repo-history/%s.json" % sid)
            dates, values = doc.get("dates") or [], doc.get("values") or []
            return sid, {"n": len(dates), "first": dates[0] if dates else None, "last": dates[-1] if dates else None, "bytes": n,
                         "step": biggest_step(dates, values),
                         "max": max(((v, d) for d, v in zip(dates, values) if isinstance(v, (int, float))), default=None),
                         "min": min(((v, d) for d, v in zip(dates, values) if isinstance(v, (int, float))), default=None)}

        with ThreadPoolExecutor(max_workers=40) as pool:
            hist = dict(pool.map(read_hist, sids))
        r.log("history files read: %d of %d series ids, %.1f MB" % (sum(1 for h in hist.values() if h["n"]), len(sids), sum(h["bytes"] for h in hist.values()) / 1e6))
        old = []
        for g in groups:
            rs = [dict(s, group=g.get("name")) for s in (g.get("series") or [])]
            hs = [hist.get(s.get("sid")) or {} for s in rs]
            firsts = sorted(h["first"] for h in hs if h.get("first"))
            lasts = sorted(h["last"] for h in hs if h.get("last"))
            tiers = {}
            for s in rs:
                tiers[s.get("tier")] = tiers.get(s.get("tier"), 0) + 1
            r.log("GROUP %s | %d series | tiers %s | first %s .. %s | last %s .. %s" % (
                g.get("name"), len(rs), json.dumps(tiers, sort_keys=True), firsts[0] if firsts else "-", firsts[-1] if firsts else "-",
                lasts[0] if lasts else "-", lasts[-1] if lasts else "-"))
            for s, h in zip(rs, hs):
                step = h.get("step")
                r.log("  %s | t%s | %s | %s..%s n=%s | last %s | max %s | step %s | %s" % (
                    s.get("id"), s.get("tier"), "/".join(str(x) for x in (s.get("tenor"), s.get("collateral"), s.get("bucket")) if x) or "-",
                    h.get("first"), h.get("last"), h.get("n"), s.get("last_value"),
                    ("%s@%s" % (round(h["max"][0], 4), h["max"][1])) if h.get("max") else "-",
                    ("%s:%s>%s" % (step[0], round(step[1], 4), round(step[2], 4))) if step else "-", (s.get("label") or "")[:90]))
                if h.get("first") and h["first"] < "2010-01-01":
                    old.append((h["first"], s.get("id"), g.get("name"), h.get("n"), (s.get("label") or "")[:80]))
        r.section("C2. Series that reach back before 2010 (%d)" % len(old))
        for first, sid, gname, n, label in sorted(old):
            r.log("  %s  %-28s n=%-6s %-34s %s" % (first, sid, n, gname[:34], label))

        r.section("D. Sibling feeds")
        for key in SIBLINGS:
            doc, when, size = jget(key)
            r.log("%s | written %s | %d bytes | %s" % (key, when.strftime("%Y-%m-%d %H:%M") if when else "-", size, shape(doc)[:700]))
        r.kv(groups=len(groups), series=len(rows), histories=sum(1 for h in hist.values() if h["n"]), before_2010=len(old),
             desk_regime=desk.get("regime"), desk_score=desk.get("repo_stress_score"), desk_as_of=desk.get("as_of"))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
