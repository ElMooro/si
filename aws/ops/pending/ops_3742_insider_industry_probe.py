#!/usr/bin/env python3
"""ops 3742 — PROBE: can we build the INSIDER INDUSTRY-CLUSTER overlay (#16)?

Canary #16: not one CEO buying, but 4+ EXECUTIVES ACROSS DIFFERENT COMPANIES
in the same industry buying inside one window. That is a peer-group conviction
signal; a single-ticker cluster is not.

AUDIT (repo, 3742): justhodl-insider-cluster-scanner clusters WITHIN a ticker
(several insiders, one company) and only enriches `industry` for its TOP 50 —
so industry is truncated at source. justhodl-industry-boom already folds
`insider_buys_30d` into a composite with weight 10, but that is a COUNT inside
a score, not a cluster detector with breadth/role/recency structure. No engine
answers "which industries are seeing broad insider accumulation right now".
Clean gap — but only buildable if a feed carries ticker + industry + role +
date at BREADTH, not truncated to 50.

PROBE
  A  data/insider-clusters.json — row shape, n rows, how many carry industry
  B  data/insider-radar.json    — row shape, n rows, industry presence
  C  data/insider-buys-enriched / insider-aggregate — alternates
  D  data/universe.json         — the ticker->industry map (key 'stocks')
     => if universe covers the tickers, we can JOIN industry ourselves and
        the top-50 truncation stops mattering
  E  role/title fields available for conviction weighting (CEO/CFO vs Director)
  F  date fields for windowing, and whether buys are separable from sells

NOTHING deployed.
"""
import json
import sys
import traceback
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"

with report("3742_insider_industry_probe") as rep:
    rep.heading("ops 3742 — insider industry-cluster feed probe (#16)")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3742.json").write_text(json.dumps({"verdict": "STARTED"}))
    findings = {}

    try:
        s3 = boto3.client("s3", region_name="us-east-1")

        def load(key):
            try:
                return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            except Exception as e:
                rep.warn("%s: %s %s" % (key, type(e).__name__, str(e)[:110]))
                return None

        def rows_of(obj):
            """Find the biggest list-of-dicts anywhere one or two levels deep."""
            best = []
            if isinstance(obj, list) and obj and isinstance(obj[0], dict):
                best = obj
            if isinstance(obj, dict):
                for v in obj.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        if len(v) > len(best):
                            best = v
                    if isinstance(v, dict):
                        for v2 in v.values():
                            if (isinstance(v2, list) and v2
                                    and isinstance(v2[0], dict)
                                    and len(v2) > len(best)):
                                best = v2
            return best

        def describe(key, label):
            d = load(key)
            if d is None:
                findings[label] = {"present": False}
                return None, []
            top = sorted(d.keys()) if isinstance(d, dict) else ["<list>"]
            rep.ok("%s: top-level=%s" % (label, top[:14]))
            rws = rows_of(d)
            rep.log("  biggest row list: n=%d" % len(rws))
            if rws:
                rep.log("  row keys: %s" % sorted(rws[0].keys())[:26])
                rep.log("  sample: %s" % json.dumps(rws[0])[:340])
                has_ind = sum(1 for r in rws if (r.get("industry") or "").strip())
                has_tkr = sum(1 for r in rws
                              if (r.get("ticker") or r.get("symbol") or "").strip())
                rep.log("  rows WITH industry: %d/%d · with ticker: %d/%d"
                        % (has_ind, len(rws), has_tkr, len(rws)))
                findings[label] = {"present": True, "n": len(rws),
                                   "with_industry": has_ind,
                                   "with_ticker": has_tkr,
                                   "keys": sorted(rws[0].keys())[:30]}
            return d, rws

        rep.section("A — data/insider-clusters.json")
        dc, rc = describe("data/insider-clusters.json", "clusters")

        rep.section("B — data/insider-radar.json")
        dr, rr = describe("data/insider-radar.json", "radar")

        rep.section("C — alternates")
        for k, lbl in (("data/insider-buys-enriched.json", "buys_enriched"),
                       ("data/insider-aggregate.json", "aggregate"),
                       ("data/edgar-insiders.json", "edgar")):
            describe(k, lbl)

        rep.section("D — universe ticker->industry map")
        uni = load("data/universe.json") or {}
        stocks = uni.get("stocks") or []
        ind_of = {}
        for s0 in stocks:
            t = (s0.get("symbol") or "").upper()
            ind = (s0.get("industry") or "").strip()
            if t and ind and ind.lower() != "unknown":
                ind_of[t] = ind
        rep.ok("universe stocks=%d with industry=%d distinct industries=%d"
               % (len(stocks), len(ind_of), len(set(ind_of.values()))))
        findings["universe"] = {"stocks": len(stocks), "mapped": len(ind_of),
                                "industries": len(set(ind_of.values()))}

        # JOIN TEST — the whole build hinges on this
        rep.section("D2 — JOIN TEST: can we recover industry ourselves?")
        for lbl, rws in (("clusters", rc), ("radar", rr)):
            if not rws:
                continue
            tk = [(r.get("ticker") or r.get("symbol") or "").upper() for r in rws]
            tk = [t for t in tk if t]
            hit = sum(1 for t in tk if t in ind_of)
            rep.ok("  %s: %d/%d tickers resolve to an industry via universe (%.0f%%)"
                   % (lbl, hit, len(tk), (hit / len(tk) * 100) if tk else 0))
            findings["join_%s" % lbl] = {"tickers": len(tk), "resolved": hit}
            if hit:
                c = Counter(ind_of[t] for t in tk if t in ind_of)
                rep.log("  top industries by distinct names: %s" % c.most_common(10))
                multi = [(i, n) for i, n in c.items() if n >= 4]
                rep.log("  industries with >=4 distinct tickers: %d %s"
                        % (len(multi), sorted(multi, key=lambda x: -x[1])[:8]))
                findings["clusterable_%s" % lbl] = len(multi)

        rep.section("E — role / conviction fields")
        for lbl, rws in (("clusters", rc), ("radar", rr)):
            if not rws:
                continue
            keys = set()
            for r in rws[:60]:
                keys |= set(r.keys())
            rolekeys = [k for k in sorted(keys)
                        if any(w in k.lower() for w in
                               ("role", "title", "officer", "director", "insider"))]
            rep.log("  %s role-ish keys: %s" % (lbl, rolekeys))
            datekeys = [k for k in sorted(keys)
                        if any(w in k.lower() for w in ("date", "time", "day"))]
            rep.log("  %s date-ish keys: %s" % (lbl, datekeys))
            valkeys = [k for k in sorted(keys)
                       if any(w in k.lower() for w in
                              ("value", "amount", "usd", "shares", "buy", "sell", "net"))]
            rep.log("  %s value-ish keys: %s" % (lbl, valkeys))

        rep.section("VERDICT")
        buildable = (findings.get("join_clusters", {}).get("resolved", 0) > 0
                     or findings.get("join_radar", {}).get("resolved", 0) > 0)
        rep.kv(buildable=str(buildable),
               clusterable_industries=findings.get("clusterable_clusters", 0)
               or findings.get("clusterable_radar", 0),
               universe_mapped=findings.get("universe", {}).get("mapped", 0))
        Path("aws/ops/reports/3742.json").write_text(
            json.dumps({"verdict": "PASS" if buildable else "BLOCKED",
                        "findings": findings}, indent=2, default=str))
        if not buildable:
            rep.fail("no insider feed joins to industry — #16 not buildable as designed")
            sys.exit(1)
        rep.ok("PROBE COMPLETE — join is viable, build against these shapes")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3742.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
