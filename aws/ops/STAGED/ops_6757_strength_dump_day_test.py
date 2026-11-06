"""ops 6757 - the strength desk's thesis test on the 2026-11-05 dump (Khalid 2026-11-05 19:25 CT: "Today would be a good day
to test strong stocks engine and thesis. The strongest stocks should have held up and either stayed flat went a little bit
up or went down less than sp500 dump today. Test that engine. Also make sure it covers all asset classes").

  1. wait for the Polygon EOD bank to carry the 2026-11-05 session (the fold runs 03:10 UTC; polled up to 30 min);
  2. the deployed justhodl-strength must be 1.0.7 (release receipt); invoke it once (RequestResponse) - a root run;
  3. read data/strength.json back: version / as_of / classes counted and ranked / the leveraged funds kept out of the
     ranks / boards.by_class (every asset class's median move vs SPY today, best and worst) / the thesis block's
     last_session (today's test: deciles for the whole universe and for stocks alone, the top names and the bottom
     names with their moves, every asset class) / the year's base rate (dump days, deep dump days, all days) / verdict;
  4. INDEPENDENT CROSS-CHECK from the ranks kept BEFORE the session (ops 6756: data/strength/history/2026-11-04.json.gz,
     the live 1.0.5 ranks) x the bank's own 2026-11-05 closes: the same decile table computed here, the leaders board's
     40 names one by one (up / flat / down less than SPY / down more), with and without the leveraged funds that board
     carried (SOXS, TECS, FAS) - so the engine's figure is checked by a second computation, not trusted.
Writes nothing but the engine's own artifacts and this report. Re-runnable.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import polygon_bankfirst as PB  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-strength"
OUT, SNAP = "data/strength.json", "data/strength/history/2026-11-04.json.gz"
SESSION, PRIOR = "2026-11-05", "2026-11-04"
WAIT_S = 1800
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=330)
FLAT = 0.25


def pct(v, d=2):
    return "-" if v is None else ("%+." + str(d) + "f%%") % v


def verdict_word(r, spy):
    if r is None:
        return "no bar"
    if r > FLAT:
        return "UP"
    if r >= -FLAT:
        return "FLAT"
    return "down less than SPY" if r > spy else "down MORE than SPY"


def bucket(rets, spy):
    n = len(rets)
    if not n:
        return {"n": 0}
    rets = sorted(rets)
    held = sum(1 for r in rets if r >= -FLAT or r > spy)
    return {"n": n, "mean": round(sum(rets) / n, 2), "median": round(rets[n // 2], 2), "excess_pp": round(sum(rets) / n - spy, 2), "hit_pct": round(100.0 * sum(1 for r in rets if r > spy) / n, 1),
            "up_pct": round(100.0 * sum(1 for r in rets if r > FLAT) / n, 1), "flat_pct": round(100.0 * sum(1 for r in rets if -FLAT <= r <= FLAT) / n, 1),
            "down_less_pct": round(100.0 * sum(1 for r in rets if r < -FLAT and r > spy) / n, 1), "down_more_pct": round(100.0 * sum(1 for r in rets if r < -FLAT and r <= spy) / n, 1),
            "held_up_pct": round(100.0 * held / n, 1)}


def fmt_bucket(b):
    if not b or not b.get("n"):
        return "n 0"
    return "n %d mean %s median %s excess %+.2f pp | beat SPY %s%% | up %s%% flat %s%% down-less %s%% down-more %s%% | HELD UP %s%%" % (
        b["n"], pct(b.get("mean") if "mean" in b else b.get("mean_ret_pct")), pct(b.get("median") if "median" in b else b.get("median_ret_pct")), b.get("excess_pp", b.get("mean_excess_pp")) or 0.0,
        b.get("hit_pct"), b.get("up_pct"), b.get("flat_pct"), b.get("down_less_pct"), b.get("down_more_pct"), b.get("held_up_pct"))


def semver(v):
    """Version strings compare as tuples (1.0.10 > 1.0.9)."""
    try:
        return tuple(int(x) for x in str(v or "0").split("."))
    except ValueError:
        return (0,)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6757_strength_dump_day_test") as r:
        r.heading("ops 6757 - strength desk thesis test on the %s dump %s" % (SESSION, datetime.now(timezone.utc).isoformat(timespec="minutes")))
        r.section("1. the bank must carry %s" % SESSION)
        t0 = time.time()
        last = None
        while time.time() - t0 < WAIT_S:
            man = json.loads(s3.get_object(Bucket=LIVE, Key=PB.MANIFEST)["Body"].read())
            last = man.get("last_session")
            if last and last >= SESSION:
                break
            time.sleep(30)
        r.log("bank manifest last_session %s (updated %s) after %.0fs" % (last, man.get("updated_at"), time.time() - t0))
        if not last or last < SESSION:
            r.fail("the bank has not folded %s yet (last %s) - run again after the 03:10 UTC fold" % (SESSION, last))
            raise SystemExit(1)

        r.section("2. the deployed engine, invoked once")
        cf = lam.get_function_configuration(FunctionName=FN)
        try:
            rec = json.loads(s3.get_object(Bucket=LIVE, Key="data/ops/releases/%s.json" % FN)["Body"].read())
        except Exception:  # noqa: BLE001
            rec = {}
        r.log("%s: modified %s, sha %s, env MAX_UNIVERSE %s | receipt commit %s code_sha256 %s" % (FN, (cf.get("LastModified") or "")[:19], (cf.get("CodeSha256") or "")[:12],
              ((cf.get("Environment") or {}).get("Variables") or {}).get("MAX_UNIVERSE"), rec.get("commit"), (rec.get("code_sha256") or "")[:12]))
        if rec.get("code_sha256") and rec.get("code_sha256") != cf.get("CodeSha256"):
            r.fail("the live CodeSha256 is not the receipt's")
            raise SystemExit(1)
        t1 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6757_dump_day_test"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        r.log("invoke: status %s error %s %.0fs payload %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t1, json.dumps(payload)[:400]))
        if resp.get("FunctionError"):
            r.fail("the engine raised: %s" % json.dumps(payload)[:800])
            raise SystemExit(1)
        doc = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        r.log("data/strength.json: version %s as_of %s generated %s rows %s ranked %s run %ss errors %s | SPY %s h %s" % (
            doc.get("version"), doc.get("as_of"), doc.get("generated_at"), len(doc.get("rows") or []), (doc.get("boards") or {}).get("n_ranked"), doc.get("run_seconds"), doc.get("n_errors"),
            (doc.get("benchmark") or {}).get("px"), json.dumps((doc.get("benchmark") or {}).get("h"))))
        if semver(doc.get("version")) < (1, 0, 8) or doc.get("as_of") != SESSION:
            r.fail("the artifact is not >= 1.0.8 as of %s: %s %s" % (SESSION, doc.get("version"), doc.get("as_of")))
            raise SystemExit(1)
        cls = doc.get("classes") or {}
        r.log("classes counted %s" % json.dumps(cls.get("counts")))
        r.log("classes ranked  %s" % json.dumps(cls.get("ranked")))
        ex = ((doc.get("boards") or {}).get("excluded") or {}).get("leveraged") or []
        r.log("leveraged funds kept out of the ranks (%d): %s" % (len(ex), ", ".join("%s %s" % (x["ticker"], pct(x.get("h_d"))) for x in ex[:40])))
        r.log("universe %s | unbanked %s | skipped %s" % (json.dumps({k: v for k, v in (doc.get("universe") or {}).items() if k in ("n", "n_rows", "n_ranked", "scopes", "sp500_rows", "etf_desk_rows", "f13_rows", "max")}),
              (doc.get("universe") or {}).get("unbanked"), (doc.get("universe") or {}).get("skipped_for_time")))

        r.section("3. every asset class today (boards.by_class: median move vs SPY, best and worst by strength)")
        bc = (doc.get("boards") or {}).get("by_class") or {}
        spy_d = ((doc.get("benchmark") or {}).get("h") or {}).get("d")
        r.log("SPY %s on %s" % (pct(spy_d), SESSION))
        for c in (doc.get("boards") or {}).get("class_order") or []:
            x = bc.get(c) or {}
            r.log("  %-20s n %4s | median strength %5s | median vs SPY: today %7s week %7s month %7s quarter %7s | above SPY 1M %5s%% 3M %5s%% | RS highs %s coiled %s" % (
                x.get("label"), x.get("n"), x.get("median_strength"), pct(x.get("median_vs_d")), pct(x.get("median_vs_w")), pct(x.get("median_vs_m")), pct(x.get("median_vs_q")),
                x.get("pct_above_spy_m"), x.get("pct_above_spy_q"), x.get("rs_new_highs"), x.get("coiled")))
            r.log("      best %s | worst %s" % (", ".join("%s %s" % (b["ticker"], b["strength"]) for b in x.get("best") or []), ", ".join("%s %s" % (b["ticker"], b["strength"]) for b in x.get("worst") or [])))

        r.section("4. the engine's own test of today (thesis.last_session: ranked as of %s, moved on %s) - two definitions of strong" % (PRIOR, SESSION))
        th = doc.get("thesis") or {}
        ls = th.get("last_session") or {}
        r.log("VERDICT: %s" % th.get("verdict"))
        r.log("last session %s ranked_as_of %s SPY %s dump %s rally %s n %s" % (ls.get("date"), ls.get("ranked_as_of"), pct(ls.get("spy_ret_pct")), ls.get("dump"), ls.get("rally"), ls.get("n")))
        if ls.get("date") != SESSION or ls.get("ranked_as_of") != PRIOR:
            r.fail("the thesis block's last session is not %s ranked as of %s" % (SESSION, PRIOR))
            raise SystemExit(1)
        for pred in ("strength", "asymmetry"):
            r.log("== %s (%s)" % (pred.upper(), (th.get("predictors") or {}).get(pred)))
            for grp in ("all", "stocks"):
                g = (ls.get(pred) or {}).get(grp) or {}
                if not g:
                    continue
                r.log("%s/%s (%s names): TOP decile  %s" % (pred, grp, g.get("n"), fmt_bucket(g.get("top"))))
                r.log("%s/%s: BOTTOM decile %s" % (pred, grp, fmt_bucket(g.get("bottom"))))
                r.log("%s/%s deciles (0 = weakest .. 9 = strongest) mean excess pp: %s | held up %%: %s" % (pred, grp, [d.get("mean_excess_pp") for d in g.get("deciles") or []], [d.get("held_up_pct") for d in g.get("deciles") or []]))
                names = g.get("top_names") or []
                r.log("%s/%s TOP-DECILE names (%d), best to worst: %s" % (pred, grp, len(names), ", ".join("%s %s (%s)" % (x["ticker"], pct(x["ret_pct"]), (x.get("asset_class") or "")[:6]) for x in names[:120])))
                names = g.get("bottom_names") or []
                r.log("%s/%s BOTTOM-DECILE names (%d), best to worst: %s" % (pred, grp, len(names), ", ".join("%s %s" % (x["ticker"], pct(x["ret_pct"])) for x in names[:60])))
        ic = ls.get("confirmed") or {}
        r.log("industry confirmation (industries ranked %s): top tenth of stocks %s | CONFIRMED %s | not confirmed %s" % (ic.get("n_industries_ranked"), fmt_bucket(ic.get("top")), fmt_bucket(ic.get("confirmed")), fmt_bucket(ic.get("unconfirmed"))))
        r.log("industry-confirmed names: %s" % ", ".join("%s %s (%s p%s)" % (x["ticker"], pct(x["ret_pct"]), x.get("industry"), x.get("industry_pct")) for x in ic.get("confirmed_names") or []))
        r.log("by asset class today:")
        for c, x in sorted((ls.get("by_class") or {}).items(), key=lambda kv: -(kv[1].get("mean_ret_pct") or -99)):
            r.log("  %-12s %s | best %s | worst %s" % (c, fmt_bucket(x), ", ".join("%s %s" % (b["ticker"], pct(b["ret_pct"])) for b in x.get("best") or []), ", ".join("%s %s" % (b["ticker"], pct(b["ret_pct"])) for b in x.get("worst") or [])))

        r.section("5. the base rate by window (21 / 63 / 252 sessions): dump days, rally days, both sides, industry confirmation")
        for k in ("21", "63", "252"):
            w = (th.get("windows") or {}).get(k) or {}
            r.log("WINDOW %s: %s -> %s | dump days %s (deep %s) | rally days %s" % (k, w.get("from"), w.get("to"), w.get("n_dump_days"), w.get("n_deep_dump_days"), w.get("n_rally_days")))
            for pred in ("strength", "asymmetry"):
                for kind in ("dump", "rally", "all_days"):
                    a = ((w.get(kind) or {}).get(pred) or {}).get("stocks") or {}
                    td, bd = a.get("top_decile") or {}, a.get("bottom_decile") or {}
                    r.log("  %-9s %-8s stocks: days %s | top tenth excess %s pp held %s%% beat-SPY-names %s%% days-beat %s/%s | bottom tenth excess %s held %s%% | deciles %s | monotonic %s" % (
                        pred, kind, a.get("n_days"), td.get("mean_excess_pp"), td.get("held_up_pct"), td.get("hit_pct"), td.get("days_beat_spy"), a.get("n_days"), bd.get("mean_excess_pp"), bd.get("held_up_pct"), a.get("by_decile_mean_excess_pp"), a.get("monotonic")))
                bs = (w.get("both_sides") or {}).get(pred) or {}
                r.log("  %-9s BOTH SIDES: dump excess %s pp (held %s%%) | rally excess %s pp (beat %s%%) | both positive: %s" % (pred, bs.get("dump_excess_pp"), bs.get("dump_held_up_pct"), bs.get("rally_excess_pp"), bs.get("rally_hit_pct"), bs.get("both_sides")))
            for kind in ("dump", "rally", "all_days"):
                ic = (w.get("industry") or {}).get(kind) or {}
                r.log("  industry %-8s top tenth %s | CONFIRMED %s | not confirmed %s" % (kind, json.dumps(ic.get("top_decile_stocks")), json.dumps(ic.get("industry_confirmed")), json.dumps(ic.get("not_confirmed"))))
            r.log("  by class on dump days: %s" % json.dumps(w.get("by_class_on_dumps"))[:1000])
        for d in (th.get("dump_days") or [])[:15]:
            a, b_ = (d["strength"].get("stocks") or d["strength"]["all"])["top"], (d["asymmetry"].get("stocks") or d["asymmetry"]["all"])["top"]
            c_ = (d.get("confirmed") or {}).get("confirmed") or {}
            r.log("  dump %s SPY %s | momentum top: excess %s pp held %s%% | asymmetry top: excess %s held %s%% | confirmed: excess %s held %s%% n %s" % (
                d["date"], pct(d["spy_ret_pct"]), a.get("mean_excess_pp"), a.get("held_up_pct"), b_.get("mean_excess_pp"), b_.get("held_up_pct"), c_.get("mean_excess_pp"), c_.get("held_up_pct"), c_.get("n")))
        for d in (th.get("rally_days") or [])[:10]:
            a, b_ = (d["strength"].get("stocks") or d["strength"]["all"])["top"], (d["asymmetry"].get("stocks") or d["asymmetry"]["all"])["top"]
            r.log("  rally %s SPY %s | momentum top: excess %s pp beat %s%% | asymmetry top: excess %s beat %s%%" % (d["date"], pct(d["spy_ret_pct"]), a.get("mean_excess_pp"), a.get("hit_pct"), b_.get("mean_excess_pp"), b_.get("hit_pct")))
        bi = (doc.get("boards") or {}).get("industries") or {}
        r.log("industries in this universe: %s ranked (>= 3 members), %s confirmed; strongest: %s | weakest: %s" % (bi.get("n"), bi.get("n_confirmed"),
              ", ".join("%s %s (n%s, leaders %s)" % (x["industry"], x["median_strength"], x["n"], (x.get("leaders") or {}).get("strength")) for x in (bi.get("strongest") or [])[:12]),
              ", ".join("%s %s" % (x["industry"], x["median_strength"]) for x in (bi.get("weakest") or [])[:8])))
        ab = (doc.get("boards") or {}).get("asymmetry") or {}
        r.log("asymmetry leaders (63-session down-day + up-day excess): %s" % ", ".join("%s %s (dn %s up %s)" % (x["ticker"], x.get("asym_pp"), x.get("down_excess_pp"), x.get("up_excess_pp")) for x in (ab.get("leaders") or [])[:20]))
        r.log("industry-confirmed momentum leaders (strength >= 90 & industry confirmed): %s" % ", ".join("%s %s (%s p%s)" % (x["ticker"], x["strength"], x.get("industry"), x.get("industry_rank")) for x in (ab.get("confirmed_leaders") or [])[:30]))

        r.section("6. independent cross-check: the ranks kept before the session (ops 6756, live 1.0.5) x the bank's own %s closes" % SESSION)
        snap = json.loads(gzip.decompress(s3.get_object(Bucket=LIVE, Key=SNAP)["Body"].read()))
        rows = [x for x in (snap.get("rows") or []) if x.get("strength") is not None and not x.get("stale")]
        r.log("snapshot %s: version %s as_of %s, %d ranked rows, leaders %s" % (SNAP, snap.get("version"), snap.get("as_of"), len(rows), [x["ticker"] for x in (snap.get("boards") or {}).get("leaders", [])][:12]))
        if snap.get("as_of") != PRIOR:
            r.fail("the snapshot is not as of %s" % PRIOR)
            raise SystemExit(1)

        def ret_of(t):
            try:
                raw = s3.get_object(Bucket=LIVE, Key=PB.ROOT + t + ".json.gz")["Body"].read()
                rows_ = json.loads(gzip.decompress(raw))
                rows_ = rows_.get("rows") if isinstance(rows_, dict) else rows_
                by = {PB._row_date(x[0]).isoformat(): x for x in rows_[-6:] if isinstance(x, list) and x and x[0] is not None}
                a, b = by.get(PRIOR), by.get(SESSION)
                if not a or not b:
                    return t, None
                ci, cj = float(a[PB.FIELDS.index("c")]), float(b[PB.FIELDS.index("c")])
                return t, (cj / ci - 1.0) * 100.0 if ci > 0 else None
            except Exception:  # noqa: BLE001
                return t, None
        tickers = sorted({x["ticker"] for x in rows} | {"SPY"})
        rets = {}
        with ThreadPoolExecutor(max_workers=12) as pool:
            for t, v in pool.map(ret_of, tickers):
                rets[t] = v
        spy = rets.get("SPY")
        r.log("bank: SPY %s -> %s = %s ; %d of %d snapshot names have both closes" % (PRIOR, SESSION, pct(spy), sum(1 for t in tickers if rets.get(t) is not None), len(tickers)))
        if spy is None:
            r.fail("no SPY bar for %s in the bank" % SESSION)
            raise SystemExit(1)
        lev = set(((doc.get("boards") or {}).get("excluded") or {}).get("leveraged") and [x["ticker"] for x in ((doc.get("boards") or {}).get("excluded") or {}).get("leveraged")] or [])
        for label, pool_rows in (("with the leveraged funds the 1.0.5 ranks carried", rows), ("without leveraged funds", [x for x in rows if x["ticker"] not in lev])):
            ranked = sorted(pool_rows, key=lambda x: x["strength"])
            m = len(ranked)
            dec = {d: [] for d in range(10)}
            for idx, x in enumerate(ranked):
                v = rets.get(x["ticker"])
                if v is not None:
                    dec[min(9, int(10 * idx / m))].append(v)
            r.log("%s (%d names): TOP decile %s" % (label, m, fmt_bucket(bucket(dec[9], spy))))
            r.log("%s: BOTTOM decile %s" % (label, fmt_bucket(bucket(dec[0], spy))))
            r.log("%s: deciles mean excess pp %s" % (label, [bucket(dec[d], spy).get("excess_pp") for d in range(10)]))
        # stocks only, from the snapshot's kind
        st_rows = [x for x in rows if x.get("kind") == "stock" and x["ticker"] not in lev]
        ranked = sorted(st_rows, key=lambda x: x["strength"])
        m = len(ranked)
        dec = {d: [] for d in range(10)}
        for idx, x in enumerate(ranked):
            v = rets.get(x["ticker"])
            if v is not None:
                dec[min(9, int(10 * idx / m))].append(v)
        r.log("STOCKS only (%d): TOP decile %s" % (m, fmt_bucket(bucket(dec[9], spy))))
        r.log("STOCKS only: BOTTOM decile %s" % fmt_bucket(bucket(dec[0], spy)))
        r.log("STOCKS only: deciles mean excess pp %s" % [bucket(dec[d], spy).get("excess_pp") for d in range(10)])
        r.log("the leaders board as of %s (top 40 by strength, live 1.0.5), one by one on %s:" % (PRIOR, SESSION))
        lead = [x for x in (snap.get("boards") or {}).get("leaders", [])]
        words = {}
        for x in lead:
            v = rets.get(x["ticker"])
            w = verdict_word(v, spy)
            words[w] = words.get(w, 0) + 1
            r.log("  %-6s strength %5s  %8s  %s%s" % (x["ticker"], x["strength"], pct(v), w, "  [leveraged - excluded from 1.0.7 ranks]" if x["ticker"] in lev else ""))
        r.log("leaders board tally: %s" % json.dumps(words))
        held = sum(v for k, v in words.items() if k in ("UP", "FLAT", "down less than SPY"))
        tot = sum(v for k, v in words.items() if k != "no bar")
        r.log("leaders board held up: %d of %d (%.0f%%)" % (held, tot, 100.0 * held / tot if tot else 0))
        eng_top = (((ls.get("strength") or {}).get("stocks") or {}).get("top") or {})
        r.kv(session=SESSION, spy=str(pct(spy)), engine_top_excess=str(eng_top.get("mean_excess_pp")), engine_top_held=str(eng_top.get("held_up_pct")), engine_top_hit=str(eng_top.get("hit_pct")),
             leaders_held=("%d/%d" % (held, tot)), dump_days_q=str(((th.get("windows") or {}).get("63") or {}).get("n_dump_days")), version=str(doc.get("version")))
        r.ok("ops 6757 complete: %s" % (th.get("verdict") or "")[:400])


if __name__ == "__main__":
    main()
    sys.exit(0)
