"""ops 6744 - READ-ONLY dry run of the leaders engine on the live warehouse, before the Lambda exists (Khalid 2026-11-04:
"build a new engine called leaders ...").

The engine's own build() (aws/lambdas/justhodl-leaders/source) is imported on the runner and run against the live
bucket with the runner's read access. NOTHING is written to S3 - the artifact stays in memory. The report prints what a
first light would: the universe drawn from the factor book, the benchmark, breadth, the accumulation / distribution
histogram, every fresh golden / death cross, every double top / bottom, every climax, the tight bases, the smart-money
verdicts, fund-flow coverage by source (and how many Polygon-bank fund files the identity rule refused), the strongest
and weakest industries, the splits rebased, sample rows, the run time and the artifact size. The artifact itself is
embedded gzip+base64 in a second report file so the page can be built against real rows offline.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-leaders" / "source"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
SAMPLE = ["NVDA", "AAPL", "TSM", "BRK.B", "JPM", "XOM", "LLY", "GOOGL"]


def money(v):
    if v is None:
        return "-"
    a, s = abs(float(v)), "-" if v < 0 else "+" if v > 0 else ""
    return "%s$%.2fB" % (s, a / 1e9) if a >= 1e9 else "%s$%.0fM" % (s, a / 1e6) if a >= 1e6 else "%s$%.0fK" % (s, a / 1e3) if a >= 1e3 else "%s$%.0f" % (s, a)


def pct(v, d=1):
    return "-" if v is None else ("%+." + str(d) + "f%%") % v


def q(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return round(xs[min(len(xs) - 1, int(p * len(xs)))], 1) if xs else None


def line(r):
    vs, ma, cr, ad = r.get("vs") or {}, r.get("ma") or {}, r.get("cross") or {}, r.get("ad") or {}
    return "%-6s %-22s #%s/%s %-34s str %5s | vs 1W %7s 1M %7s 3M %7s 6M %7s 1Y %8s | d20 %6s d50 %6s d100 %6s d200 %6s d250 %6s %s %s/%s | x %s%s | AD %6s %-19s | %s" % (
        r["ticker"], (r.get("name") or "")[:22], r.get("rank"), r.get("rank_us"), (r.get("industry") or "")[:34], r.get("strength"), pct(vs.get("w")), pct(vs.get("m")), pct(vs.get("q")), pct(vs.get("h")), pct(vs.get("y")),
        ma.get("d20"), ma.get("d50"), ma.get("d100"), ma.get("d200"), ma.get("d250"), ma.get("stack"), ma.get("above"), ma.get("of"),
        cr.get("regime"), (" %s %sd" % (cr.get("type"), cr.get("ago"))) if cr.get("type") else "", ad.get("score"), ad.get("label"), ", ".join(r.get("signals") or []))


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6744_leaders_dry_run") as r:
        r.heading("ops 6744 - leaders engine dry run on the live warehouse (read-only, nothing written) %s" % now.isoformat(timespec="minutes"))
        import lambda_function as lf
        r.log("engine %s %s contract %s | top_n %s min cap %s min $vol %s lookback %s" % (lf.ENGINE, lf.VERSION, lf.CONTRACT, lf.TOP_N, lf.MIN_CAP, lf.MIN_DV, lf.LOOKBACK))
        logs = []
        t0 = time.time()
        out = lf.build(s3, now, log=logs.append, remaining_s=600.0)
        wall = time.time() - t0
        for x in logs:
            r.log(x)
        body = json.dumps(out, separators=(",", ":"), default=str).encode()
        slim = json.loads(body)
        for k in ("rows", "etfs"):
            for x in slim[k]:
                x.pop("spark", None)
                x.pop("rs_spark", None)
        slim_body = json.dumps(slim, separators=(",", ":")).encode()
        r.log("wall %.1fs (engine run_seconds %s) | artifact %d bytes (gzip %d) | without sparks %d bytes (gzip %d) | errors %d" % (
            wall, out["run_seconds"], len(body), len(gzip.compress(body)), len(slim_body), len(gzip.compress(slim_body)), out["n_errors"]))
        rows, etfs, inds = out["rows"], out["etfs"], out["industries"]
        by = {x["ticker"]: x for x in rows}

        r.section("1. universe")
        u = out["universe"]
        r.log("stats %s" % json.dumps(u["stats"]))
        r.log("leaders %d (top-%d %d, ADRs %d) | industries %d | sectors %d | funds %d | book as of %s" % (u["n_leaders"], u["top_n"], u["n_top"], u["n_adr"], u["n_industries"], u["n_sectors"], u["n_etfs"], json.dumps(u["book_as_of"])))
        r.log("unbanked %s | skipped for time %s" % (u["unbanked"], u["skipped_for_time"]))
        r.log("industries without a leader (%d): %s" % (len(u["industries_without_leader"]), u["industries_without_leader"]))
        r.log("sessions per leader: %s" % json.dumps(Counter("600" if x["sessions"] >= 600 else "250-599" if x["sessions"] >= 250 else "70-249" if x["sessions"] >= 70 else "<70" for x in rows)))
        r.log("stale rows: %s | small-industry leaders: %s" % ([x["ticker"] for x in rows if x["stale"]], [x["ticker"] for x in rows if x["small"]]))
        r.log("share classes folded: %s" % {x["ticker"]: x["classes"] for x in rows if x["classes"]})
        r.log("by sector: %s" % json.dumps(Counter(x["sector"] for x in rows).most_common()))

        r.section("2. benchmark + breadth")
        bm = out["benchmark"]
        r.log("SPY %s as of %s | h %s | ma %s | cross %s | A/D %s | 52w pos %s off high %s" % (bm["px"], bm["as_of"], json.dumps(bm["h"]), json.dumps(bm["ma"]), json.dumps(bm["cross"]), json.dumps(bm["ad"]), bm.get("pos_52w"), bm.get("off_high_pct")))
        r.log("summary %s" % json.dumps(out["summary"]))
        r.log("z stats %s" % json.dumps(out["z_stats"]))

        r.section("3. accumulation / distribution")
        for name, group in (("leaders", rows), ("funds", etfs)):
            sc = [(x.get("ad") or {}).get("score") for x in group]
            r.log("%s: labels %s | score p5 %s p25 %s p50 %s p75 %s p95 %s" % (name, json.dumps(Counter((x.get("ad") or {}).get("label") for x in group).most_common()), q(sc, 0.05), q(sc, 0.25), q(sc, 0.5), q(sc, 0.75), q(sc, 0.95)))
        for name, key, rev in (("STRONGEST ACCUMULATION", lambda x: -((x.get("ad") or {}).get("score") or -999), False), ("STRONGEST DISTRIBUTION", lambda x: ((x.get("ad") or {}).get("score") or 999), False)):
            r.log("%s:" % name)
            for x in sorted(rows, key=key)[:15]:
                a, m = x["ad"], (x.get("mf") or {})
                r.log("  %-6s %-28s score %6s d21 %6s | cmf21 %6s cmf63 %6s ud50 %5s obv63 %6s days %s/%s | $flow 5d %s 21d %s 63d %s (21d net %s%%) | vs 1M %s" % (
                    x["ticker"], (x.get("industry") or "")[:28], a.get("score"), a.get("delta_21"), a.get("cmf21"), a.get("cmf63"), a.get("ud50"), a.get("obv63"), a.get("acc_days"), a.get("dist_days"),
                    money((m.get("w") or {}).get("net")), money((m.get("m") or {}).get("net")), money((m.get("q") or {}).get("net")), (m.get("m") or {}).get("net_pct"), pct((x.get("vs") or {}).get("m"))))

        r.section("4. averages and crosses")
        r.log("stack: %s | above-count: %s" % (json.dumps(Counter((x.get("ma") or {}).get("stack") for x in rows).most_common()), json.dumps(sorted(Counter((x.get("ma") or {}).get("above") for x in rows).items(), key=lambda kv: str(kv[0])))))
        r.log("regime: %s | whipsaws (2+ crosses in 120 sessions): %d" % (json.dumps(Counter((x.get("cross") or {}).get("regime") for x in rows).most_common()), sum(1 for x in rows if ((x.get("cross") or {}).get("n_120") or 0) >= 2)))
        b = out["boards"]["leaders"]
        for name in ("golden_cross", "death_cross"):
            r.log("%s (%d):" % (name.upper(), len(b[name])))
            for t in b[name]:
                x = by[t]
                c = x["cross"]
                r.log("  %-6s %-30s %s %s (%s sessions ago) px then %s since %s | gap %s%% 200 rising %s crosses/120 %s | vs 3M %s AD %s" % (
                    t, (x.get("industry") or "")[:30], c["type"], c["date"], c["ago"], c["px_at"], pct(c["since_pct"]), c["gap_pct"], c["s200_rising"], c["n_120"], pct((x.get("vs") or {}).get("q")), (x.get("ad") or {}).get("label")))
        for name in ("golden_near", "death_near"):
            r.log("%s (%d): %s" % (name.upper(), len(b[name]), ", ".join("%s(gap %s%%, eta %s)" % (t, by[t]["cross"]["gap_pct"], by[t]["cross"]["eta"]) for t in b[name])))

        r.section("5. double tops / double bottoms")
        for name, key in (("double_top", "dtop"), ("double_bottom", "dbot")):
            r.log("%s (%d):" % (name.upper(), len(b[name])))
            for t in b[name]:
                x = by[t]
                p = x[key]
                r.log("  %-6s %-26s %-9s p1 %s @%s  p2 %s @%s%s  neck %s @%s | diff %s%% depth %s%% sep %s ago %s | break %s | to neck %s%% target %s (%s%%) | vol p2/p1 %s | theta %s" % (
                    t, (x.get("industry") or "")[:26], p["state"], p["p1"]["date"], p["p1"]["px"], p["p2"]["date"], p["p2"]["px"], "*" if p["p2"]["provisional"] else "", p["neck"]["date"], p["neck"]["px"],
                    p["diff_pct"], p["depth_pct"], p["sep"], p["ago"], json.dumps(p["break"]), p["to_neck_pct"], p["target"], p["target_pct"], p["vol_ratio"], x.get("theta_pct")))

        r.section("6. climaxes, volume, tight bases")
        for name in ("buying_climax", "selling_climax"):
            r.log("%s (%d):" % (name.upper(), len(b[name])))
            for t in b[name]:
                r.log("  %-6s %-30s %s" % (t, (by[t].get("industry") or "")[:30], json.dumps(by[t]["climax"])))
        r.log("fund climaxes: %s" % {x["ticker"]: x["climax"] for x in etfs if x.get("climax")})
        r.log("heaviest volume of the last five sessions (x the 50-session average), top 20: %s" % ", ".join(
            "%s %sx %s %s%%" % (x["ticker"], x["volx"]["x"], x["volx"]["date"][5:], x["volx"]["move_pct"]) for x in sorted([x for x in rows if x.get("volx")], key=lambda x: -x["volx"]["x"])[:20]))
        r.log("tight tiers: %s | pinned %s" % (json.dumps(Counter((x.get("tight") or {}).get("tier") for x in rows).most_common()), [x["ticker"] for x in rows if (x.get("tight") or {}).get("flag")]))
        for t in b["tight"][:25]:
            x = by[t]
            c = x["tight"]
            r.log("  %-6s %-30s %-10s base %3s range %5s%% score %5s | bb pctile %s dry %s cmf %s ud %s rs-hold %s | AD %s | 13F %s" % (
                t, (x.get("industry") or "")[:30], c["tier"], c["base_len"], c["base_range_pct"], c["score"], c["bb_width_pctile_252"], c["vol_dry"], c["cmf_base"], c["updown_vol"], c["rs_hold_base"],
                (x.get("ad") or {}).get("label"), (x.get("f13") or {}).get("stance")))

        r.section("7. smart money")
        r.log("verdicts: %s" % json.dumps(Counter((x.get("smart") or {}).get("verdict") for x in rows).most_common()))
        r.log("13F block %s" % json.dumps(out["f13"]))
        r.log("13F stances among leaders: %s" % json.dumps(Counter((x.get("f13") or {}).get("stance") for x in rows).most_common()))
        r.log("insiders %s" % json.dumps(out["insiders"]))
        for name, key in (("largest insider purchases", lambda x: -((x.get("insider") or {}).get("buy_usd") or 0)), ("largest insider sales", lambda x: -((x.get("insider") or {}).get("sell_usd") or 0))):
            r.log("%s: %s" % (name, " | ".join("%s buys %s (%s by %s) sells %s (%s by %s)" % (x["ticker"], x["insider"]["buys"], money(x["insider"]["buy_usd"]), x["insider"]["buyers"], x["insider"]["sells"],
                                                                                         money(x["insider"]["sell_usd"]), x["insider"]["sellers"]) for x in sorted([x for x in rows if x.get("insider")], key=key)[:10])))
        for name in ("smart_buying", "smart_selling"):
            r.log("%s (%d):" % (name.upper(), len(b[name])))
            for t in b[name][:20]:
                x = by[t]
                f = x.get("f13") or {}
                r.log("  %-6s %-30s 13F %s holders (prior %s) bull %s bear %s in %s out %s net %s (%s%%) %s | insiders %s | score %s | AD %s vs 3M %s" % (
                    t, (x.get("industry") or "")[:30], f.get("n_holders"), f.get("n_prior_holders"), f.get("bull"), f.get("bear"), money(f.get("inflow_usd")), money(f.get("outflow_usd")), money(f.get("net_usd")), f.get("net_pct"),
                    f.get("stance"), json.dumps(x.get("insider")), x["smart"]["score"], (x.get("ad") or {}).get("label"), pct((x.get("vs") or {}).get("q"))))
        r.log("desks join %s | phases among leaders: %s" % (json.dumps(out["desks"]), json.dumps(Counter((x.get("desks") or {}).get("phase") for x in rows).most_common())))

        r.section("8. industry funds and their flows")
        fl = out["flows"]
        r.log("flows %s" % json.dumps({k: v for k, v in fl.items() if k != "no_flow"}))
        r.log("no current flow (%d): %s" % (len(fl["no_flow"]), " ".join(fl["no_flow"])))
        for x in sorted([x for x in etfs if (x.get("flow") or {}).get("current")], key=lambda x: -(((x["flow"].get("w21") or {}).get("pct_aum")) or -999))[:60]:
            w = x["flow"]
            ma = x.get("ma") or {}
            r.log("  %-5s %-9s %-22s aum %9s | 1 %9s 5 %9s (%6s%%) 21 %9s (%6s%%) | px vs 20 %6s 50 %6s 100 %6s 200 %6s 250 %6s %s | %s | AD %s | vs 1M %s 3M %s | %s" % (
                x["ticker"], x["role"], w["source"], money(w.get("aum_usd")), money((w.get("w1") or {}).get("usd")), money((w.get("w5") or {}).get("usd")), (w.get("w5") or {}).get("pct_aum"),
                money((w.get("w21") or {}).get("usd")), (w.get("w21") or {}).get("pct_aum"), ma.get("d20"), ma.get("d50"), ma.get("d100"), ma.get("d200"), ma.get("d250"), ma.get("stack"),
                ((x.get("cross") or {}).get("regime")), (x.get("ad") or {}).get("label"), pct((x.get("vs") or {}).get("m")), pct((x.get("vs") or {}).get("q")), ", ".join(x.get("signals") or [])))
        r.log("fund boards: %s" % json.dumps({k: v for k, v in out["boards"]["etfs"].items() if v and k not in ("bull_stack", "bear_stack")}))

        r.section("9. industries")
        r.log("ranked %d of %d | with a 13F industry row %d | with own-fund flows %d | sector-proxy flows %d | no flows %d" % (
            sum(1 for x in inds if x.get("rank")), len(inds), sum(1 for x in inds if x.get("f13")), sum(1 for x in inds if x["etf_flow"]["funds"] and not x["etf_flow"]["proxy"]),
            sum(1 for x in inds if x["etf_flow"]["proxy"]), sum(1 for x in inds if not x["etf_flow"]["funds"])))
        ranked = [x for x in inds if x.get("rank")]
        for name, group in (("STRONGEST 20", ranked[:20]), ("WEAKEST 20", ranked[-20:])):
            r.log("%s:" % name)
            for x in group:
                vs, br, ef, f13 = x.get("vs") or {}, x["breadth"], x["etf_flow"], x.get("f13") or {}
                r.log("  #%-3s %-40s %-22s str %5s | vs 1W %7s 1M %7s 3M %7s 6M %7s | >50 %s/%s >200 %s/%s | AD %6s %s (%s/%s) | $flow21 %s | 13F ind net %s %s | funds %s %s 21obs %s (%s%%) | %s" % (
                    x["rank"], x["industry"][:40], (x.get("sector") or "")[:22], x.get("strength"), pct(vs.get("w")), pct(vs.get("m")), pct(vs.get("q")), pct(vs.get("h")), br["above_50"], br["n"], br["above_200"], br["n"],
                    x["ad"]["score"], x["ad"]["label"], x["ad"]["acc"], x["ad"]["dist"], money(x.get("mf21_usd")), money(f13.get("net_usd")), f13.get("money_stance"), ",".join(ef["funds"]) or "-",
                    "(sector)" if ef["proxy"] else "", money((ef.get("w21") or {}).get("usd")), (ef.get("w21") or {}).get("pct_aum"), " ".join(x["leaders"])))

        r.section("10. splits rebased")
        sp = out["splits"]
        r.log("windows read %s | rebased %s (recorded %s, inferred %s)" % (sp["windows_read"], sp["n_rebased"], sp["recorded"], sp["inferred"]))
        for t, ev in sp["tickers"].items():
            r.log("  %-6s %s" % (t, json.dumps(ev)))

        r.section("11. the strongest and the weakest leaders")
        for x in rows[:25]:
            r.log("  " + line(x))
        r.log("  ...")
        for x in [x for x in rows if x.get("strength") is not None][-25:]:
            r.log("  " + line(x))

        r.section("12. sample rows")
        for t in SAMPLE:
            x = by.get(t)
            if x:
                y = {k: v for k, v in x.items() if k not in ("spark", "rs_spark")}
                r.log("%s: %s" % (t, json.dumps(y, default=str)))
            else:
                r.log("%s: not a leader row" % t)
        r.log("first industry row: %s" % json.dumps(inds[0], default=str))
        r.log("first fund row: %s" % json.dumps({k: v for k, v in etfs[0].items() if k not in ("spark", "rs_spark")}, default=str))
        r.log("changes %s" % json.dumps(out["changes"])[:400])
        r.log("sources %s" % json.dumps(out["sources"]))
        r.log("errors %s" % json.dumps(out["errors"][:30]))

        # the artifact for offline page work: sparks kept for the first 80 rows only
        keep = json.loads(body)
        for k, n in (("rows", 80), ("etfs", 30)):
            for i, x in enumerate(keep[k]):
                if i >= n:
                    x.pop("spark", None)
                    x.pop("rs_spark", None)
        blob = base64.b64encode(gzip.compress(json.dumps(keep, separators=(",", ":")).encode(), compresslevel=9)).decode("ascii")
        art = ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6744_leaders_dry_run_artifact.md"
        art.write_text("# ops 6744 - leaders dry-run artifact (gzip + base64 of the JSON; sparks kept for the first 80 rows / 30 funds)\n\n<!-- ARTIFACT_GZ_B64 -->\n" +
                       "\n".join(blob[i:i + 120] for i in range(0, len(blob), 120)) + "\n<!-- /ARTIFACT_GZ_B64 -->\n", encoding="utf-8")
        r.log("artifact embedded: %s (%d base64 chars)" % (art.name, len(blob)))
        r.kv(leaders=str(len(rows)), industries=str(len(inds)), funds=str(len(etfs)), as_of=str(out["as_of"]), wall_s="%.1f" % wall, bytes=str(len(body)), errors=str(out["n_errors"]),
             flows_current=str(fl["n_current"]), golden=str(len(b["golden_cross"])), death=str(len(b["death_cross"])), dtop=str(len(b["double_top"])), dbot=str(len(b["double_bottom"])),
             climax="%d/%d" % (len(b["buying_climax"]), len(b["selling_climax"])), tight=str(len(b["tight"])))
        r.ok("dry run complete (nothing written to S3)")


if __name__ == "__main__":
    main()
    sys.exit(0)
