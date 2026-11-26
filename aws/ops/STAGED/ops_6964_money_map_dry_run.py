"""ops 6964 - the money map, built by the engine's own code from the live warehouse, in memory (dry run; nothing written).

Khalid 2026-11-25: "Enhance that engine and tracks what etfs and sectors is the money flowing out and flowing into and
covers as many asset classes and etfs as possible."

flow_map.py (beside the research view in justhodl-global-flow-desk) is imported from the repository and run on the runner
against the real warehouse. This op then checks what it built, independently of its code:

  1. the build: time, memory, funds, assets, what was taken out and why;
  2. every fund's five-day figure again, from the share counts in the by-date month file (code of this op, not the engine's);
  3. the same figures in the provider document the site already publishes (capital-flow-radar, 300 funds);
  4. the tables add up: every fund in one segment, segments to the total;
  5. the issuers' own share counts beside the provider's;
  6. what was done to the provider's rows, row by row (splits, misprints, round trips, jumps that stay).

The first dry run agreed with the provider to the dollar and that was the fault: ops 6965 measured that the provider's
figure is not money on split days ($401 billion for five Vanguard funds), after a missing day (502 rows, $14.3 billion)
and on round trips. The engine now reads the share count's own path; this op checks that.

main(write=False) is the dry run. The go-live op calls main(write=True): the same build, the same gate, then the
engine's own publish(). Safe to run again.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import random
import resource
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
STAGING = ROOT / "aws" / "ops" / "staging" / "moneymap"                      # the engine's module before it is deployed; removed when it ships
sys.path.insert(0, str(STAGING if (STAGING / "flow_map.py").exists() else ROOT / "aws" / "lambdas" / "justhodl-global-flow-desk" / "source"))
NAME = "ops_6964_money_map_dry_run"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"


def usd(v):
    if v is None:
        return "n/a"
    a = abs(v)
    s = "-" if v < 0 else "+"
    return "%s$%.2fB" % (s, a / 1e9) if a >= 1e9 else "%s$%.1fM" % (s, a / 1e6) if a >= 1e6 else "%s$%.0f" % (s, a)


def main(write=False, name=NAME):
    import boto3
    from botocore.config import Config
    from ops_report import report
    import flow_map as M

    cfg = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=48)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    fails, fixtures = [], {}

    with report(name) as r:
        r.heading("%s - the money map from the live warehouse (%s), %s" % (name.split("_")[1], "WRITE" if write else "dry run, nothing written", now.isoformat(timespec="minutes")))

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.warn("CHECK FAILED: " + msg)
            return cond

        # ------------------------------------------------------------------ 1. the build
        r.section("1. The build, by the engine's own code (flow_map %s)" % M.VERSION)
        t0 = time.time()
        series, profiles, issuer, months, splits = M.collect(s3, LIVE, r.log)
        t1 = time.time()
        doc, fdoc, hist, shards = M.build(series, profiles, issuer, now, months, splits)
        t2 = time.time()
        q = doc["quality"]
        sizes = {k: len(gzip.compress(M.encode(v), 6)) for k, v in (("main", doc), ("funds", fdoc), ("history", hist))}
        sizes["fund_history_shards"] = sum(len(gzip.compress(M.encode(v), 6)) for v in shards.values())
        r.log("read the warehouse in %.0f s, built in %.0f s, peak memory %.0f MB | stored sizes (gzip) %s, %d fund-history files" % (
            t1 - t0, t2 - t1, resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, json.dumps(sizes), len(shards)))
        r.log("as of %s (orders through that day) | windows %s" % (doc["as_of"], ", ".join("%s %s..%s (%d days%s)" % (w["label"], w["start"], w["end"], w["n"], "" if w["complete"] else ", not a full window") for w in doc["windows"])))
        r.log("quality: %s" % json.dumps({k: v for k, v in q.items() if k not in ("splits_taken_out", "late_largest", "unusual_week", "issuer_check", "splits", "round_trips", "misprints_dropped", "jumps_last_21_days", "provider_figure")}))
        r.log("splits: %s" % json.dumps(q["splits"]))
        r.log("round trips: in history %d, last 63 days %d, %s each way | largest: %s" % (q["round_trips"]["in_history"], q["round_trips"]["last_63_days"], usd(q["round_trips"]["usd_each_way_last_63_days"]),
              ", ".join("%s %s %s" % (t, d[5:], usd(v)) for t, d, v, _ in q["round_trips"]["largest_last_63_days"][:18])))
        r.log("misprints dropped: in history %d | last 63 days: %s" % (q["misprints_dropped"]["in_history"], json.dumps(q["misprints_dropped"]["last_63_days"])[:900]))
        pf = q["provider_figure"]
        r.log("the provider's figure beside the count path, untouched rows of the last 63 days: compared %d, equal %d, differing %d (%d after a missing day; %s apart) | largest: %s" % (
            pf["rows_compared"], pf["rows_equal"], pf["rows_differing"], pf["differing_after_a_missing_day"], usd(pf["usd_apart"]),
            ", ".join("%s %s provider %s counted %s%s" % (t, d[5:], usd(a), usd(b), " (gap)" if g else "") for t, d, a, b, g in pf["largest"][:10])))
        r.log("jumps that stay, last 21 days: %s" % ", ".join("%s %s %s" % (t, d[5:], usd(v)) for t, d, v, _ in q["jumps_last_21_days"][:30]))
        r.log("largest funds left out as late: %s" % ", ".join("%s (%s, $%.0fB)" % (t, d, a / 1e9) for t, d, a in q["late_largest"][:12]))
        r.log("splits taken out, last 63 days (%d): %s" % (len(q["splits_taken_out"]), ", ".join("%s %s %s" % (t, d[5:], x.replace("split ", "")) for t, d, x in q["splits_taken_out"])[:1500]))
        r.log("five-day flows over a quarter of a fund's assets (%d): %s" % (len(q["unusual_week"]), ", ".join("%s %s %.0f%%" % (t, usd(v), p) for t, v, p in q["unusual_week"][:25])))
        reasons = M.guard(doc)
        ck(not reasons, "the engine's own guard refuses: %s" % reasons)

        def show(title, rows, n=40):
            r.log(title)
            for g in rows[:n]:
                w = g["w"]
                r.log("    %-44s %4d funds $%8.1fB | 1D %10s  1W %10s  1M %10s  3M %10s  YTD %11s | 1W in %s out %s" % (
                    str(g["id"])[:44], g["n"], (g["assets"] or 0) / 1e9, usd(w["d1"]["net"]), usd(w["w1"]["net"]), usd(w["m1"]["net"]), usd(w["m3"]["net"]), usd(w["ytd"]["net"]),
                    ",".join(t for t, _ in w["w1"]["top_in"]), ",".join(t for t, _ in w["w1"]["top_out"])))
        tot = doc["totals"]
        for k in ("plain", "leveraged", "all"):
            r.log("TOTAL %-9s %d funds, $%.2fT | %s" % (k, tot[k]["n"], (tot[k]["assets"] or 0) / 1e12, " | ".join("%s net %s (in %s, out %s)" % (
                wk.upper(), usd(tot[k]["w"][wk]["net"]), usd(tot[k]["w"][wk]["in"]), usd(tot[k]["w"][wk]["out"])) for wk in ("d1", "w1", "m1", "m3", "ytd"))))
        g = doc["groups"]
        show("SEGMENTS (every current fund in exactly one), by the week's net:", g["segments"])
        show("THE PROVIDER'S ASSET CLASSES (unleveraged funds):", g["classes"])
        show("SECTOR FUNDS:", g["sectors"])
        show("SECTORS THROUGH EVERY EQUITY FUND (flow x the provider's sector weights):", g["sectors_through"])
        show("COUNTRIES (equity funds 60%+ in one country):", g["countries"], 45)
        show("REGIONS (equity):", g["regions"])
        show("BONDS:", g["bonds"])
        show("BOND MATURITY:", g["bond_maturity"])
        show("COMMODITIES:", g["commodities"])
        show("CRYPTO:", g["crypto"])
        show("LEVERAGED & INVERSE:", g["leveraged"])
        show("STYLES (first 25):", g["styles"], 25)
        show("ISSUERS (first 15):", sorted(g["issuers"], key=lambda x: -(x["assets"] or 0)), 15)
        r.log("categories %d, focus groups %d" % (len(g["categories"]), len(g["focus"])))
        b = doc["boards"]["w1"]
        r.log("WEEK, largest in: %s" % ", ".join("%s %s (%.1f%%)" % (t, usd(v), p or 0) for t, v, p in b["in_usd"][:20]))
        r.log("WEEK, largest out: %s" % ", ".join("%s %s (%.1f%%)" % (t, usd(v), p or 0) for t, v, p in b["out_usd"][:20]))
        r.log("WEEK, largest in against assets ($50M+ funds): %s" % ", ".join("%s %.1f%% (%s)" % (t, p, usd(v)) for t, v, p in b["in_pct"][:15]))
        r.log("WEEK, largest out against assets: %s" % ", ".join("%s %.1f%% (%s)" % (t, p, usd(v)) for t, v, p in b["out_pct"][:15]))
        r.log("turned to inflow: %s" % ", ".join("%s %s (was %s)" % (t, usd(v), usd(pv)) for t, v, pv in doc["turned"]["to_in"][:12]))
        r.log("turned to outflow: %s" % ", ".join("%s %s (was %s)" % (t, usd(v), usd(pv)) for t, v, pv in doc["turned"]["to_out"][:12]))
        col = fdoc["cols"].index
        frow = {x[0]: x for x in fdoc["rows"]}
        segc = Counter(x[col("seg")] for x in fdoc["rows"] if x[col("behind")] <= M.LATE_DAYS)
        ck(q["funds_current"] >= 4500, "only %d current funds" % q["funds_current"])
        ck(q["funds_unclassified"] <= 0.02 * q["funds_current"], "%d funds have no class" % q["funds_unclassified"])
        ck(sizes["main"] < 900_000 and sizes["funds"] < 1_500_000, "documents too large: %s" % sizes)

        # ------------------------------------------------------------------ 2. every five-day figure again, from the month file
        r.section("2. Five-day figures again, straight from the by-date month files")
        raw = {}
        keys = sorted(k["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix=M.BY_DATE) for k in page.get("Contents", []))
        for k in keys[-2:]:
            body = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
            for x in json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body):
                kk = (x["composite_ticker"], str(x["effective_date"])[:10])
                if kk not in raw or str(x["processed_date"]) >= str(raw[kk]["processed_date"]):
                    raw[kk] = x
        five = [w for w in doc["windows"] if w["key"] == "w1"][0]
        cal = hist["dates"]
        days5, before = cal[-5:], cal[-6]
        ck(days5[0] == five["start"] and days5[-1] == five["end"], "the five days %s are not the window %s..%s" % (days5, five["start"], five["end"]))
        cur = [t for t, x in frow.items() if x[col("behind")] <= M.LATE_DAYS]
        same = diff = touched = holes = same_p = diff_p = 0
        worst, worst_p = [], []
        for t in cur:
            six = [raw.get((t, d)) for d in [before] + days5]
            if any(f[:10] in days5 or f[:10] == before for f in frow[t][col("flags")]):
                touched += 1
                continue
            if any(x is None or not x.get("shares_outstanding") or not x.get("nav") for x in six):
                holes += 1
                continue
            path = sum((float(b_["shares_outstanding"]) - float(a_["shares_outstanding"])) * float(a_["nav"]) for a_, b_ in zip(six, six[1:]))
            given = sum(float(x.get("fund_flow") or 0) for x in six[1:])
            mine = frow[t][col("w1")] or 0
            if abs(mine - path) <= max(5.0, 1e-9 * abs(path)):
                same += 1
            else:
                diff += 1
                worst.append((t, mine, path))
            if abs(mine - given) <= max(10000.0, 0.003 * abs(given), 2e-4 * (frow[t][col("assets")] or 0)):
                same_p += 1
            else:
                diff_p += 1
                worst_p.append((t, round(mine), round(given)))
        r.log("current funds %d | with all six raw rows and nothing done to them: equal to this op's own count path %d, different %d %s | of those equal to the provider's own figures %d, not %d %s | "
              "funds with a row missing among the six %d | funds with a split, round trip, jump or misprint in the window %d" % (
                  len(cur), same, diff, json.dumps(worst[:4]), same_p, diff_p, json.dumps(worst_p[:6]), holes, touched))
        ck(diff == 0 and same >= 4000, "five-day figures differ from the count path of the month file for %d funds (equal %d)" % (diff, same))
        ck(diff_p <= 0.002 * max(1, same_p), "the provider's own figures differ on %d untouched funds with every row present" % diff_p)
        on_end = sum(1 for t in cur if (t, days5[-1]) in raw)
        r.log("funds with a row on the newest day %s: %d of %d current" % (days5[-1], on_end, len(cur)))
        # a fund with a row missing in the window: its five days are still the difference between the two counts it has
        gone = 0
        for t in cur:
            have = [raw.get((t, d)) for d in [before] + days5]
            if have[0] and have[-1] and any(x is None for x in have[1:-1]) and not any(f[:10] in days5 or f[:10] == before for f in frow[t][col("flags")]):
                rows6 = [x for x in have if x]
                path = sum((float(b_["shares_outstanding"]) - float(a_["shares_outstanding"])) * float(a_["nav"]) for a_, b_ in zip(rows6, rows6[1:]))
                given = sum(float(x.get("fund_flow") or 0) for x in rows6[1:])
                mine = frow[t][col("w1")] or 0
                gone += 1
                if abs(mine - path) > max(5.0, 1e-9 * abs(path)):
                    ck(False, "%s: five days %s against its count path %s across a missing row" % (t, usd(mine), usd(path)))
                elif abs(given - path) > 5e7:
                    r.log("    across a missing row %-6s the count path says %s; the provider's figures say %s" % (t, usd(path), usd(given)))
        r.log("funds with a row missing inside the five days, both ends present: %d, every one equal to its count path" % gone)

        # ------------------------------------------------------------------ 3. the provider document the site already publishes
        r.section("3. The same figures in capital-flow-radar (the site's 300-fund provider document)")
        try:
            body = s3.get_object(Bucket=LIVE, Key="data/capital-flow-radar.json")["Body"].read()
            radar = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
            pf = radar.get("funds")
            recs = list(pf.values()) if isinstance(pf, dict) else pf or []
            eq = ne = other_end = adj = 0
            gaps = []
            for x in recs:
                t = x.get("ticker")
                w5 = (x.get("aligned_windows") or {}).get("5") or {}
                if t not in frow or w5.get("status") != "matched_reporting_window" or w5.get("flow_usd_decimal") is None:
                    continue
                if w5.get("end_date") != doc["as_of"] or w5.get("dates") != days5:
                    other_end += 1
                    continue
                if any(f[:10] in days5 or f[:10] == before for f in frow[t][col("flags")]) or any((t, d) not in raw for d in [before] + days5):
                    adj += 1
                    continue
                a, b_ = frow[t][col("w1")], float(w5["flow_usd_decimal"])
                if a is not None and abs(a - b_) <= max(10000.0, 0.003 * abs(b_), 2e-4 * (frow[t][col("assets")] or 0)):
                    eq += 1
                else:
                    ne += 1
                    gaps.append((t, a, b_))
            r.log("radar generated %s | funds on the same five days: equal %d, different %d %s | on other days %d | with something done to them here or a row missing %d" % (
                radar.get("generated_at"), eq, ne, json.dumps(gaps[:4]), other_end, adj))
            ck(ne == 0 and (eq >= 200 or other_end > 100), "the radar disagrees on %d funds" % ne)
        except Exception as exc:  # noqa: BLE001
            r.log("radar not compared: %s: %s" % (type(exc).__name__, str(exc)[:120]))

        # ------------------------------------------------------------------ 4. the tables add up
        r.section("4. The tables add up")
        for wk in ("d1", "w1", "m1", "m3", "ytd", "y1"):
            want = sum(frow[t][col(wk)] or 0 for t in cur)
            got = sum(x["w"][wk]["net"] for x in g["segments"])
            plain = sum(frow[t][col(wk)] or 0 for t in cur if frow[t][col("seg")] != "Leveraged & inverse")
            got_c = sum(x["w"][wk]["net"] for x in g["classes"])
            ok = abs(want - got) < 10 and abs(doc["totals"]["all"]["w"][wk]["net"] - want) < 10 and abs(plain - got_c) < 10 + 1e-9 * abs(plain) and abs(doc["totals"]["plain"]["w"][wk]["net"] - plain) < 10
            r.log("%s: funds sum %s | segments %s | total %s | unleveraged funds %s = classes %s%s" % (wk, usd(want), usd(got), usd(doc["totals"]["all"]["w"][wk]["net"]), usd(plain), usd(got_c), "" if ok else "  <-- DIFFERENT"))
            ck(ok, "%s does not add up" % wk)
        ck(sum(x["n"] for x in g["segments"]) == len(cur) and sum(segc.values()) == len(cur), "segment counts %d against %d current funds" % (sum(x["n"] for x in g["segments"]), len(cur)))
        noclass = [t for t in cur if not frow[t][col("ac")]]
        r.log("segments by fund count: %s | funds with no provider class: %d %s" % (dict(segc.most_common()), len(noclass), noclass[:12]))
        last = {k: v[-1] for k, v in hist["series"].items() if k.startswith("segments:")}
        d1 = {x["id"]: x["w"]["d1"]["net"] for x in g["segments"]}
        ck(all(abs(last["segments:" + k] - v) <= 0.5 * x_n + 1 for k, v in d1.items() for x_n in [next(x["n"] for x in g["segments"] if x["id"] == k)]), "the history's last day is not the 1D table")
        r.log("history: %d days %s..%s, %d series; the last day of every segment equals its 1D figure" % (len(hist["dates"]), hist["dates"][0], hist["dates"][-1], len(hist["series"])))
        t = "SPY"
        sh = shards[t[0]]["funds"][t]
        ck(abs(sum(v or 0 for v in sh["f"][-5:]) - frow[t][col("w1")]) < 5, "SPY's own history does not sum to its week")
        r.log("SPY's sheet history: %d days, last five sum %s = its week %s; assets %s" % (len(sh["f"]), usd(sum(v or 0 for v in sh["f"][-5:])), usd(frow[t][col("w1")]), usd(sh["a"][-1])))

        # ------------------------------------------------------------------ 5. the issuers' own counts
        r.section("5. The issuers' own share counts beside the provider's")
        c = q["issuer_check"]
        r.log("funds with an issuer file: %d | the issuer's next-day file matches: %d | its same-day file matches: %d | too few telling days: %d | neither (%d): %s" % (
            c["funds"], c["next_day"], c["same_day"], c["too_few_days"], len(c["no_match"]), " ".join(c["no_match"])))
        ck(c["funds"] == 0 or c["next_day"] + c["same_day"] >= 0.6 * c["funds"], "the issuers' files match for only %d of %d funds" % (c["next_day"] + c["same_day"], c["funds"]))

        # ------------------------------------------------------------------ 6. what was done to the provider's rows
        r.section("6. What was done to the provider's rows, row by row")
        paths = {}

        def path_of(t):
            if t not in paths:
                paths[t] = M.count_path([x for x in series.get(t, []) if x[0] in calset], (splits or {}).get(t))
            return paths[t]
        calset = set(M.calendar(series))

        def line(t, n=9, around=None):
            rr = path_of(t)[0]
            if around:
                i = next((j for j, x in enumerate(rr) if x[0] >= around), len(rr) - 1)
                rr = rr[max(0, i - 3):i + 4]
            else:
                rr = rr[-n:]
            given = {x[0]: x[3] for x in series.get(t, [])}
            return " ; ".join("%s sh %.4gM nav %.2f flow %s%s%s" % (d[5:], (s_ or 0) / 1e6, n_ or 0, usd(f_), "" if given.get(d) is None or abs((given.get(d) or 0) - (f_ or 0)) < 1e5 else " (provider %s)" % usd(given.get(d)),
                                                                " [" + fl + "]" if fl else "") for d, s_, n_, f_, fl in rr)
        for t, d in (("VGT", "2026-04-21"), ("VUG", "2026-04-21"), ("MSTU", "2026-08-21"), ("FSOL", "2026-11-05"), ("OUNZ", "2025-12-05"), ("BITQ", "2025-12-09"), ("TBIL", "2025-12-24")):
            r.log("%s around %s: %s" % (t, d, line(t, around=d)))
        for t in ("CNEQ", "ALAI", "ARKK", "ARKQ", "PDEC", "UDEC", "SGVA", "DNOV", "FNOV", "XRT", "SOXX", "QUAL"):
            r.log("%s week %s, month %s, 3M %s, YTD %s | %s" % (t, usd(frow[t][col("w1")]), usd(frow[t][col("m1")]), usd(frow[t][col("m3")]), usd(frow[t][col("ytd")]), line(t, 8)) if t in frow else "%s absent" % t)
        odd = [x for x in q["unusual_week"] if abs(x[1]) >= 1.5e8][:16]
        r.log("five-day flows over a quarter of a fund's assets, $150M+ (%d): %s" % (len(odd), ", ".join("%s %s %.0f%%" % (t, usd(v), p) for t, v, p in odd)))
        impossible = [(t, x[col("w1")], x[col("p_w1")]) for t, x in frow.items() if x[col("behind")] <= M.LATE_DAYS and x[col("p_w1")] is not None and x[col("p_w1")] < -100.5]
        r.log("funds whose five days say they lost more than they had: %d %s" % (len(impossible), json.dumps(impossible[:8])))
        ck(not [x for x in impossible if (frow[x[0]][col("assets")] or 0) > 5e7 or abs(x[1]) > 5e7], "a fund lost more than it held: %s" % impossible[:5])
        for t in "FXI MCHI KWEB KTEC GXC ASHR EWH EWT CQQQ FLCH".split():
            r.log("geography %s -> country %s, segment %s" % (t, frow[t][col("cty")] if t in frow else "absent", frow[t][col("seg")] if t in frow else ""))
        strays = [(x[0], x[col("sec")], x[col("foc")]) for x in fdoc["rows"] if x[col("seg")] == "Equity · sector funds" and x[col("sec")] not in M.SECTOR_NAMES.values()]
        r.log("sector funds outside the eleven sectors: %s" % json.dumps(strays))
        old = Counter()
        for t in cur:
            rr = path_of(t)[0]
            have = {x[0] for x in rr}
            miss = [d for d in cal[-63:] if d not in have and rr and d > rr[0][0]]
            old[0 if not miss else 1 if len(miss) <= 2 else 2 if len(miss) <= 10 else 3] += 1
        r.log("current funds by rows missing in the last 63 days: none %d, one or two %d, three to ten %d, more %d (their flows land on the day the count is next seen)" % (old[0], old[1], old[2], old[3]))

        # ------------------------------------------------------------------ attachments, and the write
        pick = lambda rows: rows                                                    # noqa: E731
        fixtures["money-map"] = gzip.compress(M.encode(doc), 9)
        fixtures["money-map-history"] = gzip.compress(M.encode(hist), 9)
        keep = set(random.Random(7).sample(sorted(frow), min(500, len(frow)))) | {x[0] for bb in doc["boards"].values() for side in bb.values() for x in side} | set("SPY VOO IVV QQQ XLK XLE TLT GLD IBIT SOXX QUAL ROM QID VTI VGT VUG CNEQ ALAI ARKK PDEC FSOL KWEB FXI MCHI SGOV BIL HYG LQD SMH XLF XLV EFA EEM IWM DIA SLV USO TQQQ SQQQ SOXL".split())
        fixtures["money-map-funds-cut"] = gzip.compress(M.encode(dict(fdoc, rows=[x for x in fdoc["rows"] if x[0] in keep], cut_from=len(fdoc["rows"]))), 9)
        fixtures["money-map-shard-S-cut"] = gzip.compress(M.encode(dict(shards["S"], funds={k: v for k, v in shards["S"]["funds"].items() if k in ("SPY", "SOXX", "SGOV", "SOXL")})), 9)
        r.section("attachments")
        for k, body in fixtures.items():
            b64 = base64.b64encode(body).decode("ascii")
            r.lines.append("<!-- FIXTURE %s %d bytes" % (k, len(body)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("FIXTURE-END -->")
        wrote = 0
        if write and not fails:
            r.section("published by the engine's own publish()")
            wrote = M.publish(s3, LIVE, doc, fdoc, hist, shards)
            back = s3.get_object(Bucket=LIVE, Key=M.OUT)["Body"].read()
            back = json.loads(gzip.decompress(back) if back[:2] == b"\x1f\x8b" else back)
            ck(back["as_of"] == doc["as_of"] and back["generated_at"] == doc["generated_at"], "the published document did not read back")
            r.log("%d objects written; %s read back: version %s, as of %s, %d current funds" % (wrote, M.OUT, back["version"], back["as_of"], back["quality"]["funds_current"]))
        r.kv(write=write, wrote=wrote, failed_checks=len(fails), as_of=doc["as_of"], funds_current=q["funds_current"], funds_late=q["funds_late"], assets="$%.2fT" % (q["assets_current_usd"] / 1e12),
             splits=len(q["splits_taken_out"]), read_s=round(t1 - t0), build_s=round(t2 - t1), week_net=usd(tot["plain"]["w"]["w1"]["net"]))
        for f in fails[:30]:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("%s: %d funds, $%.2f trillion, orders through %s; every check passed" % ("PUBLISHED" if wrote else "DRY RUN", q["funds_current"], q["assets_current_usd"] / 1e12, doc["as_of"]))
    return doc


if __name__ == "__main__":
    main()
