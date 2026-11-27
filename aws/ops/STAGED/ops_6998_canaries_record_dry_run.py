"""ops 6998 - dry run on the runner: the canaries' record, the war room 2.0 and the macro grid's history completion.

Khalid, 2026-11-27 11:06 ET: "next improve the engine : canaries ... test it against historical data to calibrate it".

Read-only. Nothing is invoked, nothing is written to the bucket.
  1. The record is built with the engine's own code (justhodl-warroom-weights/source: record_io.run, dry) from the live
     warehouse - every series through the site's resolver, one request at a time - and checked.
  2. The macro grid's change is exercised on the eight credit series FRED now hands out three years of: FRED is asked
     exactly as the grid asks (the key is read from the parameter store, sent, never printed), the answer is completed
     from the warehouse by the grid's own merge_older, and the canary's stress worked out by the grid's own functions is
     set beside the record's rebuilt reading.
  3. The war room's handler runs dry on today's live feeds, reading the record of step 1 from memory; then once more
     with the eight credit canaries at their completed readings, to show what the board says after the grid's next run.

Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import ast
import json
import re
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_6998_canaries_record_dry_run"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
WEIGHTS = ROOT / "aws/lambdas/justhodl-warroom-weights/source"
WARROOM = ROOT / "aws/lambdas/justhodl-canary-warroom/source"
GRID = ROOT / "aws/lambdas/justhodl-canary-grid/source/lambda_function.py"
CREDIT = ["hy_credit_oas", "euro_hy_oas", "em_corp_oas", "em_hy_oas", "us_hy_ytw", "em_hy_ytw", "bbb_aaa"]
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***"), (re.compile(r"\b[0-9a-f]{32}\b"), "***")]


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def grid_functions():
    """The grid's own pure functions and definitions, read out of its source (importing it would read a key)."""
    src = GRID.read_text(encoding="utf-8")
    tree = ast.parse(src)
    scope = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in ("merge_older", "transform", "mean", "stdev", "to_stress"):
            exec(compile(ast.Module(body=[node], type_ignores=[]), "grid", "exec"), scope)
    sig = next(n for n in tree.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "SIGNALS")
    scope["SIGNALS"] = [{k.arg: ast.literal_eval(k.value) for k in el.keywords} for el in sig.value.elts]
    return scope


def main():
    import boto3
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    now = datetime.now(timezone.utc)
    failed = []

    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        def check(ok, text):
            r.log(("PASS  " if ok else "FAIL  ") + clean(text))
            if not ok:
                failed.append(text)

        r.heading("6998 - the canaries' record, the war room 2.0 and the grid's history completion: dry run (read-only), %s" % now.isoformat(timespec="minutes"))

        # ── 1. the record ───────────────────────────────────────────────────────────────────────────────────────
        r.section("1. The record, built by the engine's own code from the live warehouse")
        sys.path.insert(0, str(WEIGHTS))
        import canary_record as R
        import record_io as IO

        class NoWrite:
            def __init__(self, real):
                self.real, self.puts = real, []

            def get_object(self, **kw):
                return self.real.get_object(**kw)

            def put_object(self, **kw):
                self.puts.append(kw.get("Key"))
                raise RuntimeError("a dry run must not write")

        guard = NoWrite(s3)
        t0 = time.time()
        summary, rec = IO.run(guard, LIVE, dry_run=True, log=lambda m: say("    " + str(m)[:300]))
        say("built in %.0f s; summary %s" % (time.time() - t0, json.dumps(summary)[:500]))
        check(guard.puts == [] and summary["written"] is False, "the dry run wrote nothing")
        check(summary["ok"] and rec["tested"] >= 150 and rec["defined"] == 192, "%d of %d canaries rebuilt (missing series: %s)" % (rec["tested"], rec["defined"], summary["missing"]))
        inp = rec["inputs"]
        say("inputs: %s" % json.dumps(inp)[:600])
        mk = rec["market"]
        say("index: %d closes %s to %s, last %s, %.1f%% from its one-year high; %d month-ends counted, %d followed by a 10%% fall (%.1f%%); near a high %d of %d (%.1f%%)" % (
            mk["closes"], mk["from"], mk["to"], mk["last_close"], 100 * mk["from_high_now"], mk["months"], mk["fell"], 100 * mk["fall_share"], mk["early_fell"], mk["early_months"], 100 * mk["early_fall_share"]))
        peaks = [f["peak"] for f in mk["falls"]]
        check(all(p in peaks for p in (19870825, 20000324, 20071009, 20200219, 20220103, 20250219)) and not any(20210101 <= p <= 20211231 for p in peaks),
              "the known falls are in the index and none is invented in 2021 (stray rows dropped: %s)" % json.dumps(inp["closes_dropped"]))
        say("falls since 2015: %s" % json.dumps([[f["peak"], f["low"], round(100 * f["depth"], 1)] for f in mk["falls"] if f["peak"] >= 20150101]))
        say("verdicts: %s" % json.dumps(rec["verdicts"]))
        b = rec["barometer"]
        lvl, t3 = b["views"]["per_mechanism"], b["turns"]["3"]
        say("barometer: %d months from %s" % (len(b["months"]), b["from"]))
        say("LEVEL (each mechanism one vote): early score %s +- %s over %d months; every month %s" % (lvl["auc_early"], lvl["auc_early_se"], lvl["months"], lvl["auc"]))
        for row in lvl["reliability"]:
            say("    %-10s %5.0f-%-5.0f months %4d fell %3d (%s) | near a high %3d fell %2d (%s)" % (row["band"], row["from_pct"], row["to_pct"], row["months"], row["fell"], row["fall_share"], row["early_months"], row["early_fell"], row["early_fall_share"]))
        say("TURN (3 months, mechanisms weighted by their own record, walk-forward): early score %s +- %s over %d months from %s; every month %s +- %s; weights now %s" % (
            t3["earned"]["auc_early"], t3["earned"]["auc_early_se"], t3["earned"]["months"], t3["earned"]["from"], t3["earned"]["auc"], t3["earned"]["auc_se"], json.dumps(t3["weights_now"])))
        for row in t3["reliability"]:
            say("    %-11s %5.0f-%-5.0f months %4d fell %3d (%s) deep %s | near a high %3d fell %2d (%s)" % (row["band"], row["from_pct"], row["to_pct"], row["months"], row["fell"], row["fall_share"], row["deep_share"], row["early_months"], row["early_fell"], row["early_fall_share"]))
        say("every span tried: %s" % json.dumps({k: [v["earned"]["auc_early"], v["earned"]["auc_early_se"], v["equal"]["auc_early"]] for k, v in b["turns"].items()}))
        say("by period: %s" % json.dumps([[p["from"], p["to"], p["months"], p["auc_early"], p["auc_early_se"]] for p in t3.get("by_period") or []]))
        for c in b["level_against_turn"]:
            say("    level %-6s turn %-7s months %3d fell %3d (%s) | near a high %3d fell %2d (%s)" % (c["level"], c["turn"], c["months"], c["fell"], c["fall_share"], c["early_months"], c["early_fell"], c["early_fall_share"]))
        for m in b["mechanisms"]:
            say("    mechanism %-11s %3d canaries | level early %s +- %s | turn early %s +- %s | weight %s" % (m["mechanism"], m["canaries"], m["level"]["auc_early"], m["level"]["auc_early_se"], m["turn"]["auc_early"], m["turn"]["auc_early_se"], m["turn_weight_now"]))
        check(0.45 <= lvl["auc_early"] <= 0.56, "the level does not warn: early score %s (near chance)" % lvl["auc_early"])
        check(t3["earned"]["auc_early"] - 1.64 * t3["earned"]["auc_early_se"] > 0.5, "the turn's early score stands above chance beyond its error: %s - 1.64 x %s" % (t3["earned"]["auc_early"], t3["earned"]["auc_early_se"]))
        shares = [row["fall_share"] for row in t3["reliability"]]
        check(shares[0] < shares[1] < max(shares[2], shares[3]), "falls followed a rising turn more often than an easing one: %s" % shares)
        check(sum(row["months"] for row in t3["reliability"]) == sum(c["months"] for c in b["level_against_turn"]), "the cross table holds the same months as the turn's bands")
        by = dict((c["id"], c) for c in rec["canaries"])
        for label, want in (("leads", "leads"), ("wrong way", "wrong way"), ("stuck", "stuck"), ("short record", "short record")):
            rows = sorted((c for c in rec["canaries"] if c["verdict"] == want), key=lambda c: -(c.get("auc_early") or 0))
            say("%s (%d): %s" % (label, len(rows), "; ".join("%s %s" % (c["id"].split(":", 1)[1][:44], c.get("auc_early")) for c in rows)[:1500]))
        lc = rec["live_check"] or {}
        say("against the engines' own readings today: %s" % json.dumps(lc)[:900])
        check(lc.get("median_gap") is not None and lc["median_gap"] <= 2.0 and lc.get("canaries", 0) >= 100, "the rebuild matches the engines: middle gap %s points over %s canaries" % (lc.get("median_gap"), lc.get("canaries")))
        for cid in lc.get("flagged") or []:
            say("    flagged %s: %s" % (cid, by[cid].get("flag")))
        # nothing later enters an earlier reading, on live data
        defs = json.loads((WEIGHTS / "canary_defs.json").read_text(encoding="utf-8"))
        text = json.dumps(rec, separators=(",", ":"))
        say("the document would be %d bytes" % len(text))
        known = set(IO.series_ids(defs)) | set(p["id"] for p in defs["plumbing"])       # a FRED series id can have an identifier's shape (BAMLH0A0HYM2)
        shaped = sorted(set(re.findall(r"\b[A-Z]{2}[A-Z0-9]{9}[0-9]\b", text)) - known)
        check(not shaped, "nothing shaped like a security identifier is in the record beyond the warehouse's own series ids (%d other)" % len(shaped))

        # ── 2. the grid's change, on the eight credit series ────────────────────────────────────────────────────
        r.section("2. The macro grid: a short answer from FRED completed from the warehouse")
        G = grid_functions()
        sigs = dict((s["key"], s) for s in G["SIGNALS"])
        key = None
        try:
            key = boto3.client("ssm", region_name=REGION).get_parameter(Name="/justhodl/fred/api-key", WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:  # noqa: BLE001
            say("key not read: %s" % type(e).__name__)
        grid_doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/canary-grid.json")["Body"].read())
        live_grid = dict((s.get("key"), s) for s in grid_doc.get("signals") or [])

        def fred_rows(sid, limit):
            url = "https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=%d" % (sid, key, limit)
            with urllib.request.urlopen(url, timeout=30) as resp:
                obs = json.loads(resp.read()).get("observations", [])
            out = []
            for o in obs:
                try:
                    v = float(o.get("value"))
                except (TypeError, ValueError):
                    v = None
                out.append((o.get("date"), v))
            return out

        def house_rows(sid):
            req = urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/series?id=fred%3A" + sid, headers={"User-Agent": "JustHodl-Ops/6998"})
            with urllib.request.urlopen(req, timeout=45) as resp:
                p = json.loads(resp.read().decode("utf-8", "replace"))
            return [(str(o[0])[:10], float(o[1])) for o in (p.get("obs") or []) if o[1] is not None]

        fixed = {}
        if key:
            for k in CREDIT:
                sg = sigs[k]
                try:
                    spec = sg["fred"] if isinstance(sg["fred"], str) else sg["fred"][0]
                    legs = spec.split(":")[1:] if spec.startswith("spread:") else [spec]
                    series = []
                    for n, sid in enumerate(legs):
                        lim = sg["limit"] * (8 if n == 1 else 1)
                        got = fred_rows(sid, lim)
                        time.sleep(0.4)
                        whole = G["merge_older"](got, house_rows(sid), lim) if got and len(got) < lim * 0.9 else got
                        say("    %-12s %-20s asked %5d, FRED %4d, after the warehouse %5d (oldest %s)" % (k, sid, lim, len(got), len(whole), min(d for d, _ in whole)))
                        check(len(got) < lim * 0.9 and len(whole) > len(got) and whole[:len(got)] == got, "%s: FRED's own rows are kept and older ones added behind them" % sid)
                        series.append(whole)
                        time.sleep(0.3)
                    if len(series) == 2:
                        import bisect
                        b_sorted = sorted((d, v) for d, v in series[1] if v is not None)
                        bd = [d for d, _ in b_sorted]
                        obs = []
                        for d, v in series[0]:
                            if v is None:
                                continue
                            i = bisect.bisect_right(bd, d) - 1
                            if i >= 0:
                                obs.append((d, round(v - b_sorted[i][1], 4)))
                    else:
                        obs = series[0]
                    tr = G["transform"](obs, sg["kind"], sg["win"])
                    hist = [v for _, v in tr]
                    mu = G["mean"](hist)
                    z = (tr[0][1] - mu) / G["stdev"](hist, mu)
                    stress = G["to_stress"](z, sg["dir"])
                    fixed[k] = stress
                    rebuilt = by["macro_grid:" + k]["last_stress"]
                    say("    %-12s the grid's own arithmetic on the completed history: %s (z %.2f over %d observations) | live today %s | the record's rebuild %s" % (
                        k, stress, z, len(hist), (live_grid.get(k) or {}).get("stress"), rebuilt))
                    check(abs(stress - rebuilt) <= 8.0, "%s: the completed reading (%s) agrees with the record's rebuild (%s)" % (k, stress, rebuilt))
                except Exception as e:  # noqa: BLE001
                    check(False, "%s: not worked out (%s)" % (k, type(e).__name__))
            got = fred_rows("T10Y2Y", 2600)
            check(len(got) >= 2600 * 0.9, "a series FRED answers in full is left to FRED alone (T10Y2Y: %d of 2600)" % len(got))
        else:
            check(False, "the key could not be read, so the grid's change was not exercised")
        key = None

        # ── 3. the war room, dry ────────────────────────────────────────────────────────────────────────────────
        r.section("3. The war room 2.0 on today's live feeds (dry)")
        for p in (str(WARROOM), str(ROOT / "aws/shared")):
            if p not in sys.path:
                sys.path.insert(0, p)
        import importlib.util
        spec = importlib.util.spec_from_file_location("warroom_dry", str(WARROOM / "lambda_function.py"))
        W = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(W)
        real_gj = W.gj
        writes = []
        W.S3 = NoWrite(s3)

        def run(grid_override=None):
            def gj(key):
                if key == "data/canary-record.json":
                    return json.loads(text)
                d = real_gj(key)
                if key == "data/canary-grid.json" and grid_override and d:
                    for s in d.get("signals") or []:
                        if s.get("key") in grid_override:
                            s["stress"] = grid_override[s["key"]]
                return d
            W.gj = gj
            out = W.lambda_handler({"dry_run": True}, None)
            return out["document"]

        published = json.loads(s3.get_object(Bucket=LIVE, Key="data/canary-warroom.json")["Body"].read())
        doc = run()
        check(W.S3.puts == [], "the war room's dry run wrote nothing")
        missing = sorted(set(published) - set(doc) - {"bus_canaries"})
        check(not missing, "every key published today is still published (missing: %s); added: %s" % (missing, sorted(set(doc) - set(published))))
        for part in ("barometer", "master"):
            gone = sorted(set(published[part]) - set(doc[part]))
            check(not gone, "%s keeps every field it had (missing: %s)" % (part, gone))
        say("headline: %s" % doc["master"]["headline"])
        say("existing views: per canary %s, per mechanism %s, earned %s, early-warning %s" % (
            doc["barometer"]["views"]["per_canary"]["score"], doc["barometer"]["views"]["per_mechanism"]["score"], doc["barometer"]["views"]["earned"]["score"], doc["master"]["early_warning_0_100"]))
        cal = doc["calibrated"]
        check(bool(cal) and cal["level"] and cal["turn"], "the calibrated reading is there")

        def show(tag, c):
            L, T = c["level"], c["turn"]
            say("%s: LEVEL %s = %s percentile, %s (after it: %s of %s months fell, %s) | TURN %+.1f against %s (%s days, %s) = %s percentile, %s (after it: %s of %s fell, %s; near a high %s of %s, %s) | by mechanism %s | cell %s" % (
                tag, L["score"], L["percentile"], L["band"], L["after"]["fell"], L["after"]["months"], L["after"]["fall_share"], T["change"], T["against_day"], T["days_back"], T["against_from"],
                T["percentile"], T["band"], T["after"]["fell"], T["after"]["months"], T["after"]["fall_share"], T["after"]["early_fell"], T["after"]["early_months"], T["after"]["early_fall_share"],
                json.dumps(T["by_mechanism"]), json.dumps(c.get("cell"))))
        show("today, feeds as they are", cal)
        n_rec = sum(1 for c in doc["all_canaries"] if c["record"]["verdict"] != "no record")
        check(n_rec == cal["canaries"] and n_rec >= 100, "%d canaries on the board carry a record; %d do not, each with its reason" % (n_rec, len(doc["all_canaries"]) - n_rec))
        check(all(c["record"].get("why") for c in doc["all_canaries"] if c["record"]["verdict"] == "no record"), "every canary without a record says why")
        if fixed:
            doc2 = run(fixed)
            show("after the grid's next run (credit canaries at their completed readings)", doc2["calibrated"])
            say("existing views then: per canary %s, per mechanism %s, earned %s" % (doc2["barometer"]["views"]["per_canary"]["score"], doc2["barometer"]["views"]["per_mechanism"]["score"], doc2["barometer"]["views"]["earned"]["score"]))
        W.gj = real_gj
        r.kv(failed_checks=len(failed), canaries_rebuilt=rec["tested"], turn_early_score=t3["earned"]["auc_early"], level_early_score=lvl["auc_early"])
        if failed:
            r.fail("%d checks failed: %s" % (len(failed), "; ".join(failed)[:900]))
            sys.exit(1)
        r.ok("dry run clean: the record builds from the warehouse, the grid's completion agrees with it, the war room publishes everything it did and the calibrated reading beside it")


if __name__ == "__main__":
    main()
