"""ops 7003 - dry run on the runner: the Damodaran bank's own code against his live site, and the EVA engine's own
whole-market pass on the premium it would read, beside the fixed 5.50% it uses today.

Nothing is written and no engine is invoked: the bank's run() is called with dry_run and an S3 client that refuses
every write; the EVA engine's market_pass() is called twice here (it only reads the factor book and the statement
bank) - once on the fixed premium, once on the bank's - with the risk-free rate and credit spread of its own last
run, so the two differ by the premium alone.

Part 1  the bank: what was listed, read and derived; his home page's five premiums against the monthly file's
        newest row; every heading of the monthly file; the annual and the returns tables; the country table; the
        industry tables by region, what could not be read as one and why, and the joined benchmarks.
Part 2  EVA: before and after, for every US stock of $2B or more the factor book holds and for the engine's fifty.

Safe to run again. Its first run (2026-11-28 04:04 UTC) read all 251 of his files (21.7 MB, 99 s, 233 industry tables in
eight regions, every benchmark column found). So as not to ask his server for 251 files at every re-run, it now reads
the premium and country files and the US and Global files the benchmarks are joined from (ONLY below).
"""
from datetime import datetime, timezone
from pathlib import Path
import importlib.util
import json
import os
import re
import statistics
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_7003_damodaran_bank_dry_run"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
os.environ.setdefault("AWS_DEFAULT_REGION", REGION)
ONLY = [r"^(ERPbymonth|histimpl|histretSP|ctryprem\w*|ratings)\.xlsx?$",
        r"^(wacc|margin|roe|EVA|pedata|pe|pbvdata|pbv|psdata|ps|vebitda|betas|beta)(Global)?\.xls$"]
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***")]


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def load(name, path, extra):
    for p in extra:
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class ReadOnly:
    """The warehouse, for reading. A write is a failed dry run."""
    def __init__(self, s3):
        self._s3, self.writes = s3, []

    def get_object(self, **kw):
        return self._s3.get_object(**kw)

    def put_object(self, **kw):
        self.writes.append(kw.get("Key"))
        raise AssertionError("a dry run tried to write %s" % kw.get("Key"))

    copy_object = put_object


def pct(x, places=2):
    return "–" if x is None else ("%." + str(places) + "f%%") % (x * 100)


def main():
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name=REGION)
    now = datetime.now(timezone.utc)
    fails = []
    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        def ck(ok, text):
            say(("✅ " if ok else "✗ ") + text)
            if not ok:
                fails.append(text)

        r.heading("7003 - the Damodaran bank, dry run on the runner; EVA before and after (nothing written), %s" % now.isoformat(timespec="minutes"))

        # ------------------------------------------------------------------------------------------- part 1
        r.section("1. The bank's own run, dry")
        src = ROOT / "aws" / "lambdas" / "justhodl-damodaran" / "source"
        D = load("damodaran_engine", src / "lambda_function.py", [src])
        ro = ReadOnly(s3)
        lines = []
        summary, docs = D.run(ro, LIVE, dry_run=True, log=lines.append, budget=900, only=ONLY)
        for ln in lines[:3]:
            say(ln[:600])
        say("summary: %s" % json.dumps({k: summary.get(k) for k in ("ok", "seconds", "listed", "asked", "sweep", "stopped", "unchanged", "banked_bytes", "in_bank", "groups", "wrote", "peak_memory_mb", "premium", "full_update")}))
        say("changed (would be banked): %d files, %.1f MB" % (len(summary.get("changed") or []), (summary.get("banked_bytes") or 0) / 1e6))
        for t in summary.get("trouble") or []:
            say("  trouble: %s" % t)
        ck(summary.get("ok") is True and not ro.writes, "the run finished and wrote nothing (%d writes refused)" % len(ro.writes))
        ck((summary.get("listed") or 0) >= 200 and not summary.get("trouble"), "his two pages listed %s spreadsheets; the %s read here gave no trouble" % (summary.get("listed"), summary.get("asked")))
        ck(all(v == "derived" for v in (summary.get("groups") or {}).values()) and len(summary.get("groups") or {}) == 3, "all three groups of documents were derived: %s" % summary.get("groups"))

        r.section("1a. The premium")
        P = docs.get(D.PREMIUM_KEY) or {}
        us, home, mo = P.get("us") or {}, P.get("home_page") or {}, P.get("monthly") or {}
        say("his page: %s -> %s %s" % (home.get("page_date"), home.get("page_values_pct"), home.get("page_labels")))
        say("the file's newest row: %s -> %s" % (us.get("as_of"), [(m["key"], m["pct"], m["his_heading"]) for m in us.get("measures") or []]))
        say("headline: %s%% over %s (T.Bond %s%%), S&P 500 %s, expected return %s%% | a month before %s | a year before %s" % (
            us.get("premium_pct"), us.get("over"), us.get("tbond_pct"), us.get("sp500"), us.get("expected_return_pct"), us.get("a_month_before"), us.get("a_year_before")))
        say("monthly: %s rows, %s to %s | headings: %s" % (mo.get("n"), mo.get("from"), mo.get("to"), " | ".join(c["label"] for c in mo.get("cols") or [])))
        say("measure columns: %s" % mo.get("measure_columns"))
        for row in (mo.get("rows") or [])[-3:]:
            say("  %s" % row)
        first_head = next((row for row in mo.get("rows") or [] if (mo.get("measure_columns") or {}).get("sustainable_payout") and row[[c["key"] for c in mo["cols"]].index(mo["measure_columns"]["sustainable_payout"])] is not None), None)
        say("the headline measure starts %s" % (first_head[0] if first_head else None))
        have = set(row[0][:7] for row in mo.get("rows") or [])
        y, m, gaps = 2008, 9, []
        while "%04d-%02d" % (y, m) <= (mo.get("to") or "")[:7]:
            if "%04d-%02d" % (y, m) not in have:
                gaps.append("%04d-%02d" % (y, m))
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        days = sorted(set(row[0][8:] for row in mo.get("rows") or []))
        say("months with no row in his file: %s | days of the month his rows are dated: %s | two rows in one month: %s" % (
            gaps or "none", days, sorted(k for k in have if sum(1 for row in mo["rows"] if row[0][:7] == k) > 1) or "none"))
        ck(len(us.get("measures") or []) == 5, "the five premiums his page states are each found in the monthly file")
        pv, fv = home.get("page_values_pct") or [], home.get("file_values_pct") or []
        ck(home.get("agree") is True and len(pv) == 5 and len(fv) == 5 and all(abs(a - b) < 0.011 for a, b in zip(pv, fv)),
           "the file's newest row is the page's five figures, in the page's order: %s against %s" % (fv, pv))
        ck((mo.get("from") or "")[:7] == "2008-09" and (mo.get("n") or 0) >= 200 and (mo.get("to") or "")[:7] == (home.get("page_date") or "")[:7],
           "monthly since %s, %s rows, to the month of his page (%s)" % (mo.get("from"), mo.get("n"), mo.get("to")))
        an, rt = P.get("annual") or {}, P.get("returns") or {}
        say("annual: %s rows, %s to %s | headings: %s" % (an.get("n"), an.get("from"), an.get("to"), " | ".join(c["label"] for c in an.get("cols") or [])))
        for row in (an.get("rows") or [])[-2:]:
            say("  %s" % row)
        say("returns: %s rows, %s to %s, updated %s | headings: %s" % (rt.get("n"), rt.get("from"), rt.get("to"), rt.get("updated"), " | ".join(c["label"] for c in rt.get("cols") or [])))
        for row in (rt.get("rows") or [])[-2:]:
            say("  %s" % row[:9])
        for p in rt.get("premiums_worked_here") or []:
            say("  premiums worked here %s-%s (%s years): stocks over bills %s arithmetic, %s compounded; over bonds %s, %s; stocks %s, %s" % (
                p["from"], p["to"], p["years"], pct(p["stocks_minus_bills"]["arithmetic"]), pct(p["stocks_minus_bills"]["compounded"]),
                pct(p["stocks_minus_bonds"]["arithmetic"]), pct(p["stocks_minus_bonds"]["compounded"]), pct(p["stocks"]["arithmetic"]), pct(p["stocks"]["compounded"])))
        ck(an.get("from") == 1960 and (an.get("n") or 0) >= 60 and rt.get("from") == 1928 and (rt.get("n") or 0) >= 95 and bool(rt.get("premiums_worked_here")),
           "annual premiums since 1960 (%s rows) and annual returns since 1928 (%s rows), with the premiums worked from them" % (an.get("n"), rt.get("n")))

        r.section("1b. Countries")
        K = docs.get(D.COUNTRY_KEY) or {}
        say("file %s, as of %s | mature market premium %s, US %s, equity-to-bond volatility %s | %s countries | other files %s" % (
            K.get("file"), K.get("as_of"), pct(K.get("mature_market_premium")), pct(K.get("us_premium")), K.get("equity_to_bond_volatility"), K.get("n"), K.get("other_files")))
        for inp in K.get("inputs") or []:
            say("  input: %s = %s (%s)" % (str(inp.get("label"))[:70], inp.get("value"), str(inp.get("note"))[:70]))
        rows = K.get("rows") or []
        for row in rows[:3] + [x for x in rows if x[0] in ("United States", "Germany", "Japan", "China", "India", "Brazil", "Turkey")]:
            say("  %s" % row)
        usa = next((x for x in rows if x[0] == "United States"), None)
        ck(len(rows) >= 140 and K.get("mature_market_premium") and 0.02 < K["mature_market_premium"] < 0.08 and usa is not None,
           "%s countries, a mature-market premium of %s, and the United States among them (%s)" % (len(rows), pct(K.get("mature_market_premium")), usa[4] if usa else None))
        fr = K.get("frontier") or {}
        say("frontier markets: %s | %s rows | headings: %s" % (fr.get("title"), len(fr.get("rows") or []), " | ".join(c["label"] for c in fr.get("cols") or [])))
        for x in (fr.get("rows") or [])[:3]:
            say("  %s" % x)
        odd = [x for x in rows if x[4] is not None and not 0.01 < x[4] < 0.6]
        blank = [x[0] for x in rows if x[4] is None]
        for x in odd[:12]:
            say("  outside 1%% to 60%%: %s" % x)
        say("countries with no premium: %s" % (blank or "none"))
        ck(not odd and not blank and len(rows) >= 140 and len(fr.get("rows") or []) >= 5, "every country's total premium is a premium (1%% to 60%%): %d outside, %d without one" % (len(odd), len(blank)))

        r.section("1c. Industries")
        I = docs.get(D.INDUSTRY_KEY) or {}
        say("as of %s | %s tables | regions: %s" % (I.get("as_of"), I.get("n_tables"), "; ".join("%s %s tables, %s industries, missing %s" % (g["code"], g["tables"], g["industries"], g["missing"]) for g in I.get("regions") or [])))
        say("files that hold no industry table (%d): %s" % (len(I.get("not_tables") or []), "; ".join("%s (%s)" % (x["file"], x["why"][:40]) for x in I.get("not_tables") or [])))
        topics = sorted(set(t["topic"] for t in I.get("tables") or []))
        say("topics (%d): %s" % (len(topics), ", ".join(topics)))
        tabs = {k.rsplit("/", 1)[-1][:-5]: v for k, v in docs.items() if "/tables/" in k}
        for tid in ("wacc-us", "margin-us", "roe-us", "eva-us", "pe-us", "pbv-us", "ps-us", "vebitda-us", "beta-us", "wacc-global"):
            t = tabs.get(tid)
            say("  %s: %s" % (tid, "absent" if not t else "%s rows, updated %s, header row of sheet '%s' | %s" % (t["n"], t["updated"], t["sheet"], " | ".join(c["label"] for c in t["cols"]))))
        b = (I.get("benchmarks") or {}).get("US") or {}
        say("US benchmarks: %s" % " | ".join(c["label"] for c in b.get("cols") or []))
        for row in (b.get("rows") or [])[:2] + [x for x in b.get("rows") or [] if x[0].lower().startswith(("total market", "software (system", "semiconductor", "banks (money"))]:
            say("  %s" % row)
        us_r = next((g for g in I.get("regions") or [] if g["code"] == "US"), {})
        gl_r = next((g for g in I.get("regions") or [] if g["code"] == "Global"), {})
        ck(us_r.get("tables", 0) >= 9 and us_r.get("industries", 0) >= 90 and gl_r.get("tables", 0) >= 9, "US: %s tables and %s industries; Global: %s tables (of the files read here)" % (us_r.get("tables"), us_r.get("industries"), gl_r.get("tables")))
        ck(not us_r.get("missing") and not gl_r.get("missing"), "every benchmark column is found for the US and for Global (missing: %s / %s)" % (us_r.get("missing"), gl_r.get("missing")))
        tm = next((x for x in b.get("rows") or [] if x[0].lower() == "total market"), None)
        keys = [c["key"] for c in b.get("cols") or []]
        coc = tm[keys.index("cost_of_capital")] if tm and "cost_of_capital" in keys else None
        ck(coc is not None and 0.04 < coc < 0.12, "the US total market's cost of capital reads %s" % pct(coc))
        sizes = {k: len(json.dumps(v, separators=(",", ":"))) for k, v in docs.items()}
        say("document sizes: premium %d, countries %d, industries %d bytes; %d tables, %d bytes in all" % (
            sizes.get(D.PREMIUM_KEY, 0), sizes.get(D.COUNTRY_KEY, 0), sizes.get(D.INDUSTRY_KEY, 0), len(tabs), sum(v for k, v in sizes.items() if "/tables/" in k)))

        # ------------------------------------------------------------------------------------------- part 2
        r.section("2. EVA: the engine's own whole-market pass, on 5.50% and on the bank's premium")
        new = us.get("premium_pct")
        try:
            live = json.loads(s3.get_object(Bucket=LIVE, Key="data/eva-spread.json")["Body"].read())
            w = live.get("wacc_inputs") or {}
            rf, ig = w.get("risk_free_10y_pct"), w.get("ig_credit_spread_pct")
            say("the live document: generated %s, version %s | Rf %s, credit spread %s, premium %s (%s)" % (live.get("generated_at"), live.get("version"), rf, ig, w.get("equity_risk_premium_pct"), w.get("source_erp")))
            esrc = ROOT / "aws" / "lambdas" / "justhodl-eva-spread" / "source"
            os.environ.setdefault("FMP_KEY", "")
            E = load("eva_engine", esrc / "lambda_function.py", [ROOT / "aws" / "shared", esrc])
            say("the engine here: version %s, factor book %s, statement bank %s" % (E.VERSION, "available" if E.book_ok() else "absent", "present" if E._bank else "absent"))

            def one(prem):
                E.set_premium(prem)
                m = E.market_pass(rf, ig)
                return m

            before = one(None)
            after = one({"pct": new, "as_of": us.get("as_of"), "fixed": False, "measure": us.get("measure"), "age_days": 0})
            lm = live.get("market") or {}
            say("live document's market block: %s valid, %s creators (%s%%), medians %s, regime %s, book %s" % (lm.get("n_valid"), lm.get("n_value_creators"), lm.get("pct_value_creators"), lm.get("medians"), lm.get("regime"), (lm.get("book_as_of") or {})))
            for tag, m in (("on 5.50%% (today's rule)", before), ("on %s%% (the bank's)" % new, after)):
                say("%s: %s valid, %s creators (%s%%), %s destroyers, medians %s, regime %s, super compounders %s, book %s" % (
                    tag, m.get("n_valid"), m.get("n_value_creators"), m.get("pct_value_creators"), m.get("n_value_destroyers"), m.get("medians"), m.get("regime"), m.get("n_super_compounders"), m.get("book_as_of")))
            ck(before.get("available") and after.get("available") and before.get("n_valid") == after.get("n_valid") and (before.get("n_valid") or 0) > 300,
               "both passes ran on the same %s companies" % before.get("n_valid"))
            same_book = (lm.get("book_as_of") or {}) == (before.get("book_as_of") or {})
            ck(not same_book or (lm.get("n_value_creators") == before.get("n_value_creators") and lm.get("medians") == before.get("medians")),
               "on the fixed premium the pass %s" % ("reproduces the live document's market block" if same_book else "runs on a newer factor book than the live document's, so the two are not compared"))
            rows = E._book.screen(lambda x: (x.get("mcap") or 0) >= E.MARKET_MIN_MCAP_USD and not x.get("is_adr") and x.get("roic") is not None and (x.get("invested_capital") or 0) > 0, fields=E.BOOK_FIELDS)
            rows.sort(key=lambda x: -(x.get("mcap") or 0))
            rows = rows[:E.MARKET_CANDIDATES]
            pair = []
            for x in rows:
                E.set_premium(None)
                a = E._eva_spread_only(x["symbol"], x.get("sector"), rf, ig, x)
                E.set_premium({"pct": new, "as_of": us.get("as_of"), "fixed": False, "measure": None, "age_days": 0})
                c = E._eva_spread_only(x["symbol"], x.get("sector"), rf, ig, x)
                if a.get("ok") and c.get("ok"):
                    pair.append((x["symbol"], x.get("mcap"), a, c))
            E.set_premium(None)
            dw = [a["wacc_pct"] - c["wacc_pct"] for _s, _m, a, c in pair]
            say("cost of capital, per company: falls by a median %.2f points (from %.2f to %.2f at the least and most); median %.2f%% before, %.2f%% after" % (
                statistics.median(dw), min(dw), max(dw), statistics.median(a["wacc_pct"] for _s, _m, a, c in pair), statistics.median(c["wacc_pct"] for _s, _m, a, c in pair)))
            crossed = [(s, m, a, c) for s, m, a, c in pair if a["eva_spread_pct"] <= 0 < c["eva_spread_pct"]]
            say("companies that turn from below their cost of capital to above it: %d of %d: %s" % (len(crossed), len(pair), ", ".join("%s (%.2f to %.2f)" % (s, a["eva_spread_pct"], c["eva_spread_pct"]) for s, m, a, c in sorted(crossed, key=lambda t: -(t[1] or 0))[:25])))
            say("the other way: %d" % sum(1 for s, m, a, c in pair if a["eva_spread_pct"] > 0 >= c["eva_spread_pct"]))
            top_b = [x["ticker"] for x in before.get("top_25_eva_spread") or []][:10]
            top_a = [x["ticker"] for x in after.get("top_25_eva_spread") or []][:10]
            say("top ten by spread before: %s" % ", ".join(top_b))
            say("top ten by spread after:  %s" % ", ".join(top_a))
            off = []
            for s_, m_, a, c in pair:
                comp = a["wacc_components"]
                beta = comp.get("beta") if comp.get("beta") is not None else 1.0
                want = comp["weight_equity"] * beta * (5.50 - new)
                if abs((a["wacc_pct"] - c["wacc_pct"]) - want) > 0.0021:
                    off.append((s_, round(a["wacc_pct"] - c["wacc_pct"], 3), round(want, 3)))
            ck(not off, "every company's cost of capital moves by its equity weight times its beta times the %.2f points the premium fell: %d of %d off%s" % (
                5.50 - new, len(off), len(pair), (" " + str(off[:5])) if off else ""))
            betas = [(a["wacc_components"].get("beta"), s_, a["wacc_pct"], c["wacc_pct"]) for s_, m_, a, c in pair if a["wacc_components"].get("beta") is not None]
            neg = sorted(b for b in betas if b[0] < 0)
            big = sorted((b for b in betas if b[0] > 3), reverse=True)
            say("found, not changed: %d companies carry a negative beta in the factor book (their cost of capital rises when the premium falls) and %d a beta above 3; a beta is used as it is: %s" % (
                len(neg), len(big), "; ".join("%s beta %.2f, cost of capital %.2f%% -> %.2f%%" % (s_, b, w0, w1) for b, s_, w0, w1 in (neg[:3] + big[:3]))))
            fifty = []
            for sym, sector in E.STATIC_TOP50_SPX:
                row = E._book.lookup(sym, fields=E.BOOK_FIELDS) if E.book_ok() else None
                if not row:
                    continue
                E.set_premium(None)
                a = E._eva_spread_only(sym, sector, rf, ig, row)
                E.set_premium({"pct": new, "as_of": us.get("as_of"), "fixed": False, "measure": None, "age_days": 0})
                c = E._eva_spread_only(sym, sector, rf, ig, row)
                if a.get("ok") and c.get("ok"):
                    fifty.append((sym, a, c))
            E.set_premium(None)
            say("the engine's fifty (%d in the book): creators %d before, %d after; median cost of capital %.2f%% to %.2f%%; median spread %.2f to %.2f" % (
                len(fifty), sum(1 for s, a, c in fifty if a["value_creator"]), sum(1 for s, a, c in fifty if c["value_creator"]),
                statistics.median(a["wacc_pct"] for s, a, c in fifty), statistics.median(c["wacc_pct"] for s, a, c in fifty),
                statistics.median(a["eva_spread_pct"] for s, a, c in fifty), statistics.median(c["eva_spread_pct"] for s, a, c in fifty)))
            for s, a, c in fifty[:6]:
                say("  %s: beta %s, cost of capital %.2f%% -> %.2f%%, spread %.2f -> %.2f" % (s, a["wacc_components"].get("beta"), a["wacc_pct"], c["wacc_pct"], a["eva_spread_pct"], c["eva_spread_pct"]))
            r.kv(eva_companies=len(pair), eva_median_wacc_before=round(statistics.median(a["wacc_pct"] for _s, _m, a, c in pair), 2),
                 eva_median_wacc_after=round(statistics.median(c["wacc_pct"] for _s, _m, a, c in pair), 2), eva_turned_creator=len(crossed))
        except Exception as e:
            ck(False, "the EVA comparison stopped: %s: %s" % (type(e).__name__, str(e)[:300]))

        r.kv(listed=summary.get("listed"), would_bank=len(summary.get("changed") or []), megabytes=round((summary.get("banked_bytes") or 0) / 1e6, 1),
             premium_pct=new, countries=len(K.get("rows") or []), tables=I.get("n_tables"), failed_checks=len(fails), writes=len(ro.writes))
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            sys.exit(1)
        r.ok("dry run complete: nothing written, no engine invoked")


if __name__ == "__main__":
    main()
