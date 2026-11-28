"""ops 7008 - read-only: can each company be placed in one of Damodaran's industries, by his own company list?

Khalid, 2026-11-28: the chart gets an EVA button; betas - "your call just implement what you think is best"; GF Value -
"replace its flat 9% with the industry cost of capital now in the warehouse". Both need each company's industry in his
terms. His two pages do not link a company list (251 files, none of companies). He keeps one, indname.xls.

This asks his site for that one file, and reads: its sheets and headings, a few rows, how its industry names agree with
the industry table already banked (wacc-us), how many of the EVA engine's 1,500 companies and of GF Value's companies
it places by ticker and which large ones it does not, and how many betas the factor book holds outside 0.2 to 3.
It also prints the shape of the GF Value document. Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
import collections
import importlib.util
import json
import os
import re
import statistics
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_7008_company_to_industry_probe"
LIVE = "justhodl-dashboard-live"
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
TRY = ["pc/datasets/indname.xls", "pc/datasets/indname.xlsx", "pc/indname.xls"]


def load(name, path, extra):
    for p in extra:
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def norm(t):
    return re.sub(r"[./ ]", "-", str(t).split(":")[-1].strip().upper())


def main():
    import boto3
    from ops_report import report
    s3 = boto3.client("s3")
    src = ROOT / "aws" / "lambdas" / "justhodl-damodaran" / "source"
    D = load("damodaran_engine", src / "lambda_function.py", [src])
    C = D.C
    with report(NAME) as r:
        say = r.log
        r.heading("7008 - his company list, and what it places (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. His pages and the file")
        for url in (D.HOME, D.CURRENT, C.SITE + "New_Home_Page/data.html", C.SITE + "New_Home_Page/datafile/indname.html"):
            try:
                _s, _h, body = D.fetch(url)
                html = body.decode("latin-1", "replace")
                say("%s: %d bytes; links naming a company list: %s" % (url.rsplit("/", 1)[-1], len(body), sorted(set(re.findall(r'href\s*=\s*["\']?([^"\'\s>]*(?:indname|company)[^"\'\s>]*)', html, re.I)))[:6]))
            except Exception as e:
                say("%s: %s" % (url.rsplit("/", 1)[-1], type(e).__name__))
        raw = None
        for path in TRY:
            try:
                st, hd, raw = D.fetch(C.SITE + path)
                say("%s: HTTP %s, %d bytes, Last-Modified %s, ETag %s" % (path, st, len(raw), hd.get("Last-Modified"), hd.get("ETag")))
                break
            except Exception as e:
                say("%s: %s %s" % (path, type(e).__name__, getattr(e, "code", "")))
        if raw is None:
            r.fail("his company list could not be fetched under any of the names tried")
            sys.exit(1)
        bk = C.book(raw)
        say("sheets: %s" % "; ".join("%s (%d rows)" % (n, len(rows)) for n, rows in bk["sheets"].items()))
        us_name = next((n for n in bk["sheets"] if re.search(r"\bus\b", n, re.I)), None) or next(iter(bk["sheets"]))
        for n, rows in bk["sheets"].items():
            used = [x for x in rows if any(c not in (None, "") for c in x)]
            for x in used[:4 if n == us_name else 2]:
                say("  %s | %s" % (n[:22], " | ".join(str(c)[:26] for c in x[:9])))
        rows = bk["sheets"][us_name]
        h = next(i for i, x in enumerate(rows[:20]) if any(isinstance(c, str) and re.search(r"ticker", c, re.I) for c in x))
        head = [str(c or "").strip() for c in rows[h]]
        jt = next(i for i, c in enumerate(head) if re.search(r"ticker", c, re.I))
        ji = next(i for i, c in enumerate(head) if re.search(r"industry", c, re.I))
        listed = {}
        for x in rows[h + 1:]:
            if len(x) > max(jt, ji) and isinstance(x[jt], str) and isinstance(x[ji], str) and x[jt].strip() and x[ji].strip():
                listed.setdefault(norm(x[jt]), x[ji].strip())
        say("the US sheet '%s': headings %s | %d companies with a ticker and an industry, %d industries" % (us_name, head[:9], len(listed), len(set(listed.values()))))
        exch = collections.Counter(str(x[jt]).split(":")[0] for x in rows[h + 1:] if len(x) > jt and isinstance(x[jt], str) and ":" in x[jt])
        say("exchanges in its tickers: %s" % exch.most_common(8))

        r.section("2. Its industries against the banked industry table")
        W = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/damodaran/tables/wacc-us.json")["Body"].read())
        table = set(x[0] for x in W["rows"])
        mine = set(listed.values())
        say("wacc-us headings: %s" % [c["label"] for c in W["cols"]])
        say("industries in both: %d | only in the company list: %s | only in the table: %s" % (len(mine & table), sorted(mine - table)[:12], sorted(table - mine)[:12]))

        r.section("3. What it places")
        esrc = ROOT / "aws" / "lambdas" / "justhodl-eva-spread" / "source"
        os.environ.setdefault("FMP_KEY", "")
        E = load("eva_engine", esrc / "lambda_function.py", [ROOT / "aws" / "shared", esrc])
        book = E._book.screen(lambda x: (x.get("mcap") or 0) >= E.MARKET_MIN_MCAP_USD and not x.get("is_adr") and x.get("roic") is not None and (x.get("invested_capital") or 0) > 0, fields=E.BOOK_FIELDS + ["industry"])
        book.sort(key=lambda x: -(x.get("mcap") or 0))
        book = book[:E.MARKET_CANDIDATES]
        say("a factor book row carries: %s" % sorted(book[0].keys()))
        hit = [x for x in book if norm(x["symbol"]) in listed]
        miss = [x for x in book if norm(x["symbol"]) not in listed]
        say("EVA's %d companies: %d placed by ticker (%.1f%%), %d not: %s" % (len(book), len(hit), 100.0 * len(hit) / len(book), len(miss), ", ".join("%s (%s)" % (x["symbol"], x.get("sector")) for x in miss[:40])))
        bad = [x for x in book if x.get("beta") is None or not 0.2 <= x["beta"] <= 3.0]
        say("betas outside 0.2 to 3, or absent: %d of %d (negative %d, 0 to 0.2 %d, above 3 %d, absent %d); of them placed by his list: %d" % (
            len(bad), len(book), sum(1 for x in bad if x.get("beta") is not None and x["beta"] < 0), sum(1 for x in bad if x.get("beta") is not None and 0 <= x["beta"] < 0.2),
            sum(1 for x in bad if x.get("beta") is not None and x["beta"] > 3), sum(1 for x in bad if x.get("beta") is None), sum(1 for x in bad if norm(x["symbol"]) in listed)))
        for x in sorted(bad, key=lambda x: -(x.get("mcap") or 0))[:14]:
            say("  %s: beta %s, $%.0fbn, sector %s, his industry %s" % (x["symbol"], x.get("beta"), (x.get("mcap") or 0) / 1e9, x.get("sector"), listed.get(norm(x["symbol"]))))
        good = [x for x in book if x.get("beta") is not None and 0.2 <= x["beta"] <= 3.0]
        by = collections.defaultdict(list)
        for x in good:
            by[x.get("sector")].append(x["beta"])
        say("median usable beta by sector: %s" % {k: round(statistics.median(v), 2) for k, v in sorted(by.items(), key=lambda kv: str(kv[0]))})
        jb = [c["key"] for c in W["cols"]].index("beta")
        ind_beta = {x[0]: x[jb] for x in W["rows"]}
        gaps = [abs(x["beta"] - ind_beta[listed[norm(x["symbol"])]]) for x in good if norm(x["symbol"]) in listed and ind_beta.get(listed[norm(x["symbol"])]) is not None]
        say("a usable own beta against its industry's beta in his table: median distance %.2f over %d companies" % (statistics.median(gaps), len(gaps)))

        G = json.loads(s3.get_object(Bucket=LIVE, Key="data/gf-value.json")["Body"].read())
        say("GF Value document: keys %s" % sorted(G.keys()))
        lists = {k: v for k, v in G.items() if isinstance(v, list) and v and isinstance(v[0], dict)}
        for k, v in lists.items():
            say("  %s: %d rows; a row: %s" % (k, len(v), json.dumps(v[0])[:420]))
        syms = set()
        for v in lists.values():
            syms.update(str(x.get("ticker") or x.get("symbol") or "") for x in v)
        syms.discard("")
        say("GF Value's companies: %d named in the document, %d placed by his list; not placed: %s" % (len(syms), sum(1 for t in syms if norm(t) in listed), sorted(t for t in syms if norm(t) not in listed)[:30]))
        say("GF Value: version %s, generated %s, summary %s" % (G.get("version"), G.get("generated_at"), json.dumps({k: v for k, v in G.items() if not isinstance(v, (list, dict))})[:500]))
        say("GF Value methodology: %s" % json.dumps(G.get("methodology") or G.get("config") or {})[:500])
        r.kv(companies_listed=len(listed), industries=len(mine), eva_placed=len(hit), eva_not_placed=len(miss), unusable_betas=len(bad))
        r.ok("read; nothing invoked, nothing written")


if __name__ == "__main__":
    main()
