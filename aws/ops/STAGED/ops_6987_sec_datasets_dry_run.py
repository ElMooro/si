"""ops 6987 - justhodl-sec-datasets, dry run on the runner: the engine's own code against the SEC's real files.

Khalid 2026-11-26: "go and add as many files as my system needs ...". Before the engine is deployed or anything is
banked, its modules are run here against sec.gov and NOTHING is written to the warehouse:

  1. the engine's own dry run: every data-set page read, what a first run would bank and in what order
  2. the newest month of money market funds through sec_derive.nmfp_month, with its own arithmetic checked
     (categories add up to the funds' assets, repo adds up from its parts, one filing per fund)
  3. the newest quarter of fund holdings through sec_derive.nport_quarter, timed stage by stage (to size the Lambda),
     with the 24 bonds of the chart he sent looked up in the table it builds
  4. the identifier check: every CUSIP and ISIN in the SEC's file is collected, and none may appear in any table the
     engine would publish
  5. the documents embedded (gzip + base64) so the desks can be rehearsed offline

Security identifiers are held in memory for step 4 only and never logged. The SEC contact string is sent in a header
and never logged; every logged line passes through clean(). Safe to run again.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import os
import re
import resource
import sys
import tempfile
import time
import traceback
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-sec-datasets" / "source"))
sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))
import lambda_function as LF  # noqa: E402
import sec_derive as D  # noqa: E402
import sec_sets as S  # noqa: E402

NAME = "ops_6987_sec_datasets_dry_run"
MAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
CHART = [
    ("CRWV 9 '31", 9.0, "2031", r"COREWEAVE"), ("CRWV 9.75 '31", 9.75, "2031", r"COREWEAVE"), ("CRWV 9.25 '30", 9.25, "2030", r"COREWEAVE"),
    ("CRWV 9.625 '32", 9.625, "2032", r"COREWEAVE"), ("EDGCOM 7.5 '31", 7.5, "2031", r"EDG"), ("CORZ 7.75 '31", 7.75, "2031", r"CORE SCI"),
    ("DDRIVE 8.875 '31", 8.875, "2031", r"DRIVE"), ("ELKGVP 7.5 '31", 7.5, "2031", r"ELK"), ("APLD 9.25 '30", 9.25, "2030", r"APLD|APPLIED DIG"),
    ("GALAXY 9.875 '31", 9.875, "2031", r"GALAXY"), ("ELNFOR 7 '31", 7.0, "2031", r"APLD COMPUTECO 3|ELLENDALE|ELN"), ("SECMOS 8.875 '31", 8.875, "2031", r"COSMOS"),
    ("ZENARC 8.875 '31", 8.875, "2031", r"ZENITH"), ("TRACTC 5.875 '31", 5.875, "2031", r"SV RNO PROPERTY|TRACT"),
    ("PFORGE 6.75 '31", 6.75, "2031", r"APLD COMPUTECO 2|POLARIS|FORGE"), ("TRACTD 6.5 '31", 6.5, "2031", r"PR RNO PROPERTY|TRACT"),
    ("YNDRDC 6.875 '31", 6.875, "2031", r"YONDR|YNDR"), ("FLASHC 7.25 '30", 7.25, "2030", r"FLASH"), ("MERIDI 6.25 '31", 6.25, "2031", r"MERIDIAN"),
    ("CIFR 7.125 '30", 7.125, "2030", r"CIPHER|CIFR"), ("WULF 7.75 '30", 7.75, "2030", r"WULF"), ("CLSPRK 7.875 '31", 7.875, "2031", r"SPARK|CLSP"),
    ("STNGRY 6 '31", 6.0, "2031", r"STINGRAY|STNGR"), ("BLKPRL 6.125 '31", 6.125, "2031", r"PEARL|BLKP"),
]
HINT = re.compile(r"COMPUTE|DATA CENT|DATACENT|COREWEAVE|CORE SCI|CIPHER|WULF|GALAXY DIG|APLD|APPLIED DIG|ZENITH ARC|MERIDIAN ARC|YONDR|TRACT CAP|HUT 8|"
                  r"BEACON POINT|CLEANSPARK|\bIREN\b|CRUSOE|NSCALE|FLUIDSTACK|BLACK PEARL|STINGRAY|COSMOS|VANTAGE DATA|ALIGNED DATA|CYRUSONE|STACK INFRA|"
                  r"\bX\.?AI\b|BEIGNET|RNO PROPERTY", re.I)
_say = None


def clean(text):
    return MAIL.sub("(address held back)", str(text))


def say(msg):
    _say(clean(msg))


def bn(x):
    return "$%.1fbn" % ((x or 0) / 1e9)


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def embed(r, name, obj):
    blob = base64.b64encode(gzip.compress(json.dumps(obj, separators=(",", ":"), allow_nan=False).encode(), 9)).decode()
    r.lines.append("<!-- FIXTURE %s %d bytes gzip+base64" % (name, len(blob)))
    r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
    r.lines.append("FIXTURE-END -->")
    say("fixture %s embedded: %d characters" % (name, len(blob)))


def fetch(url, work):
    path = os.path.join(work, url.rsplit("/", 1)[-1])
    t0 = time.time()
    status, _, headers = LF.sec_get(url, to_path=path, timeout=600)
    if status != 200:
        raise RuntimeError("download answered %s for %s" % (status, url.rsplit("/", 1)[-1]))
    say("fetched %s: %.1f MB in %.0fs (declared %s bytes)" % (url.rsplit("/", 1)[-1], os.path.getsize(path) / 1048576, time.time() - t0, headers.get("content-length")))
    return path


def main():
    global _say
    from ops_report import report
    with report(NAME) as r:
        _say = r.log
        fails = []

        def check(ok, text):
            (r.ok if ok else r.fail)(clean(text))
            if not ok:
                fails.append(text)

        r.heading("ops 6987 - justhodl-sec-datasets, dry run on the runner %s" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        work = tempfile.mkdtemp(prefix="secds-dry-")
        LF.WORK = os.path.join(work, "engine")

        r.section("1. the engine's own dry run: pages read, nothing fetched, nothing written")
        t0 = time.time()
        dry = LF.lambda_handler({"dry_run": True}, None)
        say("version %s | %.0fs | advertised %s" % (dry["version"], time.time() - t0, json.dumps(dry["advertised"])))
        say("newest file of each set: %s" % json.dumps(dry["newest"]))
        say("already banked: %s | waiting to be read: %s" % (json.dumps(dry["already_banked"]), json.dumps(dry["waiting_to_be_read"])))
        say("a first run would bank %d files, %.2f GB by the sizes the pages show:" % (len(dry["would_bank"]), dry["would_bank_bytes_shown"] / 1024 ** 3))
        for s_, name, size, tier in dry["would_bank"]:
            say("  %-9s %-40s %10s %s" % (s_, name, ("%.1f MB" % (size / 1048576)) if size else "?", tier))
        for note in dry["notes"]:
            say("  note: %s" % note)
        check(set(dry["advertised"]) == set(S.SETS), "all seven data-set pages answered with files")
        check(len({b[0] for b in dry["would_bank"][:7]}) == 7 and all(b[3] == "newest" for b in dry["would_bank"][:7]), "the newest file of every set is banked before any history")
        total_files = sum(dry["advertised"].values())
        say("files advertised across the seven sets: %d" % total_files)

        library = {}
        for name, spec in S.SETS.items():
            status, body, _ = LF.sec_get(spec["page"])
            library[name] = S.advertised(body, spec["page"], spec["match"]) if status == 200 and body else []
        say("history each set reaches back to: %s" % json.dumps({n: (f[-1]["name"], len(f)) for n, f in library.items() if f}))
        say("bytes shown on the pages, all files: %s" % json.dumps({n: "%.2f GB" % (sum(x.get("size_shown") or 0 for x in f) / 1024 ** 3) for n, f in library.items()}))

        r.section("2. the newest month of money market funds (sec_derive.nmfp_month)")
        try:
            path = fetch(library["nmfp"][0]["url"], work)
            t0 = time.time()
            with zipfile.ZipFile(path) as zf:
                month = D.nmfp_month(zf)
            say("read in %.1fs | month %s | report dates %s | funds %d of %d filings | net assets %s" % (
                time.time() - t0, month["month"], json.dumps(month["report_dates"]), month["funds"], month["filings_in_file"], bn(month["net_assets"])))
            for t in month["by_type"]:
                say("  %-22s %4d funds %10s | average maturity %s days, life %s days | 7-day gross yield %s | daily liquid %s | weekly liquid %s" % (
                    t["type"], t["funds"], bn(t["net_assets"]), t["wam_days"], t["wal_days"], t["gross_yield_7d"], t["daily_liquid_pct"], t["weekly_liquid_pct"]))
            say("what they hold:")
            for h in month["holdings"]:
                say("  %-52s %7s rows %10s" % (h["category"][:52], format(h["rows"], ","), bn(h["value"])))
            rp = month["repo"]
            say("repo: total %s = Treasury %s + agency %s + other %s | cleared %s | tri-party %s | through the clearing house %s | at the Federal Reserve %s | %d counterparties" % (
                bn(rp["total"]), bn(rp["treasury"]), bn(rp["agency"]), bn(rp["other"]), bn(rp["cleared"]), bn(rp["triparty"]), bn(rp["via_ficc"]), bn(rp["federal_reserve"]), rp["n_counterparties"]))
            for c in rp["counterparties"][:20]:
                say("  %-44s %-5s %10s %4d funds" % (c[0][:44], c[1], bn(c[2]), c[3]))
            say("collateral: %s" % "; ".join("%s %s" % (k, bn(v)) for k, v in rp["collateral"][:8]))
            un = month["unsecured"]
            say("unsecured: %s to %d issuers; largest: %s" % (bn(un["total"]), un["n_issuers"], "; ".join("%s %s" % (i[0][:30], bn(i[1])) for i in un["issuers"][:12])))
            days = month["flows"]["days"]
            say("daily flows: %d days %s to %s | month's net %s | the last five: %s" % (len(days), days[0][0] if days else "-", days[-1][0] if days else "-", bn(sum(d[3] for d in days)),
                                                                                    "; ".join("%s %s" % (d[0], bn(d[3])) for d in days[-5:])))
            held = sum(h["value"] for h in month["holdings"])
            check(month["funds"] >= 250, "money funds counted: %d (both month-end dates)" % month["funds"])
            check(5e12 <= month["net_assets"] <= 1.1e13, "net assets are of the industry's size: %s" % bn(month["net_assets"]))
            check(abs(held / month["net_assets"] - 1) < 0.15, "what they hold adds up to their net assets within 15%%: %s against %s" % (bn(held), bn(month["net_assets"])))
            check(abs(rp["treasury"] + rp["agency"] + rp["other"] - rp["total"]) <= 3, "repo adds up from its three kinds of collateral")
            check(sum(t["net_assets"] for t in month["by_type"]) == month["net_assets"] or abs(sum(t["net_assets"] for t in month["by_type"]) - month["net_assets"]) <= len(month["by_type"]),
                  "fund types add up to the total")
            check(15 <= len(days) <= 24, "a month of business days carries flows: %d" % len(days))
            check(rp["via_ficc"] <= rp["total"] and rp["cleared"] <= rp["total"] and sum(c[2] for c in rp["counterparties"]) <= rp["total"] + 1, "no part of repo exceeds the whole")
            say("the month's line in the bank's manifest: %s" % json.dumps(D.nmfp_summary(month))[:600])
            embed(r, "sec-money-funds-month", month)
        except Exception:                     # noqa: BLE001
            check(False, "money funds section stopped: %s" % clean(traceback.format_exc())[-700:])
            month = None

        r.section("3. the newest quarter of fund holdings (sec_derive.nport_quarter), timed")
        docs = None
        try:
            path = fetch(library["nport"][0]["url"], work)
            marks, t0 = [], time.time()

            def progress(msg):
                if msg:
                    marks.append("%5.0fs %6.0f MB  %s" % (time.time() - t0, rss_mb(), msg))
            docs = D.nport_quarter(path, work, progress=progress)
            took = time.time() - t0
            for m in marks:
                say("  " + m)
            say("whole read: %.0fs on the runner, peak memory %.0f MB" % (took, rss_mb()))
            s = docs["summary"]
            say("as of %s | report dates %s | funds %s of %s filings | net assets %s | holding rows %s" % (
                s["as_of"], json.dumps(s["report_dates"])[:300], format(s["funds"], ","), format(s["filings_in_file"], ","), bn(s["net_assets"]), format(s["holding_rows"], ",")))
            for c in s["classes"][:14]:
                say("  %-9s %-28s %10s rows %12s | priced by model %8s rows %10s" % (c["code"], c["name"], format(c["rows"], ","), bn(c["value"]), format(c["level3_rows"], ","), bn(c["level3_value"])))
            say("net shareholder flows by month (funds reporting): %s" % "; ".join("%s %s (%d)" % (m, bn(f["net"]), f["funds"]) for m, f in list(s["flows_by_month"].items())[-6:]))
            b = docs["bonds"]
            rows = [dict(zip(b["cols"], x)) for x in b["rows"]]
            say("bonds and loans: %s | %s" % (format(len(rows), ","), json.dumps(b["totals"])))
            by_cls = {}
            for x in rows:
                g = by_cls.setdefault(x["cls"], [0, 0.0, 0])
                g[0] += 1
                g[1] += x["value"] or 0
                g[2] += 1 if x["n_px"] else 0
            say("by class (securities | value | with a mark): %s" % "; ".join("%s %s %s %s" % (k, format(v[0], ","), bn(v[1]), format(v[2], ",")) for k, v in sorted(by_cls.items(), key=lambda kv: -kv[1][1])))
            spread = sorted(x["px_hi"] - x["px_lo"] for x in rows if x["n_px"] >= 5)
            if spread:
                say("bonds marked by five or more funds: %s | gap between the 10th and 90th percentile mark: median %.2f, 90th pct %.2f, 99th pct %.2f" % (
                    format(len(spread), ","), spread[len(spread) // 2], spread[int(len(spread) * 0.9)], spread[int(len(spread) * 0.99)]))
            say("the 24 bonds of the chart, in the table (issuer | funds | face | middle mark, low, high | as of | flags):")
            found = 0
            for label, cpn, year, pat in CHART:
                rx = re.compile(pat, re.I)
                hit = [x for x in rows if x["cpn"] is not None and abs(x["cpn"] - cpn) < 0.0051 and (x["mat"] or "").startswith(year) and rx.search("%s %s" % (x["issuer"], x["title"]))]
                if not hit:
                    say("  %-18s not in the table" % label)
                    continue
                found += 1
                x = max(hit, key=lambda h: h["funds"])
                say("  %-18s %-34s %4d funds | face $%4.0fm | %6s %6s %6s | %s | restricted %d, default %d%s" % (
                    label, (x["issuer"] or "")[:34], x["funds"], (x["face"] or 0) / 1e6, x["px"], x["px_lo"], x["px_hi"], x["d"], x["n_restricted"], x["n_default"],
                    " | %d rows matched" % len(hit) if len(hit) > 1 else ""))
            ai = [x for x in rows if HINT.search(x["issuer"] or "")]
            say("AI data-centre and compute securities in the table: %d worth %s" % (len(ai), bn(sum(x["value"] or 0 for x in ai))))
            say("largest securities flagged in default (issuer | title | funds | value | mark):")
            for x in sorted((x for x in rows if x["n_default"]), key=lambda x: -(x["value"] or 0))[:10]:
                say("  %-36s %-28s %4d %9s %7s" % ((x["issuer"] or "")[:36], (x["title"] or "")[:28], x["funds"], "$%.0fm" % ((x["value"] or 0) / 1e6), x["px"]))
            c = docs["cds"]
            say("credit swaps: %s" % json.dumps(c["totals"]))
            say("names (reference | positions | funds | sold | bought | net sold):")
            for n in c["names"][:22]:
                say("  %-44s %5d %4d  sold %9s  bought %9s  net %9s" % (n[0][:44], n[1], n[2], "$%.0fm" % (n[3] / 1e6), "$%.0fm" % (n[4] / 1e6), "$%.0fm" % (n[6] / 1e6)))
            say("indices: %s" % "; ".join("%s sold %s bought %s" % (n[0], bn(n[3]), bn(n[4])) for n in c["indices"][:10]))
            ln = docs["stock_loan"]
            say("stock on loan: %s" % json.dumps(ln["totals"]))
            lent = sorted((x for x in ln["rows"] if x[3] >= 5e8 and x[6] is not None), key=lambda x: -x[6])
            say("most lent, among stocks funds hold $500m or more of (ticker | issuer | held | on loan | share | funds lending of funds):")
            for x in lent[:20]:
                say("  %-7s %-38s %9s %9s %5.1f%% %4d of %4d" % (x[0] or "-", (x[1] or "")[:38], bn(x[3]), bn(x[5]), x[6], x[8], x[7]))
            say("rows with a ticker: %d of %d" % (sum(1 for x in ln["rows"] if x[0]), len(ln["rows"])))
            sizes = {k: (len(json.dumps(v, separators=(",", ":"))), len(gzip.compress(json.dumps(v, separators=(",", ":")).encode(), 6))) for k, v in docs.items()}
            say("document sizes (plain, gzip): %s" % json.dumps({k: "%.1f MB, %.1f MB" % (a / 1048576, z / 1048576) for k, (a, z) in sizes.items()}))
            check(s["funds"] >= 10000 and 3e13 <= s["net_assets"] <= 7e13, "funds %s, net assets %s" % (format(s["funds"], ","), bn(s["net_assets"])))
            check(len(rows) >= 150000, "securities in the table: %s" % format(len(rows), ","))
            check(found >= 20, "of the chart's 24 bonds, in the table: %d" % found)
            check(c["totals"]["reference_names"] >= 400 and c["totals"]["positions"] >= 8000, "credit swaps: %d names, %d positions" % (c["totals"]["reference_names"], c["totals"]["positions"]))
            check(3e11 <= ln["totals"]["on_loan"] <= 7e11, "stock on loan: %s of %s held" % (bn(ln["totals"]["on_loan"]), bn(ln["totals"]["held"])))
            check(took < 420, "the read fits a Lambda run with room: %.0fs here" % took)

            r.section("4. no identifier in anything the engine would publish")
            cusips, isins = set(), set()
            with zipfile.ZipFile(path) as zf:
                header, it = D.rows_of(zf, D.member(zf, "FUND_REPORTED_HOLDING"))
                ci = header.index("ISSUER_CUSIP")
                for parts in it:
                    v = parts[ci]
                    if len(v) == 9 and v not in ("000000000", "N/A"):
                        cusips.add(v)
                info = D.member(zf, "IDENTIFIERS")
                if info is not None:
                    header, it = D.rows_of(zf, info)
                    ii = header.index("IDENTIFIER_ISIN")
                    for parts in it:
                        if len(parts[ii]) == 12:
                            isins.add(parts[ii])
            text = json.dumps(docs)
            if month:
                text += json.dumps(month)
            tokens9 = set(re.findall(r"(?<![A-Za-z0-9])[A-Z0-9]{9}(?![A-Za-z0-9])", text))
            tokens12 = set(re.findall(r"(?<![A-Za-z0-9])[A-Z0-9]{12}(?![A-Za-z0-9])", text))
            leak9 = {t for t in tokens9 & cusips if re.search(r"\d", t)}
            words = (tokens9 & cusips) - leak9
            leak12 = tokens12 & isins
            say("identifiers in the SEC's file: %s CUSIPs, %s ISINs | nine-character tokens in the tables: %s | twelve-character: %s" % (
                format(len(cusips), ","), format(len(isins), ","), format(len(tokens9), ","), format(len(tokens12), ",")))
            say("tokens that equal a CUSIP and carry a digit: %d%s | plain words that happen to equal one: %d | tokens that equal an ISIN: %d" % (
                len(leak9), (" (shapes: %s)" % ", ".join(sorted(re.sub(r"[A-Z]", "A", re.sub(r"\d", "9", t)) for t in list(leak9)[:6]))) if leak9 else "", len(words), len(leak12)))
            check(not leak9 and not leak12, "no CUSIP and no ISIN of the SEC's file appears in any table")

            r.section("5. the documents, for rehearsing the desks offline")
            head = {"summary": s, "cds": {"totals": c["totals"], "cols": c["cols"], "names": c["names"][:300], "indices": c["indices"][:40], "counterparties": c["counterparties"][:25]},
                    "stock_loan": {"totals": ln["totals"], "cols": ln["cols"], "most_lent": lent[:150], "largest": ln["rows"][:150]}}
            embed(r, "sec-fund-holdings-head", head)
            keep = {id(x) for x in sorted(rows, key=lambda x: -(x["value"] or 0))[:250]} | {id(x) for x in ai} | {id(x) for x in sorted((x for x in rows if x["n_default"]), key=lambda x: -(x["value"] or 0))[:150]}
            embed(r, "sec-fund-bonds-sample", {"cols": b["cols"], "rows": [[x[k] for k in b["cols"]] for x in rows if id(x) in keep], "totals": b["totals"], "as_of": b["as_of"]})
        except Exception:                     # noqa: BLE001
            check(False, "fund holdings section stopped: %s" % clean(traceback.format_exc())[-900:])

        r.kv(failed_checks=len(fails), files_advertised=total_files)
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            raise SystemExit(1)
        r.ok("dry run complete: nothing was written")


if __name__ == "__main__":
    main()
