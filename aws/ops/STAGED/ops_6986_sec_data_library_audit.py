"""ops 6986 - read-only audit of the SEC's data library, before anything is banked or built.

Khalid 2026-11-26 19:02 ET: "go and add as many files as my system needs especially the ones that would improve my
system and its data exponentially".

45 engines already read SEC filings one company at a time. None reads the SEC's bulk data sets except fails-to-deliver
and the financial statement sets. This op measures them on the runner and writes NOTHING to the warehouse:

  1. the library: every data-set page, how many files, the newest file and its size
  2. Form N-PORT, newest quarter, opened whole: funds, holdings by asset class, bonds and loans (marks, defaults,
     arrears, paid-in-kind), the 24 bonds of the chart he sent, AI data-centre issuers, credit default swaps funds
     hold (reference names, direction, counterparties), securities on loan, repo
  3. Form N-MFP, newest month: money funds by type, what they hold, who they lend to in repo, daily flows
  4. BDC data sets: the schedule-of-investments file - lenders, loans, cost against fair value
  5. a look inside the other sets small enough to open (members, columns, rows)

Security identifiers (CUSIP, ISIN, ticker columns of the identifier tables) are never printed or kept: they are used
on the runner only to group one bond's rows. The SEC contact string is sent in a header and never logged; every logged
line passes through clean(). Safe to run again.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import html
import io
import json
import os
import re
import sqlite3
import statistics
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request
import zipfile
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
NAME = "ops_6986_sec_data_library_audit"
BUCKET = "justhodl-dashboard-live"
BASE = "https://www.sec.gov/data-research/sec-markets-data/"
HUBS = ["https://www.sec.gov/data-research/sec-markets-data", "https://www.sec.gov/data-research/structured-data",
        "https://www.sec.gov/about/dera_data-library", "https://www.sec.gov/dera/data"]
SLUGS = ["form-n-port-data-sets", "dera-form-n-mfp-data-sets", "bdc-data-sets", "form-13f-data-sets", "insider-transactions-data-sets",
         "form-d-data-sets", "form-n-cen-data-sets", "financial-statement-data-sets", "financial-statement-notes-data-sets",
         "regulation-data-sets", "crowdfunding-offerings-data-sets", "mutual-fund-prospectus-risk-return-summary-data-sets",
         "form-144-data-sets", "form-n-px-data-sets", "fails-deliver-data", "variable-insurance-product-data-sets",
         "transfer-agent-data-sets", "edgar-log-file-data-sets", "form-sho-data", "security-based-swap-data"]
PEEK = re.compile(r"13f|insider|form-d-|n-cen|144|n-px|sho|swap|regulation", re.I)
PEEK_MAX = 220 * 1024 * 1024
MAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")

# the chart's rows: label as printed, coupon, maturity year, a pattern for the issuer's name where it is known
CHART = [
    ("CRWV 9 '31", 9.0, "2031", r"COREWEAVE"), ("CRWV 9.75 '31", 9.75, "2031", r"COREWEAVE"),
    ("CRWV 9.25 '30", 9.25, "2030", r"COREWEAVE"), ("CRWV 9.625 '32", 9.625, "2032", r"COREWEAVE"),
    ("EDGCOM 7.5 '31", 7.5, "2031", r"EDG"), ("CORZ 7.75 '31", 7.75, "2031", r"CORE SCI"),
    ("DDRIVE 8.875 '31", 8.875, "2031", r"DRIVE"), ("ELKGVP 7.5 '31", 7.5, "2031", r"ELK"),
    ("APLD 9.25 '30", 9.25, "2030", r"APLD|APPLIED DIG"), ("GALAXY 9.875 '31", 9.875, "2031", r"GALAXY"),
    ("ELNFOR 7 '31", 7.0, "2031", r"APLD COMPUTECO 3|ELLENDALE|ELN"), ("SECMOS 8.875 '31", 8.875, "2031", r"COSMOS"),
    ("ZENARC 8.875 '31", 8.875, "2031", r"ZENITH"), ("TRACTC 5.875 '31", 5.875, "2031", r"SV RNO PROPERTY|TRACT"),
    ("PFORGE 6.75 '31", 6.75, "2031", r"APLD COMPUTECO 2|POLARIS|FORGE"), ("TRACTD 6.5 '31", 6.5, "2031", r"PR RNO PROPERTY|TRACT"),
    ("YNDRDC 6.875 '31", 6.875, "2031", r"YONDR|YNDR"), ("FLASHC 7.25 '30", 7.25, "2030", r"FLASH"),
    ("MERIDI 6.25 '31", 6.25, "2031", r"MERIDIAN"), ("CIFR 7.125 '30", 7.125, "2030", r"CIPHER|CIFR"),
    ("WULF 7.75 '30", 7.75, "2030", r"WULF"), ("CLSPRK 7.875 '31", 7.875, "2031", r"SPARK|CLSP"),
    ("STNGRY 6 '31", 6.0, "2031", r"STINGRAY|STNGR"), ("BLKPRL 6.125 '31", 6.125, "2031", r"PEARL|BLKP"),
]
HINT = re.compile(r"COMPUTE|DATA CENT|DATACENT|COREWEAVE|CORE SCI|CIPHER|WULF|GALAXY DIG|APLD|APPLIED DIG|ZENITH ARC|MERIDIAN ARC|YONDR|"
                  r"TRACT CAP|FLASH COMPUTE|HUT 8|BEACON POINT|CLEANSPARK|\bIREN\b|CRUSOE|NSCALE|FLUIDSTACK|BLACK PEARL|STINGRAY|COSMOS|"
                  r"VANTAGE DATA|ALIGNED DATA|CYRUSONE|STACK INFRA|\bX\.?AI\b|BEIGNET|RNO PROPERTY", re.I)

_UA = None
_say = None


def clean(text):
    """The SEC contact string never reaches the report: any address-shaped text is taken out of what is logged."""
    return MAIL.sub("(address held back)", str(text))


def say(msg):
    _say(clean(msg))


def ua():
    global _UA
    if _UA is None:
        from edgar import USER_AGENT          # the house SEC contact string; sent in a header, never printed
        _UA = USER_AGENT
    return _UA


def get(url, to_path=None, timeout=90, method="GET"):
    """(status, bytes or None, headers dict). Streams to to_path when given. Polite: one request, then a short pause."""
    req = urllib.request.Request(url, method=method, headers={"User-Agent": ua(), "Accept": "*/*"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            headers = {k.lower(): v for k, v in resp.headers.items()}
            if method == "HEAD":
                return resp.status, None, headers
            if to_path:
                with open(to_path, "wb") as fh:
                    while True:
                        block = resp.read(1 << 20)
                        if not block:
                            break
                        fh.write(block)
                return resp.status, None, headers
            return resp.status, resp.read(), headers
    except urllib.error.HTTPError as exc:
        return exc.code, None, {}
    except Exception as exc:                  # noqa: BLE001 - a timeout or reset is reported, not raised
        return 0, clean(repr(exc)).encode(), {}
    finally:
        time.sleep(0.25)


def size_text(n):
    if n is None:
        return "?"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return ("%.0f %s" if unit in ("B", "KB") else "%.1f %s") % (n, unit)
        n /= 1024


def bn(x):
    return "$%.1fbn" % ((x or 0) / 1e9)


def fnum(x):
    try:
        return float(x) if x not in (None, "") else None
    except ValueError:
        return None


def zip_links(page_bytes, page_url):
    """Every .zip link on a data-set page, in page order, with the size text of its table row when there is one."""
    text = page_bytes.decode("utf-8", "replace")
    out, seen = [], set()
    for row in re.findall(r"<tr.*?</tr>", text, flags=re.S | re.I) or [text]:
        for href in re.findall(r'href="([^"]+\.zip)"', row, flags=re.I):
            url = urljoin(page_url, html.unescape(href))
            if url in seen:
                continue
            seen.add(url)
            m = re.search(r"([\d.,]+)\s*(KB|MB|GB)", re.sub(r"<[^>]+>", " ", row), flags=re.I)
            out.append((url, (m.group(1) + " " + m.group(2).upper()) if m else None))
    if not out:
        for href in re.findall(r'href="([^"]+\.zip)"', text, flags=re.I):
            url = urljoin(page_url, html.unescape(href))
            if url not in seen:
                seen.add(url)
                out.append((url, None))
    return out


def page_title(page_bytes):
    m = re.search(r"<h1[^>]*>(.*?)</h1>", page_bytes.decode("utf-8", "replace"), flags=re.S | re.I)
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", m.group(1))).strip() if m else "(no heading)"


def member(zf, stem):
    """The archive member whose file name starts with this stem, whatever its case or extension."""
    want = stem.upper()
    for info in zf.infolist():
        base = info.filename.rsplit("/", 1)[-1].upper()
        if base.rsplit(".", 1)[0] == want:
            return info
    return None


def rows_of(zf, info):
    """(header list, iterator of field lists) of one tab-delimited member; a row that is not the header's width is skipped."""
    stream = io.TextIOWrapper(zf.open(info), encoding="utf-8", errors="replace", newline="")
    header = stream.readline().rstrip("\r\n").split("\t")
    width = len(header)

    def gen():
        for line in stream:
            parts = line.rstrip("\r\n").split("\t")
            if len(parts) == width:
                yield parts
    return header, gen()


def load(db, zf, stem, table, cols, numeric=()):
    """One member into one sqlite table: only the named columns. Returns rows kept, or None when the member is missing."""
    info = member(zf, stem)
    if info is None:
        say("  %s: not in the archive" % stem)
        db.execute("CREATE TABLE %s (%s)" % (table, ", ".join("%s %s" % (c.lower(), "REAL" if c in numeric else "TEXT") for c in cols)))
        return None
    header, rows = rows_of(zf, info)
    up = [h.strip().upper() for h in header]
    idx = [up.index(c) if c in up else None for c in cols]
    missing = [c for c, i in zip(cols, idx) if i is None]
    names = [c.lower() for c in cols]
    db.execute("CREATE TABLE %s (%s)" % (table, ", ".join("%s %s" % (n, "REAL" if c in numeric else "TEXT") for n, c in zip(names, cols))))
    num = [c in numeric for c in cols]
    sql = "INSERT INTO %s VALUES (%s)" % (table, ",".join("?" * len(cols)))
    n, batch = 0, []
    for parts in rows:
        rec = []
        for i, isnum in zip(idx, num):
            v = parts[i] if i is not None else None
            rec.append(fnum(v) if isnum else v)
        batch.append(rec)
        if len(batch) >= 50000:
            db.executemany(sql, batch)
            n += len(batch)
            batch = []
    if batch:
        db.executemany(sql, batch)
        n += len(batch)
    db.commit()
    say("  %s: %s rows, %s unpacked%s" % (stem, format(n, ","), size_text(info.file_size), (" | columns not found: %s" % missing) if missing else ""))
    return n


def q(db, sql, args=()):
    return db.execute(sql, args).fetchall()


def norm_name(s):
    s = re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())
    s = re.sub(r"\b(THE|INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|PLC|SA|AG|NV|SE|LTD|LIMITED|LLC|LP|HOLDINGS|HOLDING|GROUP|REPUBLIC|OF|"
               r"KINGDOM|FEDERATIVE|UNITED|STATES|GOVERNMENT|INTERNATIONAL|BOND|SPA|ASA|AB|OYJ|BV|FINANCE|FUNDING|CAPITAL)\b", " ", s)
    return " ".join(s.split())


# ----------------------------------------------------------------------------------------------- 1. the library
def library(r):
    r.section("1. The SEC's data library: every data-set page")
    pages = {}
    for hub in HUBS:
        status, body, _ = get(hub)
        say("hub %s: HTTP %s%s" % (hub, status, "" if body is None or status == 200 else " | %s" % body[:120]))
        if status != 200 or not body:
            continue
        for href in re.findall(r'href="([^"#?]+)"', body.decode("utf-8", "replace")):
            url = urljoin(hub, html.unescape(href))
            if re.search(r"sec\.gov/(data-research/sec-markets-data/|dera/data/|about/dera_|data-research/[a-z0-9-]*data-sets)", url) and not url.endswith((".zip", ".pdf", ".csv", ".txt")):
                pages.setdefault(url.rstrip("/"), None)
    for slug in SLUGS:
        pages.setdefault(BASE + slug, None)
    say("candidate pages: %d" % len(pages))
    found = {}
    for url in sorted(pages):
        if len(found) >= 60:
            break
        status, body, _ = get(url)
        if status != 200 or not body:
            if url.startswith(BASE) and url.rsplit("/", 1)[-1] in SLUGS:
                say("  no page (HTTP %s): %s" % (status, url.rsplit("/", 1)[-1]))
            continue
        links = zip_links(body, url)
        if not links:
            continue
        title = page_title(body)
        key = links[0][0].rsplit("/", 2)[-2]
        if key in found:
            continue
        found[key] = {"url": url, "title": title, "links": links}
        newest, oldest = links[0], links[-1]
        say("  %-58s %3d files | newest %s (%s) | oldest %s" % (title[:58], len(links), newest[0].rsplit("/", 1)[-1], newest[1] or "size not shown",
                                                                oldest[0].rsplit("/", 1)[-1]))
    say("data sets with downloadable files: %d" % len(found))
    return found


# ----------------------------------------------------------------------------------------------- 2. N-PORT
def nport(r, found, work, desk_names):
    r.section("2. Form N-PORT, newest quarter, opened whole")
    entry = next((v for k, v in found.items() if "n-port" in k or "nport" in v["links"][0][0]), None)
    url = entry["links"][0][0] if entry else "https://www.sec.gov/files/dera/data/form-n-port-data-sets/2026q3_nport.zip"
    path = os.path.join(work, "nport.zip")
    t0 = time.time()
    status, _, _ = get(url, to_path=path, timeout=600)
    if status != 200:
        r.fail("N-PORT newest file did not download: HTTP %s for %s" % (status, url.rsplit("/", 1)[-1]))
        return
    say("file %s: %s, downloaded in %.0fs" % (url.rsplit("/", 1)[-1], size_text(os.path.getsize(path)), time.time() - t0))
    zf = zipfile.ZipFile(path)
    infos = sorted(zf.infolist(), key=lambda i: -i.file_size)
    say("members: %d, unpacked %s in all" % (len(infos), size_text(sum(i.file_size for i in infos))))
    for i in infos:
        say("  %-36s %10s" % (i.filename, size_text(i.file_size)))
    db = sqlite3.connect(os.path.join(work, "nport.db"))
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute("PRAGMA temp_store=FILE")
    db.execute("PRAGMA cache_size=-800000")
    t0 = time.time()
    say("loading the tables the audit needs:")
    load(db, zf, "SUBMISSION", "sub", ["ACCESSION_NUMBER", "FILING_DATE", "SUB_TYPE", "REPORT_ENDING_PERIOD", "REPORT_DATE", "IS_LAST_FILING"])
    load(db, zf, "REGISTRANT", "reg", ["ACCESSION_NUMBER", "CIK", "REGISTRANT_NAME"])
    load(db, zf, "FUND_REPORTED_INFO", "info",
         ["ACCESSION_NUMBER", "SERIES_NAME", "SERIES_ID", "TOTAL_ASSETS", "NET_ASSETS", "CREDIT_SPREAD_5YR_INVEST", "CREDIT_SPREAD_5YR_NONINVEST",
          "SALES_FLOW_MON1", "REINVESTMENT_FLOW_MON1", "REDEMPTION_FLOW_MON1", "SALES_FLOW_MON2", "REINVESTMENT_FLOW_MON2", "REDEMPTION_FLOW_MON2",
          "SALES_FLOW_MON3", "REINVESTMENT_FLOW_MON3", "REDEMPTION_FLOW_MON3"],
         numeric=("TOTAL_ASSETS", "NET_ASSETS", "CREDIT_SPREAD_5YR_INVEST", "CREDIT_SPREAD_5YR_NONINVEST", "SALES_FLOW_MON1", "REINVESTMENT_FLOW_MON1",
                  "REDEMPTION_FLOW_MON1", "SALES_FLOW_MON2", "REINVESTMENT_FLOW_MON2", "REDEMPTION_FLOW_MON2", "SALES_FLOW_MON3",
                  "REINVESTMENT_FLOW_MON3", "REDEMPTION_FLOW_MON3"))
    load(db, zf, "FUND_REPORTED_HOLDING", "hold",
         ["HOLDING_ID", "ACCESSION_NUMBER", "ISSUER_NAME", "ISSUER_TITLE", "ISSUER_CUSIP", "BALANCE", "UNIT", "CURRENCY_CODE", "CURRENCY_VALUE",
          "PAYOFF_PROFILE", "ASSET_CAT", "ISSUER_TYPE", "INVESTMENT_COUNTRY", "IS_RESTRICTED_SECURITY", "FAIR_VALUE_LEVEL", "DERIVATIVE_CAT"],
         numeric=("BALANCE", "CURRENCY_VALUE"))
    load(db, zf, "DEBT_SECURITY", "debt",
         ["HOLDING_ID", "MATURITY_DATE", "COUPON_TYPE", "ANNUALIZED_RATE", "IS_DEFAULT", "ARE_ANY_INTEREST_PAYMENT", "IS_ANY_PORTION_INTEREST_PAID"],
         numeric=("ANNUALIZED_RATE",))
    load(db, zf, "NONFOREIGN_EXCHANGE_SWAP", "swap",
         ["HOLDING_ID", "TERMINATION_DATE", "UPFRONT_PAYMENT", "UPFRONT_RECEIPT", "NOTIONAL_AMOUNT", "CURRENCY_CODE", "UNREALIZED_APPRECIATION",
          "FIXED_OR_FLOATING_RECEIPT", "FIXED_RATE_RECEIPT", "FIXED_OR_FLOATING_PAYMENT", "FIXED_RATE_PAYMENT"],
         numeric=("UPFRONT_PAYMENT", "UPFRONT_RECEIPT", "NOTIONAL_AMOUNT", "UNREALIZED_APPRECIATION", "FIXED_RATE_RECEIPT", "FIXED_RATE_PAYMENT"))
    load(db, zf, "DESC_REF_OTHER", "refo", ["HOLDING_ID", "ISSUER_NAME", "ISSUE_TITLE"])
    load(db, zf, "DESC_REF_INDEX_BASKET", "refi", ["HOLDING_ID", "INDEX_NAME", "NARRATIVE_DESC"])
    load(db, zf, "DERIVATIVE_COUNTERPARTY", "cpty", ["HOLDING_ID", "DERIVATIVE_COUNTERPARTY_NAME"])
    load(db, zf, "REPURCHASE_AGREEMENT", "repo", ["HOLDING_ID", "TRANSACTION_TYPE", "IS_CLEARED", "CENTRAL_COUNTER_PARTY", "IS_TRIPARTY", "REPURCHASE_RATE"],
         numeric=("REPURCHASE_RATE",))
    load(db, zf, "REPURCHASE_COUNTERPARTY", "repoc", ["HOLDING_ID", "NAME"])
    load(db, zf, "SECURITIES_LENDING", "lend", ["HOLDING_ID", "IS_LOAN_BY_FUND", "LOAN_VALUE"], numeric=("LOAN_VALUE",))
    for stmt in ("CREATE INDEX h_id ON hold(holding_id)", "CREATE INDEX h_cat ON hold(asset_cat)", "CREATE INDEX d_id ON debt(holding_id)",
                 "CREATE INDEX s_id ON swap(holding_id)", "CREATE INDEX s_acc ON sub(accession_number)", "CREATE INDEX ro_id ON refo(holding_id)",
                 "CREATE INDEX ri_id ON refi(holding_id)", "CREATE INDEX c_id ON cpty(holding_id)", "CREATE INDEX l_id ON lend(holding_id)",
                 "CREATE INDEX rp_id ON repo(holding_id)", "CREATE INDEX rc_id ON repoc(holding_id)"):
        db.execute(stmt)
    db.commit()
    say("loaded and indexed in %.0fs" % (time.time() - t0))

    # -- filings and funds
    say("")
    say("FILINGS AND FUNDS")
    for row in q(db, "SELECT sub_type, COUNT(*) FROM sub GROUP BY 1 ORDER BY 2 DESC"):
        say("  filings of type %s: %s" % (row[0], format(row[1], ",")))
    dates = q(db, "SELECT report_date, COUNT(*) FROM sub GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
    say("  report dates (how old the holdings are), by number of filings: %s" % ", ".join("%s x%d" % (d, n) for d, n in dates))
    fmin, fmax = q(db, "SELECT MIN(filing_date), MAX(filing_date) FROM sub")[0]
    say("  filing dates as written in the file: %s to %s (first and last by text order)" % (fmin, fmax))
    say("  is_last_filing values: %s" % dict(q(db, "SELECT is_last_filing, COUNT(*) FROM sub GROUP BY 1")))
    n_series, net = q(db, "SELECT COUNT(DISTINCT series_id), SUM(net_assets) FROM info")[0]
    say("  funds (series): %s | net assets summed over every filing in the file: %s" % (format(n_series, ","), bn(net)))
    flows = q(db, "SELECT SUM(sales_flow_mon1+reinvestment_flow_mon1-redemption_flow_mon1), SUM(sales_flow_mon2+reinvestment_flow_mon2-redemption_flow_mon2),"
                  " SUM(sales_flow_mon3+reinvestment_flow_mon3-redemption_flow_mon3), SUM(credit_spread_5yr_invest), SUM(credit_spread_5yr_noninvest) FROM info")[0]
    say("  net shareholder flows carried per fund, three months of each filing, summed: %s | %s | %s" % tuple(bn(x) for x in flows[:3]))
    say("  change in value for a 1bp move in 5-year credit spreads, summed: investment grade %s, below %s" % ("$%.0fm" % ((flows[3] or 0) / 1e6), "$%.0fm" % ((flows[4] or 0) / 1e6)))

    # -- holdings by asset class
    say("")
    say("HOLDINGS BY ASSET CLASS (the file's own codes)")
    for cat, n, val in q(db, "SELECT asset_cat, COUNT(*), SUM(currency_value) FROM hold GROUP BY 1 ORDER BY 2 DESC LIMIT 24"):
        say("  %-10s %12s rows %12s" % (cat, format(n, ","), bn(val)))
    say("  units: %s" % dict(q(db, "SELECT unit, COUNT(*) FROM hold GROUP BY 1 ORDER BY 2 DESC LIMIT 6")))
    say("  derivative kinds: %s" % dict(q(db, "SELECT derivative_cat, COUNT(*) FROM hold WHERE derivative_cat NOT IN ('', 'N/A') GROUP BY 1 ORDER BY 2 DESC LIMIT 10")))
    say("  fair value level 3 (priced by model), rows and value by class: %s" % ", ".join(
        "%s %s %s" % (c, format(n, ","), bn(v)) for c, n, v in q(db, "SELECT asset_cat, COUNT(*), SUM(currency_value) FROM hold WHERE fair_value_level='3' GROUP BY 1 ORDER BY 3 DESC LIMIT 8")))

    # -- bonds
    say("")
    say("BONDS (asset class DBT) AND WHAT EACH ROW CARRIES")
    n_debt, v_debt = q(db, "SELECT COUNT(*), SUM(currency_value) FROM hold WHERE asset_cat='DBT'")[0]
    n_sec = q(db, "SELECT COUNT(DISTINCT issuer_cusip) FROM hold WHERE asset_cat='DBT' AND length(issuer_cusip)=9 AND issuer_cusip NOT IN ('000000000','N/A')")[0][0]
    n_iss = q(db, "SELECT COUNT(DISTINCT UPPER(issuer_name)) FROM hold WHERE asset_cat='DBT'")[0][0]
    say("  bond rows: %s worth %s | distinct bonds (by the identifier, counted only): %s | distinct issuer names: %s" % (format(n_debt, ","), bn(v_debt), format(n_sec, ","), format(n_iss, ",")))
    say("  issuer types among bond rows: %s" % dict(q(db, "SELECT issuer_type, COUNT(*) FROM hold WHERE asset_cat='DBT' GROUP BY 1 ORDER BY 2 DESC LIMIT 8")))
    say("  a maturity date as written: %s | coupon kinds: %s" % (q(db, "SELECT maturity_date FROM debt WHERE maturity_date<>'' LIMIT 1")[0][0],
                                                               dict(q(db, "SELECT coupon_type, COUNT(*) FROM debt GROUP BY 1 ORDER BY 2 DESC LIMIT 6"))))
    for label, col in (("in default", "is_default"), ("interest in arrears", "are_any_interest_payment"), ("interest paid in kind", "is_any_portion_interest_paid")):
        n, v = q(db, "SELECT COUNT(*), SUM(h.currency_value) FROM debt d JOIN hold h ON h.holding_id=d.holding_id WHERE d.%s='Y'" % col)[0]
        say("  flagged %-22s %8s rows %10s" % (label + ":", format(n, ","), bn(v)))
    say("  largest issuers flagged in default, by value funds still carry (issuer | funds | value | value per 100 of face):")
    for name, funds, val, bal in q(db, "SELECT UPPER(h.issuer_name), COUNT(DISTINCT h.accession_number), SUM(h.currency_value), SUM(CASE WHEN h.unit='PA' THEN h.balance END)"
                                       " FROM debt d JOIN hold h ON h.holding_id=d.holding_id WHERE d.is_default='Y' GROUP BY 1 ORDER BY 3 DESC LIMIT 15"):
        say("    %-46s %4d %10s %8s" % (name[:46], funds, "$%.0fm" % ((val or 0) / 1e6), ("%.1f" % (val / bal * 100)) if bal else "-"))

    # -- one bond, many funds: do they mark it the same?
    say("")
    say("ONE BOND HELD BY MANY FUNDS ON THE SAME DATE: THE SPREAD OF THEIR MARKS")
    db.execute("CREATE TEMP TABLE px AS SELECT h.issuer_cusip AS k, s.report_date AS d, h.currency_value/h.balance*100 AS p, h.accession_number AS a,"
               " h.issuer_name AS nm, h.issuer_title AS ti, h.currency_value AS v FROM hold h JOIN sub s ON s.accession_number=h.accession_number"
               " WHERE h.asset_cat='DBT' AND h.unit='PA' AND h.currency_code='USD' AND h.balance>0 AND h.currency_value>0 AND length(h.issuer_cusip)=9"
               " AND h.issuer_cusip NOT IN ('000000000','N/A')")
    groups = q(db, "SELECT k, d, COUNT(DISTINCT a), MIN(p), MAX(p), SUM(v), MIN(nm), MIN(ti) FROM px WHERE p BETWEEN 1 AND 250 GROUP BY k, d HAVING COUNT(DISTINCT a)>=5")
    spreads = sorted(g[4] - g[3] for g in groups)
    if spreads:
        pick = lambda f: spreads[min(len(spreads) - 1, int(f * len(spreads)))]
        say("  bond-dates with five or more funds: %s | spread between the highest and lowest mark, in points of face: median %.2f, 90th pct %.2f, 99th pct %.2f" % (
            format(len(spreads), ","), pick(0.5), pick(0.9), pick(0.99)))
        say("  with a spread of 5 points or more: %s | 10 or more: %s" % (format(sum(s >= 5 for s in spreads), ","), format(sum(s >= 10 for s in spreads), ",")))
        say("  widest disagreements among bonds funds mark below 95 (issuer | title | date | funds | low | high):")
        wide = sorted((g for g in groups if g[3] < 95 and g[5] > 2e7), key=lambda g: g[3] - g[4])[:12]
        for g in wide:
            say("    %-34s %-30s %s %3d %7.1f %7.1f" % ((g[6] or "")[:34], (g[7] or "")[:30], g[1], g[2], g[3], g[4]))

    # -- the 24 bonds of the chart
    say("")
    say("THE 24 BONDS OF THE CHART HE SENT (matched on issuer name, coupon and maturity year; no identifier shown)")
    coupons = sorted({c for _, c, _, _ in CHART})
    cond = " OR ".join("ABS(d.annualized_rate-%s)<0.0051" % c for c in coupons)
    cand = q(db, "SELECT h.issuer_name, h.issuer_title, d.annualized_rate, d.maturity_date, h.balance, h.currency_value, h.accession_number, s.report_date,"
                 " h.is_restricted_security, h.fair_value_level, h.unit FROM debt d JOIN hold h ON h.holding_id=d.holding_id"
                 " JOIN sub s ON s.accession_number=h.accession_number WHERE h.asset_cat IN ('DBT','LON','ABS-O','ABS-MBS','ABS-CBDO','SN') AND (%s)" % cond)
    say("  rows with one of the chart's coupons: %s" % format(len(cand), ","))
    held = 0
    for label, cpn, year, pat in CHART:
        rx = re.compile(pat, re.I)
        hits = [c for c in cand if abs((c[2] or 0) - cpn) < 0.0051 and year in (c[3] or "") and rx.search("%s %s" % (c[0], c[1]))]
        if not hits:
            say("  %-18s not held by any fund in this file" % label)
            continue
        held += 1
        funds = len({c[6] for c in hits})
        par = sum(c[4] or 0 for c in hits if c[10] == "PA")
        val = sum(c[5] or 0 for c in hits)
        marks = sorted((c[5] / c[4] * 100) for c in hits if c[10] == "PA" and c[4])
        names = Counter((c[0] or "").upper()[:40] for c in hits).most_common(1)[0][0]
        dates_ = Counter(c[7] for c in hits).most_common(2)
        say("  %-18s %-40s %3d funds | face $%.0fm | value $%.0fm | marks %s | dates %s | restricted %s" % (
            label, names, funds, par / 1e6, val / 1e6, ("%.1f to %.1f" % (marks[0], marks[-1])) if marks else "-",
            ", ".join("%s x%d" % d for d in dates_), dict(Counter(c[8] for c in hits))))
    say("  of the chart's 24 bonds, held by at least one fund in this file: %d" % held)

    say("")
    say("AI DATA-CENTRE AND COMPUTE ISSUERS AMONG FUND BOND AND LOAN ROWS (name pattern; largest by value)")
    agg = defaultdict(lambda: [0.0, set(), Counter()])
    for nm, ti, val, acc, cat in q(db, "SELECT issuer_name, issuer_title, currency_value, accession_number, asset_cat FROM hold WHERE asset_cat IN ('DBT','LON','ABS-O','SN')"):
        if nm and HINT.search(nm):
            a = agg[nm.upper()[:48]]
            a[0] += val or 0
            a[1].add(acc)
            a[2][cat] += 1
    say("  issuer names matched: %d" % len(agg))
    for nm, a in sorted(agg.items(), key=lambda kv: -kv[1][0])[:40]:
        say("    %-48s %10s %4d funds %s" % (nm, "$%.0fm" % (a[0] / 1e6), len(a[1]), dict(a[2])))

    # -- loans
    say("")
    say("LOANS (asset class LON)")
    n, v = q(db, "SELECT COUNT(*), SUM(currency_value) FROM hold WHERE asset_cat='LON'")[0]
    say("  loan rows: %s worth %s | distinct borrower names: %s" % (format(n, ","), bn(v), format(q(db, "SELECT COUNT(DISTINCT UPPER(issuer_name)) FROM hold WHERE asset_cat='LON'")[0][0], ",")))
    lp = q(db, "SELECT currency_value/balance*100, currency_value FROM hold WHERE asset_cat='LON' AND unit='PA' AND currency_code='USD' AND balance>0 AND currency_value>0")
    tot = sum(x[1] for x in lp) or 1
    for cut in (90, 80, 70, 50):
        say("  marked below %d: %.1f%% of loan value (%s rows)" % (cut, 100 * sum(x[1] for x in lp if x[0] < cut) / tot, format(sum(1 for x in lp if x[0] < cut), ",")))
    say("  largest borrowers marked below 80 (borrower | funds | value | mark):")
    for nm, funds, val, bal in q(db, "SELECT UPPER(issuer_name), COUNT(DISTINCT accession_number), SUM(currency_value), SUM(balance) FROM hold WHERE asset_cat='LON' AND unit='PA'"
                                     " AND currency_code='USD' AND balance>0 AND currency_value/balance<0.8 GROUP BY 1 ORDER BY 3 DESC LIMIT 12"):
        say("    %-46s %4d %10s %7.1f" % (nm[:46], funds, "$%.0fm" % (val / 1e6), val / bal * 100))

    # -- credit default swaps
    say("")
    say("CREDIT DERIVATIVES FUNDS HOLD (asset class DCR)")
    for kind, n in q(db, "SELECT derivative_cat, COUNT(*) FROM hold WHERE asset_cat='DCR' GROUP BY 1 ORDER BY 2 DESC"):
        say("  kind %-6s %s rows" % (kind, format(n, ",")))
    n_sw, notional, funds = q(db, "SELECT COUNT(*), SUM(ABS(s.notional_amount)), COUNT(DISTINCT h.accession_number) FROM hold h JOIN swap s ON s.holding_id=h.holding_id WHERE h.asset_cat='DCR'")[0]
    say("  credit swaps with a swap record: %s | notional %s | funds holding any: %s" % (format(n_sw or 0, ","), bn(notional), format(funds or 0, ",")))
    say("  how the two legs are written (received | paid | rows | notional):")
    for rec, pay, n, notl in q(db, "SELECT s.fixed_or_floating_receipt, s.fixed_or_floating_payment, COUNT(*), SUM(ABS(s.notional_amount)) FROM hold h JOIN swap s"
                                   " ON s.holding_id=h.holding_id WHERE h.asset_cat='DCR' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 8"):
        say("    received %-10s paid %-10s %8s rows %10s" % (rec, pay, format(n, ","), bn(notl)))
    n_single = q(db, "SELECT COUNT(DISTINCT h.holding_id) FROM hold h JOIN refo o ON o.holding_id=h.holding_id WHERE h.asset_cat='DCR'")[0][0]
    n_index = q(db, "SELECT COUNT(DISTINCT h.holding_id) FROM hold h JOIN refi i ON i.holding_id=h.holding_id WHERE h.asset_cat='DCR'")[0][0]
    say("  written on a named issuer or security: %s rows | written on an index or basket: %s rows" % (format(n_single, ","), format(n_index, ",")))
    say("  the indices most written on (index | rows | notional):")
    for nm, n, notl in q(db, "SELECT UPPER(i.index_name), COUNT(*), SUM(ABS(s.notional_amount)) FROM hold h JOIN refi i ON i.holding_id=h.holding_id LEFT JOIN swap s"
                             " ON s.holding_id=h.holding_id WHERE h.asset_cat='DCR' GROUP BY 1 ORDER BY 3 DESC LIMIT 10"):
        say("    %-60s %6s %10s" % ((nm or "")[:60], format(n, ","), bn(notl)))
    singles = q(db, "SELECT UPPER(o.issuer_name), COUNT(*), COUNT(DISTINCT h.accession_number), SUM(ABS(s.notional_amount)),"
                    " SUM(CASE WHEN s.fixed_or_floating_receipt='Fixed' THEN ABS(s.notional_amount) ELSE 0 END),"
                    " SUM(CASE WHEN s.fixed_or_floating_payment='Fixed' THEN ABS(s.notional_amount) ELSE 0 END)"
                    " FROM hold h JOIN refo o ON o.holding_id=h.holding_id LEFT JOIN swap s ON s.holding_id=h.holding_id WHERE h.asset_cat='DCR' GROUP BY 1 ORDER BY 4 DESC")
    say("  distinct reference names (as written by funds): %s" % format(len(singles), ","))
    say("  the names most written on (name | rows | funds | notional | fund receives the fixed leg = sold protection | pays it = bought):")
    for nm, n, f, notl, sold, bought in singles[:40]:
        say("    %-44s %5d %4d %9s  sold %9s  bought %9s" % ((nm or "")[:44], n, f, "$%.0fm" % ((notl or 0) / 1e6), "$%.0fm" % ((sold or 0) / 1e6), "$%.0fm" % ((bought or 0) / 1e6)))
    say("  three swap records as they are written (reference | notional | received kind/rate | paid kind/rate | upfront paid | upfront received | ends | unrealised):")
    for row in q(db, "SELECT o.issuer_name, s.notional_amount, s.fixed_or_floating_receipt, s.fixed_rate_receipt, s.fixed_or_floating_payment, s.fixed_rate_payment,"
                     " s.upfront_payment, s.upfront_receipt, s.termination_date, s.unrealized_appreciation FROM hold h JOIN refo o ON o.holding_id=h.holding_id"
                     " JOIN swap s ON s.holding_id=h.holding_id WHERE h.asset_cat='DCR' AND s.notional_amount>5000000 LIMIT 3"):
        say("    %s" % " | ".join(str(x)[:30] for x in row))
    say("  counterparties of credit swaps (name | rows):")
    for nm, n in q(db, "SELECT UPPER(c.derivative_counterparty_name), COUNT(*) FROM hold h JOIN cpty c ON c.holding_id=h.holding_id WHERE h.asset_cat='DCR' GROUP BY 1 ORDER BY 2 DESC LIMIT 10"):
        say("    %-50s %s" % ((nm or "")[:50], format(n, ",")))
    if desk_names:
        have = {}
        for nm, n, f, notl, sold, bought in singles:
            have.setdefault(norm_name(nm), [nm, 0.0])[1] += notl or 0
        hit, miss = [], []
        for d in sorted(desk_names):
            key = norm_name(d)
            match = next((v for k, v in have.items() if key and (k == key or k.startswith(key + " ") or key.startswith(k + " ")) and k), None)
            (hit if match else miss).append(d)
        say("  names on our CDS desk: %d | funds hold a credit swap on %d of them | not found among fund swaps: %s" % (len(desk_names), len(hit), ", ".join(miss[:30]) or "none"))

    # -- securities on loan, repo
    say("")
    say("SECURITIES FUNDS HAVE OUT ON LOAN, AND REPO")
    n, v = q(db, "SELECT COUNT(*), SUM(loan_value) FROM lend WHERE is_loan_by_fund='Y'")[0]
    say("  holdings with a part out on loan: %s | value on loan %s" % (format(n or 0, ","), bn(v)))
    say("  on loan by asset class: %s" % ", ".join("%s %s" % (c, bn(x)) for c, x in q(
        db, "SELECT h.asset_cat, SUM(l.loan_value) FROM lend l JOIN hold h ON h.holding_id=l.holding_id WHERE l.is_loan_by_fund='Y' GROUP BY 1 ORDER BY 2 DESC LIMIT 6")))
    say("  common stocks with the largest share of fund holdings out on loan, among those funds hold $500m or more of (issuer | held | on loan | share):")
    for nm, heldv, loanv in q(db, "SELECT UPPER(h.issuer_name), SUM(h.currency_value), SUM(CASE WHEN l.is_loan_by_fund='Y' THEN l.loan_value ELSE 0 END) FROM hold h"
                                  " LEFT JOIN lend l ON l.holding_id=h.holding_id WHERE h.asset_cat='EC' GROUP BY 1 HAVING SUM(h.currency_value)>=5e8"
                                  " ORDER BY SUM(CASE WHEN l.is_loan_by_fund='Y' THEN l.loan_value ELSE 0 END)*1.0/SUM(h.currency_value) DESC LIMIT 15"):
        say("    %-44s %10s %10s %5.1f%%" % (nm[:44], bn(heldv), bn(loanv), 100 * (loanv or 0) / heldv))
    for kind, n, v in q(db, "SELECT r.transaction_type, COUNT(*), SUM(h.currency_value) FROM repo r JOIN hold h ON h.holding_id=r.holding_id GROUP BY 1 ORDER BY 3 DESC"):
        say("  repo rows of kind %-22s %7s %10s" % (kind, format(n, ","), bn(v)))
    say("  repo counterparties (name | value): %s" % "; ".join("%s %s" % ((nm or "")[:28], bn(v)) for nm, v in q(
        db, "SELECT UPPER(c.name), SUM(h.currency_value) FROM repoc c JOIN hold h ON h.holding_id=c.holding_id GROUP BY 1 ORDER BY 2 DESC LIMIT 8")))
    db.close()
    r.ok("N-PORT newest quarter read whole")


# ----------------------------------------------------------------------------------------------- 3. N-MFP
def table(zf, stem):
    info = member(zf, stem)
    if info is None:
        return None, []
    header, rows = rows_of(zf, info)
    up = [h.strip().upper() for h in header]
    return up, [dict(zip(up, parts)) for parts in rows]


def stream(zf, stem):
    """Rows of one member as dicts, one at a time (for the long tables)."""
    info = member(zf, stem)
    if info is None:
        return
    header, rows = rows_of(zf, info)
    up = [h.strip().upper() for h in header]
    for parts in rows:
        yield dict(zip(up, parts))


def nmfp(r, found, work):
    r.section("3. Form N-MFP, newest month: money market funds")
    entry = next((v for k, v in found.items() if "mfp" in k or "nmfp" in v["links"][0][0].lower()), None)
    if not entry:
        r.warn("N-MFP page not found in the library")
        return
    url = entry["links"][0][0]
    path = os.path.join(work, "nmfp.zip")
    status, _, _ = get(url, to_path=path, timeout=300)
    if status != 200:
        r.fail("N-MFP newest file did not download: HTTP %s" % status)
        return
    say("file %s: %s | files on the page: %d, oldest %s" % (url.rsplit("/", 1)[-1], size_text(os.path.getsize(path)), len(entry["links"]), entry["links"][-1][0].rsplit("/", 1)[-1]))
    zf = zipfile.ZipFile(path)
    for i in sorted(zf.infolist(), key=lambda i: -i.file_size):
        say("  %-40s %10s" % (i.filename, size_text(i.file_size)))
    stems = {i.filename.rsplit("/", 1)[-1].rsplit(".", 1)[0].upper(): i for i in zf.infolist()}

    def find(*words):
        for stem in stems:
            if all(w in stem for w in words):
                return stem
        return None

    _, subs = table(zf, find("SUBMISSION") or "NMFP_SUBMISSION")
    dates = Counter(s.get("REPORTDATE") for s in subs)
    say("filings: %s | report dates: %s | filing dates %s to %s" % (format(len(subs), ","), ", ".join("%s x%d" % d for d in dates.most_common(4)),
                                                                    min((s.get("FILING_DATE") or "") for s in subs), max((s.get("FILING_DATE") or "") for s in subs)))
    month = dates.most_common(1)[0][0]
    latest = {}
    for s in subs:
        if s.get("REPORTDATE") != month:
            continue
        sid = s.get("SERIESID") or s.get("ACCESSION_NUMBER")
        if sid not in latest or (s.get("ACCESSION_NUMBER") or "") > (latest[sid].get("ACCESSION_NUMBER") or ""):
            latest[sid] = s
    keep = {s["ACCESSION_NUMBER"] for s in latest.values()}
    say("funds reporting for %s (one filing each, the latest): %s" % (month, format(len(keep), ",")))
    _, series = table(zf, find("SERIESLEVEL") or "NMFP_SERIESLEVELINFO")
    by_cat = defaultdict(lambda: [0, 0.0, 0.0])
    for s in series:
        if s.get("ACCESSION_NUMBER") not in keep:
            continue
        net = fnum(s.get("NETASSETOFSERIES")) or 0
        c = by_cat[(s.get("MONEYMARKETFUNDCATEGORY") or "?")[:40]]
        c[0] += 1
        c[1] += net
        c[2] += net * (fnum(s.get("SEVENDAYGROSSYIELD")) or 0)
    total = sum(c[1] for c in by_cat.values())
    say("net assets of all money funds for the month: %s" % bn(total))
    for cat, c in sorted(by_cat.items(), key=lambda kv: -kv[1][1]):
        say("  %-40s %4d funds %10s  asset-weighted 7-day gross yield %s" % (cat, c[0], bn(c[1]), ("%.4f" % (c[2] / c[1])) if c[1] else "-"))
    hdr, secs = table(zf, find("SCHPORTFOLIO") or "NMFP_SCHPORTFOLIOSECURITIES")
    secs = [s for s in secs if s.get("ACCESSION_NUMBER") in keep]
    say("portfolio rows for the month: %s" % format(len(secs), ","))
    valcol = "INCLUDINGVALUEOFANYSPONSORSUPP" if hdr and "INCLUDINGVALUEOFANYSPONSORSUPP" in hdr else "EXCLUDINGVALUEOFANYSPONSORSUPP"
    cats = defaultdict(lambda: [0, 0.0])
    for s in secs:
        c = cats[(s.get("INVESTMENTCATEGORY") or "?")[:60]]
        c[0] += 1
        c[1] += fnum(s.get(valcol)) or 0
    say("what money funds hold (category | rows | value):")
    for cat, c in sorted(cats.items(), key=lambda kv: -kv[1][1]):
        say("  %-60s %7s %10s" % (cat, format(c[0], ","), bn(c[1])))
    repo = [s for s in secs if "REPURCHASE" in (s.get("INVESTMENTCATEGORY") or "").upper()]
    cp = defaultdict(float)
    for s in repo:
        cp[re.sub(r"\s+", " ", (s.get("NAMEOFISSUER") or "?").upper())[:44]] += fnum(s.get(valcol)) or 0
    rtot = sum(cp.values()) or 1
    fed = sum(v for k, v in cp.items() if "FEDERAL RESERVE" in k)
    cleared = sum(fnum(s.get(valcol)) or 0 for s in repo if (s.get("REPURCHASEAGREEMENTCLEAREDFLA") or "").upper() == "Y")
    tri = sum(fnum(s.get(valcol)) or 0 for s in repo if (s.get("REPURCHASEAGREEMENTTRIPARTYFL") or "").upper() == "Y")
    say("repo lent by money funds: %s to %d counterparty names | to the Federal Reserve %s | centrally cleared %s | tri-party %s" % (bn(rtot), len(cp), bn(fed), bn(cleared), bn(tri)))
    say("largest repo counterparties (name as written | value | share):")
    for nm, v in sorted(cp.items(), key=lambda kv: -kv[1])[:18]:
        say("  %-44s %10s %5.1f%%" % (nm, bn(v), 100 * v / rtot))
    bank = defaultdict(float)
    for s in secs:
        cat = (s.get("INVESTMENTCATEGORY") or "").upper()
        if any(w in cat for w in ("CERTIFICATE OF DEPOSIT", "COMMERCIAL PAPER", "TIME DEPOSIT")):
            bank[re.sub(r"\s+", " ", (s.get("NAMEOFISSUER") or "?").upper())[:44]] += fnum(s.get(valcol)) or 0
    say("unsecured lending by money funds (certificates of deposit, commercial paper, time deposits): %s to %d issuer names; largest:" % (bn(sum(bank.values())), len(bank)))
    for nm, v in sorted(bank.items(), key=lambda kv: -kv[1])[:14]:
        say("  %-44s %10s" % (nm, bn(v)))
    day, n_flows = defaultdict(lambda: [0.0, 0.0]), 0
    for f in stream(zf, find("DLYSHAREHOLDERFLOW") or "NMFP_DLYSHAREHOLDERFLOWREPORT"):
        n_flows += 1
        if f.get("ACCESSION_NUMBER") in keep:
            d = day[f.get("DAILYSHAREHOLDERFLOWDATE")]
            d[0] += fnum(f.get("DAILYGROSSSUBSCRIPTIONS")) or 0
            d[1] += fnum(f.get("DAILYGROSSREDEMPTIONS")) or 0
    say("daily flow table: %s rows covering %d days | a date as written: %s" % (format(n_flows, ","), len(day), next(iter(day), "-")))
    for d in list(day)[-5:]:
        say("  %s  subscriptions %s  redemptions %s  net %s" % (d, bn(day[d][0]), bn(day[d][1]), bn(day[d][0] - day[d][1])))
    kinds = defaultdict(float)
    for c in stream(zf, find("COLLATERALISSUERS") or "NMFP_COLLATERALISSUERS"):
        if c.get("ACCESSION_NUMBER") in keep:
            kinds[(c.get("CTGRYINVESTMENTSRPRSNTSCOLLATE") or "?")[:50]] += fnum(c.get("VALUEOFCOLLATERALTOTHENEARESTC")) or 0
    say("collateral behind the repo, by kind: %s" % "; ".join("%s %s" % (k, bn(v)) for k, v in sorted(kinds.items(), key=lambda kv: -kv[1])[:7]))
    r.ok("N-MFP newest month read whole")


# ----------------------------------------------------------------------------------------------- 4. BDC
def bdc(r, found, work):
    r.section("4. BDC data sets: what private-credit lenders hold and how they mark it")
    entry = next((v for k, v in found.items() if "bdc" in k or "business-development" in k), None)
    if not entry:
        r.warn("BDC page not found in the library")
        return
    say("files on the page: %d | newest six: %s" % (len(entry["links"]), ", ".join("%s (%s)" % (u.rsplit("/", 1)[-1], s or "?") for u, s in entry["links"][:6])))
    best, best_size = None, -1
    for url, _ in entry["links"][:5]:
        status, _, headers = get(url, method="HEAD")
        size = int(headers.get("content-length") or 0)
        if status == 200 and size > best_size:
            best, best_size = url, size
    if not best:
        r.fail("no BDC file answered")
        return
    path = os.path.join(work, "bdc.zip")
    status, _, _ = get(best, to_path=path, timeout=300)
    say("opened the largest of the newest five: %s, %s (HTTP %s)" % (best.rsplit("/", 1)[-1], size_text(os.path.getsize(path)), status))
    zf = zipfile.ZipFile(path)
    for i in sorted(zf.infolist(), key=lambda i: -i.file_size):
        say("  %-30s %10s" % (i.filename, size_text(i.file_size)))
    info = next((i for i in zf.infolist() if "soi" in i.filename.lower()), None)
    if info is None:
        r.warn("no schedule-of-investments member in this file")
        return
    header, rows = rows_of(zf, info)
    say("schedule of investments: %d columns: %s" % (len(header), " | ".join(header)))
    rows = [dict(zip(header, p)) for p in rows]
    say("rows: %s" % format(len(rows), ","))

    def col(*words):
        for h in header:
            if all(w.lower() in h.lower() for w in words):
                return h
        return None

    c_fv, c_cost, c_par = col("fair value"), col("cost"), col("principal")
    c_rate, c_spread, c_mat = col("interest rate"), col("basis spread"), col("maturity")
    c_ind, c_id, c_type, c_aff = col("industry"), col("identifier"), col("investment type"), col("affiliation")
    say("columns read as: fair value=%s | cost=%s | principal=%s | rate=%s | spread=%s | maturity=%s | industry=%s | name=%s | type=%s" % (
        c_fv, c_cost, c_par, c_rate, c_spread, c_mat, c_ind, c_id, c_type))
    extra = [h for h in header if re.search(r"accru|kind|pik|default|floor|unfunded|shares|rate", h, re.I)]
    say("other columns that bear on stress: %s" % (extra or "none"))
    say("forms: %s | statement dates: %s" % (dict(Counter(x.get("form") for x in rows).most_common(5)), ", ".join("%s x%d" % d for d in Counter(x.get("ddate") for x in rows).most_common(5))))
    cur = [x for x in rows if x.get("ddate") and x.get("ddate") == x.get("period")] or rows
    say("rows dated at the filing's own period end (the current schedule, not the comparison): %s from %d lenders" % (format(len(cur), ","), len({x.get("cik") for x in cur})))
    fv = sum(fnum(x.get(c_fv)) or 0 for x in cur)
    cost = sum(fnum(x.get(c_cost)) or 0 for x in cur)
    say("fair value %s against cost %s: %.1f%% of cost" % (bn(fv), bn(cost), 100 * fv / cost if cost else 0))
    both = [(fnum(x.get(c_fv)), fnum(x.get(c_cost)), x) for x in cur if fnum(x.get(c_cost)) and fnum(x.get(c_fv)) is not None and fnum(x.get(c_cost)) > 0]
    tot = sum(c for _, c, _ in both) or 1
    for cut in (0.9, 0.8, 0.5):
        say("  carried below %d%% of cost: %.1f%% of cost (%s rows)" % (cut * 100, 100 * sum(c for f, c, _ in both if f / c < cut) / tot, format(sum(1 for f, c, _ in both if f / c < cut), ",")))
    rates = sorted(fnum(x.get(c_rate)) for x in cur if c_rate and fnum(x.get(c_rate)))
    spreads = sorted(fnum(x.get(c_spread)) for x in cur if c_spread and fnum(x.get(c_spread)))
    say("interest rate: %d rows, median %s | spread over the floating rate: %d rows, median %s" % (len(rates), statistics.median(rates) if rates else "-", len(spreads), statistics.median(spreads) if spreads else "-"))
    lenders = defaultdict(lambda: [0.0, 0.0, 0])
    for x in cur:
        ln = lenders[(x.get("name") or "?")[:44]]
        ln[0] += fnum(x.get(c_fv)) or 0
        ln[1] += fnum(x.get(c_cost)) or 0
        ln[2] += 1
    say("largest lenders (name | rows | fair value | fair value as a share of cost):")
    for nm, ln in sorted(lenders.items(), key=lambda kv: -kv[1][0])[:12]:
        say("  %-44s %6d %10s %6.1f%%" % (nm, ln[2], bn(ln[0]), 100 * ln[0] / ln[1] if ln[1] else 0))
    for label, c in (("by investment type", c_type), ("by industry", c_ind)):
        if not c:
            continue
        g = defaultdict(lambda: [0.0, 0.0])
        for x in cur:
            a = g[(x.get(c) or "(blank)")[:50]]
            a[0] += fnum(x.get(c_fv)) or 0
            a[1] += fnum(x.get(c_cost)) or 0
        say("%s (member | fair value | %% of cost): %s" % (label, "; ".join("%s %s %.0f%%" % (k, bn(a[0]), 100 * a[0] / a[1] if a[1] else 0) for k, a in sorted(g.items(), key=lambda kv: -kv[1][0])[:8])))
    if c_id:
        say("three borrower labels as lenders write them: %s" % " || ".join((x.get(c_id) or "")[:90] for x in cur[1000:1003] or cur[:3]))
        by = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
        for f, c, x in both:
            label = re.sub(r"\[Member\]", "", x.get(c_id) or "")
            key = norm_name(re.split(r",| - |\(", label)[0])
            if len(key) >= 5:
                a = by[key][x.get("cik")]
                a[0] += f
                a[1] += c
        multi = {k: v for k, v in by.items() if len(v) >= 3}
        say("borrowers carried by three or more lenders (by the first part of the label): %s" % format(len(multi), ","))
        rng = []
        for k, v in multi.items():
            marks = sorted(a[0] / a[1] * 100 for a in v.values() if a[1] > 0)
            rng.append((marks[-1] - marks[0], k, len(v), marks[0], marks[-1], sum(a[1] for a in v.values())))
        for gap, k, n, lo, hi, costv in sorted(rng, reverse=True)[:12]:
            say("  %-44s %2d lenders | marks %5.1f%% to %5.1f%% of cost | cost %s" % (k[:44], n, lo, hi, "$%.0fm" % (costv / 1e6)))
    r.ok("BDC schedule of investments read whole")


# ----------------------------------------------------------------------------------------------- 5. the others
def peek(r, found, work):
    r.section("5. The other data sets small enough to open: members, columns, rows")
    for key, entry in sorted(found.items()):
        if not PEEK.search(key) and not PEEK.search(entry["url"]):
            continue
        url = entry["links"][0][0]
        status, _, headers = get(url, method="HEAD")
        size = int(headers.get("content-length") or 0)
        say("%s | newest %s | %s | %d files back to %s" % (entry["title"][:60], url.rsplit("/", 1)[-1], size_text(size) if size else "size unknown (HTTP %s)" % status,
                                                           len(entry["links"]), entry["links"][-1][0].rsplit("/", 1)[-1]))
        if status != 200 or not size or size > PEEK_MAX:
            say("  not opened (%s)" % ("too large for a look" if size > PEEK_MAX else "no answer"))
            continue
        path = os.path.join(work, "peek.zip")
        status, _, _ = get(url, to_path=path, timeout=300)
        if status != 200:
            say("  did not download: HTTP %s" % status)
            continue
        try:
            zf = zipfile.ZipFile(path)
            for info in sorted(zf.infolist(), key=lambda i: -i.file_size)[:12]:
                if info.is_dir() or not re.search(r"\.(tsv|txt|csv)$", info.filename, re.I):
                    say("  %-34s %10s" % (info.filename, size_text(info.file_size)))
                    continue
                with zf.open(info) as fh:
                    first = fh.readline().decode("utf-8", "replace").rstrip("\r\n")
                    n = sum(block.count(b"\n") for block in iter(lambda: fh.read(1 << 20), b""))
                cols = re.split(r"\t|\|", first) if ("\t" in first or "|" in first) else first.split(",")
                shown = [c for c in cols if not re.search(r"cusip|isin|figi|sedol", c, re.I)]
                say("  %-34s %10s %10s rows | %d columns: %s" % (info.filename, size_text(info.file_size), format(n, ","), len(cols), ", ".join(shown)[:420]))
        except Exception as exc:              # noqa: BLE001
            say("  could not be read: %s" % clean(repr(exc))[:200])
        finally:
            if os.path.exists(path):
                os.remove(path)


def desk_cds_names(r):
    """Names on our CDS desk, read from the warehouse document (read only)."""
    try:
        import boto3
        doc = json.loads(boto3.client("s3").get_object(Bucket=BUCKET, Key="data/cds-desk.json")["Body"].read())
    except Exception as exc:                  # noqa: BLE001
        say("our CDS desk document could not be read: %s" % clean(repr(exc))[:160])
        return set()
    names = set()

    def walk(x):
        if isinstance(x, dict):
            if isinstance(x.get("name"), str) and x.get("group") in ("us_corp", "global_corp", "sub", "sovereign"):
                names.add(x["name"])
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk(doc)
    return names


def main():
    global _say
    from ops_report import report
    with report(NAME) as r:
        _say = r.log
        r.heading("ops 6986 - the SEC's data library, measured before anything is banked")
        say("run at %s | read only: nothing is written to the warehouse" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        work = tempfile.mkdtemp(prefix="secaudit-")
        found = {}
        desk = desk_cds_names(r)
        for label, fn in (("library", lambda: found.update(library(r))), ("N-PORT", lambda: nport(r, found, work, desk)), ("N-MFP", lambda: nmfp(r, found, work)),
                          ("BDC", lambda: bdc(r, found, work)), ("the others", lambda: peek(r, found, work))):
            t0 = time.time()
            try:
                fn()
            except Exception:                 # noqa: BLE001 - one section failing must not hide the others
                r.fail("%s section stopped: %s" % (label, clean(traceback.format_exc())[-900:]))
            say("(%s took %.0fs)" % (label, time.time() - t0))
        r.kv(data_sets=len(found))
        r.ok("audit complete")


if __name__ == "__main__":
    main()
