"""Compact tables read out of two SEC data-set files. No network, no AWS: a zip in, plain dictionaries out.

    nmfp_month(zf)            one month of money market funds: what they hold, who they lend to, daily flows
    nport_quarter(path, dir)  one quarter of fund holdings: every bond and loan funds hold (how many funds, how much,
                              how they mark it), the credit default swaps funds hold (by name, sold or bought), and
                              the stock funds have out on loan

Identifiers: the SEC files carry CUSIP and ISIN. They are used here only to tell that two funds hold the same
security, inside a scratch database on local disk that is deleted with the run. Nothing this module returns carries a
CUSIP or an ISIN; a bond is described by issuer, title, coupon and maturity. Tickers of listed stocks are kept.

What these tables are not: a fund's holdings are as of its own report date (two to five months before the file is
posted) and its marks are its own valuations, not trades.
"""
from collections import Counter, defaultdict
import io
import os
import re
import sqlite3
import statistics

MONTHS = {m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())}
BOND_CLASSES = ("DBT", "LON", "ABS-O", "ABS-CBDO", "ABS-APCP", "SN")
CLASS_NAMES = {"EC": "Common stock", "EP": "Preferred stock", "DBT": "Bonds", "LON": "Loans", "ABS-MBS": "Mortgage-backed", "ABS-O": "Other asset-backed",
               "ABS-CBDO": "CLOs and CDOs", "ABS-APCP": "Asset-backed commercial paper", "STIV": "Short-term vehicles", "RA": "Repo", "SN": "Structured notes",
               "RE": "Real estate", "COMM": "Commodities", "DCR": "Credit derivatives", "DIR": "Rate derivatives", "DE": "Equity derivatives",
               "DFE": "Currency derivatives", "DCO": "Commodity derivatives", "DO": "Other derivatives", "OTHER": "Other"}
INDEX_WORDS = re.compile(r"\bCDX\b|ITRAXX|\bCMBX\b|\bMARKIT\b|\bINDEX\b|\bLCDX\b|\bMCDX\b|\bIBOXX\b", re.I)
JUNK_NAMES = {"", "N/A", "NA", "NONE", "CREDIT DEFAULT SWAPS", "CREDIT DEFAULT SWAP", "CDS", "VARIOUS", "OTHER", "UNKNOWN"}
MARK_RANGE = (1.0, 250.0)             # value per 100 of face outside this is a units slip, not a mark
MIN_LOOSE_VALUE = 1e6                 # a bond or loan with no identifier is kept when two funds hold it or it is worth this much
MIN_STOCK_HELD = 25e6                 # a stock is listed when funds hold this much of it


class NotHandled(Exception):
    """The file is not laid out the way this module reads (an older form version). Nothing is guessed."""


# ----------------------------------------------------------------------------------------------- small helpers
def d_iso(text):
    """'31-JUL-2026' -> '2026-07-31'; ISO dates pass through; anything else -> None."""
    t = (text or "").strip().upper()
    m = re.fullmatch(r"(\d{1,2})-([A-Z]{3})-(\d{4})", t)
    if m and m.group(2) in MONTHS:
        return "%s-%02d-%02d" % (m.group(3), MONTHS[m.group(2)], int(m.group(1)))
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", t[:10])
    return t[:10] if m else None


def fnum(x):
    try:
        v = float(x) if x not in (None, "", "N/A") else None
    except ValueError:
        return None
    return v if v is not None and v == v and abs(v) != float("inf") else None


ID_LIKE = re.compile(r"\b(?:[A-Z]{2}[A-Z0-9]{9}\d|(?=[A-Z0-9]*\d[A-Z0-9]*\d[A-Z0-9]*\d)[A-Z0-9]{9})\b")


def no_ids(text):
    """A name or title with anything shaped like an ISIN (12) or a CUSIP (9 characters, three or more digits) taken out."""
    t = (text or "").strip()
    if not ID_LIKE.search(t):
        return t
    return re.sub(r"\s+", " ", ID_LIKE.sub("", t)).strip(" -/|,") or "(not named)"


def tidy(name):
    """A name as funds write it, made comparable: upper case, no dots or commas, single spaces."""
    n = (name or "").upper().replace(".", "").replace(",", " ")
    return re.sub(r"\s+", " ", n).strip()


def repo_party(name):
    """(counterparty, 'FICC' or '') - a repo cleared at the Fixed Income Clearing Corp is written 'FICC - sponsor'."""
    n = tidy(name)
    m = re.match(r"^FIXED INCOME CLEARING CORP(?:ORATION)?(?: TRI ?PARTY REPO)?\s*[-/:]\s*(.+)$", n)
    if m:
        return m.group(1).strip() or "FIXED INCOME CLEARING CORP", "FICC"
    if n.startswith("FIXED INCOME CLEARING") or n in ("FICC", "FICC GSD"):
        return "FIXED INCOME CLEARING CORP", "FICC"
    return n or "(not named)", ""


def bank_name(name):
    """An issuer of deposits or paper, without the branch it books through."""
    n = re.sub(r"\s*\([^)]*\)", "", tidy(name))
    n = re.sub(r"[ /-]+(NEW YORK|NY|LONDON|CAYMAN|CHICAGO|HOUSTON|GRAND CAYMAN)( BRANCH)?$", "", n)
    return n.strip() or "(not named)"


def category_label(text):
    """The form's long category sentences cut to their name: '..Repurchase Agreement, if collateralized only by ..' -> '..Repurchase Agreement'."""
    c = (text or "").strip()
    for cut in (", if ", " (if ", ", collateralized"):
        i = c.find(cut)
        if i > 0:
            c = c[:i]
    return c or "(not stated)"


def ticker_of(text):
    """'AAPL US' or 'aapl' -> 'AAPL'; None for what is not a plain ticker."""
    t = (text or "").strip().upper().split(" ")[0]
    return t if re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,6}", t) and t not in ("N/A", "NA", "NONE") else None


def member(zf, stem):
    want = stem.upper()
    for info in zf.infolist():
        base = info.filename.rsplit("/", 1)[-1].upper()
        if base.rsplit(".", 1)[0] == want:
            return info
    return None


def rows_of(zf, info):
    """(upper-case header, iterator of field lists); a row that is not the header's width is skipped."""
    stream = io.TextIOWrapper(zf.open(info), encoding="utf-8", errors="replace", newline="")
    header = [h.strip().upper() for h in stream.readline().rstrip("\r\n").split("\t")]
    width = len(header)

    def gen():
        for line in stream:
            parts = line.rstrip("\r\n").split("\t")
            if len(parts) == width:
                yield parts
    return header, gen()


def dicts(zf, stem):
    info = member(zf, stem)
    if info is None:
        return
    header, rows = rows_of(zf, info)
    for parts in rows:
        yield dict(zip(header, parts))


def pct(sorted_values, q):
    """Nearest-rank percentile of a sorted list."""
    return sorted_values[min(len(sorted_values) - 1, max(0, int(round(q * (len(sorted_values) - 1)))))]


def top(counter):
    """The commonest value; a tie goes to the one that sorts last, so the answer never depends on the order rows were read in."""
    return max(counter.items(), key=lambda kv: (kv[1], str(kv[0])))[0] if counter else None


# ----------------------------------------------------------------------------------------------- N-MFP
def nmfp_month(zf, top_n=80):
    """One month of money market funds from a Form N-MFP data-set zip (the N-MFP3 layout)."""
    stems = {i.filename.rsplit("/", 1)[-1].rsplit(".", 1)[0].upper() for i in zf.infolist()}

    def stem(*words):
        return next((s for s in sorted(stems) if all(w in s for w in words)), None)

    s_sub, s_ser, s_sec = stem("SUBMISSION"), stem("SERIESLEVEL"), stem("SCHPORTFOLIO")
    if not (s_sub and s_ser and s_sec):
        raise NotHandled("tables missing: submission %s, series %s, portfolio %s" % (bool(s_sub), bool(s_ser), bool(s_sec)))
    subs = list(dicts(zf, s_sub))
    if not subs or "REPORTDATE" not in subs[0] or "SERIESID" not in subs[0]:
        raise NotHandled("submission table has no REPORTDATE / SERIESID")
    for s in subs:
        s["_d"], s["_f"] = d_iso(s.get("REPORTDATE")), d_iso(s.get("FILING_DATE")) or ""
    months = Counter(s["_d"][:7] for s in subs if s["_d"])
    if not months:
        raise NotHandled("no readable report date")
    month, n_month = months.most_common(1)[0]
    others = {m: n for m, n in months.items() if m != month and n >= 0.2 * n_month}
    if others:
        raise NotHandled("several months in one file (%s beside %s): a quarter-wide file is not read as one month" % (sorted(others), month))
    latest = {}
    for s in subs:
        if not s["_d"] or s["_d"][:7] != month:
            continue
        fund = s.get("SERIESID") or s.get("ACCESSION_NUMBER")
        if fund not in latest or (s["_f"], s.get("ACCESSION_NUMBER") or "") > (latest[fund]["_f"], latest[fund].get("ACCESSION_NUMBER") or ""):
            latest[fund] = s
    keep = {s["ACCESSION_NUMBER"] for s in latest.values()}
    dates = Counter(s["_d"] for s in latest.values())

    kind, net = {}, {}
    types = defaultdict(lambda: {"funds": 0, "net_assets": 0.0, "wam": 0.0, "wal": 0.0})
    first = None
    for s in dicts(zf, s_ser):
        first = first or s
        acc = s.get("ACCESSION_NUMBER")
        if acc not in keep:
            continue
        t = (s.get("MONEYMARKETFUNDCATEGORY") or "").strip() or "(not stated)"
        n = fnum(s.get("NETASSETOFSERIES")) or 0.0
        kind[acc], net[acc] = t, n
        g = types[t]
        g["funds"] += 1
        g["net_assets"] += n
        g["wam"] += n * (fnum(s.get("AVERAGEPORTFOLIOMATURITY")) or 0)
        g["wal"] += n * (fnum(s.get("AVERAGELIFEMATURITY")) or 0)
    if first is None or "NETASSETOFSERIES" not in first:
        raise NotHandled("series table has no NETASSETOFSERIES")

    def last_per_fund(stem_words, value_col, date_col):
        """{accession: value on its last date} from one of the dated tables."""
        s_ = stem(*stem_words)
        best = {}
        if not s_:
            return best
        for row in dicts(zf, s_):
            acc, d, v = row.get("ACCESSION_NUMBER"), d_iso(row.get(date_col)), fnum(row.get(value_col))
            if acc in keep and d and v is not None and (acc not in best or d > best[acc][0]):
                best[acc] = (d, v)
        return best

    gross = last_per_fund(("SEVENDAYGROSSYIELD",), "SEVENDAYGROSSYIELDVALUE", "SEVENDAYGROSSYIELDDATE")
    liq_d = last_per_fund(("LIQUIDASSETSDETAILS",), "PCTDAILYLIQUIDASSETS", "TOTLIQUIDASSETSNEARPCTDATE")
    liq_w = last_per_fund(("LIQUIDASSETSDETAILS",), "PCTWEEKLYLIQUIDASSETS", "TOTLIQUIDASSETSNEARPCTDATE")

    def weighted(table, t):
        num = sum(net[a] * v for a, (_, v) in table.items() if kind.get(a) == t)
        den = sum(net[a] for a in table if kind.get(a) == t)
        return round(num / den, 4) if den else None

    by_type = []
    for t, g in sorted(types.items(), key=lambda kv: -kv[1]["net_assets"]):
        n = g["net_assets"]
        by_type.append({"type": t, "funds": g["funds"], "net_assets": round(n), "wam_days": round(g["wam"] / n, 1) if n else None,
                        "wal_days": round(g["wal"] / n, 1) if n else None, "gross_yield_7d": weighted(gross, t),
                        "daily_liquid_pct": weighted(liq_d, t), "weekly_liquid_pct": weighted(liq_w, t)})

    cats = defaultdict(lambda: [0, 0.0])
    repo = {"total": 0.0, "treasury": 0.0, "agency": 0.0, "other": 0.0, "cleared": 0.0, "triparty": 0.0, "via_ficc": 0.0, "federal_reserve": 0.0}
    parties = defaultdict(lambda: [0.0, set()])
    banks = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, set()])          # total, deposits (CD), paper (CP), time deposits, funds
    n_rows, valcol = 0, None
    for s in dicts(zf, s_sec):
        if valcol is None:
            valcol = "INCLUDINGVALUEOFANYSPONSORSUPP" if "INCLUDINGVALUEOFANYSPONSORSUPP" in s else "EXCLUDINGVALUEOFANYSPONSORSUPP"
            if valcol not in s or "INVESTMENTCATEGORY" not in s:
                raise NotHandled("portfolio table has no value / INVESTMENTCATEGORY column")
        acc = s.get("ACCESSION_NUMBER")
        if acc not in keep:
            continue
        n_rows += 1
        v = fnum(s.get(valcol)) or 0.0
        raw = s.get("INVESTMENTCATEGORY") or ""
        c = cats[category_label(raw)]
        c[0] += 1
        c[1] += v
        up = raw.upper()
        if "REPURCHASE" in up:
            party, via = repo_party(s.get("NAMEOFISSUER"))
            if not via and "FIXED INCOME CLEARING" in tidy(s.get("NAMEOFCCP")):
                via = "FICC"
            repo["total"] += v
            repo["treasury" if up.startswith("U.S. TREASURY") else "agency" if up.startswith("U.S. GOVERNMENT AGENCY") else "other"] += v
            if (s.get("REPURCHASEAGREEMENTCLEAREDFLA") or "").upper() == "Y":
                repo["cleared"] += v
            if (s.get("REPURCHASEAGREEMENTTRIPARTYFL") or "").upper() == "Y":
                repo["triparty"] += v
            if via:
                repo["via_ficc"] += v
            if "FEDERAL RESERVE" in party:
                repo["federal_reserve"] += v
            p = parties[(party, via)]
            p[0] += v
            p[1].add(acc)
        else:
            col = 1 if "CERTIFICATE OF DEPOSIT" in up else 2 if "COMMERCIAL PAPER" in up else 3 if "TIME DEPOSIT" in up else 0
            if col:
                b = banks[bank_name(s.get("NAMEOFISSUER"))]
                b[0] += v
                b[col] += v
                b[4].add(acc)

    collateral = defaultdict(float)
    s_col = stem("COLLATERALISSUERS")
    if s_col:
        for c in dicts(zf, s_col):
            if c.get("ACCESSION_NUMBER") in keep:
                collateral[(c.get("CTGRYINVESTMENTSRPRSNTSCOLLATE") or "(not stated)").strip()[:60]] += fnum(c.get("VALUEOFCOLLATERALTOTHENEARESTC")) or 0.0

    days = defaultdict(lambda: [0.0, 0.0])
    days_type = defaultdict(lambda: defaultdict(float))
    s_flow = stem("DLYSHAREHOLDERFLOW")
    if s_flow:
        for f in dicts(zf, s_flow):
            acc, d = f.get("ACCESSION_NUMBER"), d_iso(f.get("DAILYSHAREHOLDERFLOWDATE"))
            if acc in keep and d:
                sub, red = fnum(f.get("DAILYGROSSSUBSCRIPTIONS")) or 0.0, fnum(f.get("DAILYGROSSREDEMPTIONS")) or 0.0
                days[d][0] += sub
                days[d][1] += red
                days_type[kind.get(acc, "(not stated)")][d] += sub - red

    ranked = sorted(parties.items(), key=lambda kv: -kv[1][0])
    ranked_banks = sorted(banks.items(), key=lambda kv: -kv[1][0])
    return {
        "month": month, "report_dates": dict(sorted(dates.items())), "funds": len(keep), "filings_in_file": len(subs),
        "net_assets": round(sum(net.values())), "by_type": by_type, "portfolio_rows": n_rows,
        "holdings": [{"category": k, "rows": c[0], "value": round(c[1])} for k, c in sorted(cats.items(), key=lambda kv: -kv[1][1])],
        "repo": dict({k: round(v) for k, v in repo.items()}, n_counterparties=len(parties),
                     counterparties=[[p, via, round(v[0]), len(v[1])] for (p, via), v in ranked[:top_n]],
                     counterparty_cols=["counterparty", "cleared_through", "value", "funds"],
                     collateral=[[k, round(v)] for k, v in sorted(collateral.items(), key=lambda kv: -kv[1])]),
        "unsecured": {"total": round(sum(b[0] for b in banks.values())), "n_issuers": len(banks),
                      "issuers": [[k, round(b[0]), round(b[1]), round(b[2]), round(b[3]), len(b[4])] for k, b in ranked_banks[:top_n]],
                      "issuer_cols": ["issuer", "value", "certificates_of_deposit", "commercial_paper", "time_deposits", "funds"]},
        "flows": {"cols": ["date", "subscriptions", "redemptions", "net"],
                  "days": [[d, round(v[0]), round(v[1]), round(v[0] - v[1])] for d, v in sorted(days.items())],
                  "net_by_type": {t: [[d, round(v)] for d, v in sorted(dd.items())] for t, dd in days_type.items()}},
    }


def nmfp_summary(doc):
    """The few figures of a month kept in the bank's manifest, so a history can be drawn without opening every month."""
    r = doc["repo"]
    return {"month": doc["month"], "funds": doc["funds"], "net_assets": doc["net_assets"],
            "by_type": {t["type"]: t["net_assets"] for t in doc["by_type"]},
            "repo": r["total"], "repo_via_ficc": r["via_ficc"], "repo_fed": r["federal_reserve"], "repo_treasury": r["treasury"],
            "unsecured": doc["unsecured"]["total"], "top_repo": [c[:3] for c in r["counterparties"][:10]],
            "net_flow": sum(d[3] for d in doc["flows"]["days"]), "flow_days": len(doc["flows"]["days"])}


# ----------------------------------------------------------------------------------------------- N-PORT
def _load(db, zf, stem, table, cols, numeric=(), dates=(), need=None, tally=None):
    """One member of the zip into one scratch table, only the named columns. Rows kept, or None when the member is absent.

    need: a column that must be filled for the row to be kept. tally(record dict) sees every kept row.
    """
    names = [c.lower() for c in cols]
    db.execute("CREATE TABLE %s (%s)" % (table, ", ".join("%s %s" % (n, "REAL" if c in numeric else "TEXT") for n, c in zip(names, cols))))
    info = member(zf, stem)
    if info is None:
        return None
    header, rows = rows_of(zf, info)
    idx = [header.index(c) if c in header else None for c in cols]
    kinds = [1 if c in numeric else 2 if c in dates else 0 for c in cols]
    need_i = header.index(need) if need and need in header else None
    sql = "INSERT INTO %s VALUES (%s)" % (table, ",".join("?" * len(cols)))
    n, batch = 0, []
    for parts in rows:
        if need_i is not None and parts[need_i] in ("", "N/A"):
            continue
        rec = []
        for i, k in zip(idx, kinds):
            v = parts[i] if i is not None else None
            rec.append(fnum(v) if k == 1 else d_iso(v) if k == 2 else v)
        if tally:
            tally(rec)
        batch.append(rec)
        if len(batch) >= 50000:
            db.executemany(sql, batch)
            n += len(batch)
            batch = []
    if batch:
        db.executemany(sql, batch)
        n += len(batch)
    db.commit()
    return n


def cds_reference(issuer, index_name):
    """('index' | 'name' | 'unnamed', label) for one credit swap."""
    if tidy(index_name) and tidy(index_name) not in JUNK_NAMES:
        return "index", index_family(index_name)
    nm = tidy(issuer)
    if nm in JUNK_NAMES:
        return "unnamed", "(not named)"
    if INDEX_WORDS.search(issuer or ""):
        return "index", index_family(issuer)
    return "name", nm


def index_family(text):
    """'MARKIT CDX NORTH AMERICAN INVESTMENT GRADE INDEX SERIES 46-V1' and 'CDX.NA.IG.S46' -> 'CDX NA IG 46'."""
    t = " " + re.sub(r"[^A-Z0-9]+", " ", (text or "").upper()) + " "
    series = re.search(r" (?:S|SERIES )?(\d{2})(?= |V)", t)
    fam = None
    if "ITRAXX" in t:
        fam = "ITRAXX XOVER" if ("XOVER" in t or "CROSSOVER" in t) else "ITRAXX SUB FIN" if " SUB" in t else "ITRAXX SNR FIN" if (" SNR" in t or "SENIOR" in t) \
            else "ITRAXX ASIA" if ("ASIA" in t or "AUSTRALIA" in t or "JAPAN" in t) else "ITRAXX EUROPE"
    elif "CMBX" in t:
        fam = "CMBX"
    elif " CDX " in t or "MARKIT" in t:
        fam = "CDX EM" if (" EM " in t or "EMERGING" in t) else "CDX NA HY" if (" HY " in t or "HIGH YIELD" in t) else "CDX NA IG" if (" IG " in t or "INVESTMENT GRADE" in t) \
            else "CDX" if " CDX " in t else None
    if not fam:
        return "INDEX, FAMILY NOT STATED" if INDEX_WORDS.search(t) else tidy(text)[:60]
    return "%s %s" % (fam, series.group(1)) if series else fam


def nport_quarter(zip_path, work_dir, progress=lambda msg: None):
    """One quarter of fund holdings from a Form N-PORT data-set zip: {'summary', 'bonds', 'cds', 'stock_loan'}.

    progress(text or None) is called between stages and every thousand securities: the caller may raise from it to stop
    a run that is out of time (the scratch database is removed either way).
    """
    import zipfile
    zf = zipfile.ZipFile(zip_path)
    for stem in ("SUBMISSION", "FUND_REPORTED_INFO", "FUND_REPORTED_HOLDING"):
        if member(zf, stem) is None:
            raise NotHandled("the archive has no %s table" % stem)
    db_path = os.path.join(work_dir, "nport-scratch.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    db = sqlite3.connect(db_path)
    try:
        for pragma in ("journal_mode=OFF", "synchronous=OFF", "temp_store=FILE", "cache_size=-400000"):
            db.execute("PRAGMA " + pragma)
        return _nport(db, zf, progress)
    finally:
        db.close()
        zf.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def _nport(db, zf, progress):
    q = lambda sql, args=(): db.execute(sql, args).fetchall()   # noqa: E731
    _load(db, zf, "SUBMISSION", "sub", ["ACCESSION_NUMBER", "FILING_DATE", "SUB_TYPE", "REPORT_DATE"], dates=("FILING_DATE", "REPORT_DATE"))
    _load(db, zf, "FUND_REPORTED_INFO", "info",
          ["ACCESSION_NUMBER", "SERIES_NAME", "SERIES_ID", "SERIES_LEI", "NET_ASSETS", "SALES_FLOW_MON1", "REINVESTMENT_FLOW_MON1", "REDEMPTION_FLOW_MON1",
           "SALES_FLOW_MON2", "REINVESTMENT_FLOW_MON2", "REDEMPTION_FLOW_MON2", "SALES_FLOW_MON3", "REINVESTMENT_FLOW_MON3", "REDEMPTION_FLOW_MON3"],
          numeric=("NET_ASSETS", "SALES_FLOW_MON1", "REINVESTMENT_FLOW_MON1", "REDEMPTION_FLOW_MON1", "SALES_FLOW_MON2", "REINVESTMENT_FLOW_MON2",
                   "REDEMPTION_FLOW_MON2", "SALES_FLOW_MON3", "REINVESTMENT_FLOW_MON3", "REDEMPTION_FLOW_MON3"))
    # one filing per fund: its latest report date, then its latest filing (an amendment replaces what it amends)
    db.execute("CREATE TABLE keep AS SELECT * FROM (SELECT s.accession_number, s.report_date, s.filing_date, i.series_name, i.net_assets,"
               " COALESCE(NULLIF(i.series_id,''), NULLIF(i.series_lei,''), 'N:'||i.series_name) AS fund,"
               " ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(i.series_id,''), NULLIF(i.series_lei,''), 'N:'||i.series_name)"
               " ORDER BY s.report_date DESC, s.filing_date DESC, s.accession_number DESC) AS rn"
               " FROM sub s JOIN info i ON i.accession_number=s.accession_number WHERE s.report_date IS NOT NULL) WHERE rn=1")
    db.execute("CREATE UNIQUE INDEX k_acc ON keep(accession_number)")
    kept = {a for (a,) in q("SELECT accession_number FROM keep")}
    n_filings = q("SELECT COUNT(*) FROM sub")[0][0]
    if not kept:
        raise NotHandled("no filing with a readable report date")
    progress("filings %d, funds kept %d" % (n_filings, len(kept)))

    classes = defaultdict(lambda: [0, 0.0, 0, 0.0])                  # rows, value, level-3 rows, level-3 value

    def tally(rec):                                                 # rec follows the column order of the load below
        if rec[1] in kept:
            c = classes[rec[10] or "(not stated)"]
            c[0] += 1
            c[1] += rec[8] or 0.0
            if rec[14] == "3":
                c[2] += 1
                c[3] += rec[8] or 0.0

    n_hold = _load(db, zf, "FUND_REPORTED_HOLDING", "hold",
                   ["HOLDING_ID", "ACCESSION_NUMBER", "ISSUER_NAME", "ISSUER_TITLE", "ISSUER_CUSIP", "BALANCE", "UNIT", "CURRENCY_CODE", "CURRENCY_VALUE",
                    "PAYOFF_PROFILE", "ASSET_CAT", "ISSUER_TYPE", "INVESTMENT_COUNTRY", "IS_RESTRICTED_SECURITY", "FAIR_VALUE_LEVEL", "DERIVATIVE_CAT"],
                   numeric=("BALANCE", "CURRENCY_VALUE"), tally=tally)
    progress(None)
    _load(db, zf, "DEBT_SECURITY", "debt",
          ["HOLDING_ID", "MATURITY_DATE", "COUPON_TYPE", "ANNUALIZED_RATE", "IS_DEFAULT", "ARE_ANY_INTEREST_PAYMENT", "IS_ANY_PORTION_INTEREST_PAID"],
          numeric=("ANNUALIZED_RATE",), dates=("MATURITY_DATE",))
    progress(None)
    _load(db, zf, "NONFOREIGN_EXCHANGE_SWAP", "swap",
          ["HOLDING_ID", "TERMINATION_DATE", "NOTIONAL_AMOUNT", "CURRENCY_CODE", "UNREALIZED_APPRECIATION", "FIXED_OR_FLOATING_RECEIPT", "FIXED_RATE_RECEIPT",
           "FIXED_OR_FLOATING_PAYMENT", "FIXED_RATE_PAYMENT"],
          numeric=("NOTIONAL_AMOUNT", "UNREALIZED_APPRECIATION", "FIXED_RATE_RECEIPT", "FIXED_RATE_PAYMENT"), dates=("TERMINATION_DATE",))
    _load(db, zf, "DESC_REF_OTHER", "refo", ["HOLDING_ID", "ISSUER_NAME"])
    _load(db, zf, "DESC_REF_INDEX_BASKET", "refi", ["HOLDING_ID", "INDEX_NAME"])
    _load(db, zf, "DERIVATIVE_COUNTERPARTY", "cpty", ["HOLDING_ID", "DERIVATIVE_COUNTERPARTY_NAME"])
    progress(None)
    _load(db, zf, "SECURITIES_LENDING", "lend", ["HOLDING_ID", "IS_LOAN_BY_FUND", "LOAN_VALUE"], numeric=("LOAN_VALUE",))
    progress(None)
    _load(db, zf, "IDENTIFIERS", "idt", ["HOLDING_ID", "IDENTIFIER_TICKER"], need="IDENTIFIER_TICKER")
    for stmt in ("CREATE INDEX h_cat ON hold(asset_cat)", "CREATE INDEX d_id ON debt(holding_id)", "CREATE INDEX s_id ON swap(holding_id)",
                 "CREATE INDEX ro_id ON refo(holding_id)", "CREATE INDEX ri_id ON refi(holding_id)", "CREATE INDEX c_id ON cpty(holding_id)",
                 "CREATE INDEX l_id ON lend(holding_id)", "CREATE INDEX t_id ON idt(holding_id)"):
        db.execute(stmt)
    db.commit()
    progress("holdings loaded: %s rows" % format(n_hold or 0, ","))

    # ---- summary
    dates = dict(q("SELECT report_date, COUNT(*) FROM keep GROUP BY 1 ORDER BY 1"))
    as_of = max(dates, key=lambda d: (dates[d], d))
    flows = defaultdict(lambda: [0.0, 0.0, 0.0, 0])
    for rd, s1, r1, x1, s2, r2, x2, s3, r3, x3 in q(
            "SELECT k.report_date, i.sales_flow_mon1, i.reinvestment_flow_mon1, i.redemption_flow_mon1, i.sales_flow_mon2, i.reinvestment_flow_mon2,"
            " i.redemption_flow_mon2, i.sales_flow_mon3, i.reinvestment_flow_mon3, i.redemption_flow_mon3 FROM keep k JOIN info i ON i.accession_number=k.accession_number"):
        y, m = int(rd[:4]), int(rd[5:7])
        for back, (sales, reinv, red) in ((2, (s1, r1, x1)), (1, (s2, r2, x2)), (0, (s3, r3, x3))):
            if sales is None and red is None:
                continue
            mm = (y * 12 + m - 1) - back
            f = flows["%04d-%02d" % (mm // 12, mm % 12 + 1)]
            f[0] += sales or 0.0
            f[1] += reinv or 0.0
            f[2] += red or 0.0
            f[3] += 1
    summary = {
        "as_of": as_of, "report_dates": dates, "filings_in_file": n_filings, "funds": len(kept),
        "net_assets": round(q("SELECT SUM(net_assets) FROM keep")[0][0] or 0), "holding_rows": sum(c[0] for c in classes.values()),
        "classes": [{"code": k, "name": CLASS_NAMES.get(k, k), "rows": c[0], "value": round(c[1]), "level3_rows": c[2], "level3_value": round(c[3])}
                    for k, c in sorted(classes.items(), key=lambda kv: -kv[1][1])],
        "flows_by_month": {m: {"sales": round(f[0]), "reinvested": round(f[1]), "redemptions": round(f[2]), "net": round(f[0] + f[1] - f[2]), "funds": f[3]}
                           for m, f in sorted(flows.items())},
    }

    # ---- bonds and loans: one row per security, across the funds that hold it
    marks = "(%s)" % ",".join("'%s'" % c for c in BOND_CLASSES)
    cur = db.execute(
        "SELECT CASE WHEN length(h.issuer_cusip)=9 AND h.issuer_cusip NOT IN ('000000000','N/A') THEN h.issuer_cusip"
        " ELSE 'N:'||UPPER(h.issuer_name)||'|'||UPPER(h.issuer_title) END AS k,"
        " h.accession_number, k.report_date, h.issuer_name, h.issuer_title, h.balance, h.unit, h.currency_code, h.currency_value, h.asset_cat, h.issuer_type,"
        " h.investment_country, h.is_restricted_security, h.fair_value_level, d.maturity_date, d.coupon_type, d.annualized_rate, d.is_default,"
        " d.are_any_interest_payment, d.is_any_portion_interest_paid"
        " FROM hold h JOIN keep k ON k.accession_number=h.accession_number LEFT JOIN debt d ON d.holding_id=h.holding_id"
        " WHERE h.asset_cat IN %s ORDER BY 1" % marks)
    cols = ["issuer", "title", "cls", "itype", "ctry", "cur", "cpn", "ck", "mat", "d", "funds", "face", "value", "px", "px_lo", "px_hi", "n_px",
            "n_default", "n_arrears", "n_pik", "n_restricted", "n_l3"]
    bonds, totals = [], {"securities": 0, "with_identifier": 0, "set_aside_small": 0, "flag_default": 0, "flag_arrears": 0, "flag_pik": 0}

    def close(key, grp):
        funds = {g[0] for g in grp}
        value = sum(g[7] or 0.0 for g in grp)
        loose = key.startswith("N:")
        if loose and len(funds) < 2 and value < MIN_LOOSE_VALUE:
            totals["set_aside_small"] += 1
            return
        day = top(Counter(g[1] for g in grp))
        px = sorted(g[7] / g[4] * 100 for g in grp if g[1] == day and g[5] == "PA" and g[6] == "USD" and g[4] and g[4] > 0 and g[7] and g[7] > 0
                    and MARK_RANGE[0] <= g[7] / g[4] * 100 <= MARK_RANGE[1])
        n_def, n_arr, n_pik = sum(g[16] == "Y" for g in grp), sum(g[17] == "Y" for g in grp), sum(g[18] == "Y" for g in grp)
        rates = Counter(round(g[15], 4) for g in grp if g[15] is not None)
        bonds.append([
            no_ids(top(Counter((g[2] or "").strip() for g in grp))), no_ids(top(Counter((g[3] or "").strip() for g in grp))), top(Counter(g[8] for g in grp)),
            top(Counter(g[9] for g in grp)), top(Counter(g[10] for g in grp)), top(Counter(g[6] for g in grp)), top(rates), top(Counter(g[14] for g in grp if g[14])),
            top(Counter(g[13] for g in grp if g[13])), day, len(funds), round(sum(g[4] or 0.0 for g in grp if g[5] == "PA")), round(value),
            round(statistics.median(px), 2) if px else None, round(pct(px, 0.1) if len(px) >= 5 else px[0], 2) if px else None,
            round(pct(px, 0.9) if len(px) >= 5 else px[-1], 2) if px else None, len(px), n_def, n_arr, n_pik,
            sum(g[11] == "Y" for g in grp), sum(g[12] == "3" for g in grp)])
        totals["securities"] += 1
        totals["with_identifier"] += 0 if loose else 1
        totals["flag_default"] += 1 if n_def else 0
        totals["flag_arrears"] += 1 if n_arr else 0
        totals["flag_pik"] += 1 if n_pik else 0

    key, grp = None, []
    for i, row in enumerate(cur):
        if row[0] != key:
            if grp:
                close(key, grp)
            key, grp = row[0], []
            if i % 1000 == 0:
                progress(None)
        grp.append(row[1:])
    if grp:
        close(key, grp)
    bonds.sort(key=lambda b: -(b[12] or 0))
    progress("bonds and loans: %s securities" % format(len(bonds), ","))

    # ---- credit default swaps funds hold
    names = defaultdict(lambda: {"rows": 0, "funds": set(), "sold": 0.0, "bought": 0.0, "unclear": 0.0, "other_ccy": 0, "ends": Counter()})
    indices = defaultdict(lambda: {"rows": 0, "funds": set(), "sold": 0.0, "bought": 0.0, "unclear": 0.0, "other_ccy": 0, "ends": Counter()})
    parties, cds_tot = defaultdict(lambda: [0, 0.0]), {"rows": 0, "notional": 0.0, "funds": set(), "unnamed": 0, "not_swaps": 0}
    cds_tot["not_swaps"] = q("SELECT COUNT(*) FROM hold h JOIN keep k ON k.accession_number=h.accession_number WHERE h.asset_cat='DCR' AND h.derivative_cat<>'SWP'")[0][0]
    for hid, acc, issuer, idx, notional, ccy, end, recv, pay in q(
            "SELECT h.holding_id, h.accession_number, (SELECT o.issuer_name FROM refo o WHERE o.holding_id=h.holding_id LIMIT 1),"
            " (SELECT i.index_name FROM refi i WHERE i.holding_id=h.holding_id LIMIT 1), s.notional_amount, s.currency_code, s.termination_date,"
            " s.fixed_or_floating_receipt, s.fixed_or_floating_payment FROM hold h JOIN keep k ON k.accession_number=h.accession_number"
            " JOIN swap s ON s.holding_id=h.holding_id WHERE h.asset_cat='DCR' AND h.derivative_cat='SWP'"):
        kind, label = cds_reference(issuer, idx)
        amount = abs(notional or 0.0)
        cds_tot["rows"] += 1
        cds_tot["funds"].add(acc)
        if kind == "unnamed":
            cds_tot["unnamed"] += 1
            continue
        book = names[label] if kind == "name" else indices[label]
        book["rows"] += 1
        book["funds"].add(acc)
        if end:
            book["ends"][end[:4]] += 1
        if (ccy or "USD").upper() != "USD":
            book["other_ccy"] += 1
            continue
        cds_tot["notional"] += amount
        side = "sold" if recv == "Fixed" and pay != "Fixed" else "bought" if pay == "Fixed" and recv != "Fixed" else "unclear"
        book[side] += amount
    for name, n in q("SELECT UPPER(c.derivative_counterparty_name), COUNT(*) FROM hold h JOIN keep k ON k.accession_number=h.accession_number"
                     " JOIN cpty c ON c.holding_id=h.holding_id WHERE h.asset_cat='DCR' GROUP BY 1"):
        parties[tidy(name)][0] += n

    def book_rows(book):
        out = [[k, v["rows"], len(v["funds"]), round(v["sold"]), round(v["bought"]), round(v["unclear"]), round(v["sold"] - v["bought"]), v["other_ccy"],
                top(v["ends"])] for k, v in book.items()]
        return sorted(out, key=lambda r_: -(r_[3] + r_[4] + r_[5]))
    cds = {"as_of": as_of, "cols": ["reference", "positions", "funds", "sold", "bought", "unclear", "net_sold", "not_in_dollars", "usual_end_year"],
           "names": book_rows(names), "indices": book_rows(indices),
           "counterparties": [[k, v[0]] for k, v in sorted(parties.items(), key=lambda kv: -kv[1][0])[:40]],
           "totals": {"positions": cds_tot["rows"], "funds": len(cds_tot["funds"]), "notional_usd": round(cds_tot["notional"]), "not_named": cds_tot["unnamed"],
                      "credit_derivatives_that_are_not_swaps": cds_tot["not_swaps"], "reference_names": len(names), "indices": len(indices)},
           "reading": "sold = the fund receives the fixed payment (it sold protection and carries the credit risk); bought = it pays it"}
    progress("credit swaps: %d names, %d indices" % (len(names), len(indices)))

    # ---- stock funds have out on loan
    cur = db.execute(
        "SELECT CASE WHEN length(h.issuer_cusip)=9 AND h.issuer_cusip NOT IN ('000000000','N/A') THEN h.issuer_cusip ELSE 'N:'||UPPER(h.issuer_name) END AS k,"
        " h.accession_number, h.issuer_name, h.balance, h.unit, h.currency_value, h.investment_country,"
        " (SELECT SUM(l.loan_value) FROM lend l WHERE l.holding_id=h.holding_id AND l.is_loan_by_fund='Y'),"
        " (SELECT t.identifier_ticker FROM idt t WHERE t.holding_id=h.holding_id LIMIT 1)"
        " FROM hold h JOIN keep k ON k.accession_number=h.accession_number WHERE h.asset_cat='EC' ORDER BY 1")
    loans, loan_tot = [], {"held": 0.0, "on_loan": 0.0, "stocks": 0, "set_aside_small": 0}

    def close_stock(grp):
        held = sum(g[4] or 0.0 for g in grp)
        lent = sum(g[6] or 0.0 for g in grp)
        loan_tot["held"] += held
        loan_tot["on_loan"] += lent
        if held < MIN_STOCK_HELD:
            loan_tot["set_aside_small"] += 1
            return
        tick = Counter(t for t in (ticker_of(g[7]) for g in grp) if t)
        loans.append([top(tick), no_ids(top(Counter((g[1] or "").strip() for g in grp))), top(Counter(g[5] for g in grp)), round(held),
                      round(sum(g[2] or 0.0 for g in grp if g[3] == "NS")), round(lent), round(100 * lent / held, 2) if held > 0 else None,
                      len({g[0] for g in grp}), len({g[0] for g in grp if g[6]})])
        loan_tot["stocks"] += 1

    key, grp = None, []
    for i, row in enumerate(cur):
        if row[0] != key:
            if grp:
                close_stock(grp)
            key, grp = row[0], []
            if i % 1000 == 0:
                progress(None)
        grp.append(row[1:])
    if grp:
        close_stock(grp)
    loans.sort(key=lambda r_: -(r_[3] or 0))
    stock_loan = {"as_of": as_of, "cols": ["ticker", "issuer", "ctry", "held", "shares", "on_loan", "on_loan_pct", "funds", "funds_lending"], "rows": loans,
                  "totals": {"held": round(loan_tot["held"]), "on_loan": round(loan_tot["on_loan"]), "stocks_listed": loan_tot["stocks"],
                             "stocks_under_the_floor": loan_tot["set_aside_small"], "floor": MIN_STOCK_HELD},
                  "reading": "on_loan = the value of the shares funds reported as lent out on their report date; lent shares are what short sellers borrow"}
    progress("stock on loan: %d stocks" % len(loans))
    return {"summary": summary, "bonds": {"as_of": as_of, "cols": cols, "rows": bonds, "totals": totals,
                                          "reading": "px = the middle mark (value per 100 of face) among funds reporting on the commonest date; px_lo / px_hi = the "
                                                     "10th and 90th percentile (lowest and highest under five marks); marks are funds' own valuations, in dollars only"},
            "cds": cds, "stock_loan": stock_loan}


def nport_summary(docs):
    """The few figures of a quarter kept in the bank's manifest."""
    s, c, l, b = docs["summary"], docs["cds"]["totals"], docs["stock_loan"]["totals"], docs["bonds"]["totals"]
    return {"as_of": s["as_of"], "funds": s["funds"], "net_assets": s["net_assets"], "holding_rows": s["holding_rows"], "securities": b["securities"],
            "flag_default": b["flag_default"], "cds_positions": c["positions"], "cds_names": c["reference_names"], "cds_notional_usd": c["notional_usd"],
            "stock_held": l["held"], "stock_on_loan": l["on_loan"]}
