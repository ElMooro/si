"""justhodl-sec-datasets offline tests.  Archives are built here in the layouts of the SEC's own readmes (every column
of the tables the engine reads, in the SEC's order); an in-memory S3 and a made-up sec.gov stand in for the outside.

Checked: what a data-set page advertises and how a run is planned (newest of every set first, a byte budget, history
newest first); a month of money funds (one filing per fund, both month-end dates, repo by counterparty with the
clearing house split out, unsecured lending, daily flows; files this module does not read are refused, never
guessed); a quarter of fund holdings (an amendment replaces what it amends, one bond across funds that name it
differently, marks, flags, loans nobody else holds set aside, credit swaps sold and bought, stock on loan); that no
published table carries a CUSIP or an ISIN; the whole engine twice over (dry run writes nothing, a run banks, reads
and publishes, a second run does not fetch again, a file the SEC changed is banked again, raw files stay outside
data/, a run short of time reads the quarter later).  No network, no AWS.

Run: python3 aws/lambdas/justhodl-sec-datasets/tests/run_tests.py
"""
from types import SimpleNamespace
import gzip
import io
import json
import os
import re
import shutil
import sys
import tempfile
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "source")
sys.path[:0] = [SRC]
BEGUN = []
sys.modules["chain_guard"] = SimpleNamespace(begin=lambda event, context=None: BEGUN.append(event) or 0)
sys.modules.setdefault("boto3", SimpleNamespace(client=lambda *a, **k: None))
sys.modules.setdefault("botocore", SimpleNamespace())
sys.modules.setdefault("botocore.config", SimpleNamespace(Config=lambda **k: None))

import sec_derive as D  # noqa: E402
import sec_sets as S  # noqa: E402
import lambda_function as LF  # noqa: E402

CUSIPS = {"cw": "21873SAB4", "cz": "21874AAC0", "ford": "345370DA5", "dead": "111111AA1", "nvda": "67066G104", "ionq": "46222L108"}
ISIN = "US21873SAB42"


def tsv(header, rows):
    return ("\t".join(header) + "\n" + "".join("\t".join("" if x is None else str(x) for x in r) + "\n" for r in rows)).encode()


def zbytes(members):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as z:
        for n, data in members.items():
            z.writestr(n, data)
    return b.getvalue()


# ----------------------------------------------------------------------------------------------- made-up archives
def nport_zip(amend=True):
    sub_h = "ACCESSION_NUMBER FILING_DATE FILE_NUM SUB_TYPE REPORT_ENDING_PERIOD REPORT_DATE IS_LAST_FILING".split()
    info_h = ["ACCESSION_NUMBER", "SERIES_NAME", "SERIES_ID", "SERIES_LEI", "TOTAL_ASSETS", "TOTAL_LIABILITIES", "NET_ASSETS", "SALES_FLOW_MON1", "REINVESTMENT_FLOW_MON1",
              "REDEMPTION_FLOW_MON1", "SALES_FLOW_MON2", "REINVESTMENT_FLOW_MON2", "REDEMPTION_FLOW_MON2", "SALES_FLOW_MON3", "REINVESTMENT_FLOW_MON3", "REDEMPTION_FLOW_MON3"]
    hold_h = ("ACCESSION_NUMBER HOLDING_ID ISSUER_NAME ISSUER_LEI ISSUER_TITLE ISSUER_CUSIP BALANCE UNIT OTHER_UNIT_DESC CURRENCY_CODE CURRENCY_VALUE EXCHANGE_RATE "
              "PERCENTAGE PAYOFF_PROFILE ASSET_CAT OTHER_ASSET ISSUER_TYPE OTHER_ISSUER INVESTMENT_COUNTRY IS_RESTRICTED_SECURITY FAIR_VALUE_LEVEL DERIVATIVE_CAT").split()
    debt_h = "HOLDING_ID MATURITY_DATE COUPON_TYPE ANNUALIZED_RATE IS_DEFAULT ARE_ANY_INTEREST_PAYMENT IS_ANY_PORTION_INTEREST_PAID IS_CONVTIBLE_MANDATORY IS_CONVTIBLE_CONTINGENT".split()
    swap_h = ("HOLDING_ID SWAP_FLAG TERMINATION_DATE UPFRONT_PAYMENT PMNT_CURRENCY_CODE UPFRONT_RECEIPT RCPT_CURRENCY_CODE NOTIONAL_AMOUNT CURRENCY_CODE "
              "UNREALIZED_APPRECIATION FIXED_OR_FLOATING_RECEIPT FIXED_RATE_RECEIPT FLOATING_RATE_INDEX_RECEIPT FLOATING_RATE_SPREAD_RECEIPT CURRENCY_CODE_RECEIPT "
              "AMOUNT_RECEIPT FIXED_OR_FLOATING_PAYMENT FIXED_RATE_PAYMENT FLOATING_RATE_INDEX_PAYMENT FLOATING_RATE_SPREAD_PAYMENT CURRENCY_CODE_PAYMENT AMOUNT_PAYMENT").split()
    subs, infos, holds, debts, swaps, refo, refi, cpty, lend, idt = [], [], [], [], [], [], [], [], [], []
    hid = [0]

    def hold(acc, name, title, cusip, bal, unit, value, cat, restricted="N", level="2", deriv="N/A", ctry="US", itype="CORP", cur="USD"):
        hid[0] += 1
        holds.append([acc, hid[0], name, "", title, cusip, bal, unit, "", cur, value, 1, 0.1, "Long", cat, "", itype, "", ctry, restricted, level, deriv])
        return hid[0]

    marks = [98.0, 99.0, 100.0, 101.0, 102.0, 97.0]                # CoreWeave marks of the six funds
    for f in range(6):
        acc = "0001-26-%06d" % f
        day = "31-JUL-2026" if f < 4 else "30-JUN-2026"
        subs.append([acc, "15-SEP-2026", "811-1", "NPORT-P", "30-SEP-2026", day, "N"])
        infos.append([acc, "Fund %d" % f, "S%09d" % f, "", 5e9, 1e8, 4.9e9, 10, 1, 20, 30, 1, 20, 50, 1, 20])
        # one bond, written three ways by the funds, tied by its identifier
        name = ["CoreWeave Inc", "COREWEAVE INC", "CoreWeave, Inc. %s" % ISIN][f % 3]
        h = hold(acc, name, "CRWV 9.25 06/01/30 144A", CUSIPS["cw"], 1e7, "PA", 1e7 * marks[f] / 100, "DBT", restricted="Y" if f % 2 else "N")
        debts.append([h, "01-JUN-2030", "Fixed", 9.25, "N", "N", "N", "N", "N"])
        h = hold(acc, "Dead Co LLC", "DEAD 8 03/01/28", CUSIPS["dead"], 2e6, "PA", 2e6 * 0.4, "DBT")
        debts.append([h, "01-MAR-2028", "Fixed", 8.0, "Y", "Y", "N", "N", "N"])
        # a loan with no identifier that every fund holds (kept) and one only this fund holds, tiny (set aside)
        h = hold(acc, "Shared Borrower LLC", "Term Loan B", "N/A", 5e6, "PA", 5e6 * 0.75, "LON", level="3")
        debts.append([h, "01-JAN-2030", "Floating", 8.5, "N", "N", "Y", "N", "N"])
        h = hold(acc, "Consumer loan %d" % f, "Loan %d" % f, "000000000", 1000, "PA", 900, "LON", level="3")
        debts.append([h, "01-JAN-2027", "Fixed", 12.0, "N", "N", "N", "N", "N"])
        # credit swaps: Ford sold by even funds, bought by odd; an index; one not named; one in euros
        h = hold(acc, "ICE Clear Credit", "CDS Ford", "N/A", 1, "NC", 12345, "DCR", deriv="SWP")
        sold = f % 2 == 0
        swaps.append([h, "Y", "20-DEC-2030", 0, "USD", 0, "USD", 1e7, "USD", 5000, "Fixed" if sold else "Other", 5.0 if sold else "", "", "", "USD", 0,
                      "Other" if sold else "Fixed", "" if sold else 5.0, "", "", "USD", 0])
        refo.append([h, h, "Ford Motor Co.", "F 4.346 2026", CUSIPS["ford"], "", "", "", ""])
        cpty.append([h, h, "ICE Clear Credit LLC", ""])
        h = hold(acc, "ICE Clear Credit", "CDX HY", "N/A", 1, "NC", 999, "DCR", deriv="SWP")
        swaps.append([h, "Y", "20-DEC-2030", 0, "USD", 0, "USD", 2e7, "USD", 100, "Fixed", 5.0, "", "", "USD", 0, "Other", "", "", "", "USD", 0])
        refi.append([h, "CDX.NA.HY.S46", "X", ""])
        if f == 0:
            h = hold(acc, "Bank", "CDS", "N/A", 1, "NC", 1, "DCR", deriv="SWP")
            swaps.append([h, "Y", "20-DEC-2028", 0, "USD", 0, "USD", 3e6, "USD", 1, "Fixed", 1.0, "", "", "USD", 0, "Other", "", "", "", "USD", 0])
            refo.append([h, h, "N/A", "", "", "", "", "", ""])
            h = hold(acc, "Bank", "CDS Intesa", "N/A", 1, "NC", 1, "DCR", deriv="SWP")
            swaps.append([h, "Y", "20-DEC-2029", 0, "EUR", 0, "EUR", 4e6, "EUR", 1, "Other", "", "", "", "EUR", 0, "Fixed", 1.0, "", "", "EUR", 0])
            refo.append([h, h, "Intesa Sanpaolo SpA", "", "", "", "", "", ""])
            h = hold(acc, "Bank", "CDX option", "N/A", 1, "NC", 1, "DCR", deriv="SWO")
        # stock: Nvidia barely lent, IonQ mostly lent, a small company under the floor
        h = hold(acc, "NVIDIA Corp", "Common", CUSIPS["nvda"], 1e6, "NS", 6e8, "EC", level="1")
        lend.append([h, "N", 0, "N", 0, "Y", 6e6])
        idt.append([h, h, "US67066G1040", "NVDA", "", ""])
        h = hold(acc, "IonQ, Inc.", "Common", CUSIPS["ionq"], 2e6, "NS", 1e8, "EC", level="1")
        lend.append([h, "N", 0, "N", 0, "Y" if f < 5 else "N", 8e7 if f < 5 else 0])
        idt.append([h, h, "", "ionq us", "", ""])
        h = hold(acc, "Tiny Co", "Common", "999999999", 10, "NS", 1e5, "EC", level="1")
        lend.append([h, "N", 0, "N", 0, "N", 0])
    if amend:                                    # fund 0 amends: the amendment must replace its first filing, not add to it
        acc = "0001-26-900000"
        subs.append([acc, "20-SEP-2026", "811-1", "NPORT-P/A", "30-SEP-2026", "31-JUL-2026", "N"])
        infos.append([acc, "Fund 0", "S%09d" % 0, "", 5e9, 1e8, 4.8e9, 10, 1, 20, 30, 1, 20, 50, 1, 20])
        h = hold(acc, "CoreWeave Inc", "CRWV 9.25 06/01/30 144A", CUSIPS["cw"], 1e7, "PA", 1e7 * 0.985, "DBT")
        debts.append([h, "01-JUN-2030", "Fixed", 9.25, "N", "N", "N", "N", "N"])
    return zbytes({
        "SUBMISSION.tsv": tsv(sub_h, subs), "FUND_REPORTED_INFO.tsv": tsv(info_h, infos), "FUND_REPORTED_HOLDING.tsv": tsv(hold_h, holds),
        "DEBT_SECURITY.tsv": tsv(debt_h, debts), "NONFOREIGN_EXCHANGE_SWAP.tsv": tsv(swap_h, swaps),
        "DESC_REF_OTHER.tsv": tsv("HOLDING_ID DESC_REF_OTHER_ID ISSUER_NAME ISSUE_TITLE CUSIP ISIN TICKER OTHER_IDENTIFIER OTHER_DESC".split(), refo),
        "DESC_REF_INDEX_BASKET.tsv": tsv("HOLDING_ID INDEX_NAME INDEX_IDENTIFIER NARRATIVE_DESC".split(), refi),
        "DERIVATIVE_COUNTERPARTY.tsv": tsv("HOLDING_ID DERIVATIVE_COUNTERPARTY_ID DERIVATIVE_COUNTERPARTY_NAME DERIVATIVE_COUNTERPARTY_LEI".split(), cpty),
        "SECURITIES_LENDING.tsv": tsv("HOLDING_ID IS_CASH_COLLATERAL CASH_COLLATERAL_AMOUNT IS_NON_CASH_COLLATERAL NON_CASH_COLLATERAL_VALUE IS_LOAN_BY_FUND LOAN_VALUE".split(), lend),
        "IDENTIFIERS.tsv": tsv("HOLDING_ID IDENTIFIERS_ID IDENTIFIER_ISIN IDENTIFIER_TICKER OTHER_IDENTIFIER OTHER_IDENTIFIER_DESC".split(), idt)})


def nmfp_zip(n=5, month="OCT", year=2026, net=5e10, extra_month=0, drop=None):
    sub, ser, sec, flow, col, gy, liq = [], [], [], [], [], [], []
    for i in range(n):
        acc = "A%02d" % i
        day = "31-%s-%d" % (month, year) if i % 2 == 0 else "30-%s-%d" % (month, year)          # funds use either month-end date
        sub.append([acc, "05-NOV-%d" % year, "N-MFP3", "1", day, "Registrant", "1", "", "Series", "Series", "", "SID%02d" % i])
        kind = "Government" if i < 3 else "Prime"
        ser.append([acc, kind, net, 30 + i, 60 + i])
        sec += [[acc, 1, "Federal Reserve Bank of New York", "U.S. Treasury Repurchase Agreement, if collateralized only by U.S. Treasuries (including Strips) and cash", "N", "N", "", 1e10],
                [acc, 2, "Fixed Income Clearing Corp - Morgan Stanley", "U.S. Treasury Repurchase Agreement, if collateralized only by U.S. Treasuries (including Strips) and cash", "Y", "N", "", 2e10],
                [acc, 3, "J.P. Morgan Securities, LLC", "U.S. Government Agency Repurchase Agreement, collateralized only by U.S. Government Agency securities", "N", "Y", "", 5e9],
                [acc, 4, "JP MORGAN SECURITIES LLC", "Other Repurchase Agreement, if collateral falls outside Treasury, Government Agency and cash", "N", "Y", "", 1e9],
                [acc, 5, "Sumitomo Mitsui Banking Corp (New York Branch)", "Certificate of Deposit", "N", "N", "", 4e9],
                [acc, 6, "SUMITOMO MITSUI BANKING CORP", "Financial Company Commercial Paper", "N", "N", "", 1e9],
                [acc, 7, "U.S. Treasury Bill", "U.S. Treasury Debt", "N", "N", "", 9e9]]
        flow += [[acc, "C1", 1e9, 9e8, "%02d-%s-%d" % (d, month, year)] for d in (1, 2, 5)]
        col.append([acc, 1, "UST", "U.S. Treasuries (including strips)", 1.02e10])
        gy += [[acc, 4.10, "29-%s-%d" % (month, year)], [acc, 4.20 + 0.1 * (kind == "Prime"), "30-%s-%d" % (month, year)]]
        liq.append([acc, 5e9, 7e9, 0.61, 0.78, "30-%s-%d" % (month, year)])
    if n:                                        # fund 0 files twice for the month: the later filing counts, once
        sub.append(["A00b", "09-NOV-%d" % year, "N-MFP3/A", "1", "31-%s-%d" % (month, year), "Registrant", "1", "", "Series", "Series", "", "SID00"])
        ser.append(["A00b", "Government", net * 2, 31, 61])
        sec.append(["A00b", 1, "Federal Reserve Bank of New York", "U.S. Treasury Repurchase Agreement, if collateralized only by U.S. Treasuries (including Strips) and cash", "N", "N", "", 3e10])
    for i in range(extra_month):
        sub.append(["B%02d" % i, "05-OCT-%d" % year, "N-MFP3", "1", "30-SEP-%d" % year, "R", "1", "", "S", "S", "", "OLD%02d" % i])
    members = {
        "NMFP_SUBMISSION.tsv": tsv("ACCESSION_NUMBER FILING_DATE SUBMISSIONTYPE CIK REPORTDATE REGISTRANTFULLNAME FILER_CIK REGISTRANTLEIID SERIES_NAME NAMEOFSERIES LEIOFSERIES SERIESID".split(), sub),
        "NMFP_SERIESLEVELINFO.tsv": tsv("ACCESSION_NUMBER MONEYMARKETFUNDCATEGORY NETASSETOFSERIES AVERAGEPORTFOLIOMATURITY AVERAGELIFEMATURITY".split(), ser),
        "NMFP_SCHPORTFOLIOSECURITIES.tsv": tsv("ACCESSION_NUMBER SECURITY_ID NAMEOFISSUER INVESTMENTCATEGORY REPURCHASEAGREEMENTCLEAREDFLA REPURCHASEAGREEMENTTRIPARTYFL NAMEOFCCP INCLUDINGVALUEOFANYSPONSORSUPP".split(), sec),
        "NMFP_DLYSHAREHOLDERFLOWREPORT.tsv": tsv("ACCESSION_NUMBER CLASSESID DAILYGROSSSUBSCRIPTIONS DAILYGROSSREDEMPTIONS DAILYSHAREHOLDERFLOWDATE".split(), flow),
        "NMFP_COLLATERALISSUERS.tsv": tsv("ACCESSION_NUMBER SECURITY_ID NAMEOFCOLLATERALISSUER CTGRYINVESTMENTSRPRSNTSCOLLATE VALUEOFCOLLATERALTOTHENEARESTC".split(), col),
        "NMFP_SEVENDAYGROSSYIELD.tsv": tsv("ACCESSION_NUMBER SEVENDAYGROSSYIELDVALUE SEVENDAYGROSSYIELDDATE".split(), gy),
        "NMFP_LIQUIDASSETSDETAILS.tsv": tsv("ACCESSION_NUMBER TOTVALUEDAILYLIQUIDASSETS TOTVALUEWEEKLYLIQUIDASSETS PCTDAILYLIQUIDASSETS PCTWEEKLYLIQUIDASSETS TOTLIQUIDASSETSNEARPCTDATE".split(), liq)}
    for name in (drop or []):
        members.pop(name)
    return zbytes(members)


# ----------------------------------------------------------------------------------------------- the made-up outside
class FakeS3:
    def __init__(self):
        self.objects, self.meta, self.puts = {}, {}, []

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.puts.append(Key)

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def upload_file(self, path, Bucket, Key, ExtraArgs=None):
        with open(path, "rb") as fh:
            self.objects[Key] = fh.read()
        self.meta[Key] = (ExtraArgs or {}).get("Metadata")
        self.puts.append(Key)

    def download_file(self, Bucket, Key, path):
        with open(path, "wb") as fh:
            fh.write(self.objects[Key])

    def doc(self, key):
        body = self.objects[key]
        return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)


def page(links):
    return ("<html><h1>x</h1><table>" + "".join('<tr><td><a href="%s">f</a></td><td>ZIP</td><td>%s</td></tr>' % (u, s) for u, s in links) + "</table></html>").encode()


def web():
    """A small sec.gov: seven pages, their files."""
    files = {
        "/files/dera/data/form-n-port-data-sets/2026q3_nport.zip": nport_zip(),
        "/files/dera/data/form-n-port-data-sets/2026q2_nport.zip": nport_zip(amend=False),
        "/files/dera/data/form-n-mfp-data-sets/20261008-20261106_nmfp.zip": nmfp_zip(),
        "/files/dera/data/form-n-mfp-data-sets/20260909-20261007_nmfp.zip": nmfp_zip(month="SEP"),
        "/files/dera/data/form-n-mfp-data-sets/2010q4_nmfp.zip": nmfp_zip(month="DEC", year=2010, extra_month=4),
        "/files/x/bdc-data-sets/2026_10_bdc.zip": zbytes({"soi.tsv": b"adsh\n1\n"}),
        "/files/x/form-13f-data-sets/01jun2026-31aug2026_form13f.zip": zbytes({"INFOTABLE.tsv": b"A\n1\n"}),
        "/files/x/insider-transactions-data-sets/2026q3_form345.zip": zbytes({"SUBMISSION.tsv": b"A\n1\n"}),
        "/files/x/form-d-data-sets/2026q3_d.zip": zbytes({"OFFERING.tsv": b"A\n1\n"}),
        "/files/x/form-n-cen-data-sets/2026q3_ncen.zip": zbytes({"FUND_REPORTED_INFO.tsv": b"A\n1\n"}),
    }
    site = {"https://www.sec.gov" + k: v for k, v in files.items()}
    for name, spec in S.SETS.items():
        mine = [(k, "%.2f KB" % (len(v) / 1024)) for k, v in files.items() if re.search(spec["match"], k)]
        site[spec["page"]] = page(mine + [("/files/other/stranger.zip", "1 MB"), ("https://example.com/x/2026q3_nport.zip", "1 MB")])
    return site


class World:
    def __init__(self):
        self.s3, self.site, self.calls, self.modified = FakeS3(), web(), [], {}
        self.work = tempfile.mkdtemp(prefix="secds-test-")
        LF._S3, LF.WORK = self.s3, os.path.join(self.work, "w")
        LF.sec_get = self.get
        LF.MIN_MONEY_FUNDS, LF.MIN_MONEY_NET, LF.MIN_FUNDS, LF.MIN_SECURITIES = 3, 1e9, 3, 2

    def get(self, url, to_path=None, method="GET", timeout=0):
        self.calls.append((method, url))
        body = self.site.get(url)
        if body is None:
            return 404, None, {}
        headers = {"content-length": str(len(body)), "last-modified": self.modified.get(url, "Wed, 30 Sep 2026 12:00:00 GMT")}
        if method == "HEAD":
            return 200, None, headers
        if to_path:
            with open(to_path, "wb") as fh:
                fh.write(body)
            return 200, None, headers
        return 200, body, headers

    def close(self):
        shutil.rmtree(self.work, ignore_errors=True)


class Ctx:
    def __init__(self, seconds):
        self.ms = seconds * 1000

    def get_remaining_time_in_millis(self):
        return self.ms


def quarter(amend=True):
    tmp = tempfile.mkdtemp(prefix="secds-q-")
    try:
        path = os.path.join(tmp, "q.zip")
        with open(path, "wb") as fh:
            fh.write(nport_zip(amend))
        return D.nport_quarter(path, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ----------------------------------------------------------------------------------------------- tests
def test_file_names_are_dated():
    assert S.period_of("2026q3_nport.zip") == (2026, 9, 30) and S.period_of("2026_10_bdc.zip") == (2026, 10, 31)
    assert S.period_of("20261008-20261106_nmfp.zip") == (2026, 11, 6) and S.period_of("01jun2026-31aug2026_form13f.zip") == (2026, 8, 31)
    assert S.period_of("2010q4_nmfp.zip") == (2010, 12, 31) and S.period_of("readme.zip") is None
    assert S.size_of("440.95 MB") == int(440.95 * 1024 ** 2) and S.size_of("602.94 KB") == int(602.94 * 1024) and S.size_of("ZIP") is None


def test_a_page_advertises_only_its_own_files():
    site = web()
    spec = S.SETS["nport"]
    files = S.advertised(site[spec["page"]], spec["page"], spec["match"])
    assert [f["name"] for f in files] == ["2026q3_nport.zip", "2026q2_nport.zip"], files           # newest first; the stranger and the off-site link are not taken
    assert all(f["url"].startswith("https://www.sec.gov/") for f in files) and files[0]["size_shown"] > 0
    spec = S.SETS["nmfp"]
    assert [f["name"] for f in S.advertised(site[spec["page"]], spec["page"], spec["match"])] == ["20261008-20261106_nmfp.zip", "20260909-20261007_nmfp.zip", "2010q4_nmfp.zip"]
    assert S.advertised(b"<html>moved</html>", spec["page"], spec["match"]) == []


def test_a_run_is_planned_newest_of_every_set_first():
    lib = {"a": [{"name": "a3", "period": (2026, 9, 30), "size_shown": 400}, {"name": "a2", "period": (2026, 6, 30), "size_shown": 400}, {"name": "a1", "period": (2026, 3, 31), "size_shown": 400}],
           "b": [{"name": "b2", "period": (2026, 10, 31), "size_shown": 10}, {"name": "b1", "period": (2026, 9, 30), "size_shown": 10}]}
    plan = S.plan(lib, {}, byte_budget=830)
    assert [(s, f["name"], t) for s, f, t in plan] == [("b", "b2", 0), ("a", "a3", 0), ("b", "b1", 1), ("a", "a2", 1)], plan      # a1 does not fit
    assert [f["name"] for _, f, _ in S.plan(lib, {"a": {"a3": {}}, "b": {"b2": {}, "b1": {}}}, 10 ** 9)] == ["a2", "a1"]        # history only, newest first
    assert [f["name"] for _, f, _ in S.plan(lib, {}, byte_budget=1)] == ["b2"]                                             # one file always moves
    assert S.plan(lib, {"a": {"a3": {}, "a2": {}, "a1": {}}, "b": {"b2": {}, "b1": {}}}, 10 ** 9) == []
    assert len(S.plan(lib, {}, 10 ** 9, max_files=2)) == 2


def test_small_readers():
    assert D.d_iso("31-JUL-2026") == "2026-07-31" and D.d_iso("2026-07-31") == "2026-07-31" and D.d_iso("soon") is None and D.d_iso("") is None
    assert D.fnum("1.5") == 1.5 and D.fnum("") is None and D.fnum("N/A") is None and D.fnum("nan") is None and D.fnum("inf") is None and D.fnum("x") is None
    assert D.repo_party("Fixed Income Clearing Corp. Tri Party Repo - BNY Mellon") == ("BNY MELLON", "FICC")
    assert D.repo_party("FIXED INCOME CLEARING CORP - MORGAN STANLEY") == ("MORGAN STANLEY", "FICC") and D.repo_party("FICC")[1] == "FICC"
    assert D.repo_party("J.P. Morgan Securities, LLC") == ("JP MORGAN SECURITIES LLC", "") == D.repo_party("JP MORGAN SECURITIES LLC")
    assert D.bank_name("Toronto-Dominion Bank (New York Branch)") == "TORONTO-DOMINION BANK" and D.bank_name("MUFG BANK LTD/NEW YORK") == "MUFG BANK LTD"
    assert D.category_label("U.S. Treasury Repurchase Agreement, if collateralized only by U.S. Treasuries") == "U.S. Treasury Repurchase Agreement"
    assert D.category_label("U.S. Government Agency Debt (if categorized as coupon-paying notes)") == "U.S. Government Agency Debt" and D.category_label("") == "(not stated)"
    assert D.ticker_of("ionq us") == "IONQ" and D.ticker_of("BRK.B") == "BRK.B" and D.ticker_of("N/A") is None and D.ticker_of("US67066G1040") is None
    assert D.no_ids("CRWV 9.25 06/01/30 144A") == "CRWV 9.25 06/01/30 144A" and D.no_ids("Apple Inc 037833100") == "Apple Inc"
    assert D.no_ids("%s CoreWeave" % ISIN) == "CoreWeave" and D.no_ids(CUSIPS["cw"]) == "(not named)" and D.no_ids("COMPUTECO 3 LLC") == "COMPUTECO 3 LLC"
    for text, want in (("CDX.NA.IG.S46", "CDX NA IG 46"), ("MARKIT CDX NORTH AMERICAN INVESTMENT GRADE INDEX SERIES 46-V1", "CDX NA IG 46"), ("ICE: (CDX.NA.IG.46.V1)", "CDX NA IG 46"),
                       ("CDX.NA.HY.S45", "CDX NA HY 45"), ("ITRAXX EUROPE CROSSOVER S44", "ITRAXX XOVER 44"), ("ITRAXX EUROPE S45", "ITRAXX EUROPE 45"), ("CMBX.NA.BBB-.S14", "CMBX 14")):
        assert D.index_family(text) == want, (text, D.index_family(text))
    assert D.cds_reference("IHS Markit Ltd", None) == ("index", "INDEX, FAMILY NOT STATED") and D.cds_reference("Oracle Corp.", None) == ("name", "ORACLE CORP")
    assert D.cds_reference("N/A", "") == ("unnamed", "(not named)") and D.cds_reference("x", "CDX.NA.HY.S46") == ("index", "CDX NA HY 46")
    assert D.pct([1, 2, 3, 4, 5], 0.5) == 3 and D.pct([1, 2, 3, 4, 5], 0.1) == 1 and D.pct([7], 0.9) == 7


def test_a_month_of_money_funds():
    doc = D.nmfp_month(zipfile.ZipFile(io.BytesIO(nmfp_zip())))
    assert doc["month"] == "2026-10" and doc["funds"] == 5 and doc["filings_in_file"] == 6
    assert doc["report_dates"] == {"2026-10-30": 2, "2026-10-31": 3}                              # both month-end dates are the month
    assert doc["net_assets"] == round(5e10 * 4 + 1e11)                                             # fund 0 counted once, by its later filing
    gov = next(t for t in doc["by_type"] if t["type"] == "Government")
    assert gov["funds"] == 3 and gov["net_assets"] == round(2e11) and gov["gross_yield_7d"] == 4.2 and gov["daily_liquid_pct"] == 0.61
    assert next(t for t in doc["by_type"] if t["type"] == "Prime")["gross_yield_7d"] == 4.3         # the last date's yield, not the one before
    r = doc["repo"]
    # four funds lend 10 + 20 + 5 + 1 bn in repo; fund 0's later filing holds only 30 bn at the Fed
    assert r["total"] == round(4 * 36e9 + 30e9) and r["federal_reserve"] == round(4 * 1e10 + 3e10) and r["via_ficc"] == round(4 * 2e10)
    assert r["treasury"] == round(4 * 3e10 + 3e10) and r["agency"] == round(4 * 5e9) and r["other"] == round(4 * 1e9)
    assert r["cleared"] == round(4 * 2e10) and r["triparty"] == round(4 * 6e9)
    names = {(c[0], c[1]): c for c in r["counterparties"]}
    assert names[("MORGAN STANLEY", "FICC")][2] == round(8e10) and names[("JP MORGAN SECURITIES LLC", "")][2] == round(4 * 6e9)      # two spellings, one dealer
    assert names[("FEDERAL RESERVE BANK OF NEW YORK", "")][3] == 5 and r["n_counterparties"] == 3
    assert r["collateral"] == [["U.S. Treasuries (including strips)", round(4 * 1.02e10)]]         # fund 0's superseded filing is not counted
    u = doc["unsecured"]
    assert u["total"] == round(4 * 5e9) and u["issuers"] == [["SUMITOMO MITSUI BANKING CORP", round(2e10), round(1.6e10), round(4e9), 0, 4]]
    assert [h["category"] for h in doc["holdings"]][:2] == ["U.S. Treasury Repurchase Agreement", "U.S. Treasury Debt"]
    assert doc["flows"]["days"] == [["2026-10-01", round(4e9), round(3.6e9), round(4e8)], ["2026-10-02", round(4e9), round(3.6e9), round(4e8)], ["2026-10-05", round(4e9), round(3.6e9), round(4e8)]]
    assert doc["flows"]["net_by_type"]["Prime"][0] == ["2026-10-01", round(2e8)]
    s = D.nmfp_summary(doc)
    assert s["month"] == "2026-10" and s["repo"] == r["total"] and s["net_flow"] == round(1.2e9) and s["by_type"]["Prime"] == round(1e11) and len(s["top_repo"]) == 3
    json.dumps(doc, allow_nan=False)


def test_a_file_this_module_does_not_read_is_refused():
    for kw, word in ((dict(drop=["NMFP_SERIESLEVELINFO.tsv"]), "tables missing"), (dict(month="DEC", year=2010, extra_month=4), "several months")):
        try:
            D.nmfp_month(zipfile.ZipFile(io.BytesIO(nmfp_zip(**kw))))
        except D.NotHandled as exc:
            assert word in str(exc), exc
        else:
            raise AssertionError("read a file it should refuse: %s" % kw)
    assert D.nmfp_month(zipfile.ZipFile(io.BytesIO(nmfp_zip(extra_month=1))))["funds"] == 5          # one straggler from the month before does not stop the month


def test_a_quarter_of_fund_holdings():
    docs = quarter()
    s = docs["summary"]
    assert s["funds"] == 6 and s["filings_in_file"] == 7 and s["as_of"] == "2026-07-31" and s["report_dates"] == {"2026-06-30": 2, "2026-07-31": 4}
    assert s["net_assets"] == round(5 * 4.9e9 + 4.8e9)                                              # fund 0 by its amendment
    classes = {c["code"]: c for c in s["classes"]}
    assert classes["DBT"]["rows"] == 5 * 2 + 1 and classes["LON"]["level3_rows"] == 10 and classes["EC"]["name"] == "Common stock"
    assert s["flows_by_month"]["2026-07"]["funds"] == 4 and s["flows_by_month"]["2026-05"]["net"] == round(4 * (10 + 1 - 20) + 2 * (30 + 1 - 20))
    b = docs["bonds"]
    rows = [dict(zip(b["cols"], r)) for r in b["rows"]]
    cw = [r for r in rows if r["cpn"] == 9.25]
    assert len(cw) == 1, "one bond, however the funds spell its issuer"
    cw = cw[0]
    assert cw["funds"] == 6 and cw["face"] == round(6e7) and cw["mat"] == "2030-06-01" and cw["cls"] == "DBT" and cw["d"] == "2026-07-31" and cw["ck"] == "Fixed"
    assert cw["n_px"] == 4 and cw["px_lo"] == 98.5 and cw["px_hi"] == 101.0 and cw["px"] == 99.5      # the four funds of the commonest date: 98.5 (amended), 99, 100, 101
    assert ISIN not in cw["issuer"] and cw["title"] == "CRWV 9.25 06/01/30 144A"
    dead = next(r for r in rows if r["cpn"] == 8.0)
    assert dead["n_default"] == 5 and dead["n_arrears"] == 5 and dead["px"] == 40.0 and dead["funds"] == 5
    loan = next(r for r in rows if r["cls"] == "LON")
    assert loan["issuer"] == "Shared Borrower LLC" and loan["funds"] == 5 and loan["n_pik"] == 5 and loan["n_l3"] == 5 and loan["px"] == 75.0
    assert len(rows) == 3 and b["totals"] == {"securities": 3, "with_identifier": 2, "set_aside_small": 5, "flag_default": 1, "flag_arrears": 1, "flag_pik": 1}
    c = docs["cds"]
    ford = dict(zip(c["cols"], c["names"][0]))
    assert ford["reference"] == "FORD MOTOR CO" and ford["positions"] == 5 and ford["funds"] == 5 and ford["usual_end_year"] == "2030"
    assert ford["sold"] == round(2e7) and ford["bought"] == round(3e7) and ford["net_sold"] == round(-1e7)      # funds 2 and 4 sold, 1, 3 and 5 bought (fund 0's first filing is replaced)
    assert c["indices"] == [["CDX NA HY 46", 5, 5, round(1e8), 0, 0, round(1e8), 0, "2030"]]
    assert c["totals"]["positions"] == 10 and c["totals"]["not_named"] == 0 and c["totals"]["notional_usd"] == round(5e7 + 1e8) and c["counterparties"] == [["ICE CLEAR CREDIT LLC", 5]]
    first = quarter(amend=False)["cds"]                                                            # without the amendment fund 0's own swaps are there
    assert first["totals"]["not_named"] == 1 and first["totals"]["credit_derivatives_that_are_not_swaps"] == 1
    intesa = dict(zip(first["cols"], next(n for n in first["names"] if n[0] == "INTESA SANPAOLO SPA")))
    assert intesa["not_in_dollars"] == 1 and intesa["bought"] == 0 and intesa["sold"] == 0           # a euro notional is counted, never added to dollars
    ln = docs["stock_loan"]
    by = {r[0]: dict(zip(ln["cols"], r)) for r in ln["rows"]}
    assert set(by) == {"NVDA", "IONQ"} and ln["totals"]["stocks_under_the_floor"] == 1
    # five funds hold stock (fund 0's amendment carries none): IonQ 5 x $100m held, four of them lend $80m each
    assert by["IONQ"]["on_loan_pct"] == 64.0 and by["IONQ"]["funds_lending"] == 4 and by["IONQ"]["funds"] == 5 and by["IONQ"]["shares"] == round(1e7) and by["NVDA"]["on_loan_pct"] == 1.0
    assert D.nport_summary(docs)["securities"] == 3 and D.nport_summary(docs)["cds_names"] == 1
    json.dumps(docs, allow_nan=False)


def test_no_published_table_carries_an_identifier():
    for docs in (quarter(), quarter(amend=False)):
        text = json.dumps(docs)
        for ident in list(CUSIPS.values()) + [ISIN, "US67066G1040", "999999999"]:
            assert ident not in text, ident
    text = json.dumps(D.nmfp_month(zipfile.ZipFile(io.BytesIO(nmfp_zip()))))
    assert not re.search(r"\b[A-Z]{2}[A-Z0-9]{9}\d\b", text)


def test_a_quarter_without_its_tables_is_refused():
    tmp = tempfile.mkdtemp(prefix="secds-bad-")
    try:
        path = os.path.join(tmp, "q.zip")
        with open(path, "wb") as fh:
            fh.write(zbytes({"SUBMISSION.tsv": b"ACCESSION_NUMBER\n"}))
        try:
            D.nport_quarter(path, tmp)
        except D.NotHandled:
            pass
        else:
            raise AssertionError("read an archive with no holdings")
        assert os.listdir(tmp) == ["q.zip"], "the scratch database is removed"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def test_a_dry_run_writes_nothing():
    w = World()
    try:
        out = LF.lambda_handler({"dry_run": True}, Ctx(900))
        assert out["dry_run"] and w.s3.puts == [] and not any(u.endswith(".zip") for _, u in w.calls), "pages only: no file is fetched"
        assert out["advertised"] == {"nport": 2, "nmfp": 3, "bdc": 1, "form13f": 1, "insiders": 1, "formd": 1, "ncen": 1}
        assert [b[1] for b in out["would_bank"][:7]] == ["20261008-20261106_nmfp.zip", "2026_10_bdc.zip", "2026q3_nport.zip", "2026q3_d.zip", "2026q3_form345.zip",
                                                      "2026q3_ncen.zip", "01jun2026-31aug2026_form13f.zip"] or len({b[0] for b in out["would_bank"][:7]}) == 7
        assert {b[3] for b in out["would_bank"][:7]} == {"newest"} and {b[3] for b in out["would_bank"][7:]} == {"history"}
        assert BEGUN and BEGUN[-1] == {"dry_run": True}, "the trigger cap sees every start"
    finally:
        w.close()


def test_a_run_banks_reads_and_publishes():
    w = World()
    try:
        out = LF.lambda_handler({"trigger": "test"}, Ctx(900))
        assert out["ok"] and out["banked"] == 10 and out["months_read"] == 2 and out["quarters_read"] == 1, out
        raw = [k for k in w.s3.objects if k.endswith(".zip")]
        assert len(raw) == 10 and all(k.startswith("sec-datasets/raw/") for k in raw), "raw files stay outside data/"
        assert all(w.s3.meta[k]["sha256"] and w.s3.meta[k]["pulled_at"] and w.s3.meta[k]["source"] == "sec.gov" for k in raw)
        assert w.s3.objects["sec-datasets/raw/nport/2026q3_nport.zip"] == w.site["https://www.sec.gov/files/dera/data/form-n-port-data-sets/2026q3_nport.zip"], "verbatim"
        m = w.s3.doc("data/warm/sec-datasets/manifest.json")
        assert m["version"] == LF.VERSION and m["totals"]["nport"] == {"advertised": 2, "banked": 2, "bytes": m["totals"]["nport"]["bytes"], "newest_banked": "2026q3_nport.zip",
                                                                      "read_into_tables": 1, "waiting_to_be_read": 1}
        assert m["totals"]["nmfp"]["read_into_tables"] == 2 and m["totals"]["nmfp"]["waiting_to_be_read"] == 0 and m["totals"]["bdc"]["waiting_to_be_read"] is None
        old = m["sets"]["nmfp"]["files"]["2010q4_nmfp.zip"]["derived"]
        assert "several months" in old["not_handled"], "a quarter-wide legacy file is banked, and said to be unread"
        assert m["runs"][-1]["trigger"] == "test" and m["runs"][-1]["published"] == {"money_funds": "2026-10", "fund_holdings": "2026-07-31"}
        money = w.s3.doc("data/sec-money-funds.json")
        assert money["as_of"] == "2026-10" and [h["month"] for h in money["history"]] == ["2026-09", "2026-10"] and money["month"]["repo"]["via_ficc"] == round(8e10)
        hold = w.s3.doc("data/sec-fund-holdings.json")
        assert hold["as_of"] == "2026-07-31" and hold["source"]["file"] == "2026q3_nport.zip" and hold["cds"]["names"][0][0] == "FORD MOTOR CO"
        assert [r[0] for r in hold["stock_loan"]["most_lent"]] == ["IONQ", "NVDA"] and [r[0] for r in hold["stock_loan"]["largest"]] == ["NVDA", "IONQ"]
        bonds = w.s3.doc(hold["tables"]["bonds"])
        assert bonds["source_file"] == "2026q3_nport.zip" and len(bonds["rows"]) == 3 and w.s3.doc(hold["tables"]["stock_loan"])["rows"][0][0] == "NVDA"
        public = b"".join(v for k, v in w.s3.objects.items() if k.startswith("data/"))
        text = b"".join(gzip.decompress(v) if v[:2] == b"\x1f\x8b" else v for k, v in w.s3.objects.items() if k.startswith("data/")).decode()
        assert public and all(i not in text for i in list(CUSIPS.values()) + [ISIN]), "nothing under data/ carries an identifier"
        assert not os.path.exists(LF.WORK), "local copies are removed"

        # a second run: nothing is fetched again, the older quarter is read
        w.calls.clear()
        puts = len(w.s3.puts)
        out = LF.lambda_handler({}, Ctx(900))
        assert out["banked"] == 0 and out["quarters_read"] == 1 and out["months_read"] == 0 and not any(m_ == "GET" and u.endswith(".zip") for m_, u in w.calls), w.calls
        assert len(w.s3.puts) > puts and w.s3.doc("data/warm/sec-datasets/manifest.json")["totals"]["nport"]["waiting_to_be_read"] == 0
        assert [h["as_of"] for h in w.s3.doc("data/sec-fund-holdings.json")["history"]] == ["2026-07-31", "2026-07-31"]

        # the SEC refreshes a file: its size changes, and it is banked and read again
        url = "https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20261008-20261106_nmfp.zip"
        w.site[url] = nmfp_zip(net=6e10)                             # the same size to the byte: only the SEC's own date tells
        out = LF.lambda_handler({}, Ctx(900))
        assert out["banked"] == 0, "a date before our pull is not a change"
        w.modified[url] = "Fri, 01 Jan 2100 00:00:00 GMT"
        out = LF.lambda_handler({}, Ctx(900))
        assert out["banked"] == 1 and out["months_read"] == 1, out
        m = w.s3.doc("data/warm/sec-datasets/manifest.json")
        assert "20261008-20261106_nmfp.zip#stale" in m["sets"]["nmfp"]["files"] and any("changed on sec.gov" in n for n in m["runs"][-1]["notes"])
        assert w.s3.doc("data/sec-money-funds.json")["month"]["net_assets"] == round(6e10 * 4 + 1.2e11)
    finally:
        w.close()


def test_a_run_short_of_time_reads_the_quarter_later():
    w = World()
    try:
        out = LF.lambda_handler({"only": ["nport", "nmfp"]}, Ctx(300))                              # under NPORT_SECONDS: the quarter waits, nothing is half-read
        assert out["quarters_read"] == 0 and out["months_read"] == 2 and out["banked"] >= 2 and any("wait for a run" in n for n in out["notes"]), out
        m = w.s3.doc("data/warm/sec-datasets/manifest.json")
        assert m["totals"]["nport"]["waiting_to_be_read"] >= 1 and set(m["sets"]) == {"nport", "nmfp"}
        assert "data/sec-fund-holdings.json" not in w.s3.objects and "data/sec-money-funds.json" in w.s3.objects
        out = LF.lambda_handler({"bank": False}, Ctx(900))
        assert out["quarters_read"] == 1 and out["banked"] == 0 and "data/sec-fund-holdings.json" in w.s3.objects
    finally:
        w.close()


def test_a_month_that_is_too_small_is_not_shown_as_the_newest():
    w = World()
    try:
        LF.MIN_MONEY_FUNDS = 5
        w.site["https://www.sec.gov/files/dera/data/form-n-mfp-data-sets/20261008-20261106_nmfp.zip"] = nmfp_zip(n=4)      # four funds: a partial file
        LF.lambda_handler({"only": ["nmfp"]}, Ctx(900))
        money = w.s3.doc("data/sec-money-funds.json")
        assert money["as_of"] == "2026-09" and money["set_aside"] == ["2026-10"] and len(money["history"]) == 2
    finally:
        w.close()


def test_a_page_that_does_not_answer_stops_nothing_else():
    w = World()
    try:
        del w.site[S.SETS["bdc"]["page"]]
        out = LF.lambda_handler({}, Ctx(900))
        assert out["ok"] and out["totals"]["bdc"]["banked"] == 0 and out["totals"]["nmfp"]["banked"] == 3
        for page_url in [s["page"] for s in S.SETS.values()]:
            w.site.pop(page_url, None)
        try:
            LF.lambda_handler({}, Ctx(900))
        except RuntimeError as exc:
            assert "no SEC data-set page answered" in str(exc)
        else:
            raise AssertionError("a run with no page at all must fail loudly, not publish")
    finally:
        w.close()


def test_only_sec_gov_is_ever_asked():
    real = sys.modules["lambda_function"]
    import importlib
    fresh = importlib.reload(real)
    assert fresh.sec_get("https://example.com/2026q3_nport.zip") == (0, None, {}) and fresh.sec_get("http://www.sec.gov/x.zip") == (0, None, {})


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print("PASS", name)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            print("FAIL", name, "-", repr(exc)[:300])
            traceback.print_exc(limit=3)
    print("%d tests, %d failed" % (len(tests), failed))
    sys.exit(1 if failed else 0)
