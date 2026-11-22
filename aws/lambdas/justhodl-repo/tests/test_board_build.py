"""The whole board build, end to end, on a stub bucket and a stub wire (2026-11-21, engine 3.0).  No network, no AWS.

The engine is one long handler with a dozen inline feeders; until now only its helpers were tested.  This runs the
handler itself and reads the board it publishes: the refreshed OFR layout is read, the series the inventory never
listed are on the board, the policy and funding series are banked and bucketed, the derived spreads come out with the
right numbers, the banked DTCC history survives a 404, FRED is asked by each series' rhythm, and a second run writes
no history file again.
"""
from datetime import date, datetime, timedelta, timezone
import gzip
import io
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent / "source"), str(HERE.parents[2] / "shared"), str(HERE.parents[2] / "shared" / "tests")]
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")

TODAY = datetime.now(timezone.utc).date()


def business_days(n, end=None):
    """The last n weekdays up to `end` (default: two days ago, so a daily series is due)."""
    d, out = (end or TODAY - timedelta(days=2)), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return out[::-1]


DAYS = business_days(80)
SPIKE = DAYS[40]                                     # one day on which repo trades far above everything else


def level(base, spike=None):
    return [[d, (spike if (spike is not None and d == SPIKE) else base)] for d in DAYS]


def ofr_doc(m, rows):
    return gzip.compress(json.dumps({"mnemonic": m, "source_url": "u", "raw_snapshot_key": None, "as_of": "x", "refreshed_by": "dataset",
                                     "payload": {m: {"timeseries": {"aggregation": rows}, "metadata": {"mnemonic": m}}}}).encode())


def fred_doc(fid, rows, title="t"):
    return json.dumps({"id": fid, "title": title, "observations": [{"date": d, "value": v} for d, v in rows]}).encode()


OFR = {"FNYR-SOFR-A": level(3.91, 5.25), "FNYR-EFFR-A": level(3.88, 3.95), "FNYR-TGCR-A": level(3.88, 5.10),
       "REPO-TRIV1_AR_TOT-P": level(3.90, 4.93), "REPO-DVP_AR_TOT-P": level(3.92, 5.60), "REPO-GCF_AR_TOT-P": level(3.95, 6.00),
       "REPO-TRIV1_TV_OO-P": level(1.8e12), "REPO-TRIV1_TV_TOT-P": level(2.3e12), "NYPD-PD_AFtD_T-A": level(1.0e11),
       "MMF-MMF_RP_wFR-M": level(6.8e9), "MMF-MMF_RP_T_OO-M": level(9.0e11), "NYPD-PD_SB_T_TOT-A": level(4.0e11),
       "NYPD-PD_SL_TOT-A": []}                                                                      # banked, empty at the source
INVENTORY = ["FNYR-SOFR-A", "FNYR-EFFR-A", "FNYR-TGCR-A", "REPO-TRIV1_AR_TOT-P", "REPO-DVP_AR_TOT-P", "REPO-GCF_AR_TOT-P",
             "REPO-TRIV1_TV_OO-P", "REPO-TRIV1_TV_TOT-P", "NYPD-PD_AFtD_T-A"]
NAMES = {"repo": {"REPO-TRIV1_AR_TOT-P": "Tri-Party Average Rate: Total", "REPO-DVP_AR_TOT-P": "DVP Average Rate: Total", "REPO-GCF_AR_TOT-P": "GCF Average Rate: Total",
                  "REPO-TRIV1_TV_OO-P": "Tri-Party Volume: Overnight", "REPO-TRIV1_TV_TOT-P": "Tri-Party Volume: Total"},
         "nypd": {"NYPD-PD_AFtD_T-A": "Fails to Deliver: Treasury", "NYPD-PD_SB_T_TOT-A": "Primary Dealer Securities Borrowed Backed by U.S. Treasury Securities: Total",
                  "NYPD-PD_SL_TOT-A": "Primary Dealer Securities Lent: Total"},
         "fnyr": {"FNYR-SOFR-A": "Secured Overnight Financing Rate", "FNYR-EFFR-A": "Effective Federal Funds Rate", "FNYR-TGCR-A": "Tri-Party General Collateral Rate"},
         "mmf": {"MMF-MMF_RP_wFR-M": "MMF Repo with the Federal Reserve", "MMF-MMF_RP_T_OO-M": "MMF Repo Backed by U.S. Treasury Securities: Overnight/Open"}}
QUARTERS = [("%d-%02d-01" % (y, m), 100.0 + i) for i, (y, m) in enumerate([(TODAY.year - 2, 1), (TODAY.year - 2, 4), (TODAY.year - 2, 7), (TODAY.year - 2, 10),
                                                                             (TODAY.year - 1, 1), (TODAY.year - 1, 4), (TODAY.year - 1, 7), (TODAY.year - 1, 10)])]
RECENT_QUARTERS = [((TODAY - timedelta(days=91 * k + 20)).isoformat(), 50.0 + k) for k in range(7, -1, -1)]      # last print 20 days ago: not due
FRED_TITLES = {"SOFR": "Secured Overnight Financing Rate", "IORB": "Interest Rate on Reserve Balances", "IOER": "Interest Rate on Excess Reserves",
               "DFF": "Federal Funds Effective Rate", "DFEDTARU": "Federal Funds Target Range - Upper Limit", "RPONTSYD": "Overnight Repurchase Agreements: Treasury Securities Purchased by the Federal Reserve in the Temporary Open Market Operations",
               "RPONMBSD": "Overnight Repurchase Agreements: Mortgage-Backed Securities Purchased by the Federal Reserve in the Temporary Open Market Operations",
               "RRPONTSYD": "Overnight Reverse Repurchase Agreements: Treasury Securities Sold by the Federal Reserve", "WRESBAL": "Reserve Balances with Federal Reserve Banks",
               "BOGZ1FL000Q": "Some Sector; Security Repurchase Agreements; Liability, Level", "BOGZ1FL111Q": "Another Sector; Security Repurchase Agreements; Asset, Level"}
FRED_OBS = {"SOFR": [(d, 5.25 if d == SPIKE else 3.91) for d in DAYS] + [((TODAY - timedelta(days=1)).isoformat(), 3.92)],
            "IORB": [(DAYS[0], 3.90)], "IOER": [], "DFF": [(d, 3.88) for d in DAYS], "DFEDTARU": [(DAYS[0], 4.00)],
            "RPONTSYD": [(d, 40.85 if d == SPIKE else 0.0) for d in DAYS], "RPONMBSD": [(d, 11.7 if d == SPIKE else 0.0) for d in DAYS],
            "RRPONTSYD": [(d, 0.4) for d in DAYS], "WRESBAL": [((TODAY - timedelta(days=7 * k + 10)).isoformat(), 2979656.0) for k in range(8, -1, -1)],
            "BOGZ1FL000Q": QUARTERS, "BOGZ1FL111Q": RECENT_QUARTERS,
            "DTWEXBGS": [(d, 120.0 + i / 10.0) for i, d in enumerate(DAYS)]}


class Answer(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Wire:
    """Every URL the handler asks, answered from the fixtures above or refused like a dead host."""

    def __init__(self):
        self.calls = []

    def __call__(self, req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        self.calls.append(url)
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
        if "financialresearch.gov/v1/metadata/mnemonics" in url:
            return Answer(json.dumps([{"mnemonic": m, "series_name": n} for m, n in NAMES[q["dataset"]].items()]).encode())
        if "/fred/tags/series" in url:
            return Answer(json.dumps({"seriess": [{"id": i, "title": FRED_TITLES[i]} for i in ("RPONTSYD", "RPONMBSD", "RRPONTSYD", "BOGZ1FL000Q", "BOGZ1FL111Q")]}).encode())
        if "/fred/series/search" in url:
            return Answer(json.dumps({"seriess": [{"id": "SOFR", "title": FRED_TITLES["SOFR"]}]}).encode())
        if "/fred/series/observations" in url:
            rows = [{"date": d, "value": str(v)} for d, v in FRED_OBS.get(q["series_id"], []) if d >= q.get("observation_start", "")]
            return Answer(json.dumps({"observations": rows}).encode())
        if "/fred/series?" in url:
            sid = q["series_id"]
            return Answer(json.dumps({"seriess": [{"title": FRED_TITLES[sid]}] if sid in FRED_TITLES else []}).encode())
        if "markets.newyorkfed.org/api/rates/secured/" in url:
            rt = url.split("/secured/")[1].split("/")[0]
            rows = [{"effectiveDate": d, "type": rt.upper(), "percentRate": 3.91, "percentPercentile1": 3.85, "percentPercentile25": 3.88,
                     "percentPercentile75": 5.85 if d == SPIKE else 3.95, "percentPercentile99": 9.0 if d == SPIKE else 3.98} for d in DAYS]
            return Answer(json.dumps({"refRates": rows}).encode())
        if "markets.newyorkfed.org/api/rp/results/search.json" in url:
            ops = [{"operationDate": d, "operationId": "RP %s" % d, "operationType": "Repo", "totalAmtAccepted": 50.35e9 if d == SPIKE else 1.0e6,
                    "details": [{"minimumBidRate": 4.0}]} for d in DAYS]
            return Answer(json.dumps({"repo": {"operations": ops}}).encode())
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)


class Bucket:
    def __init__(self, objects):
        self.objects, self.puts = dict(objects), []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError("NoSuchKey " + Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError("404 " + Key)
        return {}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body
        self.puts.append(Key)

    def list_objects_v2(self, **kw):
        keys = sorted(k for k in self.objects if k.startswith(kw["Prefix"]))
        if kw.get("Delimiter"):
            cut = len(kw["Prefix"])
            return {"CommonPrefixes": [{"Prefix": p} for p in sorted({k[:cut] + k[cut:].split("/")[0] + "/" for k in keys if "/" in k[cut:]})]}
        return {"Contents": [{"Key": k, "ETag": '"%s"' % __import__("hashlib").md5(self.objects[k]).hexdigest()} for k in keys]}


def seeded():
    obj = {"data/repo-master-inventory.json": json.dumps({"series": [
        {"mnemonic": m, "name": "", "family": m.split("-")[0], "tenor": "total", "collateral": "total", "s3_key": "data/warm/ofr/series/%s.json.gz" % m,
         "api_url": "u", "tier": 1 if m in ("FNYR-SOFR-A", "REPO-TRIV1_AR_TOT-P") else 2} for m in INVENTORY]}).encode(),
        "data/warm/ofr/state.json": json.dumps({"series": {m: ["sig", len(r), r[-1][0] if r else None] for m, r in OFR.items()}}).encode(),
        "data/settlement-fails.json": b"{}",
        "data/warm/dtcc-fails/treasury.json": fred_doc("DTCC-TREASURY-FAILS", [(d, 2.0e10) for d in DAYS[:40]]),
        "data/warm/fred-scoped/FRED_Repo_Complex/BOGZ1FL111Q.json": fred_doc("BOGZ1FL111Q", RECENT_QUARTERS),
        "data/warm/fred-scoped/FRED_Repo_Complex/SOFR.json": fred_doc("SOFR", [(d, 5.25 if d == SPIKE else 3.91) for d in DAYS]),
        # the dollar complex as it was found: banked once, ten weeks behind
        "data/warm/fred-scoped/Dollar_TradeWeighted/DTWEXBGS.json": json.dumps({"id": "DTWEXBGS", "source": "FRED API via justhodl-repo", "banked_at": "x",
            "observations": [{"date": d, "value": str(120.0 + i / 10.0), "realtime_start": "r"} for i, d in enumerate(DAYS[:30])]}).encode()}
    for m, rows in OFR.items():
        obj["data/warm/ofr/series/%s.json.gz" % m] = ofr_doc(m, rows)
    return obj


def build(bucket, wire):
    import lambda_function as lf
    lf.s3 = bucket
    os.environ["FRED_API_KEY"] = "k"
    os.environ.pop("TE_API_KEY", None)
    real = urllib.request.urlopen
    urllib.request.urlopen = wire
    try:
        res = lf.lambda_handler({}, None)
    finally:
        urllib.request.urlopen = real
    assert res["statusCode"] == 200, res
    return json.loads(res["body"]), json.loads(bucket.objects["data/repo.json"])


def rows_of(board):
    return {s["id"]: dict(s, group=g["name"]) for g in board["groups"] for s in g["series"]}


def test_the_board_build_end_to_end():
    bucket, wire = Bucket(seeded()), Wire()
    res, board = build(bucket, wire)
    rows = rows_of(board)
    assert board["engine_v"] == "3.1" == res["v"] and res["ok"]

    # the refreshed OFR layout is read, names come from the OFR metadata
    sofr = rows["FNYR-SOFR-A"]
    assert sofr["group"] == "Reference rates & volumes" and sofr["last"] == DAYS[-1] and sofr["last_value"] == 3.91 and sofr["n_obs"] == 80
    assert sofr["label"] == "Secured Overnight Financing Rate" and sofr["tier"] == 1

    # series the inventory never listed
    mmf = rows["MMF-MMF_RP_T_OO-M"]
    assert mmf["group"] == "Money market funds (OFR)" == rows["MMF-MMF_RP_wFR-M"]["group"] and mmf["label"].startswith("MMF Repo Backed by U.S. Treasury")
    sb = rows["NYPD-PD_SB_T_TOT-A"]
    assert sb["group"] == "Primary dealer securities borrowed & lent" and sb["last_value"] == 4.0e11 and sb["tier"] == 2
    assert rows["NYPD-PD_AFtD_T-A"]["group"] == "Primary dealer FAILS"
    assert {"id": "NYPD-PD_SL_TOT-A", "reason": "no_pairs_extracted"} in board["skipped"]       # empty at the source: a skipped row with its reason
    assert len([i for i in rows if i == "MMF-MMF_RP_wFR-M"]) == 1

    # policy and funding series: banked from FRED on first touch, in their own bucket
    for fid in ("DFF", "DFEDTARU", "WRESBAL"):
        assert rows[fid]["group"] == "FRED repo & SOFR complex" and rows[fid]["bucket"] == "Policy rates, reserves and Fed lending", rows[fid]
        assert "data/warm/fred-scoped/FRED_Repo_Complex/%s.json" % fid in bucket.objects
    assert rows["IORB"]["bucket"] != "Policy rates, reserves and Fed lending" and rows["RPONTSYD"]["bucket"] == "Fed repo ops (incl SRF)"
    assert "IOER" not in rows and "TEDRATE" not in rows                                           # nothing at FRED for them here: absent, not invented

    # FRED is asked by rhythm: the daily series two days old is, the quarterly line 20 days after its print is not
    asked = [dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(u).query))["series_id"] for u in wire.calls if "/fred/series/observations" in u]
    assert "SOFR" in asked and "BOGZ1FL111Q" not in asked and "BOGZ1FL000Q" in asked, asked
    assert rows["SOFR"]["last"] == (TODAY - timedelta(days=1)).isoformat() and rows["SOFR"]["last_value"] == 3.92
    assert rows["BOGZ1FL111Q"]["n_obs"] == 8                                                       # still on the board from the bank

    # the dollar complex: banked ten weeks behind, brought up to the source, its older rows untouched
    dollar = rows["DTWEXBGS"]
    assert dollar["group"] == "Dollar (FRED trade-weighted)" and dollar["last"] == DAYS[-1] and dollar["n_obs"] == 80 and dollar["tier"] == 1
    banked = json.loads(bucket.objects["data/warm/fred-scoped/Dollar_TradeWeighted/DTWEXBGS.json"])
    assert len(banked["observations"]) == 80 and banked["observations"][0]["realtime_start"] == "r" and "refreshed_at" in banked and "asked_at" in banked
    assert "DTWEXBGS" in asked

    # derived spreads, with the numbers of the spike day in their histories
    def hist(sid):
        h = json.loads(bucket.objects["data/repo-history/%s.json" % sid])
        return dict(zip(h["dates"], h["values"]))

    assert round(hist("D_SOFR_EFFR")[SPIKE], 6) == 130.0                      # 5.25 against 3.95
    assert round(hist("D_SOFR_TAIL")[SPIKE], 6) == 375.0                      # 9.00 against 5.25
    assert round(hist("D_SOFR_ADMIN")[SPIKE], 6) == 135.0                     # against IORB 3.90
    assert round(hist("D_EFFR_TARGET")[DAYS[-1]], 6) == -12.0                 # 3.88 against the 4.00 top of the range
    assert round(hist("D_FED_REPO")[SPIKE], 6) == 52.55 and hist("D_FED_REPO")[DAYS[0]] == 0.0
    for sid in ("D_SOFR_EFFR", "D_SOFR_TAIL", "D_SOFR_ADMIN", "D_EFFR_TARGET", "D_FED_REPO"):
        assert rows[sid]["group"] == "Derived stress indicators" and rows[sid]["tier"] == 1
    assert round(hist("D_GCF_TRI")[SPIKE], 6) == 107.0 and round(hist("D_SOFR_P75_P25")[SPIKE], 6) == 197.0     # the older derived rows still build
    assert rows["SRF_TAKEUP"]["group"] == "Standing Repo Facility" and round(hist("SRF_TAKEUP")[SPIKE], 6) == 50.35

    # DTCC answered 404: the banked history stays on the board and says so
    dtcc = rows["DTCC-TREASURY-FAILS"]
    assert dtcc["group"] == "DTCC daily settlement fails (FICC)" and dtcc["n_obs"] == 40 and "banked history" in dtcc["label"]
    assert board["diag"]["dtcc"].get("banked_only") is True and "404" in board["diag"]["dtcc"]["error"]
    assert "DTCC-AGENCY-FAILS" not in rows                                                         # never banked here: not offered
    # the New York Fed's workbooks are not reachable here either: the reason is on the board, nothing is invented
    own = board["diag"]["own_sources"]
    assert own["survey_rate"]["status"] == "error" == own["haircuts"]["status"] and "404" in own["survey_rate"]["error"]
    assert "GC_SURVEY" not in rows and "D_REPO_TARGET" not in rows and not any(k.startswith("data/warm/nyfed-research/") for k in bucket.puts)

    # counts add up and every row has its history file
    assert board["counts"]["series"] == len(rows) == board["counts"]["history_files"]
    assert board["counts"]["history_written"] == len(rows) and board["counts"]["history_unchanged"] == 0
    assert all("data/repo-history/%s.json" % r["sid"] in bucket.objects for r in rows.values())

    # a second run on the same inputs writes no history file again
    before = len([k for k in bucket.puts if k.startswith("data/repo-history/")])
    wire2 = Wire()
    res2, board2 = build(bucket, wire2)
    assert board2["counts"]["history_written"] == 0 and board2["counts"]["history_unchanged"] == len(rows), board2["counts"]
    asked2 = [dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(u).query))["series_id"] for u in wire2.calls if "/fred/series/observations" in u]
    assert "BOGZ1FL000Q" in asked and "BOGZ1FL000Q" not in asked2, asked2       # asked a moment ago and still silent: not again for 30 days
    assert "SOFR" not in asked2 and "DTWEXBGS" in asked2                         # yesterday's print is in: not asked; a daily series two days old is
    assert len([k for k in bucket.puts if k.startswith("data/repo-history/")]) == before


def test_without_the_ofr_names_call_the_bank_catalog_still_brings_the_extra_series():
    bucket = Bucket(seeded())

    class NoNames(Wire):
        def __call__(self, req, timeout=None):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            if "metadata/mnemonics" in url:
                raise urllib.error.URLError("down")
            return Wire.__call__(self, req, timeout)

    _, board = build(bucket, NoNames())
    rows = rows_of(board)
    assert rows["MMF-MMF_RP_T_OO-M"]["group"] == "Money market funds (OFR)" and rows["NYPD-PD_SB_T_TOT-A"]["group"] == "Primary dealer securities borrowed & lent"
    assert rows["MMF-MMF_RP_T_OO-M"]["label"] == ""                       # no name without the metadata call - the row is still there


def test_the_board_reads_its_own_sources():
    """2026-11-22: the dealer survey rate is banked and spans 1998 to SOFR; the haircut bank catches up with the New
    York Fed's workbook; DTCC's moved file is read with a fresh `v` and merged into the banked history."""
    import repo_sources as S
    import test_repo_sources as T
    old_book, new_book = T.triparty_book(T.OLD_DAYS, T.HEAD_OLD), T.triparty_book(T.NEW_DAYS, T.HEAD_NEW, shift=0.5)
    full = S.haircut_series(S.triparty_rows(old_book), S.triparty_rows(new_book))
    survey = T.survey_book()

    class Own(Wire):
        def __call__(self, req, timeout=None):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            if url == S.SURVEY_URL:
                self.calls.append(url)
                return Answer(survey)
            if url in (S.TRIPARTY_NOW, S.TRIPARTY_OLD):
                self.calls.append(url)
                return Answer(new_book if url == S.TRIPARTY_NOW else old_book)
            if url.startswith(S.DTCC_FAILS_URLS[0] + "?v="):
                self.calls.append(url)
                assert req.get_header("User-agent") == "Mozilla/5.0"
                return Answer(T.fails_file(DAYS).encode())
            return Wire.__call__(self, req, timeout)

    extra_obs = {"DFEDTAR": [("1998-01-02", 5.5)], "DFEDTARL": [(DAYS[0], 3.75)]}
    extra_titles = {"DFEDTAR": "Federal Funds Target Rate (DISCONTINUED)", "DFEDTARL": "Federal Funds Target Range - Lower Limit"}
    FRED_OBS.update(extra_obs)
    FRED_TITLES.update(extra_titles)
    try:
        objects = seeded()
        for key, doc in T.banked_from(full, "2025-05-09").items():
            objects[key] = json.dumps(doc).encode()
        bucket, wire = Bucket(objects), Own()
        res, board = build(bucket, wire)
        rows = rows_of(board)

        def hist(sid):
            h = json.loads(bucket.objects["data/repo-history/%s.json" % sid])
            return dict(zip(h["dates"], h["values"]))

        # 1. the repo rate before SOFR
        gc = rows["GC_SURVEY"]
        assert gc["group"] == "Reference rates & volumes" and gc["n_obs"] == 4100 and gc["first"] == "1998-02-20" and gc["tier"] == 1 and "survey rate" in gc["label"]
        assert json.loads(bucket.objects[S.SURVEY_KEY])["final"] is True
        above, below = hist("D_REPO_TARGET"), hist("D_TARGET_REPO")
        assert round(above["1998-02-20"], 6) == -10.0 and round(below["1998-02-20"], 6) == 10.0            # 5.40 against the 5.50 target
        assert round(above[SPIKE], 6) == 137.5                                                               # SOFR 5.25 against the middle of 3.75-4.00
        assert min(above) == "1998-02-20" and max(above) == (TODAY - timedelta(days=1)).isoformat() and len(above) == 4100 + 81
        assert rows["D_REPO_TARGET"]["group"] == "Derived stress indicators" == rows["D_TARGET_REPO"]["group"] and rows["D_REPO_TARGET"]["first"] == "1998-02-20"

        # 2. the haircut bank caught up: two more months on every series, the ended asset class still there
        own = board["diag"]["own_sources"]
        assert own["survey_rate"] == {"status": "written", "n": 4100, "first": "1998-02-20", "last": gc["last"]}
        assert own["haircuts"]["status"] == "written" and own["haircuts"]["observations_added"] == 2 * len(full) and own["haircuts"]["bank_newest_after"] == "2025-07-10"
        eq = rows["HAIRCUT-equities--median_haircut"]
        assert eq["group"] == "Tri-party haircuts (NY Fed monthly)" and eq["last"] == "2025-07-10" and eq["bucket"] == "median haircut"
        assert rows["HAIRCUT-whole-loans--median_haircut"]["last"] == "2011-08-09"
        assert sum(1 for i in rows if i.startswith("HAIRCUT-")) == len(full) + 1

        # 3. DTCC's file at its new address, asked with a fresh v, merged over the banked days
        dtcc = rows["DTCC-TREASURY-FAILS"]
        assert dtcc["n_obs"] == 80 and dtcc["last"] == DAYS[-1] and "banked history" not in dtcc["label"] and dtcc["last_value"] == 2.0e10 + 79e8
        assert board["diag"]["dtcc"]["url"] == S.DTCC_FAILS_URLS[0] and board["diag"]["dtcc"]["live_rows"] == 80 and "banked_only" not in board["diag"]["dtcc"]
        assert rows["DTCC-AGENCY-FAILS"]["n_obs"] == 80
        banked = json.loads(bucket.objects["data/warm/dtcc-fails/treasury.json"])
        assert banked["source"] == S.DTCC_FAILS_URLS[0] and len(banked["observations"]) == 80
        assert not any("www.dtcc.com" in u for u in wire.calls)                                           # the first address answered: the dead one is not asked

        # a second run: the survey rate is read from the bank, the haircut source is asked and is level, nothing is rewritten
        wire2 = Own()
        _, board2 = build(bucket, wire2)
        own2 = board2["diag"]["own_sources"]
        assert own2["survey_rate"] == {"status": "banked", "n": 4100} and S.SURVEY_URL not in wire2.calls
        assert own2["haircuts"]["status"] == "level" and own2["haircuts"]["written"] == 0 and wire2.calls.count(S.TRIPARTY_NOW) == 1 and S.TRIPARTY_OLD not in wire2.calls
        assert board2["counts"]["history_written"] == 0, board2["counts"]
    finally:
        for k in extra_obs:
            FRED_OBS.pop(k, None)
            FRED_TITLES.pop(k, None)


def test_fred_refusing_a_whole_run_leaves_its_group_on_the_board():
    """2026-11-22 06:10 UTC: FRED refused every request for half a minute and the board published 766 series instead
    of 1,800 - no FRED group, no policy rates, none of the spreads built on them.  The group is now served from its
    bank on such a day, at its banked values, and nothing is asked of FRED beyond the one retry."""
    import time
    import urllib.error
    import repo_sources as S

    class Down(Wire):
        def __call__(self, req, timeout=None):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            if "api.stlouisfed.org" in url:
                self.calls.append(url)
                raise urllib.error.HTTPError(url, 503, "Service Unavailable", {}, None)
            return Wire.__call__(self, req, timeout)

    def fred_rows(board):
        return {i: r for i, r in rows_of(board).items() if r["group"] == "FRED repo & SOFR complex"}

    real_sleep, naps = time.sleep, []
    time.sleep = naps.append
    try:
        bucket = Bucket(seeded())
        _, first = build(bucket, Wire())
        good = fred_rows(first)
        assert len(good) == 10 and {"SOFR", "IORB", "DFF", "DFEDTARU", "RPONTSYD", "BOGZ1FL111Q"} <= set(good)
        assert first["diag"]["fred_group"] == {"ids": first["diag"]["fred_group"]["ids"], "inventory": "fred"} and naps == []
        recorded = json.loads(bucket.objects[S.FRED_INVENTORY_KEY])
        assert set(recorded["ids"]) == {"RPONTSYD", "RPONMBSD", "RRPONTSYD", "BOGZ1FL000Q", "BOGZ1FL111Q", "SOFR"}          # what FRED's discovery itself answered

        # FRED refuses everything for the whole run
        wire = Down()
        res, second = build(bucket, wire)
        held = fred_rows(second)
        assert set(held) == set(good), sorted(set(good) ^ set(held))
        for sid in ("SOFR", "IORB", "DFEDTARU", "BOGZ1FL111Q"):
            assert held[sid]["label"] == good[sid]["label"] and held[sid]["last"] == good[sid]["last"] and held[sid]["last_value"] == good[sid]["last_value"], sid
        assert "fred refused (HTTPError): served from the last good inventory" in second["diag"]["fred_group"]["inventory"]
        both = rows_of(second)
        assert {"D_SOFR_ADMIN", "D_SOFR_EFFR", "D_EFFR_TARGET"} <= set(both) and both["D_SOFR_ADMIN"]["last_value"] == rows_of(first)["D_SOFR_ADMIN"]["last_value"]
        assert second["counts"]["series"] == first["counts"]["series"]
        asked = [u for u in wire.calls if "api.stlouisfed.org" in u]
        assert sum("/fred/tags/series" in u for u in asked) == 1 + 3                 # the engine's one try, then the retry's three
        assert not any("/fred/series?" in u for u in asked)                          # no title is asked for one by one
        observed = {dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(u).query)).get("series_id") for u in asked if "/fred/series/observations" in u}
        assert observed and not observed & set(good), observed       # other blocks make their own single tries; nothing of this group is asked once FRED has refused twice
        assert json.loads(bucket.objects[S.FRED_INVENTORY_KEY]) == recorded          # a refused day is not recorded as an inventory

        # the same day before any inventory was recorded: the bank's documents are the inventory, titles and all
        del bucket.objects[S.FRED_INVENTORY_KEY]
        _, third = build(bucket, Down())
        listed = fred_rows(third)
        assert set(listed) == set(good) and "served from the bank's own listing" in third["diag"]["fred_group"]["inventory"]
        assert listed["IORB"]["label"] == good["IORB"]["label"] and listed["BOGZ1FL111Q"]["bucket"] == good["BOGZ1FL111Q"]["bucket"]

        # FRED is back: the group is asked as usual and the inventory is recorded again
        _, fourth = build(bucket, Wire())
        assert fourth["diag"]["fred_group"]["inventory"] == "fred" and set(fred_rows(fourth)) == set(good) and S.FRED_INVENTORY_KEY in bucket.objects
    finally:
        time.sleep = real_sleep


def test_the_engine_source_compiles_and_keeps_its_contact_lines_out_of_the_new_module():
    src = (HERE.parent / "source" / "lambda_function.py").read_text()
    compile(src, "lambda_function.py", "exec")
    scope = (HERE.parent / "source" / "repo_scope.py").read_text()
    assert "@" not in scope and "User-Agent" not in scope
    assert '"engine_v": "3.1"' in src and "refresh_due(fid, pairs," in src and "derived_series(_pairs)" in src
    own = (HERE.parent / "source" / "repo_sources.py").read_text()
    assert "@" not in own and "refresh_haircuts(" in src and "bank_survey_rate(" in src and "dtcc_fails(_txt)" in src
    assert "www.dtcc.com/data/failsdata.csv'" not in src                      # the dead address is no longer asked on its own


def main():
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for _, fn in tests:
        fn()
    print("board build: %d tests passed" % len(tests))


if __name__ == "__main__":
    main()
