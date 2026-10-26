"""justhodl-bank-factors v1.0.0 -- the whole-market factor book: every US-listed symbol's fundamentals, scores, consensus,
short interest, float, growth, surprises, passive ownership, peers and returns, joined ONCE a day from the provider
banks and written as one small document per symbol plus one columnar matrix for screens.

Why (Khalid 2026-10-25: "fuse the new data in all the engines that need them to improve and enrich those engines"):
the banks (justhodl-fmp-bank, -fmp-walker, -polygon-bank, -polygon-full) now hold whole-market files the fleet never
had -- Altman/Piotroski for every name, analyst targets and consensus for every name, TTM metrics/ratios for every
name, short interest per ticker, float per symbol -- but each is a 10-30 MB CSV or 13k per-ticker files.  An engine
that joins them itself pays seconds per container and runs out of memory at 512 MB (the first-light step-asides of
2026-10-25).  This engine pays that join once, so any engine reads ONE 3 KB document per symbol
(aws/shared/bank_enrich.factor) or ONE matrix for a whole-market screen (bank_enrich.matrix), with provenance.

What it writes (data/warm/factor-book/, write-if-changed, gzip JSON):
    symbols/<SYMBOL>.json.gz   per symbol: profile, scores, rating, dcf, price_target, consensus, key_metrics_ttm,
                               ratios_ttm, growth (latest fiscal year), surprises, float, short, etf_holders, peers,
                               returns (from the Polygon grouped tape), derived (upside %, beat rate, SI % float, ...),
                               ranks (cross-sectional percentiles, universe + sector) and composites; `as_of` per source
    matrix/core.json.gz        columnar: the curated numeric fields (FIELDS) for every symbol, for screens
    universe.json.gz           symbol -> exchange/sector/industry/market cap/flags (the cheap join key)
    manifest.json              counts, coverage per source, as_of per source, which source columns were found, formulas
    _state/state.json          per-symbol content hashes (write-if-changed), lease

v1.0.1 (2026-10-26): the first join ran out of 3 GB in 19 s decoding whole-market CSVs into strings; every bank table is
now STREAMED once (gunzip -> text -> csv.reader) into a SQLite staging file on /tmp keyed (block, symbol), and the
documents are assembled from indexed lookups - memory stays flat whatever the files weigh (ephemeral storage 2 GB).

Doctrine: bank-only (no provider call, no key); once a day (Scheduler, after the banks); one invoke, one budget, no
self-chaining; verbatim numbers (FMP's / Polygon's own values; derived fields are arithmetic on them and say so); a
missing source is recorded in coverage, never guessed; every value carries the bank's as_of.
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fmp_bank  # noqa: E402  (aws/shared, bundled)

VERSION = "1.0.4"
SCHEMA = "factor-book/1.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
ROOT = "data/warm/factor-book/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
FMP_ROOT = fmp_bank.ROOT                              # data/warm/fmp-bank/
WALKER_ROOT = fmp_bank.WALKER_ROOT                    # data/warm/fmp/
POLY_ROOT = "data/warm/polygon-bank/"
GROUPED_ROOT = "data/warm/polygon-full/grouped/"
BUDGET_S = float(os.environ.get("FACTOR_BOOK_BUDGET_S", "840"))
WORKERS = int(os.environ.get("FACTOR_BOOK_WORKERS", "24"))
RETURNS_SESSIONS = int(os.environ.get("FACTOR_BOOK_RETURNS_SESSIONS", "253"))
RETURNS_BUDGET_S = float(os.environ.get("FACTOR_BOOK_RETURNS_BUDGET_S", "300"))
MAX_SYMBOLS = int(os.environ.get("FACTOR_BOOK_MAX_SYMBOLS", "40000"))
DB_PATH = os.environ.get("FACTOR_BOOK_DB", "/tmp/factor-book.sqlite")

US_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSE AMERICAN", "NYSEARCA", "NYSE ARCA", "CBOE", "BATS", "OTC", "PNK", "OTCQB", "OTCQX", "OTCBB", "OTCM"}
OTC_EXCHANGES = {"OTC", "PNK", "OTCQB", "OTCQX", "OTCBB", "OTCM"}
PROFILE_DROP = {"description", "image", "website", "address", "phone", "zip", "city", "state", "defaultimage", "dcfdiff", "dcf", "changes", "range"}

CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, connect_timeout=5, read_timeout=120, max_pool_connections=64)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
_tls = threading.local()


def _s3():
    c = getattr(_tls, "c", None)
    if c is None:
        c = _tls.c = boto3.client("s3", region_name="us-east-1", config=CFG)
    return c


def log(msg):
    print("[bank-factors] " + str(msg), flush=True)


# ------------------------------------------------------------------------------------------------------ S3 helpers --
def _get_bytes(key, client=None):
    try:
        return (client or s3).get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except Exception as e:  # noqa: BLE001
        if "NoSuchKey" in type(e).__name__ or "NoSuchKey" in str(e) or "404" in str(e):
            return None
        return None


def _get_json(key, client=None):
    raw = _get_bytes(key, client)
    if raw is None:
        return None
    try:
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8", "replace"))
    except Exception:  # noqa: BLE001
        return None


def _put_json_gz(key, obj, meta=None, client=None):
    body = gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8"), compresslevel=6)
    (client or s3).put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json", ContentEncoding="gzip",
                              CacheControl="max-age=300", Metadata={k: str(v)[:250] for k, v in (meta or {}).items()})
    return len(body)


def _list_keys(prefix, max_keys=5000):
    out, token = [], None
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        out.extend(o["Key"] for o in r.get("Contents") or [])
        token = r.get("NextContinuationToken")
        if not token or len(out) >= max_keys:
            break
    return out


# ------------------------------------------------------------------------------------------------ field tolerance --
def _lk(row):
    return {str(k).lower().replace("_", "").replace(" ", ""): k for k in row} if isinstance(row, dict) else {}


def pick(row, *names, default=None):
    """First present, non-empty value among candidate column names (case/underscore/space-insensitive)."""
    if not isinstance(row, dict):
        return default
    lk = _lk(row)
    for n in names:
        k = lk.get(str(n).lower().replace("_", "").replace(" ", ""))
        if k is not None and row.get(k) not in (None, ""):
            return row[k]
    return default


def pick_key(row, *names):
    lk = _lk(row)
    for n in names:
        k = lk.get(str(n).lower().replace("_", "").replace(" ", ""))
        if k is not None:
            return k
    return None


def num(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v) if math.isfinite(float(v)) else None
    try:
        f = float(str(v).replace(",", ""))
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def pct(a, b):
    """a / b - 1 in percent, None when undefined."""
    a, b = num(a), num(b)
    if a is None or b is None or b == 0:
        return None
    return round((a / b - 1.0) * 100.0, 4)


def _safe_sym(sym):
    return re.sub(r"[^A-Za-z0-9._^=-]", "_", str(sym))[:60]


def _poly(sym):
    return str(sym).upper().replace("-", ".")


# --------------------------------------------------------------------------------------------- the curated matrix --
# (field, source block, candidate source columns or derived name).  The per-symbol doc carries every source column;
# the matrix carries these, so a screen reads one file.  Candidate names cover FMP's /stable bulk headers as served
# (camelCase, TTM suffix) -- `manifest.schema_found` says which one matched today.
FIELDS = [
    ("mcap", "profile", ("marketCap", "mktCap")), ("price", "profile", ("price",)), ("beta", "profile", ("beta",)),
    ("avg_volume", "profile", ("averageVolume", "volAvg")), ("last_dividend", "profile", ("lastDividend", "lastDiv")),
    ("altman_z", "scores", ("altmanZScore",)), ("piotroski", "scores", ("piotroskiScore",)),
    ("rating_score", "rating", ("overallScore", "ratingScore", "score", "ratingDetailsOverallScore")), ("dcf_score", "rating", ("discountedCashFlowScore", "ratingDetailsDCFScore")),
    ("roe_score", "rating", ("returnOnEquityScore", "ratingDetailsROEScore")), ("roa_score", "rating", ("returnOnAssetsScore", "ratingDetailsROAScore")),
    ("de_score", "rating", ("debtToEquityScore", "ratingDetailsDEScore")), ("pe_score", "rating", ("priceToEarningsScore", "ratingDetailsPEScore")),
    ("pb_score", "rating", ("priceToBookScore", "ratingDetailsPBScore")),
    ("dcf", "dcf", ("dcf",)), ("dcf_upside_pct", "derived", "dcf_upside_pct"),
    ("pt_avg_1m", "price_target", ("lastMonthAvgPriceTarget",)), ("pt_avg_1q", "price_target", ("lastQuarterAvgPriceTarget",)),
    ("pt_avg_1y", "price_target", ("lastYearAvgPriceTarget",)), ("pt_n_1m", "price_target", ("lastMonthCount",)), ("pt_n_1q", "price_target", ("lastQuarterCount",)),
    ("pt_upside_pct", "derived", "pt_upside_pct"),
    ("strong_buy", "consensus", ("strongBuy",)), ("buy", "consensus", ("buy",)), ("hold", "consensus", ("hold",)), ("sell", "consensus", ("sell",)),
    ("strong_sell", "consensus", ("strongSell",)), ("consensus_score", "derived", "consensus_score"),
    ("ev", "key_metrics_ttm", ("enterpriseValueTTM", "enterpriseValue")), ("ev_ebitda", "key_metrics_ttm", ("evToEBITDATTM", "enterpriseValueOverEBITDATTM")),
    ("ev_sales", "key_metrics_ttm", ("evToSalesTTM",)), ("fcf_yield", "key_metrics_ttm", ("freeCashFlowYieldTTM",)), ("earnings_yield", "key_metrics_ttm", ("earningsYieldTTM",)),
    ("roic", "key_metrics_ttm", ("returnOnInvestedCapitalTTM", "roicTTM")), ("roe", "key_metrics_ttm", ("returnOnEquityTTM", "roeTTM")),
    ("roa", "key_metrics_ttm", ("returnOnAssetsTTM",)), ("net_debt_ebitda", "key_metrics_ttm", ("netDebtToEBITDATTM",)),
    ("capex_to_revenue", "key_metrics_ttm", ("capexToRevenueTTM",)), ("rd_to_revenue", "key_metrics_ttm", ("researchAndDevelopementToRevenueTTM", "researchAndDevelopmentToRevenueTTM")),
    ("sbc_to_revenue", "key_metrics_ttm", ("stockBasedCompensationToRevenueTTM",)), ("dso", "key_metrics_ttm", ("daysOfSalesOutstandingTTM", "daysSalesOutstandingTTM")),
    ("dio", "key_metrics_ttm", ("daysOfInventoryOutstandingTTM", "daysOfInventoryOnHandTTM")), ("dpo", "key_metrics_ttm", ("daysOfPayablesOutstandingTTM", "daysPayablesOutstandingTTM")),
    ("cash_conversion_cycle", "key_metrics_ttm", ("cashConversionCycleTTM",)), ("graham_number", "key_metrics_ttm", ("grahamNumberTTM",)),
    ("graham_net_net", "key_metrics_ttm", ("grahamNetNetTTM",)), ("net_current_asset_value", "key_metrics_ttm", ("netCurrentAssetValueTTM",)),
    ("working_capital", "key_metrics_ttm", ("workingCapitalTTM",)), ("invested_capital", "key_metrics_ttm", ("investedCapitalTTM",)),
    ("tangible_asset_value", "key_metrics_ttm", ("tangibleAssetValueTTM",)), ("income_quality", "key_metrics_ttm", ("incomeQualityTTM",)),
    ("pe", "ratios_ttm", ("priceToEarningsRatioTTM", "peRatioTTM")), ("peg", "ratios_ttm", ("priceToEarningsGrowthRatioTTM", "pegRatioTTM")),
    ("pb", "ratios_ttm", ("priceToBookRatioTTM", "priceToBookRatioTTM")), ("ps", "ratios_ttm", ("priceToSalesRatioTTM",)),
    ("p_fcf", "ratios_ttm", ("priceToFreeCashFlowRatioTTM", "priceToFreeCashFlowsRatioTTM")), ("p_ocf", "ratios_ttm", ("priceToOperatingCashFlowRatioTTM", "priceToOperatingCashFlowsRatioTTM")),
    ("gross_margin", "ratios_ttm", ("grossProfitMarginTTM",)), ("ebitda_margin", "ratios_ttm", ("ebitdaMarginTTM",)),
    ("operating_margin", "ratios_ttm", ("operatingProfitMarginTTM",)), ("net_margin", "ratios_ttm", ("netProfitMarginTTM",)),
    ("current_ratio", "ratios_ttm", ("currentRatioTTM",)), ("quick_ratio", "ratios_ttm", ("quickRatioTTM",)),
    ("debt_to_equity", "ratios_ttm", ("debtToEquityRatioTTM", "debtEquityRatioTTM")), ("debt_to_assets", "ratios_ttm", ("debtToAssetsRatioTTM", "debtRatioTTM")),
    ("interest_coverage", "ratios_ttm", ("interestCoverageRatioTTM", "interestCoverageTTM")), ("dividend_yield", "ratios_ttm", ("dividendYieldTTM",)),
    ("payout_ratio", "ratios_ttm", ("dividendPayoutRatioTTM", "payoutRatioTTM")), ("fcf_per_share", "ratios_ttm", ("freeCashFlowPerShareTTM",)),
    ("revenue_per_share", "ratios_ttm", ("revenuePerShareTTM",)), ("book_value_per_share", "ratios_ttm", ("bookValuePerShareTTM",)),
    ("cash_per_share", "ratios_ttm", ("cashPerShareTTM",)), ("asset_turnover", "ratios_ttm", ("assetTurnoverTTM",)),
    ("inventory_turnover", "ratios_ttm", ("inventoryTurnoverTTM",)), ("effective_tax_rate", "ratios_ttm", ("effectiveTaxRateTTM",)),
    ("revenue_growth", "growth", ("growthRevenue",)), ("gross_profit_growth", "growth", ("growthGrossProfit",)), ("ebitda_growth", "growth", ("growthEBITDA",)),
    ("operating_income_growth", "growth", ("growthOperatingIncome",)), ("net_income_growth", "growth", ("growthNetIncome",)),
    ("eps_growth", "growth", ("growthEPS", "growthEPSDiluted")), ("shares_growth", "growth", ("growthWeightedAverageShsOutDil", "growthWeightedAverageShsOut")),
    ("fcf_growth", "cf_growth", ("growthFreeCashFlow",)), ("buyback_growth", "cf_growth", ("growthCommonStockRepurchased",)),
    ("beat_rate_4q", "derived", "beat_rate_4q"), ("avg_surprise_pct_4q", "derived", "avg_surprise_pct_4q"), ("last_surprise_pct", "derived", "last_surprise_pct"),
    ("float_shares", "float", ("floatShares",)), ("outstanding_shares", "float", ("outstandingShares",)), ("free_float_pct", "float", ("freeFloat",)),
    ("short_interest", "short", ("short_interest",)), ("days_to_cover", "short", ("days_to_cover",)), ("short_pct_float", "derived", "short_pct_float"),
    ("short_volume_ratio", "derived", "short_volume_ratio"),
    ("etf_holder_count", "derived", "etf_holder_count"), ("etf_weight_sum", "derived", "etf_weight_sum"), ("n_peers", "derived", "n_peers"),
    ("close", "returns", ("close",)), ("ret_1d", "returns", ("ret_1d",)), ("ret_5d", "returns", ("ret_5d",)), ("ret_21d", "returns", ("ret_21d",)),
    ("ret_63d", "returns", ("ret_63d",)), ("ret_126d", "returns", ("ret_126d",)), ("ret_252d", "returns", ("ret_252d",)),
    ("high_52w", "returns", ("high_52w",)), ("low_52w", "returns", ("low_52w",)), ("from_52w_high_pct", "returns", ("from_52w_high_pct",)),
    ("dollar_vol_20d", "returns", ("dollar_vol_20d",)), ("volatility_20d", "returns", ("volatility_20d",)),
]
IDENTITY = ("symbol", "name", "exchange", "sector", "industry", "country", "is_etf", "is_otc", "is_adr", "is_fund", "is_preferred", "is_warrant_unit", "is_shell", "ipo_date", "cik")
WARRANT_RE = re.compile(r"(WS|WW|WSA|WSB|-WT|\.WS|\.WT)$")


def is_warrant_unit(sym, name):
    """Warrants, units and rights (SPAC paper, mostly) carry a company's tickerish symbol but are not its common stock."""
    if WARRANT_RE.search(sym or ""):
        return True
    n = " " + (name or "").lower() + " "
    return any(k in n for k in (" warrant", " warrants", " unit ", " units ", " right ", " rights ", " subunit", " sub-unit"))


def is_shell(name, industry):
    """Blank-check / SPAC shells: cash-rich by construction, no operations - never a net-net, never a value screen hit."""
    ind = (industry or "").lower()
    n = (name or "").lower()
    return "shell compan" in ind or "blank check" in ind or "acquisition corp" in n or "acquisition co " in n or n.endswith("acquisition co") or "acquisition company" in n or " spac" in n
PREFERRED_RE = re.compile(r"(-P[A-Z]{0,2}|\.PR[A-Z]{0,2}|-PR[A-Z]{0,2})$")


def is_preferred(sym, name):
    """Preferred shares / units carry the issuer's fundamentals under their own ticker (ABR-PD, ABR-PE ...); they are
    documented but never ranked as common stocks."""
    if PREFERRED_RE.search(sym or ""):
        return True
    n = (name or "").lower()
    return any(k in n for k in (" preferred", " pref ", "pfd", "depositary share", "% series", "% notes", "% cumulative", "% non-cumulative"))
# percentile ranks: (field, higher_is_better) -- stored as pct_<field> (universe) and spct_<field> (within sector)
RANKED = [("pe", False), ("ev_ebitda", False), ("ps", False), ("p_fcf", False), ("pb", False), ("fcf_yield", True), ("earnings_yield", True),
          ("roic", True), ("roe", True), ("gross_margin", True), ("operating_margin", True), ("altman_z", True), ("piotroski", True),
          ("net_debt_ebitda", False), ("revenue_growth", True), ("eps_growth", True), ("beat_rate_4q", True), ("dcf_upside_pct", True), ("pt_upside_pct", True),
          ("consensus_score", True), ("short_pct_float", True), ("days_to_cover", True), ("float_shares", False), ("ret_21d", True), ("ret_63d", True),
          ("ret_126d", True), ("ret_252d", True), ("from_52w_high_pct", True), ("dollar_vol_20d", True), ("volatility_20d", False), ("dividend_yield", True), ("mcap", True)]
COMPOSITES = {
    # mean of the listed percentiles (cheapness = inverted percentile of the lower-is-better fields); None when < 2 inputs
    "value": ["pe", "ev_ebitda", "p_fcf", "ps", "fcf_yield", "earnings_yield"],
    "quality": ["roic", "roe", "gross_margin", "altman_z", "piotroski", "net_debt_ebitda"],
    "growth": ["revenue_growth", "eps_growth", "beat_rate_4q"],
    "momentum": ["ret_63d", "ret_126d", "ret_252d", "from_52w_high_pct"],
    "squeeze": ["short_pct_float", "days_to_cover", "float_shares"],
    "street": ["pt_upside_pct", "consensus_score", "dcf_upside_pct"],
}
POSITIVE_ONLY = {"pe", "ev_ebitda", "ps", "p_fcf", "pb", "peg"}       # a negative multiple is not "cheap"


# ------------------------------------------------------------------------------------------------- source loading --
def _stream_csv(key):
    """Yield (header, rows) from a banked gzip CSV WITHOUT holding it in memory: the S3 body is gunzipped and decoded as
    a stream (the first join of 2026-10-26 ran out of 3 GB in 19 s by decoding whole-market files into strings)."""
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
    except Exception:  # noqa: BLE001
        return None, iter(())
    gz = gzip.GzipFile(fileobj=body)
    text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline="")
    reader = csv.reader(text)
    try:
        header = next(reader)
    except StopIteration:
        return [], iter(())
    return header, reader


def _exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:  # noqa: BLE001
        return False


def _snapshot_keys(bulk):
    whole = FMP_ROOT + bulk + "/latest.csv.gz"
    if _exists(whole):
        return [whole]
    keys = []
    for part in range(0, 41):
        k = FMP_ROOT + "%s/part-%d/latest.csv.gz" % (bulk, part)
        if not _exists(k):
            break
        keys.append(k)
    return keys


class Sources:
    """Streams every bank table ONCE into a SQLite staging file on /tmp (block, symbol) -> typed row JSON, so memory
    stays flat whatever the files weigh; assembly then reads one symbol's rows with indexed lookups."""

    def __init__(self, db_path=DB_PATH):
        self.as_of = {}
        self.found = {}
        self.coverage = {}
        self.notes = []
        man = fmp_bank.manifest() or {}
        self.as_of["fmp-bank"] = man.get("updated_at")
        self.fmp_dated_keys = man.get("dated_keys") or []
        wman = fmp_bank.walker_manifest() or {}
        self.as_of["fmp-walker"] = wman.get("updated_at")
        pman = _get_json(POLY_ROOT + "manifest.json") or {}
        self.as_of["polygon-bank"] = pman.get("updated_at")
        try:
            os.remove(db_path)
        except OSError:
            pass
        self.db = sqlite3.connect(db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA temp_store=MEMORY")
        self.db.execute("PRAGMA cache_size=-200000")
        self.db.execute("CREATE TABLE rows (block TEXT, symbol TEXT, year INTEGER, row TEXT)")
        self.universe = {}
        self._pending = []

    # -- staging -------------------------------------------------------------------------------------------------
    def _flush(self):
        if self._pending:
            self.db.executemany("INSERT INTO rows VALUES (?,?,?,?)", self._pending)
            self._pending = []

    def _stage(self, block, sym, row, year=None):
        self._pending.append((block, sym, year, json.dumps(row, separators=(",", ":"), default=str)))
        if len(self._pending) >= 5000:
            self._flush()

    def finish_staging(self):
        self._flush()
        self.db.execute("CREATE INDEX ix_rows ON rows (block, symbol)")
        self.db.commit()

    def stage_profile(self, bulk="profile-bulk"):
        """One streaming pass: decides the universe (US-listed, actively trading) and keeps those rows."""
        keys = _snapshot_keys(bulk)
        self.coverage["profile"] = {"files": len(keys)}
        if not keys:
            self.notes.append("profile: not banked (%s)" % bulk)
            return
        for key in keys:
            header, reader = _stream_csv(key)
            if not header:
                continue
            self.found["profile.columns"] = header
            lk = {h.lower().replace("_", ""): i for i, h in enumerate(header)}

            def col(*names):
                for n in names:
                    i = lk.get(n.lower().replace("_", ""))
                    if i is not None:
                        return i
                return None
            i_sym, i_ex = col("symbol"), col("exchangeShortName", "exchange")
            i_act, i_etf, i_fund, i_adr = col("isActivelyTrading"), col("isEtf"), col("isFund"), col("isAdr")
            i_name, i_sec, i_ind, i_cty = col("companyName", "name"), col("sector"), col("industry"), col("country")
            i_mc, i_ipo, i_cik, i_cur = col("marketCap", "mktCap"), col("ipoDate"), col("cik"), col("currency")
            if i_sym is None or i_ex is None:
                self.notes.append("profile: symbol/exchange columns missing")
                continue
            for row in reader:
                if len(row) <= max(i_sym, i_ex):
                    continue
                ex = row[i_ex].strip().upper()
                if ex not in US_EXCHANGES:
                    continue
                if i_act is not None and i_act < len(row) and row[i_act].strip().lower() == "false":
                    continue
                sym = row[i_sym].strip().upper()
                if not sym or len(sym) > 12 or " " in sym or sym in self.universe:
                    continue

                def g(i):
                    return (row[i] or None) if (i is not None and i < len(row)) else None
                self.universe[sym] = {"symbol": sym, "name": g(i_name), "exchange": ex, "sector": g(i_sec), "industry": g(i_ind), "country": g(i_cty),
                                      "mcap": num(g(i_mc)), "is_etf": (g(i_etf) or "").strip().lower() == "true", "is_fund": (g(i_fund) or "").strip().lower() == "true",
                                      "is_adr": (g(i_adr) or "").strip().lower() == "true", "is_otc": ex in OTC_EXCHANGES, "is_preferred": is_preferred(sym, g(i_name)),
                                      "is_warrant_unit": is_warrant_unit(sym, g(i_name)), "is_shell": is_shell(g(i_name), g(i_ind)),
                                      "ipo_date": g(i_ipo), "cik": g(i_cik), "currency": g(i_cur)}
                self._stage("profile", sym, fmp_bank._typed(dict(zip(header, row))))
                if len(self.universe) >= MAX_SYMBOLS:
                    break
        self._flush()

    def stage_bulk(self, block, bulk):
        """Snapshot bulk (latest.csv.gz or part-N files): the universe's rows only."""
        keys = _snapshot_keys(bulk)
        self.coverage[block] = {"files": len(keys), "rows": 0}
        if not keys:
            self.notes.append("%s: not banked (%s)" % (block, bulk))
            return
        n = 0
        for key in keys:
            header, reader = _stream_csv(key)
            if not header:
                continue
            self.found[block + ".columns"] = header
            low = [h.strip().lower() for h in header]
            if "symbol" not in low:
                self.notes.append("%s: no symbol column (%s)" % (block, header[:6]))
                continue
            si = low.index("symbol")
            for row in reader:
                if si >= len(row):
                    continue
                sym = row[si].strip().upper()
                hit = sym in self.universe or sym.replace(".", "-") in self.universe
                if not hit:
                    continue
                self._stage(block, sym if sym in self.universe else sym.replace(".", "-"), fmp_bank._typed(dict(zip(header, row))))
                n += 1
        self._flush()
        self.coverage[block]["rows"] = n

    def stage_etf_holders(self, bulk="etf-holder-bulk"):
        """etf-holder-bulk lists (etf, asset, weight, shares, value) per ETF; stored inverted: asset -> one row per ETF."""
        keys = _snapshot_keys(bulk)
        self.coverage["etf_holders"] = {"files": len(keys), "rows": 0}
        if not keys:
            self.notes.append("etf_holders: not banked (%s)" % bulk)
            return
        n = 0
        for key in keys:
            header, reader = _stream_csv(key)
            if not header:
                continue
            self.found["etf_holders.columns"] = header
            lk = {h.lower().replace("_", ""): i for i, h in enumerate(header)}

            def col(*names):
                for nme in names:
                    i = lk.get(nme.lower().replace("_", ""))
                    if i is not None:
                        return i
                return None
            i_etf, i_asset = col("symbol", "etfSymbol"), col("asset", "holdingSymbol", "ticker")
            i_w, i_sh, i_mv = col("weightPercentage", "weight"), col("sharesNumber", "shares"), col("marketValue")
            if i_etf is None or i_asset is None:
                self.notes.append("etf_holders: symbol/asset columns missing (%s)" % header[:8])
                continue
            for row in reader:
                if len(row) <= max(i_etf, i_asset):
                    continue
                asset = row[i_asset].strip().upper()
                key_sym = asset if asset in self.universe else asset.replace(".", "-")
                if key_sym not in self.universe:
                    continue
                self._stage("etf_holders", key_sym, [row[i_etf], num(row[i_w]) if i_w is not None and i_w < len(row) else None,
                                                     num(row[i_sh]) if i_sh is not None and i_sh < len(row) else None, num(row[i_mv]) if i_mv is not None and i_mv < len(row) else None])
                n += 1
        self._flush()
        self.coverage["etf_holders"]["rows"] = n

    def stage_dated(self, block, bulk, years=2, period="annual"):
        """Dated bulks (growth per fiscal year, surprises per year): the newest `years` files, universe rows only."""
        held = sorted({int(k.split("/")[1]) for k in self.fmp_dated_keys if k.startswith(bulk + "/") and k.split("/")[2].startswith(period)}, reverse=True)
        if not held:
            y = datetime.now(timezone.utc).year
            held = [y, y - 1, y - 2]
        used = []
        for y in held:
            if len(used) >= years:
                break
            key = FMP_ROOT + "%s/%d/%s.csv.gz" % (bulk, y, period)
            header, reader = _stream_csv(key)
            if not header:
                continue
            used.append(y)
            low = [h.strip().lower() for h in header]
            if "symbol" not in low:
                continue
            si = low.index("symbol")
            for row in reader:
                if si >= len(row):
                    continue
                sym = row[si].strip().upper()
                key_sym = sym if sym in self.universe else sym.replace(".", "-")
                if key_sym not in self.universe:
                    continue
                self._stage(block, key_sym, fmp_bank._typed(dict(zip(header, row))), year=y)
        self._flush()
        self.coverage[block] = {"files": len(used), "years": used}
        if not used:
            self.notes.append("%s: no dated files (%s)" % (block, bulk))

    # -- reading --------------------------------------------------------------------------------------------------
    def rows(self, block, symbol):
        cur = self.db.execute("SELECT row FROM rows WHERE block=? AND symbol=?", (block, symbol))
        return [json.loads(r[0]) for r in cur.fetchall()]

    def rows_dated(self, block, symbol):
        cur = self.db.execute("SELECT row, year FROM rows WHERE block=? AND symbol=?", (block, symbol))
        out = []
        for raw, y in cur.fetchall():
            r = json.loads(raw)
            r.setdefault("_year", y)
            out.append(r)
        out.sort(key=lambda r: str(r.get("date") or r.get("fiscalYear") or r.get("_year") or ""), reverse=True)
        return out

    def holders(self, symbol):
        cur = self.db.execute("SELECT row FROM rows WHERE block='etf_holders' AND symbol=?", (symbol,))
        return [tuple(json.loads(r[0])) for r in cur.fetchall()]

    def close(self):
        try:
            self.db.close()
        except Exception:  # noqa: BLE001
            pass


# ------------------------------------------------------------------------------------------------- walker joins ---
def load_float(src: Sources):
    """shares-float-all: the newest day's pages -> symbol -> row."""
    days = sorted({k.split("/")[-2] for k in _list_keys(WALKER_ROOT + "shares-float-all/") if k.endswith(".json.gz") and k.count("/") >= 5}, reverse=True)
    out = {}
    for day in days[:1]:
        for k in sorted(_list_keys(WALKER_ROOT + "shares-float-all/%s/" % day)):
            rows = _get_json(k)
            for r in rows if isinstance(rows, list) else []:
                sym = str(pick(r, "symbol") or "").upper()
                if sym:
                    out[sym] = r
        src.as_of["float"] = day
    src.coverage["float"] = {"symbols": len(out)}
    if not out:
        src.notes.append("float: shares-float-all not banked yet")
    return out


def load_calendar_month(src: Sources, dataset, months=2):
    """window-kind calendars: current (+ previous) month files -> symbol -> rows (dated)."""
    now = datetime.now(timezone.utc)
    out = {}
    got = 0
    for i in range(months):
        y, m = now.year, now.month - i
        while m <= 0:
            y, m = y - 1, m + 12
        rows = _get_json(WALKER_ROOT + "%s/%04d-%02d.json.gz" % (dataset, y, m))
        if not isinstance(rows, list):
            continue
        got += 1
        for r in rows:
            sym = str(pick(r, "symbol") or "").upper()
            if sym:
                out.setdefault(sym, []).append(r)
    src.coverage[dataset] = {"months": got, "symbols": len(out)}
    return out


# ------------------------------------------------------------------------------------------------- returns pass ---
def returns_from_tape(uni, deadline, budget_s=None):
    """One pass over the newest RETURNS_SESSIONS grouped sessions of the Polygon tape (polygon-full, complete days):
    close at 0/1/5/21/63/126/252 sessions back, 52-week high/low, 20-session dollar volume and realised volatility."""
    keys = sorted(k for k in _list_keys(GROUPED_ROOT, max_keys=20000) if k.endswith(".json.gz") and re.search(r"/\d{4}-\d{2}-\d{2}\.json\.gz$", k))
    keys = keys[-RETURNS_SESSIONS:]
    if not keys:
        return {}, {"sessions": 0}
    want = {0, 1, 5, 21, 63, 126, 252}
    closes = {}                       # sym -> {offset: close}
    hl = {}                           # sym -> [high, low]
    recent = {}                       # sym -> list of (close, dollar volume) for the 21 newest sessions
    t_start = time.time()
    read = 0
    for i, key in enumerate(reversed(keys)):             # newest first
        if time.time() > deadline or time.time() - t_start > (budget_s or RETURNS_BUDGET_S):
            break
        doc = _get_json(key)
        rows = (doc or {}).get("results") if isinstance(doc, dict) else doc
        if not isinstance(rows, list):
            continue
        read += 1
        for r in rows:
            sym = r.get("T")
            if not sym:
                continue
            c, h, l, v, vw = r.get("c"), r.get("h"), r.get("l"), r.get("v"), r.get("vw")
            if i in want and c is not None:
                closes.setdefault(sym, {})[i] = c
            if h is not None and l is not None:
                cur = hl.get(sym)
                if cur is None:
                    hl[sym] = [h, l]
                else:
                    if h > cur[0]:
                        cur[0] = h
                    if l < cur[1]:
                        cur[1] = l
            if i <= 20 and c is not None:
                recent.setdefault(sym, []).append((c, (v or 0) * (vw if vw else c)))
    out = {}
    last_date = keys[-1].rsplit("/", 1)[-1][:10]
    for sym, cs in closes.items():
        c0 = cs.get(0)
        if c0 is None:
            continue
        d = {"close": c0, "as_of": last_date}
        for off, name in ((1, "ret_1d"), (5, "ret_5d"), (21, "ret_21d"), (63, "ret_63d"), (126, "ret_126d"), (252, "ret_252d")):
            if off < read and cs.get(off):
                d[name] = pct(c0, cs[off])
        if sym in hl and read >= 250:
            d["high_52w"], d["low_52w"] = hl[sym]
            d["from_52w_high_pct"] = pct(c0, hl[sym][0]) if hl[sym][0] else None
        rc = recent.get(sym) or []
        if len(rc) >= 20:
            d["dollar_vol_20d"] = round(sum(x[1] for x in rc[:20]) / 20.0, 2)
            lr = []
            for a, b in zip(rc[:20], rc[1:21]):
                if a[0] and b[0] and a[0] > 0 and b[0] > 0:
                    lr.append(math.log(a[0] / b[0]))
            if len(lr) >= 15:
                m = sum(lr) / len(lr)
                var = sum((x - m) ** 2 for x in lr) / (len(lr) - 1)
                d["volatility_20d"] = round(math.sqrt(var) * math.sqrt(252) * 100.0, 4)
        out[sym] = d
    return out, {"sessions": read, "last_session": last_date, "symbols": len(out), "seconds": round(time.time() - t_start, 1)}


# -------------------------------------------------------------------------------------------- per-symbol assembly --
def _first(rows):
    return rows[0] if rows else None


def _strip(row, drop=()):
    if not isinstance(row, dict):
        return None
    return {k: v for k, v in row.items() if str(k).lower() not in drop and k != "symbol" and not str(k).startswith("_")}


def assemble(sym, ident, src: Sources, floats, shorts, shortvol, cal_earn, cal_div, returns, holders_inv=None):  # holders_inv kept for signature stability
    """One symbol's document from every source (verbatim rows) + derived fields.  Pure function of the inputs."""
    doc = {"symbol": sym, "polygon_ticker": _poly(sym), "schema": SCHEMA, "identity": ident}
    prof = _first(src.rows("profile", sym))
    doc["profile"] = _strip(prof, PROFILE_DROP)
    doc["scores"] = _strip(_first(src.rows("scores", sym)))
    doc["rating"] = _strip(_first(src.rows("rating", sym)))
    doc["dcf"] = _strip(_first(src.rows("dcf", sym)))
    doc["price_target"] = _strip(_first(src.rows("price_target", sym)))
    doc["consensus"] = _strip(_first(src.rows("consensus", sym)))
    doc["key_metrics_ttm"] = _strip(_first(src.rows("key_metrics_ttm", sym)))
    doc["ratios_ttm"] = _strip(_first(src.rows("ratios_ttm", sym)))
    peers_row = _first(src.rows("peers", sym))
    peers = pick(peers_row, "peersList", "peers") if peers_row else None
    if isinstance(peers, str):
        peers = [p.strip() for p in re.split(r"[,\s|]+", peers) if p.strip()]
    doc["peers"] = peers if isinstance(peers, list) else None
    holders = src.holders(sym)
    if holders:
        hs = [{"etf": e, "weight_pct": w, "shares": sh, "market_value": mv} for e, w, sh, mv in holders]
        hs.sort(key=lambda x: -(x["weight_pct"] or 0))
        doc["etf_holders"] = {"count": len(hs), "weight_sum_pct": round(sum(x["weight_pct"] or 0 for x in hs), 4), "top": hs[:10]}
    else:
        doc["etf_holders"] = None
    g = _first(src.rows_dated("growth", sym))
    doc["growth"] = _strip(g)
    cg = _first(src.rows_dated("cf_growth", sym))
    doc["cf_growth"] = _strip(cg)
    sur = src.rows_dated("surprises", sym)
    doc["surprises"] = [_strip(r) for r in sur[:8]] or None
    fl = floats.get(sym) or floats.get(sym.replace(".", "-"))
    doc["float"] = _strip(fl)
    sh = shorts.get(_poly(sym))
    doc["short"] = sh
    sv = shortvol.get(_poly(sym))
    doc["short_volume"] = sv
    doc["calendar"] = {"earnings": [_strip(r) for r in (cal_earn.get(sym) or [])[:3]] or None,
                       "dividends": [_strip(r) for r in (cal_div.get(sym) or [])[:3]] or None}
    doc["returns"] = returns.get(_poly(sym))
    # ---- derived (arithmetic on the banked numbers; names say what they are) ----
    d = {}
    price = num(pick(prof, "price")) if prof else None
    if price is None and doc["returns"]:
        price = doc["returns"].get("close")
    d["price_used"] = price
    dcf = num(pick(doc["dcf"], "dcf")) if doc["dcf"] else None
    d["dcf_upside_pct"] = pct(dcf, price) if dcf and dcf > 0 else None
    pt, ptn = None, None
    if doc["price_target"]:
        pt = num(pick(doc["price_target"], "lastMonthAvgPriceTarget"))
        ptn = num(pick(doc["price_target"], "lastMonthCount"))
        if not pt or pt <= 0 or not ptn:                         # FMP writes 0 for "no target last month": not a -100% upside
            pt = num(pick(doc["price_target"], "lastQuarterAvgPriceTarget"))
            ptn = num(pick(doc["price_target"], "lastQuarterCount"))
        if not pt or pt <= 0 or not ptn:
            pt = None
    d["pt_upside_pct"] = pct(pt, price)
    d["pt_n_used"] = ptn if pt else None
    c = doc["consensus"]
    if c:
        sb, b, h, s, ss = (num(pick(c, "strongBuy")) or 0, num(pick(c, "buy")) or 0, num(pick(c, "hold")) or 0, num(pick(c, "sell")) or 0, num(pick(c, "strongSell")) or 0)
        tot = sb + b + h + s + ss
        d["consensus_n"] = tot
        d["consensus_score"] = round((2 * sb + b - s - 2 * ss) / tot, 4) if tot else None
    if sur:
        rows = sorted(sur, key=lambda r: str(pick(r, "date") or ""), reverse=True)
        recent = rows[:4]
        sp = []
        for r in recent:
            a, e = num(pick(r, "actualEarningResult", "actual", "epsActual")), num(pick(r, "estimatedEarning", "estimate", "epsEstimated"))
            if a is not None and e is not None:
                sp.append(((a - e) / abs(e) * 100.0) if e else (0.0 if a == 0 else None))
        sp = [x for x in sp if x is not None]
        beats = sum(1 for r in recent if (num(pick(r, "actualEarningResult", "actual", "epsActual")) or 0) >= (num(pick(r, "estimatedEarning", "estimate", "epsEstimated")) or 0)
                    and pick(r, "estimatedEarning", "estimate", "epsEstimated") is not None)
        d["beat_rate_4q"] = round(beats / len(recent), 4) if recent else None
        d["avg_surprise_pct_4q"] = round(sum(sp) / len(sp), 4) if sp else None
        d["last_surprise_pct"] = round(sp[0], 4) if sp else None
        d["last_surprise_date"] = pick(rows[0], "date") if rows else None
    if sh:
        si, flt = num(sh.get("short_interest")), num(pick(fl, "floatShares")) if fl else None
        d["short_pct_float"] = round(si / flt * 100.0, 4) if si is not None and flt else None
        d["si_settlement_date"] = sh.get("settlement_date")
    if sv:
        d["short_volume_ratio"] = sv.get("ratio")
    d["etf_holder_count"] = doc["etf_holders"]["count"] if doc["etf_holders"] else None
    d["etf_weight_sum"] = doc["etf_holders"]["weight_sum_pct"] if doc["etf_holders"] else None
    d["n_peers"] = len(doc["peers"]) if doc["peers"] else None
    ce = (cal_earn.get(sym) or [])
    if ce:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        upcoming = sorted(str(pick(r, "date") or "") for r in ce if str(pick(r, "date") or "") >= today)
        d["next_earnings_date"] = upcoming[0] if upcoming else None
    cd = (cal_div.get(sym) or [])
    if cd:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        upcoming = sorted(str(pick(r, "date") or "") for r in cd if str(pick(r, "date") or "") >= today)
        d["next_ex_dividend_date"] = upcoming[0] if upcoming else None
    doc["derived"] = d
    return doc


def core_fields(doc):
    """The curated numeric row for the matrix (FIELDS) + identity."""
    row = {k: doc["identity"].get(k) for k in IDENTITY}
    for field, block, cands in FIELDS:
        if block == "derived":
            row[field] = num(doc.get("derived", {}).get(cands))
        elif block == "returns":
            row[field] = num((doc.get("returns") or {}).get(cands[0]))
        elif block == "profile":
            row[field] = num(pick(doc.get("profile"), *cands))
        else:
            row[field] = num(pick(doc.get(block), *cands))
    return row


def schema_found(sample_docs):
    """Which candidate column answered each matrix field today (so a header rename is seen, not guessed around)."""
    found = {}
    for field, block, cands in FIELDS:
        if block in ("derived", "returns"):
            continue
        for doc in sample_docs:
            blk = doc.get(block) or {}
            k = pick_key(blk, *cands)
            if k:
                found[field] = "%s.%s" % (block, k)
                break
        else:
            found[field] = None
    return found


# -------------------------------------------------------------------------------------------- ranks / composites --
def percentiles(rows, field, higher_is_better, group=None):
    """pct = share of the comparison set with a lower (worse) value, 0..100; positive-only multiples skip <= 0."""
    vals = []
    for i, r in enumerate(rows):
        if group is not None and r.get("sector") != group:
            continue
        if r.get("is_etf") or r.get("is_fund") or r.get("is_otc") or r.get("is_preferred") or r.get("is_warrant_unit") or r.get("is_shell"):
            continue
        v = r.get(field)
        if v is None:
            continue
        if field in POSITIVE_ONLY and v <= 0:
            continue
        vals.append((v, i))
    n = len(vals)
    out = {}
    if n < 5:
        return out
    vals.sort(key=lambda x: x[0], reverse=not higher_is_better)       # worst first
    for rank, (v, i) in enumerate(vals):
        out[i] = round(rank / (n - 1) * 100.0, 2)
    return out


def rank_all(rows):
    """Adds pct_<field> (universe of US common stocks) and spct_<field> (same sector) to every row + composites."""
    by_sector = {}
    for i, r in enumerate(rows):
        if r.get("sector"):
            by_sector.setdefault(r["sector"], []).append(i)
    for field, hib in RANKED:
        for i, v in percentiles(rows, field, hib).items():
            rows[i]["pct_" + field] = v
        for sec, idx in by_sector.items():
            sub = [rows[i] for i in idx]
            for j, v in percentiles(sub, field, hib).items():
                rows[idx[j]]["spct_" + field] = v
    for r in rows:
        for name, parts in COMPOSITES.items():
            vals = [r.get("pct_" + f) for f in parts if r.get("pct_" + f) is not None]
            r["composite_" + name] = round(sum(vals) / len(vals), 2) if len(vals) >= 2 else None
    return rows


# ----------------------------------------------------------------------------------------------------- handler ----
def lambda_handler(event=None, context=None):
    event = event or {}
    t0 = time.time()
    budget = float(event.get("budget_s") or BUDGET_S)
    deadline = t0 + budget
    reserve = {"short": min(400.0, budget * 0.45), "pass1": min(240.0, budget * 0.30), "pass2": min(30.0, budget * 0.08), "returns": min(RETURNS_BUDGET_S, budget * 0.40)}
    now = datetime.now(timezone.utc)
    state = _get_json(STATE_KEY) or {}
    if float(state.get("lease_until") or 0) > time.time() and not event.get("force"):
        return {"skipped": "lease_held", "lease_until": state.get("lease_until")}
    state["lease_until"] = time.time() + 1000
    _put_json_gz_plain(STATE_KEY, state)
    log("v%s start; budget %.0fs" % (VERSION, deadline - t0))

    src = Sources()
    src.stage_profile()
    uni = src.universe
    if not uni:
        src.close()
        state.pop("lease_until", None)
        _put_json_gz_plain(STATE_KEY, state)
        return {"ok": False, "error": "profile-bulk not banked (or no US-listed rows); nothing to join", "notes": src.notes}
    log("universe %d US-listed symbols (%.0fs)" % (len(uni), time.time() - t0))
    for block, bulk in (("scores", "scores-bulk"), ("rating", "rating-bulk"), ("dcf", "dcf-bulk"), ("price_target", "price-target-summary-bulk"),
                        ("consensus", "upgrades-downgrades-consensus-bulk"), ("key_metrics_ttm", "key-metrics-ttm-bulk"), ("ratios_ttm", "ratios-ttm-bulk"), ("peers", "peers-bulk")):
        src.stage_bulk(block, bulk)
        if time.time() > deadline - reserve["pass1"] - min(300.0, budget * 0.3):
            src.notes.append("staging stopped after %s (budget)" % block)
            break
    src.stage_etf_holders()
    src.stage_dated("growth", "income-statement-growth-bulk", years=2)
    src.stage_dated("cf_growth", "cash-flow-statement-growth-bulk", years=2)
    src.stage_dated("surprises", "earnings-surprises-bulk", years=2, period="year")
    src.finish_staging()
    log("staged: %s (%.0fs)" % (json.dumps({k: v for k, v in src.coverage.items()})[:600], time.time() - t0))
    floats = load_float(src)
    cal_earn = load_calendar_month(src, "earnings-calendar")
    cal_div = load_calendar_month(src, "dividends-calendar")
    shorts, shortvol = load_polygon_short(uni, src, deadline - reserve["short"])
    log("float %d, short %d, short-volume %d, calendars %d/%d (%.0fs)" % (len(floats), len(shorts), len(shortvol), len(cal_earn), len(cal_div), time.time() - t0))
    returns, rinfo = returns_from_tape(uni, deadline - reserve["pass1"] - 60, reserve["returns"]) if not event.get("no_returns") else ({}, {"sessions": 0, "skipped": True})
    src.coverage["returns"] = rinfo
    src.as_of["polygon-tape"] = rinfo.get("last_session")
    log("returns: %s (%.0fs)" % (json.dumps(rinfo), time.time() - t0))

    holders_inv = None

    # pass 1: the curated rows for every symbol (small) -> cross-sectional ranks
    syms = sorted(uni)
    rows = []
    for sym in syms:
        rows.append(core_fields(assemble(sym, uni[sym], src, floats, shorts, shortvol, cal_earn, cal_div, returns, holders_inv)))
        if time.time() > deadline - reserve["pass1"]:
            src.notes.append("pass 1 stopped at %d/%d symbols (budget)" % (len(rows), len(syms)))
            break
    log("pass 1: %d rows (%.0fs)" % (len(rows), time.time() - t0))
    rank_all(rows)
    by_sym = {r["symbol"]: r for r in rows}
    log("ranks done (%.0fs)" % (time.time() - t0))

    # pass 2: full documents, ranks attached, write-if-changed, threaded, nothing retained
    hashes = state.get("hashes") or {}
    new_hashes = {}
    cov = {b: 0 for b in ("profile", "scores", "rating", "dcf", "price_target", "consensus", "key_metrics_ttm", "ratios_ttm", "peers", "etf_holders", "growth", "cf_growth", "surprises", "float", "short", "short_volume", "returns")}
    sample = []
    written, failed, bytes_out, unchanged, pending = 0, 0, 0, 0, 0

    def put(sym, body, h):
        _s3().put_object(Bucket=BUCKET, Key=ROOT + "symbols/%s.json.gz" % _safe_sym(sym), Body=body, ContentType="application/json", ContentEncoding="gzip",
                         CacheControl="max-age=300", Metadata={"sha1": h, "as_of": str(src.as_of.get("fmp-bank") or "")[:40]})
        return len(body)
    futs = set()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for sym in [r["symbol"] for r in rows]:
            if time.time() > deadline - reserve["pass2"]:
                src.notes.append("pass 2 stopped by the budget at %d/%d symbols" % (written + unchanged + pending, len(rows)))
                break
            doc = assemble(sym, uni[sym], src, floats, shorts, shortvol, cal_earn, cal_div, returns, holders_inv)
            r = by_sym.get(sym) or {}
            doc["ranks"] = {k: v for k, v in r.items() if k.startswith(("pct_", "spct_"))}
            doc["composites"] = {k[len("composite_"):]: v for k, v in r.items() if k.startswith("composite_")}
            doc["as_of"] = dict(src.as_of)
            doc["version"] = VERSION
            for b in cov:
                if doc.get(b):
                    cov[b] += 1
            if len(sample) < 60:
                sample.append(doc)
            body = json.dumps(doc, sort_keys=True, separators=(",", ":"), default=str)
            h = hashlib.sha1(body.encode("utf-8")).hexdigest()
            new_hashes[sym] = h
            if hashes.get(sym) == h and not event.get("rewrite"):
                unchanged += 1
                continue
            futs.add(pool.submit(put, sym, gzip.compress(body.encode("utf-8"), compresslevel=6), h))
            pending += 1
            if len(futs) >= WORKERS * 8:                      # bounded in-flight set keeps memory flat
                done = {f for f in futs if f.done()}
                if not done:
                    done = {next(as_completed(futs))}
                for f in done:
                    futs.discard(f)
                    pending -= 1
                    try:
                        bytes_out += f.result()
                        written += 1
                    except Exception as e:  # noqa: BLE001
                        failed += 1
                        if failed <= 3:
                            log("put failed: %s" % type(e).__name__)
        for f in as_completed(futs):
            pending -= 1
            try:
                bytes_out += f.result()
                written += 1
            except Exception as e:  # noqa: BLE001
                failed += 1
                if failed <= 3:
                    log("put failed: %s" % type(e).__name__)
    found = schema_found(sample)
    src.close()
    docs_n = written + unchanged
    log("pass 2: %d written, %d unchanged, %d failed, %.1f MB (%.0fs)" % (written, unchanged, failed, bytes_out / 1e6, time.time() - t0))
    # matrix + universe
    cols = {}
    keys = list(IDENTITY) + [f for f, _, _ in FIELDS] + ["pct_" + f for f, _ in RANKED] + ["spct_" + f for f, _ in RANKED] + ["composite_" + c for c in COMPOSITES]
    for k in keys:
        cols[k] = [r.get(k) for r in rows]
    matrix = {"schema": SCHEMA, "version": VERSION, "generated_at": now.isoformat(), "as_of": src.as_of, "n": len(rows), "symbols": [r["symbol"] for r in rows], "cols": cols,
              "fields": [f for f, _, _ in FIELDS], "ranked": [f for f, _ in RANKED], "composites": COMPOSITES, "positive_only": sorted(POSITIVE_ONLY)}
    mbytes = _put_json_gz(ROOT + "matrix/core.json.gz", matrix, {"n": len(rows)})
    # v1.0.3: the same matrix as one file per column (projection pushdown for bank_enrich.rows/screen/lookup): an engine
    # screening on five fields loads five small columns + the identity index, not the 37 MB whole; threaded puts
    cbytes = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = [pool.submit(lambda k=k: _put_json_gz(ROOT + "matrix/cols/%s.json.gz" % k, cols[k], {"n": len(rows)}, client=_s3())) for k in keys if k != "symbol"]
        futs.append(pool.submit(lambda: _put_json_gz(ROOT + "matrix/index.json.gz", {"schema": SCHEMA, "version": VERSION, "generated_at": now.isoformat(), "as_of": src.as_of, "n": len(rows),
                                                                                      "symbols": [r["symbol"] for r in rows], "columns": [k for k in keys if k != "symbol"],
                                                                                      "identity": {k: cols[k] for k in ("sector", "industry", "exchange", "is_etf", "is_otc", "is_fund", "is_preferred", "is_warrant_unit", "is_shell", "is_adr") if k in cols}}, {"n": len(rows)}, client=_s3())))
        for f in as_completed(futs):
            try:
                cbytes += f.result()
            except Exception as e:  # noqa: BLE001
                src.notes.append("matrix column put failed: %s" % type(e).__name__)
    _put_json_gz(ROOT + "universe.json.gz", {"schema": SCHEMA, "generated_at": now.isoformat(), "as_of": src.as_of, "n": len(uni), "symbols": uni}, {"n": len(uni)})
    # manifest + state
    state = {"version": VERSION, "runs": int(state.get("runs") or 0) + 1, "updated_at": now.isoformat(), "hashes": {**{k: v for k, v in hashes.items() if k in uni}, **new_hashes},
             "last_run": {"started": datetime.fromtimestamp(t0, timezone.utc).isoformat(), "seconds": round(time.time() - t0, 1), "symbols": docs_n, "written": written,
                          "unchanged": unchanged, "failed": failed, "bytes": bytes_out, "notes": src.notes}}
    _put_json_gz_plain(STATE_KEY, state)
    manifest = {"schema": SCHEMA, "version": VERSION, "engine": "justhodl-bank-factors", "updated_at": now.isoformat(), "as_of": src.as_of,
                "symbols": docs_n, "universe": len(uni), "common_stocks": sum(1 for u in uni.values() if not (u["is_etf"] or u["is_fund"] or u["is_otc"] or u.get("is_preferred") or u.get("is_warrant_unit") or u.get("is_shell"))),
                "preferreds": sum(1 for u in uni.values() if u.get("is_preferred")), "funds": sum(1 for u in uni.values() if u["is_fund"]),
                "warrants_units": sum(1 for u in uni.values() if u.get("is_warrant_unit")), "shells": sum(1 for u in uni.values() if u.get("is_shell")),
                "etfs": sum(1 for u in uni.values() if u["is_etf"]), "otc": sum(1 for u in uni.values() if u["is_otc"]),
                "coverage": cov, "sources": src.coverage, "schema_found": found, "source_columns": src.found, "fields": [f for f, _, _ in FIELDS], "ranked": [f for f, _ in RANKED],
                "composites": {k: "mean of pct_" + ", pct_".join(v) + " (lower-is-better fields inverted; >= 2 inputs)" for k, v in COMPOSITES.items()},
                "matrix": {"key": ROOT + "matrix/core.json.gz", "bytes": mbytes, "n": len(rows), "columns_prefix": ROOT + "matrix/cols/", "columns_bytes": cbytes, "index": ROOT + "matrix/index.json.gz"}, "objects": docs_n + 3, "pages": docs_n, "pages_bytes": bytes_out,
                "last_run": state["last_run"], "notes": src.notes, "prefix": ROOT, "read_with": "aws/shared/bank_enrich.py (factor / factors / matrix / attach)"}
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(manifest, indent=1, default=str).encode(), ContentType="application/json", CacheControl="no-cache")
    log("done: %d symbols, %d written, %d unchanged, %d failed, %.1f MB, %.0fs" % (docs_n, written, unchanged, failed, bytes_out / 1e6, time.time() - t0))
    return {"ok": failed == 0, "version": VERSION, "symbols": docs_n, "written": written, "unchanged": unchanged, "failed": failed,
            "coverage": cov, "returns": rinfo, "seconds": round(time.time() - t0, 1), "notes": src.notes}


def _put_json_gz_plain(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, default=str).encode(), ContentType="application/json", CacheControl="no-cache")


def load_polygon_short(uni, src: Sources, stop_at):
    """polygon-bank short-interest / short-volume per ticker (what the bank holds so far) -> newest row per ticker."""
    shorts, shortvol = {}, {}
    have_si = {k.rsplit("/", 1)[-1][:-len(".json.gz")] for k in _list_keys(POLY_ROOT + "short-interest/", max_keys=60000) if k.endswith(".json.gz")}
    have_sv = {k.rsplit("/", 1)[-1][:-len(".json.gz")] for k in _list_keys(POLY_ROOT + "short-volume/", max_keys=60000) if k.endswith(".json.gz")}
    want = [u for u in (_poly(s) for s in uni) if u in have_si or u in have_sv]
    src.coverage["short"] = {"banked_tickers": len(have_si), "in_universe": sum(1 for w in want if w in have_si)}
    src.coverage["short_volume"] = {"banked_tickers": len(have_sv), "in_universe": sum(1 for w in want if w in have_sv)}
    if not want:
        src.notes.append("short interest: polygon-bank per-ticker files not banked yet")
        return shorts, shortvol

    def one(tk):
        out = [None, None]
        if tk in have_si:
            doc = _get_json(POLY_ROOT + "short-interest/%s.json.gz" % tk, client=_s3())
            rows = doc.get("results") if isinstance(doc, dict) else doc
            if isinstance(rows, list) and rows:
                rows = sorted((r for r in rows if isinstance(r, dict)), key=lambda r: str(r.get("settlement_date") or ""), reverse=True)
                r = rows[0]
                out[0] = {"settlement_date": r.get("settlement_date"), "short_interest": num(r.get("short_interest")), "avg_daily_volume": num(r.get("avg_daily_volume")),
                          "days_to_cover": num(r.get("days_to_cover")), "prev_short_interest": num(rows[1].get("short_interest")) if len(rows) > 1 else None,
                          "prev_settlement_date": rows[1].get("settlement_date") if len(rows) > 1 else None}
        if tk in have_sv:
            doc = _get_json(POLY_ROOT + "short-volume/%s.json.gz" % tk, client=_s3())
            rows = doc.get("results") if isinstance(doc, dict) else doc
            if isinstance(rows, list) and rows:
                rows = sorted((r for r in rows if isinstance(r, dict)), key=lambda r: str(r.get("date") or ""), reverse=True)[:5]
                sv = [num(r.get("short_volume")) for r in rows]
                tv = [num(r.get("total_volume")) for r in rows]
                pairs = [(a, b) for a, b in zip(sv, tv) if a is not None and b]
                out[1] = {"date": rows[0].get("date"), "ratio": round(sum(a for a, _ in pairs) / sum(b for _, b in pairs), 4) if pairs else None, "sessions": len(pairs)}
        return tk, out
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = []
        for tk in want:
            if time.time() > stop_at:
                src.notes.append("short interest reads stopped by the budget at %d/%d" % (len(futs), len(want)))
                break
            futs.append(pool.submit(one, tk))
        for f in as_completed(futs):
            try:
                tk, (si, sv) = f.result()
            except Exception:  # noqa: BLE001
                continue
            if si:
                shorts[tk] = si
            if sv:
                shortvol[tk] = sv
    src.as_of["polygon-short-interest"] = max((v["settlement_date"] or "" for v in shorts.values()), default=None)
    return shorts, shortvol


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=1, default=str)[:3000])
