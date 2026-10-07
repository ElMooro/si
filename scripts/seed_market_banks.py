#!/usr/bin/env python3
"""Create the missing chart history banks for popular US ETFs (Khalid 2026-10-06: "fill in stored history for
the missing popular ETFs").

Many widely used ETFs (KRE, XBI, XOP, ITA, XRT, ...) have no bank under data/warm/tv-bars/universe/, so the chart,
the watchlist quote and every moving average for them had nothing to read. This job only CREATES banks that do
not exist; it never rewrites an existing one (refresh_market_banks.py appends new sessions afterwards).

Source: Polygon daily aggregates (adjusted=true), the same licensed provider and SSM key the approved
justhodl-polygon-daily-snapshot (APR-0001) and the market bank refresh use. Every bar is labelled
"polygon-aggs:<run date>". TradingView / Yahoo are not contacted, and the 2026-10-04 custody rule is unchanged.
Polygon's plan returns about 5 years; older sessions are then prepended from FMP's split-adjusted daily
history (labelled "fmp-eod-full:<date>") only when both providers agree on the overlapping sessions.

Bank key: data/warm/tv-bars/universe/US__<T>.json.gz with symbol "US:<T>" -- the first key the symbol
directory's warehouse lookup tries for a bare US ticker.

Usage:  python3 scripts/seed_market_banks.py [--dry-run] [--only KRE,XBI]
"""
import argparse
import gzip
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "aws", "lambdas", "justhodl-tv-bars", "source"))
from market_history_integrity import merged_document, valid_bar  # noqa: E402
from refresh_market_banks import BUCKET, NY, session_ts  # noqa: E402

# Popular US-listed ETFs: SPDR sector/industry funds, iShares/Vanguard core and industry funds, thematic, country,
# bond, commodity and leveraged funds most watched on TradingView. Existing banks are skipped automatically.
POPULAR = """
SPY IVV VOO VTI QQQ QQQM DIA IWM IWB IWD IWF IWN IWO IWR IWS IWP IJH IJR IJK IJJ IJS IJT MDY SLY RSP SPLG SCHB SCHX SCHA SCHD SCHG SCHV SCHF SCHE SCHH SCHP SCHZ
VUG VTV VB VO VXF VYM VIG VEA VWO VXUS VT VGT VHT VFH VDE VCR VDC VIS VAW VNQ VPU VOX BND BNDX VCIT VCSH VGIT VGSH VGLT EDV
XLK XLV XLF XLE XLY XLP XLI XLB XLRE XLU XLC SMH SOXX XSD IGV SKYY CLOU WCLD HACK CIBR BUG FDN ARKK ARKW ARKG ARKQ ARKF BOTZ ROBO AIQ
KRE KBE KBWB IAI KIE KCE XBI IBB XPH IHI IHF IYH XOP OIH XES AMLP MLPA XLE ITA XAR PPA JETS XHB ITB XRT XTN IYT SLX XME COPX PICK REMX LIT URA URNM TAN ICLN QCLN PBW FAN
GDX GDXJ SIL SILJ GLD IAU SLV PPLT PALL USO UNG DBC DBA DBB CPER CORN WEAT SOYB UGA BNO UUP UDN FXE FXY FXB FXA FXC FXF CYB
TLT IEF SHY SHV BIL SGOV GOVT TIP STIP SCHP AGG LQD HYG JNK USHY SJNK EMB PCY BKLN SRLN MUB HYD ANGL FALN MBB VMBS TBT TMF TMV
EFA IEFA EEM IEMG EWJ EWZ EWW EWC EWG EWU EWQ EWI EWP EWL EWN EWD EWA EWH EWS EWT EWY INDA INDY MCHI FXI KWEB ASHR CQQQ EZA TUR ECH EPU EIDO EPHE THD VNM ARGT GREK EWM QAT UAE KSA
VGK EZU HEDJ DXJ IXUS ACWI ACWX URTH IOO
MTUM QUAL USMV VLUE SIZE SPLV SPHB SPHD NOBL DGRO DVY HDV SDY FVD COWZ CALF MOAT PAVE IGF IFRA GRID
TQQQ SQQQ QLD QID SSO SDS UPRO SPXU SPXL SPXS SDOW UDOW TNA TZA SOXL SOXS TECL TECS FAS FAZ LABU LABD NUGT DUST JNUG JDST ERX ERY GUSH DRIP UCO SCO BOIL KOLD UVXY SVXY VXX VIXY SH PSQ DOG RWM
IBIT FBTC GBTC ARKB BITB BITO ETHA ETHE MSTU
BLOK BITQ DAPP JEPI JEPQ QYLD XYLD RYLD DIVO SPYD SPYG SPYV MGK MGV OEF XLG IWY IVW IVE
""".split()


def existing(s3, t):
    for ex in ("US", "NASDAQ", "NYSE", "AMEX", "ARCA", "NYSEARCA", "BATS", "CBOE"):
        key = "data/warm/tv-bars/universe/%s__%s.json.gz" % (ex, t)
        try:
            s3.head_object(Bucket=BUCKET, Key=key)
            return key
        except Exception as e:  # noqa: BLE001
            if getattr(e, "response", {}).get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise
    return None


def polygon_history(t, key):
    """All daily bars Polygon returns for t (adjusted), oldest first, as bank rows [ts, o, h, l, c, v]."""
    to = datetime.now(NY).strftime("%Y-%m-%d")
    url = ("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/1980-01-01/%s?adjusted=true&sort=asc&limit=50000&apiKey=%s"
           % (t, to, key))
    rows, seen, pages = [], set(), 0
    while url and pages < 6:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"}), timeout=90) as r:
            data = json.loads(r.read())
        for b in data.get("results") or []:
            try:
                day = datetime.fromtimestamp(b["t"] / 1000, tz=NY).strftime("%Y-%m-%d")
                row = [session_ts(day), float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"]),
                       float(b["v"]) if b.get("v") is not None else None]
            except (KeyError, TypeError, ValueError):
                continue
            if row[0] in seen or not valid_bar(row):
                continue
            seen.add(row[0])
            rows.append(row)
        nxt = data.get("next_url")
        url = (nxt + "&apiKey=" + key) if nxt else None
        pages += 1
    rows.sort(key=lambda x: x[0])
    return rows


FMP_FULL = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&from=%s&to=%s&apikey=%s"


def fmp_older(t, first_day, key):
    """Split-adjusted FMP daily bars from 1980 up to (not including) first_day, plus the 30 sessions after it
    for the overlap check. Returns (older_rows, overlap {day: close})."""
    import urllib.parse
    from datetime import date, timedelta
    end = (date.fromisoformat(first_day) + timedelta(days=45)).isoformat()
    req = urllib.request.Request(FMP_FULL % (t, "1980-01-01", end, urllib.parse.quote(key)), headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.loads(r.read())
    older, overlap, seen = [], {}, set()
    for b in data if isinstance(data, list) else []:
        day = str(b.get("date") or "")[:10]
        try:
            row = [session_ts(day), float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
                   float(b["volume"]) if b.get("volume") is not None else None]
        except (KeyError, TypeError, ValueError):
            continue
        if day >= first_day:
            overlap[day] = row[4]
        elif row[0] not in seen and valid_bar(row):
            seen.add(row[0]); older.append(row)
    older.sort(key=lambda x: x[0])
    return older, overlap


def backfill_older(s3, t, fmp_key, dry_run, rep):
    """Polygon's plan returns ~5 years. Extend a seeded bank backwards with FMP daily history -- only when the
    two providers agree on the overlapping sessions (median close gap <= 0.5%, every gap <= 2%), so a different
    split adjustment or instrument is never spliced. Existing rows are untouched (strictly older rows only)."""
    from market_history_integrity import read_bank
    bkey = "data/warm/tv-bars/universe/US__%s.json.gz" % t
    doc = read_bank(s3, BUCKET, bkey)
    if not doc or not doc.get("bars"):
        return
    srcs = doc.get("bar_sources") or []
    if not srcs or not str(srcs[0]).startswith(("polygon-aggs", "fmp-eod-full")):
        return False
    older, overlap = fmp_older(t, doc["first_date"], fmp_key)
    if not older:
        if t not in rep["older"]:
            rep["older"][t] = "no older FMP history"
        return False
    ours = {datetime.fromtimestamp(r[0], tz=NY).strftime("%Y-%m-%d"): r[4] for r in doc["bars"][:30]}
    gaps = sorted(abs(overlap[d] / ours[d] - 1) for d in ours if d in overlap and ours[d])
    if len(gaps) < 10 or gaps[len(gaps) // 2] > 0.005 or gaps[-1] > 0.02:
        rep["older"][t] = "providers disagree on overlap (%d sessions, median gap %s)" % (len(gaps), "%.4f" % gaps[len(gaps) // 2] if gaps else "n/a")
        return False
    first_ts = doc["bars"][0][0]
    older = [r for r in older if r[0] < first_ts]
    new = merged_document(doc, older, doc.get("symbol"), doc.get("tv_symbol"), "fmp-eod-full:" + datetime.now(NY).strftime("%Y-%m-%d"))
    if not dry_run:
        s3.put_object(Bucket=BUCKET, Key=bkey, Body=gzip.compress(json.dumps(new).encode()), ContentType="application/gzip", CacheControl="public, max-age=900")
    prev = rep["older"].get(t) if isinstance(rep["older"].get(t), dict) else {"added": 0}
    rep["older"][t] = {"added": prev["added"] + len(older), "first": new.get("first_date"), "median_gap": round(gaps[len(gaps) // 2], 5)}
    print("older %s: +%d bars, now from %s" % (t, len(older), new.get("first_date")))
    return len(older) >= 4000   # FMP returns at most ~5000 rows per request: ask again for the next older block


def resync_series_cache(s3, t, dry_run=False):
    """The symbol directory caches /quote and /series results built from a bank (data/series-cache/, up to 10
    days). After the bank gained older history, rebuild those entries from the bank so quotes and charts see the
    full history at once. Only entries built from this very bank are touched."""
    import hashlib
    from market_history_integrity import read_bank
    bkey = "data/warm/tv-bars/universe/US__%s.json.gz" % t
    doc = read_bank(s3, BUCKET, bkey)
    if not doc or not doc.get("bars"):
        return []
    done = []
    venues = [v + ":" + t for v in ("NYSE", "NASDAQ", "AMEX", "NYSEARCA", "ARCA", "BATS", "CBOE", "US")]
    for sid in dict.fromkeys([t, "US:" + t, "tv:US:" + t] + venues + ["tv:" + v for v in venues]):
        h = hashlib.sha1(sid.encode()).hexdigest()
        ck = "data/series-cache/%s/%s.json" % (h[:2], h)
        try:
            c = json.loads(s3.get_object(Bucket=BUCKET, Key=ck)["Body"].read())
        except Exception:  # noqa: BLE001
            continue
        if not str(c.get("source") or "").endswith(bkey) or not isinstance(c.get("obs"), list):
            continue
        if c["obs"] and c["obs"][0][0] <= doc["first_date"] and len(c["obs"]) >= doc["n"]:
            continue
        day = lambda r: datetime.fromtimestamp(r[0], tz=NY).strftime("%Y-%m-%d")
        c["obs"] = [[day(r), r[4]] for r in doc["bars"]]
        if isinstance(c.get("ohlc"), list):
            c["ohlc"] = [[day(r), r[1], r[2], r[3], r[4], r[5] if len(r) > 5 else None] for r in doc["bars"]]
        if isinstance(c.get("bar_sources"), list):
            c["bar_sources"] = list(doc.get("bar_sources") or [])
        c["n"], c["last"] = len(c["obs"]), c["obs"][-1][0]
        if "first" in c:
            c["first"] = c["obs"][0][0]
        c["as_of"] = c["last_modified"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if not dry_run:
            s3.put_object(Bucket=BUCKET, Key=ck, Body=json.dumps(c).encode(), ContentType="application/json", CacheControl="public, max-age=1800")
        done.append(sid)
    return done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--report", default="market-bank-seed.json")
    ap.add_argument("--min-bars", type=int, default=250)
    ap.add_argument("--no-older", action="store_true", help="skip the FMP backfill of history older than Polygon's window")
    a = ap.parse_args()
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    key = os.environ.get("POLYGON_API_KEY") or boto3.client("ssm", region_name="us-east-1").get_parameter(
        Name="/justhodl/polygon/api-key", WithDecryption=True)["Parameter"]["Value"]
    want = [x.strip().upper() for x in a.only.split(",") if x.strip()] or list(dict.fromkeys(POPULAR))
    today = datetime.now(NY).strftime("%Y-%m-%d")
    rep = {"started": datetime.now(timezone.utc).isoformat(timespec="seconds"), "created": {}, "present": {}, "skipped": {},
           "source": "Polygon daily aggregates (adjusted=true), APR-0001 provider", "dry_run": a.dry_run}
    for t in want:
        try:
            have = existing(s3, t)
            if have:
                rep["present"][t] = have
                continue
            rows = polygon_history(t, key)
            time.sleep(0.25)
            if len(rows) < a.min_bars:
                rep["skipped"][t] = "only %d bars from Polygon" % len(rows)
                continue
            doc = merged_document({}, rows, "US:" + t, "US:" + t, "polygon-aggs:" + today)
            bkey = "data/warm/tv-bars/universe/US__%s.json.gz" % t
            if not a.dry_run:
                s3.put_object(Bucket=BUCKET, Key=bkey, Body=gzip.compress(json.dumps(doc).encode()),
                              ContentType="application/gzip", CacheControl="public, max-age=900")
            rep["created"][t] = {"key": bkey, "n": doc.get("n"), "first": doc.get("first_date"), "last": doc.get("last_date")}
            print("created %s: %s bars %s -> %s" % (t, doc.get("n"), doc.get("first_date"), doc.get("last_date")))
        except Exception as e:  # noqa: BLE001
            rep["skipped"][t] = "error %s: %s" % (type(e).__name__, str(e)[:120])
    rep["older"] = {}
    try:
        fkey = os.environ.get("FMP_API_KEY") or boto3.client("ssm", region_name="us-east-1").get_parameter(Name="/justhodl/fmp/api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:  # noqa: BLE001
        fkey = None; rep["older_error"] = str(e)[:120]
    if fkey and not a.no_older:
        for t in want:
            try:
                for _ in range(6):
                    more = backfill_older(s3, t, fkey, a.dry_run, rep)
                    time.sleep(0.2)
                    if not more:
                        break
            except Exception as e:  # noqa: BLE001
                rep["older"][t] = "error %s: %s" % (type(e).__name__, str(e)[:120])
    rep["cache_resynced"] = {}
    for t in want:
        if t in rep["created"] or str(rep["present"].get(t, "")).endswith("US__%s.json.gz" % t):
            try:
                d = resync_series_cache(s3, t, a.dry_run)
                if d:
                    rep["cache_resynced"][t] = d
            except Exception as e:  # noqa: BLE001
                rep["cache_resynced"][t] = "error %s" % type(e).__name__
    rep["finished"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rep["n_created"], rep["n_present"], rep["n_skipped"] = len(rep["created"]), len(rep["present"]), len(rep["skipped"])
    json.dump(rep, open(a.report, "w"), indent=1)
    if not a.dry_run:
        s3.put_object(Bucket=BUCKET, Key="data/ops/market-bank-seed/latest.json", Body=json.dumps(rep).encode(),
                      ContentType="application/json", CacheControl="no-cache")
    print("created %d, already present %d, skipped %d (%s)" % (rep["n_created"], rep["n_present"], rep["n_skipped"],
          ", ".join("%s=%s" % kv for kv in list(rep["skipped"].items())[:15])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
