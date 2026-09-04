"""justhodl-bond-warroom -- the global bond heartbeat (ops 5189, 2026-09-04).

Khalid: "bonds.html: an intelligence war-room command center -- monitor all the
main bond centers worldwide so I can read the bond heartbeat at a glance:
daily Treasury-auction signals, bond volatility (a daily sell-off = huge dump
risk for stocks; big daily buying = pump for risk assets), day-over-day
percentage change in Treasury yields flashing red when huge, Japan JGBs the
same way, every ICE BofA spread, the European metrics (BTP-Bund, IT-ES ...)
that detect a eurodollar shortage. Keep what is there; add these."

Sources (all real, refreshed every run):
  * TradingView scanner (public REST, no session) for sovereign yields:
    US02Y/05Y/10Y/30Y, DE02Y/10Y/30Y, FR10Y, IT02Y/10Y, ES10Y, NL10Y, PT10Y, GR10Y,
    GB02Y/10Y, CH10Y, JP02Y/10Y/30Y, AU10Y, CA10Y, CN10Y, KR10Y, IN10Y, BR10Y,
    MX10Y and TVC:MOVE -- level, day change, week/month/quarter performance;
    every run banks the day's close so 5d/20d and z-scores grow from the bank
    (the WebSocket history route the tv-bars engine uses was refused, 400).
  * Japan MOF daily JGB curve (1Y..40Y) -- full history CSV since 1974 (authoritative).
  * Bundesbank daily 10Y Bund yield; ECB daily euro-area 10Y curve, AAA and
    all-issuers (their gap is the euro-area periphery stress proxy).
  * FRED via the fleet proxy: DGS3MO/2/5/10/30, DFII10, T10YIE, SOFR, DTB3,
    DTWEXBGS, VIXCLS and the ICE BofA OAS family (US HY/IG/AAA/BBB/BB/B/CCC,
    Euro HY, EM corp / EM HY / EM sovereign).
  * Yahoo via the fleet proxy: ^MOVE, TLT, IEF, SHY, HYG, LQD, EMB, ^TNX.
  * Fleet feeds: data/auction-desk.json (today's auction verdict + cross-asset
    read), data/usd-funding.json, data/eurodollar-plumbing.json,
    data/move-index.json, data/crisis-plumbing.json.

Every series gets: last, previous, day-over-day change (bp for yields/spreads,
% for prices/indices) and its z-score against the trailing 250 daily changes,
5-day and 20-day change, 1-year percentile of the level, a 30-point sparkline
and a flag (GREEN / AMBER / RED) from fixed shock thresholds OR |z| >= 1.5/2.5.

Verdicts (deterministic, explained in words):
  * equity_risk  -- from TLT %, US10Y bp, MOVE and HY OAS: DUMP RISK when
    bonds sell off hard; PUMP SETUP when bonds are bought hard with credit calm;
    FLIGHT TO SAFETY when bonds are bought while credit widens.
  * eurodollar_shortage -- BTP-Bund / periphery widening + dollar up + EM and
    Euro HY spreads widening + the fleet's eurodollar-plumbing composite.
  * heartbeat -- 0..100 from the share of AMBER/RED across US rates, volatility,
    Japan, Europe, rest-of-world, credit and funding, with the loudest signals
    named in the headline.

Output: data/bond-warroom.json (+ data/warm/bond-warroom/tv.json.gz cache).
"""
import gzip
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.3.1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/bond-warroom.json"
TV_KEY = "data/warm/bond-warroom/tv-bank.json.gz"
PAR_KEY = "data/warm/treasury-par/curve.json.gz"
OY_PREFIX = "data/warm/official-yields/"                 # the warehouse lane this engine reads first and banks into (schema of justhodl-repo _bank_put)
OY_STATE = OY_PREFIX + "_state.json"      # fleet Treasury par-curve bank (13 tenors since 1990, symdir ustbank 21:30 UTC)
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = "justhodl-bond-warroom/" + VERSION
s3 = boto3.client("s3", region_name="us-east-1")

TV_SYMBOLS = {
    "US02Y": ("TVC:US02Y", "US 2Y", "us", "yield"), "US05Y": ("TVC:US05Y", "US 5Y", "us", "yield"),
    "US10Y": ("TVC:US10Y", "US 10Y", "us", "yield"), "US30Y": ("TVC:US30Y", "US 30Y", "us", "yield"),
    "DE02Y": ("TVC:DE02Y", "Germany 2Y (Schatz)", "europe", "yield"), "DE10Y": ("TVC:DE10Y", "Germany 10Y (Bund)", "europe", "yield"),
    "DE30Y": ("TVC:DE30Y", "Germany 30Y", "europe", "yield"), "FR10Y": ("TVC:FR10Y", "France 10Y (OAT)", "europe", "yield"),
    "IT02Y": ("TVC:IT02Y", "Italy 2Y", "europe", "yield"), "IT10Y": ("TVC:IT10Y", "Italy 10Y (BTP)", "europe", "yield"),
    "ES10Y": ("TVC:ES10Y", "Spain 10Y (Bono)", "europe", "yield"), "NL10Y": ("TVC:NL10Y", "Netherlands 10Y", "europe", "yield"),
    "PT10Y": ("TVC:PT10Y", "Portugal 10Y", "europe", "yield"), "GR10Y": ("TVC:GR10Y", "Greece 10Y", "europe", "yield"),
    "GB02Y": ("TVC:GB02Y", "UK 2Y", "europe", "yield"), "GB10Y": ("TVC:GB10Y", "UK 10Y (Gilt)", "europe", "yield"),
    "CH10Y": ("TVC:CH10Y", "Switzerland 10Y", "europe", "yield"),
    "JP02Y": ("TVC:JP02Y", "Japan 2Y (JGB)", "japan", "yield"), "JP10Y": ("TVC:JP10Y", "Japan 10Y (JGB)", "japan", "yield"),
    "JP30Y": ("TVC:JP30Y", "Japan 30Y (JGB)", "japan", "yield"),
    "AU10Y": ("TVC:AU10Y", "Australia 10Y", "world", "yield"), "CA10Y": ("TVC:CA10Y", "Canada 10Y", "world", "yield"),
    "CN10Y": ("TVC:CN10Y", "China 10Y", "world", "yield"), "KR10Y": ("TVC:KR10Y", "Korea 10Y", "world", "yield"),
    "IN10Y": ("TVC:IN10Y", "India 10Y", "world", "yield"), "BR10Y": ("TVC:BR10Y", "Brazil 10Y", "world", "yield"),
    "MX10Y": ("TVC:MX10Y", "Mexico 10Y", "world", "yield"),
    "MOVE_TV": ("TVC:MOVE", "MOVE (TV)", "vol", "index"),
}
FRED_SERIES = {
    "DGS3MO": ("US 3M bill", "us", "yield"), "DGS2": ("US 2Y (FRED)", "us", "yield"), "DGS5": ("US 5Y (FRED)", "us", "yield"),
    "DGS10": ("US 10Y (FRED)", "us", "yield"), "DGS30": ("US 30Y (FRED)", "us", "yield"),
    "DFII10": ("US 10Y real (TIPS)", "us", "yield"), "T10YIE": ("10Y breakeven inflation", "us", "yield"),
    "SOFR": ("SOFR", "funding", "yield"), "DTB3": ("3M T-bill (secondary)", "funding", "yield"),
    "DTWEXBGS": ("Broad dollar index", "funding", "index"), "VIXCLS": ("VIX", "vol", "index"),
    "BAMLH0A0HYM2": ("US High Yield OAS", "credit", "spread"), "BAMLC0A0CM": ("US IG Corporate OAS", "credit", "spread"),
    "BAMLC0A1CAAA": ("US AAA OAS", "credit", "spread"), "BAMLC0A4CBBB": ("US BBB OAS", "credit", "spread"),
    "BAMLH0A1HYBB": ("US BB OAS", "credit", "spread"), "BAMLH0A2HYB": ("US B OAS", "credit", "spread"),
    "BAMLH0A3HYC": ("US CCC & lower OAS", "credit", "spread"), "BAMLHE00EHYIOAS": ("Euro High Yield OAS", "credit", "spread"),
    "BAMLEMCBPIOAS": ("EM Corporate OAS", "credit", "spread"), "BAMLEMHBHYCRPIOAS": ("EM High Yield OAS", "credit", "spread"),
    "BAMLEMPBPUBSICRPIOAS": ("EM Sovereign OAS", "credit", "spread"), "BAMLEMIBHGCRPIOAS": ("EM IG OAS", "credit", "spread"),
}
YAHOO = {"^MOVE": ("MOVE index", "vol", "index"), "TLT": ("TLT 20Y+ Treasury ETF", "vol", "price"), "IEF": ("IEF 7-10Y ETF", "vol", "price"),
         "SHY": ("SHY 1-3Y ETF", "vol", "price"), "HYG": ("HYG high-yield ETF", "vol", "price"), "LQD": ("LQD IG ETF", "vol", "price"),
         "EMB": ("EMB EM bond ETF", "vol", "price"), "^TNX": ("US 10Y (Yahoo)", "us", "yield")}

# shock thresholds: (RED, AMBER) in the series' own DoD unit (bp for yields/spreads, % for prices, points for indices)
THRESH = {"yield_us": (10, 6), "yield_dm": (10, 6), "yield_jp": (7, 4), "yield_em": (15, 9), "spread_periph": (10, 6),
          "oas_hy": (20, 12), "oas_ig": (7, 4), "oas_ccc": (35, 20), "oas_em": (15, 10), "price_bond": (1.5, 0.9), "move": (10, 6), "index": (999, 999), "vix": (5, 3)}


def _now():
    return datetime.now(timezone.utc)


def _iso():
    return _now().isoformat(timespec="seconds")


def _get(url, timeout=30, binary=False):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read() if binary else json.loads(r.read())


def _s3_json(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def _put_json(key, obj, gz=False):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    kw = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json", "CacheControl": "no-cache"}
    if gz:
        body = gzip.compress(body)
        kw["ContentEncoding"] = "gzip"
    s3.put_object(Body=body, **kw)


def _f(v):
    try:
        if v is None or v == "" or v == "null" or v == ".":
            return None
        return float(str(v).replace(",", ""))
    except Exception:
        return None


# ─────────────────────────── fetchers ────────────────────────────────
def tv_scan(cache):
    """TradingView scanner: current level + day change + W/1M/3M performance for every symbol; bank the close by date."""
    bank = dict(cache.get("bank") or {})
    tickers = [v[0] for v in TV_SYMBOLS.values()]
    body = json.dumps({"symbols": {"tickers": tickers, "query": {"types": []}},
                       "columns": ["close", "change", "change_abs", "Perf.W", "Perf.1M", "Perf.3M", "update_mode"]}).encode()
    req = urllib.request.Request("https://scanner.tradingview.com/global/scan", data=body,
                                 headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/128 Safari/537.36",
                                          "Content-Type": "application/json", "Origin": "https://www.tradingview.com", "Referer": "https://www.tradingview.com/"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rows = json.loads(r.read()).get("data") or []
    today = _now().date().isoformat()
    live = {}
    by_ticker = {k: v[0] for k, v in TV_SYMBOLS.items()}
    inv = {v: k for k, v in by_ticker.items()}
    for row in rows:
        key = inv.get(row.get("s"))
        d = row.get("d") or []
        if not key or len(d) < 3 or d[0] is None:
            continue
        close, chg_pct, chg_abs = _f(d[0]), _f(d[1]), _f(d[2])
        live[key] = {"close": close, "change_pct": chg_pct, "change_abs": chg_abs, "perf_w": _f(d[3]) if len(d) > 3 else None,
                     "perf_1m": _f(d[4]) if len(d) > 4 else None, "perf_3m": _f(d[5]) if len(d) > 5 else None, "mode": d[6] if len(d) > 6 else None}
        b = bank.setdefault(key, {})
        b[today] = close
        if chg_abs is not None and len(b) < 2:
            # seed yesterday from the scanner's own day change so DoD exists on day one
            yday = (_now().date() - timedelta(days=1)).isoformat()
            b.setdefault(yday, round(close - chg_abs, 6))
        if len(b) > 1500:
            for k in sorted(b)[:-1500]:
                b.pop(k, None)
    return live, bank


def series_from_bank(bank, key):
    b = bank.get(key) or {}
    ds = sorted(b)
    return {"dates": ds, "closes": [b[d] for d in ds]}


def bundesbank_10y():
    return bundesbank("WT1010")


def bundesbank(code="WT1010"):
    """Daily Bund yield from the Bundesbank API (sdmx-json). BBSSY codes: WT0202 2Y, WT1010 10Y, WT3030 30Y."""
    try:
        body = _get("https://api.statistiken.bundesbank.de/rest/data/BBSSY/D.REN.EUR.A630.000000%s.A?lastNObservations=1500&format=sdmx_json" % code, timeout=40)
        ds_struct = body["data"]["structure"]["dimensions"]["observation"][0]["values"]
        obs = list(body["data"]["dataSets"][0]["series"].values())[0]["observations"]
        ds, cs = [], []
        for idx, val in obs.items():
            v = _f(val[0])
            if v is None:
                continue
            ds.append(ds_struct[int(idx)]["id"][:10])
            cs.append(v)
        order = sorted(range(len(ds)), key=lambda i: ds[i])
        return {"dates": [ds[i] for i in order], "closes": [cs[i] for i in order]}
    except Exception as e:
        print("[warroom] bundesbank:", str(e)[:120])
        return None


def ecb_yc(kind="G_N_A"):
    """ECB euro-area 10Y spot yield, daily: G_N_A = AAA-rated governments, G_N_C = all governments."""
    try:
        raw = _get("https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.%s.SV_C_YM.SR_10Y?lastNObservations=1500&format=csvdata" % kind, timeout=40, binary=True)
        lines = raw.decode("utf-8", "ignore").splitlines()
        hdr = lines[0].split(",")
        ti, vi = hdr.index("TIME_PERIOD"), hdr.index("OBS_VALUE")
        ds, cs = [], []
        for ln in lines[1:]:
            parts = ln.split(",")
            if len(parts) > max(ti, vi) and _f(parts[vi]) is not None:
                ds.append(parts[ti][:10])
                cs.append(_f(parts[vi]))
        return {"dates": ds, "closes": cs}
    except Exception as e:
        print("[warroom] ecb yc:", str(e)[:120])
        return None


# ─────────────────────────── official daily histories (v1.2) ─────────────────
def _order(ds, cs):
    order = sorted(range(len(ds)), key=lambda i: ds[i])
    return {"dates": [ds[i] for i in order], "closes": [cs[i] for i in order]}


def treasury_par():
    """US par curve: the fleet bank (1990->) plus this month's treasury.gov XML so today's close lands before the 21:30 UTC bank run."""
    import xml.etree.ElementTree as ET
    rows = dict((_s3_json(PAR_KEY) or {}).get("rows") or {})
    try:
        ym = _now().strftime("%Y%m")
        raw = _get("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value_month=%s" % ym, timeout=40, binary=True)
        root = ET.fromstring(raw)
        cols = {"2Y": "BC_2YEAR", "5Y": "BC_5YEAR", "7Y": "BC_7YEAR", "10Y": "BC_10YEAR", "20Y": "BC_20YEAR", "30Y": "BC_30YEAR", "3M": "BC_3MONTH", "1Y": "BC_1YEAR"}
        for el in root.iter():
            if el.tag.split("}")[-1] != "properties":
                continue
            row = {c.tag.split("}")[-1]: (c.text or "").strip() for c in el}
            d = (row.get("NEW_DATE") or "")[:10]
            if d:
                rows[d] = dict(rows.get(d) or {}) | {t: _f(row.get(c)) for t, c in cols.items() if _f(row.get(c)) is not None}
    except Exception as e:
        print("[warroom] treasury month xml:", str(e)[:120])
    out = {}
    for tenor in ("3M", "1Y", "2Y", "5Y", "7Y", "10Y", "20Y", "30Y"):
        ds = [d for d in sorted(rows) if (rows[d] or {}).get(tenor) is not None]
        if len(ds) > 40:
            out[tenor] = {"dates": ds[-1500:], "closes": [rows[d][tenor] for d in ds[-1500:]]}
    return out


def bundesbank_bbsis(code="R10XX"):
    """Bundesbank BBSIS daily listed Federal securities yields by residual maturity (R02XX 2Y, R10XX 10Y, R30XX 30Y);
    CSV is ';' separated with ',' decimals and a metadata header (verified ops 5190)."""
    try:
        raw = _get("https://api.statistiken.bundesbank.de/rest/data/BBSIS/D.I.ZAR.ZI.EUR.S1311.B.A604.%s.R.A.A._Z._Z.A?format=csv&lastNObservations=1500" % code, timeout=40, binary=True)
        ds, cs = [], []
        for ln in raw.decode("utf-8", "ignore").splitlines():
            parts = ln.split(";")
            if len(parts) < 2 or len(parts[0]) != 10 or parts[0][4] != "-":
                continue
            v = _f(parts[1].replace(",", "."))
            if v is not None:
                ds.append(parts[0])
                cs.append(v)
        return _order(ds, cs) if cs else None
    except Exception as e:
        print("[warroom] bundesbank bbsis:", str(e)[:120])
        return None


def boe_gilts():
    """Bank of England nominal par gilt yields: IUDSNPY 5y, IUDMNZC 10y, IUDLNPY 20y (daily CSV)."""
    try:
        raw = _get("https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp?csv.x=yes&Datefrom=01/Jan/2019&Dateto=now&SeriesCodes=IUDSNPY,IUDMNZC,IUDLNPY&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N", timeout=60, binary=True)
        import csv as _csv
        import io as _io
        rows = list(_csv.reader(_io.StringIO(raw.decode("utf-8", "ignore"))))
        hdr = rows[0]
        out = {}
        for code, key in (("IUDSNPY", "GB05Y"), ("IUDMNZC", "GB10Y"), ("IUDLNPY", "GB20Y")):
            if code not in hdr:
                continue
            ci = hdr.index(code)
            ds, cs = [], []
            for r in rows[1:]:
                if len(r) <= ci or _f(r[ci]) is None:
                    continue
                try:
                    d = datetime.strptime(r[0], "%d %b %Y").date().isoformat()
                except Exception:
                    continue
                ds.append(d)
                cs.append(_f(r[ci]))
            if len(cs) > 40:
                out[key] = _order(ds, cs)
        return out
    except Exception as e:
        print("[warroom] boe:", str(e)[:120])
        return {}


def boc_valet():
    """Bank of Canada Valet benchmark yields: 2Y, 5Y, 10Y, long."""
    try:
        body = _get("https://www.bankofcanada.ca/valet/observations/BD.CDN.2YR.DQ.YLD,BD.CDN.5YR.DQ.YLD,BD.CDN.10YR.DQ.YLD,BD.CDN.LONG.DQ.YLD/json?recent=1500", timeout=60)
        out = {}
        for code, key in (("BD.CDN.2YR.DQ.YLD", "CA02Y"), ("BD.CDN.5YR.DQ.YLD", "CA05Y"), ("BD.CDN.10YR.DQ.YLD", "CA10Y"), ("BD.CDN.LONG.DQ.YLD", "CA30Y")):
            ds, cs = [], []
            for o in body.get("observations") or []:
                v = _f(((o.get(code) or {}).get("v")))
                if v is not None and o.get("d"):
                    ds.append(o["d"][:10])
                    cs.append(v)
            if len(cs) > 40:
                out[key] = _order(ds, cs)
        return out
    except Exception as e:
        print("[warroom] boc:", str(e)[:120])
        return {}


def rba_f2():
    """RBA table F2 daily Australian Government bond yields: FCMYGBAG2D 2Y, ...3D, ...5D, ...10D."""
    try:
        raw = _get("https://www.rba.gov.au/statistics/tables/csv/f2-data.csv", timeout=60, binary=True)
        import csv as _csv
        import io as _io
        rows = list(_csv.reader(_io.StringIO(raw.decode("utf-8", "ignore"))))
        sid = next(r for r in rows if r and r[0] == "Series ID")
        out = {}
        for code, key in (("FCMYGBAG2D", "AU02Y"), ("FCMYGBAG3D", "AU03Y"), ("FCMYGBAG5D", "AU05Y"), ("FCMYGBAG10D", "AU10Y")):
            if code not in sid:
                continue
            ci = sid.index(code)
            ds, cs = [], []
            for r in rows:
                if len(r) <= ci or _f(r[ci]) is None:
                    continue
                d = None
                for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        d = datetime.strptime(r[0], fmt).date().isoformat()
                        break
                    except Exception:
                        pass
                if d:
                    ds.append(d)
                    cs.append(_f(r[ci]))
            if len(cs) > 40:
                out[key] = _order(ds[-1500:], cs[-1500:])
        return out
    except Exception as e:
        print("[warroom] rba:", str(e)[:120])
        return {}


def snb_curve():
    """SNB rendoblid CSV cube ("Date";"D0";"Value", daily since 1988): spot yields on Swiss Confederation bonds, 2J / 5J / 10J."""
    try:
        raw = _get("https://data.snb.ch/api/cube/rendoblid/data/csv/en", timeout=60, binary=True)
        want = {"2J": "CH02Y", "5J": "CH05Y", "10J": "CH10Y"}
        acc = {k: ([], []) for k in want.values()}
        since = (_now() - timedelta(days=2200)).date().isoformat()
        for ln in raw.decode("utf-8-sig", "ignore").splitlines():
            parts = [x.strip().strip('"') for x in ln.split(";")]
            if len(parts) < 3 or len(parts[0]) != 10 or parts[0] < since or parts[1] not in want:
                continue
            v = _f(parts[2])
            if v is not None:
                acc[want[parts[1]]][0].append(parts[0])
                acc[want[parts[1]]][1].append(v)
        return {k: _order(ds, cs) for k, (ds, cs) in acc.items() if len(cs) > 40}
    except Exception as e:
        print("[warroom] snb:", str(e)[:120])
        return {}

OFFICIAL = {  # key: (label, group, thresh kind)
    "US02Y": ("US 2Y", "us", "yield_us"), "US05Y": ("US 5Y", "us", "yield_us"), "US07Y": ("US 7Y", "us", "yield_us"), "US10Y": ("US 10Y", "us", "yield_us"),
    "US20Y": ("US 20Y", "us", "yield_us"), "US30Y": ("US 30Y", "us", "yield_us"),
    "DE02Y": ("Germany 2Y (Schatz)", "europe", "yield_dm"), "DE10Y": ("Germany 10Y (Bund)", "europe", "yield_dm"), "DE30Y": ("Germany 30Y", "europe", "yield_dm"),
    "GB05Y": ("UK 5Y gilt", "europe", "yield_dm"), "GB10Y": ("UK 10Y (Gilt)", "europe", "yield_dm"), "GB20Y": ("UK 20Y gilt", "europe", "yield_dm"),
    "CH02Y": ("Switzerland 2Y", "europe", "yield_dm"), "CH05Y": ("Switzerland 5Y", "europe", "yield_dm"), "CH10Y": ("Switzerland 10Y", "europe", "yield_dm"),
    "CA02Y": ("Canada 2Y", "world", "yield_dm"), "CA05Y": ("Canada 5Y", "world", "yield_dm"), "CA10Y": ("Canada 10Y", "world", "yield_dm"), "CA30Y": ("Canada long bond", "world", "yield_dm"),
    "AU02Y": ("Australia 2Y", "world", "yield_dm"), "AU03Y": ("Australia 3Y", "world", "yield_dm"), "AU05Y": ("Australia 5Y", "world", "yield_dm"), "AU10Y": ("Australia 10Y", "world", "yield_dm"),
}


OY_SLUGS = {  # key -> (slug, id, title, source)   warehouse lane data/warm/official-yields/{slug}.json
    "DE02Y": ("de-2y-bbk", "DE02Y_BBK", "Germany 2Y Bund yield (daily, Bundesbank)", "Bundesbank BBSIS"),
    "DE10Y": ("de-10y-bbk", "DE10Y_BBK", "Germany 10Y Bund yield (daily, Bundesbank Svensson)", "Bundesbank BBSIS"),
    "DE30Y": ("de-30y-bbk", "DE30Y_BBK", "Germany 30Y Bund yield (daily, Bundesbank)", "Bundesbank BBSIS"),
    "GB05Y": ("gb-5y-boe", "GB05Y_BOE", "UK 5Y nominal par gilt yield (daily, Bank of England IUDSNPY)", "Bank of England IADB"),
    "GB10Y": ("gb-10y-boe", "GB10Y_BOE", "UK 10Y nominal par gilt yield (daily, Bank of England IUDMNZC)", "Bank of England IADB"),
    "GB20Y": ("gb-20y-boe", "GB20Y_BOE", "UK 20Y nominal par gilt yield (daily, Bank of England IUDLNPY)", "Bank of England IADB"),
    "CA02Y": ("ca-2y-boc", "CA02Y_BOC", "Canada 2Y benchmark bond yield (daily, Bank of Canada)", "Bank of Canada Valet"),
    "CA05Y": ("ca-5y-boc", "CA05Y_BOC", "Canada 5Y benchmark bond yield (daily, Bank of Canada)", "Bank of Canada Valet"),
    "CA10Y": ("ca-10y-boc", "CA10Y_BOC", "Canada 10Y benchmark bond yield (daily, Bank of Canada)", "Bank of Canada Valet"),
    "CA30Y": ("ca-long-boc", "CALONG_BOC", "Canada long-term benchmark bond yield (daily, Bank of Canada)", "Bank of Canada Valet"),
    "AU02Y": ("au-2y-rba", "AU02Y_RBA", "Australia 2Y government bond yield (daily, RBA F2)", "RBA F2"),
    "AU03Y": ("au-3y-rba", "AU03Y_RBA", "Australia 3Y government bond yield (daily, RBA F2)", "RBA F2"),
    "AU05Y": ("au-5y-rba", "AU05Y_RBA", "Australia 5Y government bond yield (daily, RBA F2)", "RBA F2"),
    "AU10Y": ("au-10y-rba", "AU10Y_RBA", "Australia 10Y government bond yield (daily, RBA F2)", "RBA F2"),
    "JP02Y": ("jp-2y-mof", "JP02Y_MOF", "Japan 2Y JGB yield (daily, Ministry of Finance)", "Japan MOF"),
    "JP10Y": ("jp-10y-mof", "JP10Y_MOF", "Japan 10Y JGB yield (daily, Ministry of Finance)", "Japan MOF"),
    "JP30Y": ("jp-30y-mof", "JP30Y_MOF", "Japan 30Y JGB yield (daily, Ministry of Finance)", "Japan MOF"),
    "EA_AAA10Y": ("ea-aaa-10y-ecb", "EA_AAA10Y_ECB", "Euro area AAA governments 10Y spot yield (daily, ECB)", "ECB YC dataset"),
    "EA_ALL10Y": ("ea-all-10y-ecb", "EA_ALL10Y_ECB", "Euro area all governments 10Y spot yield (daily, ECB)", "ECB YC dataset"),
}
TV_SLUG = {"FR10Y": "fr-10y", "IT02Y": "it-2y", "IT10Y": "it-10y", "ES10Y": "es-10y", "NL10Y": "nl-10y", "PT10Y": "pt-10y", "GR10Y": "gr-10y", "GB02Y": "gb-2y", "CH10Y": "ch-10y",
           "CN10Y": "cn-10y", "KR10Y": "kr-10y", "IN10Y": "in-10y", "BR10Y": "br-10y", "MX10Y": "mx-10y"}   # scanner-only markets banked as {slug}-tv


def official_histories():
    """Warehouse-first: read data/warm/official-yields/{slug} (and the older warehouse lanes: fred/boe/treasury-par),
    refresh from the official origin, merge (origin wins on the same date), bank back. -> {key: series}, {key: source}, banked"""
    out, srcs, banked = {}, {}, {}
    def wh_first(key):
        slug = OY_SLUGS[key][0]
        ser = read_bank(slug)
        if ser:
            banked[slug] = ser
        return ser
    def us():
        par = treasury_par()
        return {("US%02dY" % int(t[:-1])): ser for t, ser in par.items() if t.endswith("Y") and t != "1Y"}, "warehouse:treasury-par + treasury.gov"
    def de():
        r = {}
        for code, key in (("R02XX", "DE02Y"), ("R10XX", "DE10Y"), ("R30XX", "DE30Y")):
            r[key] = merge_series(wh_first(key), bundesbank_bbsis(code))
        return {k: v for k, v in r.items() if v and len(v["closes"]) > 40}, "warehouse:official-yields + Bundesbank"
    def gb():
        wh10 = wh_series("boe:IUDMNZC")
        live = boe_gilts()
        r = {k: merge_series(wh_first(k), wh10 if k == "GB10Y" else None, live.get(k)) for k in ("GB05Y", "GB10Y", "GB20Y")}
        return {k: v for k, v in r.items() if v and len(v["closes"]) > 40}, "warehouse:boe + Bank of England"
    def ca():
        live = boc_valet()
        r = {k: merge_series(wh_first(k), live.get(k)) for k in ("CA02Y", "CA05Y", "CA10Y", "CA30Y")}
        return {k: v for k, v in r.items() if v and len(v["closes"]) > 40}, "warehouse:official-yields + Bank of Canada"
    def au():
        live = rba_f2()
        r = {k: merge_series(wh_first(k), live.get(k)) for k in ("AU02Y", "AU03Y", "AU05Y", "AU10Y")}
        return {k: v for k, v in r.items() if v and len(v["closes"]) > 40}, "warehouse:official-yields + RBA"
    jobs = [us, de, gb, ca, au]
    with ThreadPoolExecutor(max_workers=5) as ex:
        for res in ex.map(lambda f: f(), jobs):
            r, name = res
            for k, ser in r.items():
                out[k] = ser
                srcs[k] = name
    for key, ser in out.items():
        if key in OY_SLUGS:
            slug, sid, title, src = OY_SLUGS[key]
            bank_yield(slug, sid, title, src, ser, banked)
    return out, srcs, banked


def with_live(ser, live, label):
    """Append the scanner's same-day level when it is newer than the official close (intraday / same-day before publication)."""
    if not live or live.get("close") is None or not ser or not ser.get("dates"):
        return ser, label
    d0 = _now().date().isoformat()
    if ser["dates"][-1] < d0:
        return {"dates": ser["dates"] + [d0], "closes": ser["closes"] + [live["close"]]}, label + " + TradingView live"
    return ser, label


# ─────────────────────────── warehouse first (v1.3) ─────────────────────────
def wh_series(sid):
    """Full history from OUR warehouse through the symdir resolver (/series?id=), never a third party.
    Returns {dates, closes} or None. fred:, ustpar:, boe:, official-yields:, bare Yahoo/TV tickers (tv-bars bank)."""
    try:
        body = _get("%s/series?id=%s" % (PROXY, urllib.parse.quote(sid, safe="")), timeout=45)
        obs = body.get("observations") or body.get("obs") or []
        ds, cs = [], []
        for o in obs:
            d, v = (o[0], o[1]) if isinstance(o, (list, tuple)) else (o.get("date"), o.get("value"))
            v = _f(v)
            if d and v is not None:
                ds.append(str(d)[:10])
                cs.append(v)
        return _order(ds, cs) if len(cs) > 3 else None
    except Exception as e:
        print("[warroom] warehouse %s: %s" % (sid, str(e)[:100]))
        return None


def merge_series(*sers):
    """Union by date; later arguments win on the same date (origin tail beats banked history)."""
    d = {}
    for ser in sers:
        if ser:
            d.update(zip(ser["dates"], ser["closes"]))
    ds = sorted(d)
    return {"dates": ds, "closes": [d[k] for k in ds]} if len(ds) > 3 else None


def bank_yield(slug, sid, title, src, ser, banked):
    """Write data/warm/official-yields/{slug}.json in the lane's schema when the series carries new points."""
    if not ser:
        return
    key = OY_PREFIX + slug + ".json"
    prev = banked.get(slug)
    if prev and prev.get("dates") and prev["dates"][-1] >= ser["dates"][-1] and len(prev["dates"]) >= len(ser["dates"]):
        return
    doc = {"id": sid, "title": title, "source": src, "banked_at": _iso(), "banked_by": "justhodl-bond-warroom/" + VERSION,
           "observations": [{"date": d, "value": v} for d, v in zip(ser["dates"], ser["closes"])]}
    _put_json(key, doc)
    banked[slug] = ser


def read_bank(slug):
    j = _s3_json(OY_PREFIX + slug + ".json")
    if not j:
        return None
    obs = j.get("observations") or []
    return _order([o["date"] for o in obs if o.get("value") is not None], [o["value"] for o in obs if o.get("value") is not None]) if obs else None


def fred_fetch(series, obs=900):
    body = _get("%s/fred?series=%s&obs=%d" % (PROXY, series, obs), timeout=40)
    ds, cs = [], []
    for b in body.get("bars") or []:
        d = b.get("date") or (datetime.fromtimestamp(int(b["time"]), tz=timezone.utc).date().isoformat() if b.get("time") else None)
        v = _f(b.get("value"))
        if d and v is not None:
            ds.append(d[:10])
            cs.append(v)
    order = sorted(range(len(ds)), key=lambda i: ds[i])
    return {"dates": [ds[i] for i in order], "closes": [cs[i] for i in order]}


def yahoo_fetch(symbol, rng="2y"):
    body = _get("%s/yf-ohlc?symbol=%s&range=%s&interval=1d" % (PROXY, urllib.parse.quote(symbol), rng), timeout=40)
    ds, cs = [], []
    for b in body.get("bars") or []:
        t, c = b.get("time"), _f(b.get("close"))
        if t is None or c is None:
            continue
        ds.append(datetime.fromtimestamp(int(t), tz=timezone.utc).date().isoformat())
        cs.append(c)
    return {"dates": ds, "closes": cs}


def mof_jgb():
    """Japan MOF daily JGB curve -- full history CSV (since 1974) + the current month file."""
    try:
        raw = _get("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv", timeout=40, binary=True)
        try:
            raw += b"\r\n" + _get("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv", timeout=30, binary=True)
        except Exception:
            pass
        text = raw.decode("shift_jis", "ignore")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        hdr_i = next(i for i, ln in enumerate(lines) if ln.startswith("Date,"))
        cols = [c.strip() for c in lines[hdr_i].split(",")]
        rows = []
        for ln in lines[hdr_i + 1:]:
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) < 3:
                continue
            try:
                d = datetime.strptime(parts[0], "%Y/%m/%d").date().isoformat()
            except Exception:
                continue
            rows.append({"date": d, **{cols[i]: _f(parts[i]) for i in range(1, min(len(cols), len(parts)))}})
        seen = {}
        for r in rows:
            seen[r["date"]] = r
        rows = [seen[d] for d in sorted(seen)]
        return {"tenors": cols[1:], "rows": rows}
    except Exception as e:
        return {"error": str(e)[:120], "rows": []}


# ─────────────────────────── metrics ─────────────────────────────────
def metrics(ser, kind, unit_dod, thresh_key, label, group, source):
    """kind: yield | spread | price | index. unit_dod: 'bp' (x100 of a % level), 'pct' (relative %), 'pts'."""
    ds, cs = ser.get("dates") or [], ser.get("closes") or []
    if len(cs) < 2:
        return None
    last, prev = cs[-1], cs[-2]
    def chg(i):
        if len(cs) <= i:
            return None
        base = cs[-1 - i]
        if unit_dod == "bp":
            return (last - base) * 100
        if unit_dod == "pct":
            return (last / base - 1) * 100 if base else None
        return last - base
    dod, d5, d20 = chg(1), chg(5), chg(20)
    dods = []
    for i in range(1, min(len(cs), 251)):
        a, b = cs[-i], cs[-i - 1]
        dods.append((a - b) * 100 if unit_dod == "bp" else ((a / b - 1) * 100 if (unit_dod == "pct" and b) else a - b))
    dods = [x for x in dods if x is not None]
    z = None
    if len(dods) >= 40 and dod is not None:
        sd = statistics.pstdev(dods)
        z = round((dod - statistics.fmean(dods)) / sd, 2) if sd > 1e-9 else 0.0
    window = cs[-250:]
    pct = round(100.0 * sum(1 for x in window if x <= last) / len(window)) if window else None
    red, amber = THRESH.get(thresh_key, (999, 999))
    a = abs(dod) if dod is not None else 0
    flag = "RED" if (a >= red or (z is not None and abs(z) >= 2.5)) else "AMBER" if (a >= amber or (z is not None and abs(z) >= 1.5)) else "GREEN"
    rel = ((last / prev - 1) * 100) if (kind == "yield" and prev and abs(prev) >= 0.5) else None   # DoD % of the yield itself
    if rel is not None and abs(rel) >= 5:
        flag = "RED"
    elif rel is not None and abs(rel) >= 3 and flag == "GREEN":
        flag = "AMBER"
    if thresh_key == "move" and last >= 120:
        flag = "RED"
    elif thresh_key == "move" and last >= 100 and flag == "GREEN":
        flag = "AMBER"
    return {"label": label, "group": group, "kind": kind, "unit": unit_dod, "source": source, "last": round(last, 4), "prev": round(prev, 4), "asof": ds[-1],
            "dod": round(dod, 2) if dod is not None else None, "dod_pct": round((last / prev - 1) * 100, 2) if prev else None,
            "d5": round(d5, 2) if d5 is not None else None, "d20": round(d20, 2) if d20 is not None else None, "z": z, "pct1y": pct,
            "flag": flag, "spark": [round(x, 4) for x in cs[-30:]], "spark_dates": ds[-30:], "history_days": len(cs), "z_ready": len(dods) >= 40}


def spread_series(a, b):
    """Difference of two yield series aligned on dates (bp-level kept in %)."""
    if not a or not b:
        return None
    mb = dict(zip(b["dates"], b["closes"]))
    ds, cs = [], []
    for d, v in zip(a["dates"], a["closes"]):
        if d in mb:
            ds.append(d)
            cs.append(v - mb[d])
    return {"dates": ds, "closes": cs} if len(cs) >= 2 else None


def word_dod(m):
    if not m or m.get("dod") is None:
        return "no change data"
    u = "bp" if m["unit"] == "bp" else ("%" if m["unit"] == "pct" else "pts")
    return "%+.1f%s%s" % (m["dod"], u, (" (z %+.1f)" % m["z"]) if m.get("z") is not None else "")


# ─────────────────────────── verdicts ────────────────────────────────
def equity_risk(m):
    tlt, us10, move, hy = m.get("TLT"), m.get("US10Y") or m.get("DGS10"), m.get("^MOVE") or m.get("MOVE_TV"), m.get("BAMLH0A0HYM2")
    tlt_d = tlt["dod"] if tlt else None
    y_d = us10["dod"] if us10 else None
    mv_d = move["dod"] if move else None
    hy_d = hy["dod"] if hy else None
    sell = (tlt_d is not None and tlt_d <= -1.5) or (y_d is not None and y_d >= 10) or (mv_d is not None and mv_d >= 10 and (tlt_d or 0) < 0)
    soft_sell = (tlt_d is not None and tlt_d <= -0.9) or (y_d is not None and y_d >= 6)
    buy = (tlt_d is not None and tlt_d >= 1.5) or (y_d is not None and y_d <= -10)
    soft_buy = (tlt_d is not None and tlt_d >= 0.9) or (y_d is not None and y_d <= -6)
    credit_widening = hy_d is not None and hy_d >= 8
    score = 50
    for v, w in ((tlt_d, -12), (y_d, 1.4), (mv_d, 0.9), (hy_d, 0.6)):
        if v is not None:
            score += v * w
    score = max(0, min(100, round(score)))
    parts = ["TLT %s" % word_dod(tlt) if tlt else None, "10Y %s" % word_dod(us10) if us10 else None, "MOVE %s%s" % (("%.0f " % move["last"]) if move else "", word_dod(move)) if move else None, "HY OAS %s" % word_dod(hy) if hy else None]
    parts = [p for p in parts if p]
    if sell:
        return {"state": "DUMP RISK", "level": "HIGH", "score": score, "tone": "bearish",
                "text": "Bonds sold off hard today (%s). Sharp Treasury sell-offs raise the discount rate for every risk asset and force de-risking -- a daily rate shock of this size is a HUGE DUMP RISK for stocks and crypto." % "; ".join(parts)}
    if buy and credit_widening:
        return {"state": "FLIGHT TO SAFETY", "level": "HIGH", "score": score, "tone": "bearish",
                "text": "Bonds were bought hard (%s) while credit spreads widened -- that is fear money hiding in Treasuries, not an easy-money bid. Risk-off, not a pump." % "; ".join(parts)}
    if buy:
        return {"state": "PUMP SETUP", "level": "HIGH", "score": score, "tone": "bullish",
                "text": "Big daily bond buying with credit calm (%s). Falling yields with spreads steady is the classic fuel for a risk-asset pump: lower discount rates, easier financial conditions." % "; ".join(parts)}
    if soft_sell:
        return {"state": "SELL-OFF", "level": "MODERATE", "score": score, "tone": "cautious", "text": "Bonds sold off (%s). Not a shock yet, but stocks and crypto trade with a rates headwind until it settles." % "; ".join(parts)}
    if soft_buy:
        return {"state": "BUYING", "level": "MODERATE", "score": score, "tone": "supportive", "text": "Bonds bid (%s). Mildly supportive for risk assets." % "; ".join(parts)}
    return {"state": "CALM", "level": "LOW", "score": score, "tone": "neutral", "text": "Bond prices and yields are inside their normal daily range (%s). No rates-driven pressure on stocks either way." % "; ".join(parts)}


def eurodollar_shortage(m, fleet):
    btp = m.get("BTP-Bund")
    ites = m.get("IT-ES")
    oat = m.get("OAT-Bund")
    dxy = m.get("DTWEXBGS")
    ehy = m.get("BAMLHE00EHYIOAS")
    emhy = m.get("BAMLEMHBHYCRPIOAS")
    ems = m.get("BAMLEMPBPUBSICRPIOAS")
    plumb = (fleet.get("eurodollar_plumbing") or {}).get("composite_score")
    funding = (fleet.get("usd_funding") or {}).get("stress_z")
    pts, notes = 0, []
    for lab, mm, red, amb in (("BTP-Bund", btp, 10, 6), ("OAT-Bund", oat, 6, 4), ("IT-ES", ites, 6, 4)):
        if mm and mm.get("dod") is not None:
            if mm["dod"] >= red or (mm.get("d5") or 0) >= 15:
                pts += 2
                notes.append("%s widening %s (5d %+.0fbp)" % (lab, word_dod(mm), mm.get("d5") or 0))
            elif mm["dod"] >= amb:
                pts += 1
                notes.append("%s %s" % (lab, word_dod(mm)))
    if dxy and dxy.get("dod_pct") is not None and dxy["dod_pct"] >= 0.4:
        pts += 1
        notes.append("dollar +%.2f%%" % dxy["dod_pct"])
    for lab, mm in (("Euro HY", ehy), ("EM HY", emhy), ("EM sovereign", ems)):
        if mm and mm.get("dod") is not None and mm["dod"] >= 8:
            pts += 1
            notes.append("%s OAS %s" % (lab, word_dod(mm)))
    if plumb is not None and plumb >= 40:
        pts += 2
        notes.append("eurodollar-plumbing composite %.0f" % plumb)
    elif plumb is not None and plumb >= 25:
        pts += 1
        notes.append("eurodollar-plumbing composite %.0f" % plumb)
    if isinstance(funding, (int, float)) and funding >= 1.5:
        pts += 1
        notes.append("USD funding stress z %.1f" % funding)
    score = min(100, pts * 12)
    state = "SHORTAGE SIGNAL" if pts >= 5 else "WATCH" if pts >= 3 else "NONE"
    text = ("Periphery spreads, the dollar and offshore credit are moving together -- the signature of a eurodollar (offshore dollar) squeeze: " + "; ".join(notes)) if pts >= 3 else \
           ("No eurodollar-shortage signature: periphery spreads, the dollar and EM/Euro credit are quiet" + ((" (" + "; ".join(notes) + ")") if notes else "") + ".")
    return {"state": state, "score": score, "points": pts, "text": text, "inputs": {"btp_bund": btp and btp["last"], "it_es": ites and ites["last"], "oat_bund": oat and oat["last"], "plumbing": plumb, "funding_z": funding}}


def heartbeat(m, eq, ed):
    groups = {}
    for k, mm in m.items():
        if not mm:
            continue
        g = groups.setdefault(mm["group"], {"n": 0, "amber": 0, "red": 0, "loud": []})
        g["n"] += 1
        if mm["flag"] == "RED":
            g["red"] += 1
            g["loud"].append((abs(mm.get("z") or 0) + 3, "%s %s" % (mm["label"], word_dod(mm))))
        elif mm["flag"] == "AMBER":
            g["amber"] += 1
            g["loud"].append((abs(mm.get("z") or 0), "%s %s" % (mm["label"], word_dod(mm))))
    weights = {"us": 3, "vol": 3, "credit": 2.5, "europe": 2, "japan": 1.5, "world": 1, "funding": 1.5}
    num = den = 0.0
    loud = []
    for g, v in groups.items():
        w = weights.get(g, 1)
        stress = (v["red"] * 1.0 + v["amber"] * 0.45) / max(v["n"], 1)
        num += w * stress
        den += w
        loud += v["loud"]
    score = round(100 * num / den) if den else 0
    score = max(score, eq["score"] - 50 if eq["state"] in ("DUMP RISK", "FLIGHT TO SAFETY") else 0, ed["score"] // 2)
    regime = "ACUTE" if score >= 70 else "ELEVATED" if score >= 45 else "WATCH" if score >= 22 else "CALM"
    loud.sort(reverse=True)
    top = [t for _, t in loud[:5]]
    head = {"ACUTE": "Bond markets are in shock", "ELEVATED": "Bond stress is building", "WATCH": "A few bond signals are moving", "CALM": "Bond markets are calm"}[regime]
    headline = head + (": " + "; ".join(top) if top else " -- every monitored market inside its normal daily range") + "."
    return {"score": score, "regime": regime, "headline": headline, "loudest": top, "groups": {g: {k: v[k] for k in ("n", "amber", "red")} for g, v in groups.items()}}


# ─────────────────────────── main ────────────────────────────────────
def lambda_handler(event, ctx):
    t0 = time.time()
    event = event or {}
    notes, freshness = [], {}
    m = {}

    # Japan MOF curve first (authoritative daily history for JP 2Y/10Y/30Y)
    jgb = mof_jgb()
    mof_series = {}
    if jgb.get("rows"):
        for tenor, key in (("2Y", "JP02Y"), ("10Y", "JP10Y"), ("30Y", "JP30Y")):
            ds = [r["date"] for r in jgb["rows"] if r.get(tenor) is not None]
            cs = [r[tenor] for r in jgb["rows"] if r.get(tenor) is not None]
            if len(cs) > 3:
                mof_series[key] = merge_series(read_bank(OY_SLUGS[key][0]), {"dates": ds, "closes": cs})
        rows_ = jgb["rows"]
        last, prev = rows_[-1], rows_[-2] if len(rows_) > 1 else None
        jgb["today"] = last["date"]
        jgb["curve"] = [{"tenor": t, "last": last.get(t), "prev": prev.get(t) if prev else None, "dod_bp": round((last.get(t) - prev.get(t)) * 100, 1) if (prev and last.get(t) is not None and prev.get(t) is not None) else None} for t in jgb["tenors"]]
        freshness["mof_jgb"] = last["date"]

    # TradingView scanner (levels + day change) with our own daily bank for history
    cache = _s3_json(TV_KEY) or {}
    tv_live, bank = {}, dict(cache.get("bank") or {})
    try:
        tv_live, bank = tv_scan(cache)
        _put_json(TV_KEY, {"version": VERSION, "as_of": _iso(), "bank": bank}, gz=True)
    except Exception as e:
        notes.append("tv scanner: %s" % str(e)[:120])
    tv = {}
    for key, (sym, label, group, kind) in TV_SYMBOLS.items():
        ser = mof_series.get(key) or (merge_series(read_bank(TV_SLUG[key] + "-tv"), series_from_bank(bank, key)) if key in TV_SLUG else series_from_bank(bank, key))
        if key == "DE10Y":
            bb = bundesbank_10y()
            if bb and len(bb["closes"]) > 40:
                ser = bb
                if tv_live.get(key) and tv_live[key].get("close") is not None:
                    d0 = _now().date().isoformat()
                    if ser["dates"][-1] < d0:
                        ser = {"dates": ser["dates"] + [d0], "closes": ser["closes"] + [tv_live[key]["close"]]}
        if len(ser.get("closes") or []) < 2:
            continue
        tv[key] = ser
        if kind == "index":
            mm = metrics(ser, "index", "pts", "move", label, group, "TradingView " + sym)
        else:
            tk = "yield_jp" if group == "japan" else "yield_em" if key in ("BR10Y", "MX10Y", "IN10Y") else "yield_us" if group == "us" else "yield_dm"
            src = "warehouse:official-yields + MOF Japan" if key in mof_series else ("Bundesbank + TradingView" if key == "DE10Y" else ("warehouse:official-yields + TradingView live " + sym) if key in TV_SLUG else "TradingView " + sym)
            mm = metrics(ser, "yield", "bp", tk, label, group, src)
        if mm:
            lv = tv_live.get(key) or {}
            # fill 5d/20d from the scanner's performance columns while the bank is young
            if mm.get("d5") is None and lv.get("perf_w") is not None and lv.get("close"):
                mm["d5"] = round(lv["close"] * lv["perf_w"] / 100 * (100 if mm["unit"] == "bp" else 1) / (1 + lv["perf_w"] / 100), 2)
            if mm.get("d20") is None and lv.get("perf_1m") is not None and lv.get("close"):
                mm["d20"] = round(lv["close"] * lv["perf_1m"] / 100 * (100 if mm["unit"] == "bp" else 1) / (1 + lv["perf_1m"] / 100), 2)
            mm["bank_days"] = len(bank.get(key) or {})
            m[key] = mm
    freshness["tradingview"] = _now().date().isoformat() if tv_live else None
    freshness["tradingview_symbols"] = len(tv_live)

    # v1.2: official daily histories (Treasury, Bundesbank, BoE, BoC, RBA, SNB) replace the young scanner bank so
    # z-scores and 5d/20d are real from day one; the scanner still supplies the same-day level when newer
    off, off_src, banked = official_histories()
    for key, ser in off.items():
        label, group, tk = OFFICIAL[key]
        ser2, src = with_live(ser, tv_live.get(key), off_src[key])
        mm = metrics(ser2, "yield", "bp", tk, label, group, src)
        if mm:
            mm["bank_days"] = (m.get(key) or {}).get("bank_days")
            m[key] = mm
            tv[key] = ser2
    freshness["official"] = {name: max((ser["dates"][-1] for k, ser in off.items() if off_src[k] == name), default=None) for name in set(off_src.values())}
    freshness["official_n"] = len(off)

    # ECB euro-area curve: AAA vs all governments (periphery stress proxy)
    ea_aaa = merge_series(read_bank("ea-aaa-10y-ecb"), ecb_yc("G_N_A"))
    ea_all = merge_series(read_bank("ea-all-10y-ecb"), ecb_yc("G_N_C"))
    for key_, ser_ in (("EA_AAA10Y", ea_aaa), ("EA_ALL10Y", ea_all)):
        if ser_:
            slug_, sid_, title_, src_ = OY_SLUGS[key_]
            bank_yield(slug_, sid_, title_, src_, ser_, banked)
    if ea_aaa and len(ea_aaa["closes"]) > 40:
        tv["EA_AAA10Y"] = ea_aaa
        mm = metrics(ea_aaa, "yield", "bp", "yield_dm", "Euro area AAA 10Y (ECB)", "europe", "warehouse:official-yields + ECB YC")
        if mm:
            m["EA_AAA10Y"] = mm
    if ea_all and len(ea_all["closes"]) > 40:
        tv["EA_ALL10Y"] = ea_all
        mm = metrics(ea_all, "yield", "bp", "yield_dm", "Euro area all-govts 10Y (ECB)", "europe", "warehouse:official-yields + ECB YC")
        if mm:
            m["EA_ALL10Y"] = mm
        sp = spread_series(ea_all, ea_aaa) if ea_aaa else None
        if sp:
            mm = metrics(sp, "spread", "bp", "spread_periph", "Euro area all-govts minus AAA 10Y (periphery stress)", "europe", "ECB YC")
            if mm:
                m["EA-periphery"] = mm
    freshness["ecb"] = m["EA_AAA10Y"]["asof"] if "EA_AAA10Y" in m else None

    # FRED: warehouse history (fred-scoped bank via the resolver) + a short origin tail for today (v1.3)
    fred = {}
    def fred_one(sid):
        hist = wh_series("fred:" + sid)
        try:
            tail = fred_fetch(sid, obs=60)
        except Exception as e:
            tail = None
            print("[warroom] fred tail %s: %s" % (sid, str(e)[:80]))
        return sid, merge_series(hist, tail), bool(hist)
    with ThreadPoolExecutor(max_workers=8) as ex:
        fred_res = {sid: (ser, wh) for sid, ser, wh in ex.map(fred_one, list(FRED_SERIES))}
    for sid, (label, group, kind) in FRED_SERIES.items():
        try:
            fred[sid], _wh = fred_res.get(sid, (None, False))
            if not fred[sid]:
                raise RuntimeError("no data")
            tk = ("oas_ccc" if sid == "BAMLH0A3HYC" else "oas_hy" if sid in ("BAMLH0A0HYM2", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLHE00EHYIOAS") else "oas_em" if sid.startswith("BAMLEM") else "oas_ig") if kind == "spread" else \
                 ("vix" if sid == "VIXCLS" else "index" if kind == "index" else "yield_us")
            unit = "bp" if kind in ("spread", "yield") else "pct" if sid == "DTWEXBGS" else "pts"
            mm = metrics(fred[sid], kind, unit, tk, label, group, ("warehouse:fred + FRED tail " if _wh else "FRED ") + sid)
            if mm:
                m[sid] = mm
        except Exception as e:
            notes.append("fred %s: %s" % (sid, str(e)[:60]))
    freshness["fred"] = max((v["asof"] for k, v in m.items() if v["source"].startswith("FRED")), default=None)

    # Yahoo
    for sym, (label, group, kind) in YAHOO.items():
        try:
            hist = wh_series(sym)                      # tv-bars universe bank (Yahoo full history, banked in our warehouse)
            try:
                tail = yahoo_fetch(sym, "1mo")
            except Exception:
                tail = None
            ser = merge_series(hist, tail) or yahoo_fetch(sym)
            ysrc = ("warehouse:tv-bars + Yahoo tail " if hist else "Yahoo ") + sym
            if sym == "^TNX":
                mm = metrics(ser, "yield", "bp", "yield_us", label, group, ysrc)
            elif kind == "index":
                mm = metrics(ser, "index", "pts", "move", label, group, ysrc)
            else:
                mm = metrics(ser, "price", "pct", "price_bond", label, group, ysrc)
            if mm:
                m[sym] = mm
        except Exception as e:
            notes.append("yahoo %s: %s" % (sym, str(e)[:60]))
    freshness["yahoo"] = max((v["asof"] for k, v in m.items() if v["source"].startswith("Yahoo")), default=None)

    # spreads (bp-level in %)
    def y(k):
        return tv.get(k)
    pairs = {"BTP-Bund": ("IT10Y", "DE10Y", "Italy - Germany 10Y (BTP-Bund)"), "OAT-Bund": ("FR10Y", "DE10Y", "France - Germany 10Y (OAT-Bund)"),
             "Bono-Bund": ("ES10Y", "DE10Y", "Spain - Germany 10Y"), "IT-ES": ("IT10Y", "ES10Y", "Italy - Spain 10Y"), "GR-Bund": ("GR10Y", "DE10Y", "Greece - Germany 10Y"),
             "PT-Bund": ("PT10Y", "DE10Y", "Portugal - Germany 10Y"), "Gilt-Bund": ("GB10Y", "DE10Y", "UK - Germany 10Y"), "US-Bund": ("US10Y", "DE10Y", "US - Germany 10Y"),
             "US-JGB": ("US10Y", "JP10Y", "US - Japan 10Y"), "US2s10s": ("US10Y", "US02Y", "US 2s10s curve"), "US5s30s": ("US30Y", "US05Y", "US 5s30s curve"),
             "DE2s10s": ("DE10Y", "DE02Y", "Germany 2s10s"), "IT2s10s": ("IT10Y", "IT02Y", "Italy 2s10s"), "JP2s30s": ("JP30Y", "JP02Y", "Japan 2s30s"), "GB5s20s": ("GB20Y", "GB05Y", "UK 5s20s"), "CA2s10s": ("CA10Y", "CA02Y", "Canada 2s10s"), "AU2s10s": ("AU10Y", "AU02Y", "Australia 2s10s"), "US2s30s": ("US30Y", "US02Y", "US 2s30s curve")}
    for key, (a, b, label) in pairs.items():
        ser = spread_series(y(a), y(b))
        if ser:
            grp = "europe" if key in ("BTP-Bund", "OAT-Bund", "Bono-Bund", "IT-ES", "GR-Bund", "PT-Bund", "Gilt-Bund", "DE2s10s", "IT2s10s", "GB5s20s") else "japan" if key in ("US-JGB", "JP2s30s") else "world" if key in ("CA2s10s", "AU2s10s") else "us"
            mm = metrics(ser, "spread", "bp", "spread_periph" if grp == "europe" and key != "DE2s10s" else "yield_us", label, grp, "spread of " + " / ".join(sorted({(m.get(a) or {}).get("source", "?").split(" +")[0], (m.get(b) or {}).get("source", "?").split(" +")[0]})))
            if mm:
                m[key] = mm
    # SOFR - 3M bill (funding) from FRED
    if fred.get("SOFR") and fred.get("DTB3"):
        ser = spread_series(fred["SOFR"], fred["DTB3"])
        mm = metrics(ser, "spread", "bp", "oas_ig", "SOFR minus 3M bill", "funding", "FRED") if ser else None
        if mm:
            m["SOFR-TB3"] = mm

    # fleet feeds
    fleet = {"auction_desk": _s3_json("data/auction-desk.json") or {}, "usd_funding": _s3_json("data/usd-funding.json") or {},
             "eurodollar_plumbing": _s3_json("data/eurodollar-plumbing.json") or {}, "move_index": _s3_json("data/move-index.json") or {},
             "crisis_plumbing": _s3_json("data/crisis-plumbing.json") or {}}
    ad = fleet["auction_desk"]
    auction = {"generated_at": ad.get("generated_at"), "today": (ad.get("today") or {}).get("date"), "verdict": (ad.get("today") or {}).get("verdict"),
               "auctions": [{k: a.get(k) for k in ("term", "type", "grade", "btc", "tail_bp", "indirect_pct", "pd_pct", "total_accepted", "verdict")} for a in ((ad.get("today") or {}).get("auctions") or [])],
               "buybacks": [{k: b.get(k) for k in ("operation_date", "accepted", "max_par", "fill_pct", "coverage", "tags", "verdict")} for b in ((ad.get("today") or {}).get("buybacks") or [])],
               "recent_days": [{k: d.get(k) for k in ("date", "headline", "tags", "risk_assets")} for d in (ad.get("recent_days") or [])[:6]],
               "prediction": [{k: p.get(k) for k in ("symbol", "name", "basis_label", "call", "confidence", "d1", "d5")} for p in ((ad.get("reactions") or {}).get("prediction") or []) if p.get("symbol") in ("SPY", "QQQ", "TLT", "HYG", "BTC-USD", "GLD")],
               "composite_now": ((ad.get("composite_history") or {}).get("series") or [{}])[-1] if ad.get("composite_history") else None}
    ff = {"usd_funding_stress_z": fleet["usd_funding"].get("stress_z"), "usd_funding_generated": fleet["usd_funding"].get("generated"),
          "eurodollar_plumbing": {k: fleet["eurodollar_plumbing"].get(k) for k in ("composite_score", "verdict", "severity", "generated_at")},
          "move_index": {k: fleet["move_index"].get(k) for k in ("level", "change_1d", "regime", "percentile", "generated_at")},
          "crisis_plumbing": (fleet["crisis_plumbing"].get("composite") or {}) | {"generated_at": fleet["crisis_plumbing"].get("generated_at")} if isinstance(fleet["crisis_plumbing"].get("composite"), dict) else None}

    # bank what only this engine fetches, so data.html / chart-pro serve it as official-yields:{slug}
    for key_ in ("JP02Y", "JP10Y", "JP30Y"):
        if mof_series.get(key_):
            slug_, sid_, title_, src_ = OY_SLUGS[key_]
            bank_yield(slug_, sid_, title_, src_, mof_series[key_], banked)
    for key_, slug_ in TV_SLUG.items():
        ser_ = series_from_bank(bank, key_)
        if ser_ and len(ser_["closes"]) > 1:
            bank_yield(slug_ + "-tv", key_ + "_TV", "%s (daily close, TradingView scanner, banked by the bond war room)" % TV_SYMBOLS[key_][1], "TradingView scanner", ser_, banked)
    try:
        catalog = sorted(banked)
        _put_json(OY_STATE, {"lane": "official-yields", "banked_at": _iso(), "engines": ["justhodl-repo", "justhodl-bond-warroom"], "count": len(catalog),
                             "catalog": [{"id": c, "key": OY_PREFIX + c + ".json", "last": banked[c]["dates"][-1], "n_obs": len(banked[c]["dates"])} for c in catalog]})
    except Exception as e:
        notes.append("lane state: %s" % str(e)[:80])
    freshness["warehouse_lane"] = {"official_yields_banked": len(banked)}

    eq = equity_risk(m)
    ed = eurodollar_shortage(m, fleet)
    hb = heartbeat(m, eq, ed)

    # panels in display order
    def rows(keys):
        return [dict(m[k], key=k) for k in keys if k in m]
    panels = {
        "us_rates": rows(["US02Y", "US05Y", "US07Y", "US10Y", "US20Y", "US30Y", "DGS3MO", "DFII10", "T10YIE", "US2s10s", "US5s30s", "US2s30s"]),
        "volatility": rows(["^MOVE", "MOVE_TV", "VIXCLS", "TLT", "IEF", "SHY", "HYG", "LQD", "EMB"]),
        "japan": rows(["JP02Y", "JP10Y", "JP30Y", "JP2s30s", "US-JGB"]),
        "europe": rows(["DE02Y", "DE10Y", "DE30Y", "FR10Y", "IT02Y", "IT10Y", "ES10Y", "NL10Y", "PT10Y", "GR10Y", "GB02Y", "GB05Y", "GB10Y", "GB20Y", "CH02Y", "CH05Y", "CH10Y", "EA_AAA10Y", "EA_ALL10Y"]),
        "europe_spreads": rows(["BTP-Bund", "OAT-Bund", "Bono-Bund", "IT-ES", "PT-Bund", "GR-Bund", "Gilt-Bund", "US-Bund", "EA-periphery", "DE2s10s", "IT2s10s", "GB5s20s"]),
        "world": rows(["AU02Y", "AU03Y", "AU05Y", "AU10Y", "CA02Y", "CA05Y", "CA10Y", "CA30Y", "CN10Y", "KR10Y", "IN10Y", "BR10Y", "MX10Y", "CA2s10s", "AU2s10s"]),
        "credit": rows(["BAMLH0A0HYM2", "BAMLC0A0CM", "BAMLC0A1CAAA", "BAMLC0A4CBBB", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC", "BAMLHE00EHYIOAS", "BAMLEMCBPIOAS", "BAMLEMIBHGCRPIOAS", "BAMLEMHBHYCRPIOAS", "BAMLEMPBPUBSICRPIOAS"]),
        "funding": rows(["SOFR", "DTB3", "SOFR-TB3", "DTWEXBGS"]),
    }
    flags = {"RED": [k for k, v in m.items() if v["flag"] == "RED"], "AMBER": [k for k, v in m.items() if v["flag"] == "AMBER"]}
    out = {"version": VERSION, "generated_at": _iso(), "elapsed_s": round(time.time() - t0, 1), "notes": notes, "freshness": freshness,
           "heartbeat": hb, "equity_risk": eq, "eurodollar_shortage": ed, "flags": flags, "panels": panels, "jgb_curve": {k: jgb.get(k) for k in ("today", "tenors", "curve", "error")},
           "auction": auction, "fleet": ff, "n_series": len(m),
           "sources_doctrine": "warehouse first: every series is read from our own AWS (fred-scoped, treasury-par, boe iadb, tv-bars, official-yields) through the symdir resolver, the official origin only adds the same-day tail, and whatever the warehouse lacked is banked into data/warm/official-yields/ so data.html and chart-pro serve it next",
           "methodology": {"dod": "day-over-day change: bp for yields and spreads, % for bond ETFs and the dollar, points for MOVE/VIX; z = today's change vs the trailing 250 daily changes",
                           "history": "sovereign yields from the TradingView scanner bank grow one close per run; z-scores appear after 40 banked days (JGB, Bund, ECB, FRED, Yahoo carry full history from day one)", "flags": "RED = shock threshold hit (US/DM 10Y 10bp, JGB 7bp, periphery spreads 10bp, HY OAS 20bp, IG OAS 7bp, CCC 35bp, EM 15bp, bond ETFs 1.5%, MOVE +10 or level >= 120) or |z| >= 2.5 or a yield moving >= 5% of its own level in a day; AMBER = 60% of those, |z| >= 1.5 or >= 3% of level",
                           "equity_risk": "DUMP RISK: TLT <= -1.5% or US10Y >= +10bp or MOVE +10 with bonds down; PUMP SETUP: TLT >= +1.5% or US10Y <= -10bp with HY OAS not widening >= 8bp; FLIGHT TO SAFETY: the same buying with HY OAS widening",
                           "eurodollar": "points from BTP-Bund / OAT-Bund / IT-ES widening, dollar up >= 0.4%, Euro-HY / EM spreads widening >= 8bp, the fleet's eurodollar-plumbing composite and USD-funding stress z; SHORTAGE SIGNAL at 5+ points",
                           "heartbeat": "weighted share of RED/AMBER across US rates, volatility, credit, Europe, Japan, rest of world and funding; ACUTE >= 70, ELEVATED >= 45, WATCH >= 22"}}
    _put_json(OUT_KEY, out)
    return {"ok": True, "elapsed_s": out["elapsed_s"], "n_series": len(m), "heartbeat": hb["score"], "regime": hb["regime"], "equity": eq["state"], "eurodollar": ed["state"],
            "red": flags["RED"][:8], "notes": notes[:4], "freshness": freshness}
