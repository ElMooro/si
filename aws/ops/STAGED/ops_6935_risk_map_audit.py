"""ops 6935 - READ ONLY: the Risk Map as it is, checked against the bars, and what the warehouse holds for every asset class.

Khalid 2026-11-24 10:05 ET: "improve this engine so that it covers all asset classes: Risk Map booming vs getting
destroyed ... then add a tab for it on chart.html make sure you double check your work to make sure whats booming is
actually booming and whats destroyed is actually destroyed and make it in a big heatmap visual like tradingview heatmap"

Before anything is built:

  1. the live data/regime-map.json: every figure it prints for every instrument is recomputed here from the warehouse's
     own daily bars (Polygon EOD bank for funds, the banked crypto bars for coins), two ways - by counting bars, as the
     engine does, and by the calendar - and each name on its "booming" and "destroyed" lists is held against its state;
  2. what the warehouse holds for a map of every asset class: each candidate fund's banked history, the factor book's
     size and dollar volume for every fund, the banked coins, the FMP price bank's spot currencies, futures and indexes
     with their age, CoinMarketCap's market values, and the forming-bar file.

Nothing is written, no engine is invoked, no provider is asked for anything.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import gzip
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared")]
BUCKET = "justhodl-dashboard-live"
EOD = "data/warm/polygon-full/eod/"
COINS = "data/warm/katlin/crypto-bars/"
FMP_PX = "data/warm/fmp/historical-price-eod-full/"

CANDIDATES = {
    "US equity - size and style": "SPY RSP QQQ QQQE DIA IWM IJR MDY IJH IWC OEF IWB VTI MAGS IWF IWD IWO IWN VUG VTV IVW IVE",
    "US equity - sectors": "XLK XLC XLY XLF XLI XLE XLB XLV XLP XLU XLRE",
    "US equity - industries": "SMH SOXX IGV SKYY CIBR HACK XBI IBB IHI XPH IHF IHE KRE KBE IAI KIE KCE XHB ITB XRT IYT JETS ITA XAR XOP OIH FCG AMLP XME GDX GDXJ SIL COPX PICK SLX URA NLR LIT TAN ICLN PAVE IYZ XTL PEJ FDN MOO PHO WOOD BIZD",
    "US equity - themes": "ARKK ARKG ARKW ARKF BOTZ ROBO AIQ QTUM IPO BLOK BITQ WGMI IBIT FBTC ETHA ESPO BETZ MSOS DRIV IDRV UFO REMX KRBN",
    "US equity - factors": "MTUM QUAL USMV VLUE SIZE SPHB SPLV SPHQ DVY VIG SCHD NOBL COWZ MOAT RPV RPG",
    "International developed": "EFA VEA VGK EZU EWJ DXJ EWG EWU EWQ EWL EWA EWC EWP EWI EWN EWD EDEN ENOR EFNL EWK EWO EIRL EWS EWH EIS ENZL",
    "Emerging markets": "EEM VWO IEMG EMXC MCHI FXI KWEB ASHR INDA EPI EWT EWY EWZ EWW EZA TUR EPOL EWM EIDO THD EPHE VNM ARGT ECH EPU KSA UAE QAT AFK FM ILF AAXJ GREK",
    "Rates - US government": "BIL SGOV SHV SHY IEI IEF TLH TLT EDV ZROZ GOVT TIP STIP VTIP SCHP LTPZ USFR TFLO",
    "Credit": "LQD VCSH VCIT VCLT IGSB IGIB HYG JNK SJNK USHY ANGL BKLN SRLN FLOT JAAA PFF PGX CWB ICVT MBB VMBS CMBS MUB VTEB HYD SHM AGG BND BSV BIV BLV MINT JPST",
    "Global bonds": "BNDX IAGG BWX IGOV WIP EMB VWOB PCY EMLC EBND EMHY HYEM CEMB",
    "Currencies": "UUP UDN USDU FXE FXY FXB FXA FXC FXF CEW CYB FXS BZF DBV",
    "Commodities": "GLD IAU SLV PPLT PALL GLTR DBP CPER DBB USO BNO UNG UGA DBE DBA CORN WEAT SOYB CANE DBC PDBC GSG COMT BCI BDRY",
    "Real estate": "VNQ IYR SCHH VNQI RWO RWX REM MORT REZ INDS SRVR",
    "Volatility": "VIXY VIXM VXX SVXY",
    "Alternatives": "DBMF KMLM CTA BTAL TAIL QAI RPAR PFIX IVOL MNA FTLS",
}
SPOT = {
    "currencies": "EURUSD USDJPY GBPUSD AUDUSD NZDUSD USDCAD USDCHF USDCNH USDCNY USDMXN USDBRL USDINR USDKRW USDTRY USDZAR USDSEK USDNOK USDPLN USDSGD USDHKD USDTWD USDIDR USDTHB USDCLP USDCOP",
    "futures": "GCUSD SIUSD PLUSD PAUSD HGUSD CLUSD BZUSD NGUSD RBUSD HOUSD ZCUSX ZSUSX KEUSX ZWUSX ZOUSX ZRUSD KCUSX SBUSX CCUSD CTUSX OJUSX LEUSX HEUSX GFUSX LBUSD ALIUSD ESUSD NQUSD RTYUSD YMUSD ZNUSD ZBUSD ZFUSD ZTUSD DXUSD",
    "indexes": "^GSPC ^NDX ^DJI ^RUT ^VIX ^VVIX ^MOVE ^TNX ^TYX ^FVX ^IRX ^STOXX50E ^GDAXI ^FCHI ^FTSE ^N225 ^HSI ^SSEC ^KS11 ^BSESN ^BVSP ^GSPTSE ^AXJO DX-Y.NYB ^SPGSCI ^BCOM",
}
LEGACY_CRYPTO = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}


def safe(t):
    return "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(t).upper())[:80]


def unz(raw):
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def engine_score(closes):
    """The live engine's own arithmetic (justhodl-regime-map 1.0.0, score()), copied to check its output. Pure."""
    if len(closes) < 130:
        return None
    last = closes[-1]

    def ret(n):
        return (last / closes[-1 - n] - 1) if len(closes) > n else 0.0
    r21, r63, r126 = ret(21), ret(63), ret(126)
    ma50 = sum(closes[-50:]) / 50
    ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else sum(closes) / len(closes)
    pts = (1 if last > ma50 else 0) + (1 if last > ma200 else 0) + (1 if ma50 > ma200 else 0)
    mom = 0.45 * r63 + 0.30 * r126 + 0.25 * r21
    return {"risk_on": round(clamp(mom * 100 * 2.2 + (pts - 1.5) / 1.5 * 25, -100, 100)), "r1m": round(r21 * 100, 1), "r3m": round(r63 * 100, 1), "r6m": round(r126 * 100, 1)}


def state_of(ro):
    return "BOOMING" if ro >= 45 else "RISK-ON" if ro >= 15 else "NEUTRAL" if ro > -15 else "RISK-OFF" if ro > -45 else "DESTROYED"


def months_back(d, n):
    y, m = d.year, d.month - n
    while m <= 0:
        y, m = y - 1, m + 12
    day = d.day
    while True:
        try:
            return date(y, m, day)
        except ValueError:
            day -= 1


def cal_return(rows, months):
    """Percent change of the last close from the last close on or before the same day `months` months earlier. Pure."""
    if not rows:
        return None
    last_d, last_c = date.fromisoformat(rows[-1][0]), rows[-1][1]
    want = months_back(last_d, months).isoformat()
    base = None
    for d, c in rows:
        if d <= want:
            base = c
        else:
            break
    return None if not base else round((last_c / base - 1) * 100, 1)


def selftest():
    assert months_back(date(2026, 3, 31), 1) == date(2026, 2, 28) and months_back(date(2026, 1, 15), 3) == date(2025, 10, 15)
    rows = [("2026-01-30", 100.0), ("2026-02-27", 110.0), ("2026-03-02", 111.0), ("2026-03-31", 121.0)]
    assert cal_return(rows, 1) == 10.0 and cal_return(rows, 2) == 21.0 and cal_return(rows, 6) is None
    flat = [100.0] * 260
    assert engine_score(flat)["risk_on"] == -25 and state_of(45) == "BOOMING" and state_of(-45) == "DESTROYED" and state_of(-44) == "RISK-OFF"
    assert safe("brk.b") == "BRK.B" and safe("^VIX") == "^VIX" and safe("DX-Y.NYB") == "DX-Y.NYB"


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, max_pool_connections=48))

    def get(key):
        try:
            o = s3.get_object(Bucket=BUCKET, Key=key)
            return unz(o["Body"].read()), o["LastModified"]
        except Exception as e:  # noqa: BLE001
            return None, "%s" % type(e).__name__

    def fund_rows(t):
        doc, _ = get(EOD + safe(t) + ".json.gz")
        rows = []
        for r in (doc or {}).get("rows") or []:
            if isinstance(r, list) and len(r) >= 6 and r[0] is not None and r[4]:
                rows.append((datetime.fromtimestamp(r[0] / 1000.0, tz=timezone.utc).date().isoformat(), float(r[4]), float(r[5] or 0)))
        return t, rows

    def coin_rows(sym):
        doc, when = get(COINS + sym + ".json.gz")
        rows = [(str(r[0])[:10], float(r[4]), float(r[5] or 0)) for r in (doc or {}).get("rows") or [] if isinstance(r, list) and len(r) >= 5 and r[4]]
        return sym, rows, (doc or {}).get("source"), when

    with report("ops_6935_risk_map_audit") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6935 - the Risk Map checked against the bars; what the warehouse holds for every asset class (read only), %s" % now.isoformat(timespec="minutes"))
        fails = []

        # ------------------------------------------------------------------ 1. the live feed
        r.section("1. The live feed, as it is")
        live, when = get("data/regime-map.json")
        prev, pwhen = get("data/regime-map-prev.json")
        if not live:
            r.log("data/regime-map.json: %s" % when)
            live = {}
        else:
            reg = live.get("regime") or {}
            r.log("data/regime-map.json written %s (%.1f h ago), engine %s %s, generated_at %s, %s instruments, elapsed %s s" % (
                when.isoformat(timespec="minutes"), (now - when).total_seconds() / 3600, live.get("engine"), live.get("version"), live.get("generated_at"), live.get("n_instruments"), live.get("elapsed_s")))
            r.log("regime: %s | equities %s, crypto %s, rates %s, commodities %s, international %s | breadth %s%% | concentration %s" % (
                reg.get("label"), reg.get("equity_avg"), reg.get("crypto_avg"), reg.get("rates_avg"), reg.get("commodities_avg"), reg.get("intl_avg"), reg.get("equity_breadth_pct"), reg.get("concentration_spread_3m")))
            r.log("summary: %s" % reg.get("summary"))
            r.log("previous snapshot written %s, label %s; transitions %s" % (pwhen.isoformat(timespec="minutes") if hasattr(pwhen, "isoformat") else pwhen, (prev or {}).get("label"), live.get("n_transitions")))
            for name in ("booming", "destroyed"):
                r.log("%s list: %s" % (name, ", ".join("%s %+d %s (1m %+.1f%%, 3m %+.1f%%, 6m %+.1f%%)" % (x["ticker"], x["risk_on"], x["state"], x["r1m"], x["r3m"], x["r6m"]) for x in live.get(name) or [])))
            states = {}
            for x in live.get("ranked") or []:
                states[x["state"]] = states.get(x["state"], 0) + 1
            r.log("states across the board: %s" % states)
            wrong_b = [x["ticker"] for x in live.get("booming") or [] if x["state"] != "BOOMING"]
            wrong_d = [x["ticker"] for x in live.get("destroyed") or [] if x["state"] != "DESTROYED"]
            r.log("on the 'booming' list without the state BOOMING: %s" % (wrong_b or "none"))
            r.log("on the 'destroyed' list without the state DESTROYED: %s" % (wrong_d or "none"))
            up_d = [(x["ticker"], x["r3m"]) for x in live.get("destroyed") or [] if x["r3m"] > 0 or x["risk_on"] > 0]
            dn_b = [(x["ticker"], x["r3m"]) for x in live.get("booming") or [] if x["r3m"] < 0 or x["risk_on"] < 0]
            r.log("'destroyed' names that are up over three months or score above zero: %s" % (up_d or "none"))
            r.log("'booming' names that are down over three months or score below zero: %s" % (dn_b or "none"))

        # ------------------------------------------------------------------ 2. every figure, recomputed
        r.section("2. Each instrument recomputed from the warehouse's own bars")
        ranked = live.get("ranked") or []
        funds = [x["ticker"] for x in ranked if x["ticker"] not in LEGACY_CRYPTO]
        with ThreadPoolExecutor(max_workers=24) as pool:
            bars = dict(pool.map(fund_rows, funds))
        coin = {}
        for sym in LEGACY_CRYPTO:
            _, rows, src, cw = coin_rows(sym)
            coin[sym] = rows
            r.log("coin %s: %d banked bars %s .. %s, source %s, file written %s" % (sym, len(rows), rows[0][0] if rows else "-", rows[-1][0] if rows else "-", src, cw.isoformat(timespec="minutes") if hasattr(cw, "isoformat") else cw))
        gen = str(live.get("generated_at") or "")[:10]
        r.log("the engine ran on %s; funds are compared on the bank's bars up to that day, coins the same" % gen)
        r.log("  %-6s %-10s | engine: score state 1m 3m 6m | same arithmetic on the bank's bars: score 1m 3m 6m | by the calendar: 1m 3m 6m | last bar" % ("name", "kind"))
        off = []
        for x in ranked:
            t = x["ticker"]
            is_coin = t in LEGACY_CRYPTO
            rows = [(d, c) for d, c, _ in (coin[t] if is_coin else bars.get(t) or []) if not gen or d <= gen]
            mine = engine_score([c for _, c in rows]) if rows else None
            cal = [cal_return(rows, m) for m in (1, 3, 6)]
            note = ""
            if mine:
                d_score = abs(mine["risk_on"] - x["risk_on"])
                if d_score > 6 or state_of(mine["risk_on"]) != x["state"]:
                    note = "  <- differs (bank %+d %s)" % (mine["risk_on"], state_of(mine["risk_on"]))
                    off.append(t)
            r.log("  %-6s %-10s | %+4d %-9s %+6.1f %+6.1f %+6.1f | %s | %s | %s%s" % (
                t, "coin, 7-day week" if is_coin else "fund", x["risk_on"], x["state"], x["r1m"], x["r3m"], x["r6m"],
                ("%+4d %+6.1f %+6.1f %+6.1f" % (mine["risk_on"], mine["r1m"], mine["r3m"], mine["r6m"])) if mine else "no bars",
                " ".join("%+6.1f" % v if v is not None else "   n/a" for v in cal), rows[-1][0] if rows else "-", note))
        r.log("instruments whose score or state differs from the same arithmetic on the bank's bars: %s" % (off or "none"))
        for sym in LEGACY_CRYPTO:
            rows = [(d, c) for d, c, _ in coin[sym]]
            if len(rows) > 130:
                r.log("%s: 21 / 63 / 126 bars back = %s / %s / %s (the engine calls these 1 / 3 / 6 months); one / three / six calendar months back = %s / %s / %s" % (
                    sym, rows[-22][0], rows[-64][0], rows[-127][0], months_back(date.fromisoformat(rows[-1][0]), 1), months_back(date.fromisoformat(rows[-1][0]), 3), months_back(date.fromisoformat(rows[-1][0]), 6)))

        # ------------------------------------------------------------------ 3. the factor book: every fund
        r.section("3. The factor book: every fund, by size")
        book, bwhen = get("data/warm/factor-book/matrix/index.json.gz")
        rows = book if isinstance(book, list) else (book or {}).get("rows") or (book or {}).get("symbols") or []
        if isinstance(book, dict) and not rows:
            r.log("factor book index keys: %s" % list(book.keys())[:20])
            cols, data = book.get("columns") or book.get("fields"), book.get("data") or book.get("matrix")
            if cols and data:
                rows = [dict(zip(cols, row)) for row in data]
        r.log("factor book index written %s, %d rows; keys of a row: %s" % (bwhen.isoformat(timespec="minutes") if hasattr(bwhen, "isoformat") else bwhen, len(rows), sorted(rows[0].keys())[:60] if rows else None))
        etfs = [x for x in rows if isinstance(x, dict) and x.get("is_etf")]
        by_t = {str(x.get("symbol") or "").upper(): x for x in rows if isinstance(x, dict)}
        r.log("funds in the book: %d; with a market value: %d; with dollar volume: %d" % (len(etfs), sum(1 for x in etfs if x.get("mcap")), sum(1 for x in etfs if x.get("dollar_vol_20d"))))
        for t in ("SPY", "TLT", "GLD", "UUP", "VIXY", "IBIT"):
            x = by_t.get(t)
            r.log("  %s: %s" % (t, {k: x[k] for k in sorted(x) if x[k] not in (None, "", False)} if x else "not in the book"))
        top = sorted((x for x in etfs if x.get("mcap")), key=lambda x: -float(x["mcap"]))[:1100]
        r.log("the 1,100 largest funds: ticker|name|market value $M|20-session dollar volume $M")
        chunk = []
        for x in top:
            chunk.append("%s|%s|%.0f|%.1f" % (x.get("symbol"), str(x.get("name") or "")[:46].replace("|", "/"), float(x["mcap"]) / 1e6, float(x.get("dollar_vol_20d") or 0) / 1e6))
            if len(chunk) == 6:
                r.log("  " + " ;; ".join(chunk))
                chunk = []
        if chunk:
            r.log("  " + " ;; ".join(chunk))

        # ------------------------------------------------------------------ 4. candidates in the EOD bank
        r.section("4. Candidate funds for a map of every asset class: what the bank holds")
        man, mwhen = get(EOD + "manifest.json")
        r.log("EOD bank manifest written %s: %s" % (mwhen.isoformat(timespec="minutes") if hasattr(mwhen, "isoformat") else mwhen, {k: man[k] for k in list(man)[:14] if not isinstance(man[k], (list, dict))} if isinstance(man, dict) else man))
        want = sorted({t for v in CANDIDATES.values() for t in v.split()})
        with ThreadPoolExecutor(max_workers=32) as pool:
            cb = dict(pool.map(fund_rows, want))
        newest = max((rows[-1][0] for rows in cb.values() if rows), default=None)
        r.log("%d candidates; newest session in the bank %s" % (len(want), newest))
        for cls, names in CANDIDATES.items():
            good, short, late, absent = [], [], [], []
            for t in names.split():
                rows = cb.get(t) or []
                if not rows:
                    absent.append(t)
                elif rows[-1][0] != newest:
                    late.append("%s(last %s)" % (t, rows[-1][0]))
                elif len(rows) < 260:
                    short.append("%s(%d bars from %s)" % (t, len(rows), rows[0][0]))
                else:
                    good.append(t)
            r.log("  %s: %d current with a year or more of bars" % (cls, len(good)))
            for label, lst in (("under a year of bars", short), ("last bar is not the newest session", late), ("not in the bank", absent)):
                if lst:
                    r.log("      %s: %s" % (label, " ".join(lst)))
        thin = []
        for t in want:
            rows = cb.get(t) or []
            if len(rows) >= 21:
                dv = sum(c * v for _, c, v in rows[-20:]) / 20
                x = by_t.get(t) or {}
                if dv < 1e6:
                    thin.append("%s $%.2fM/day" % (t, dv / 1e6))
                if not x:
                    r.log("      %s is in the bank and not in the factor book" % t)
        r.log("candidates trading under $1M a day (20 sessions): %s" % (thin or "none"))
        gaps = []
        for t in want:
            rows = cb.get(t) or []
            jumps = [(rows[i][0], round(rows[i][1] / rows[i - 1][1], 3)) for i in range(max(1, len(rows) - 300), len(rows)) if rows[i - 1][1] and not 0.6 < rows[i][1] / rows[i - 1][1] < 1.6]
            if jumps:
                gaps.append("%s %s" % (t, jumps[:3]))
        r.log("close-to-close moves beyond 40%% down or 60%% up in the last 300 sessions (a split the bank still holds unadjusted, or a real move): %s" % (gaps or "none"))

        # ------------------------------------------------------------------ 5. coins
        r.section("5. Coins: the banked bars and CoinMarketCap's market values")
        keys, tok = [], None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": COINS}
            if tok:
                kw["ContinuationToken"] = tok
            page = s3.list_objects_v2(**kw)
            keys += [(o["Key"], o["LastModified"], o["Size"]) for o in page.get("Contents") or []]
            tok = page.get("NextContinuationToken")
            if not tok:
                break
        syms = sorted(k[len(COINS):-len(".json.gz")] for k, _, _ in keys if k.endswith(".json.gz"))
        with ThreadPoolExecutor(max_workers=24) as pool:
            cr = list(pool.map(coin_rows, syms))
        newest_c = max((rows[-1][0] for _, rows, _, _ in cr if rows), default=None)
        cur = [(s, rows) for s, rows, _, _ in cr if rows and rows[-1][0] == newest_c]
        r.log("%d coin files; newest bar %s; %d current: %s" % (len(syms), newest_c, len(cur), " ".join("%s(%d)" % (s, len(rows)) for s, rows in cur)))
        r.log("not current: %s" % " ".join("%s(last %s)" % (s, rows[-1][0] if rows else "-") for s, rows, _, _ in cr if not rows or rows[-1][0] != newest_c))
        pb = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/warm/provider-bank/", Delimiter="/")
        r.log("provider bank folders: %s" % [p["Prefix"] for p in pb.get("CommonPrefixes") or []])
        for pref in ("data/warm/provider-bank/cmc/", "data/warm/provider-bank/coinmarketcap/"):
            sub = s3.list_objects_v2(Bucket=BUCKET, Prefix=pref, Delimiter="/")
            folders = [p["Prefix"] for p in sub.get("CommonPrefixes") or []]
            if not folders and not sub.get("Contents"):
                continue
            r.log("%s: folders %s files %s" % (pref, folders, [(o["Key"][len(pref):], o["Size"], o["LastModified"].isoformat(timespec="minutes")) for o in (sub.get("Contents") or [])[:12]]))
            for f in folders:
                inner = s3.list_objects_v2(Bucket=BUCKET, Prefix=f, MaxKeys=1000)
                objs = sorted(inner.get("Contents") or [], key=lambda o: o["LastModified"])
                if objs:
                    r.log("    %s %d files, newest %s (%s, %d bytes)" % (f[len(pref):], len(objs), objs[-1]["Key"][len(f):], objs[-1]["LastModified"].isoformat(timespec="minutes"), objs[-1]["Size"]))
                if "listing" in f and objs:
                    doc, _ = get(objs[-1]["Key"])
                    data = doc.get("data") if isinstance(doc, dict) else doc
                    data = data.get("data") if isinstance(data, dict) and "data" in data else data
                    if isinstance(data, list) and data:
                        r.log("    listings: %d coins; keys %s" % (len(data), sorted(data[0].keys())[:30]))
                        q = (data[0].get("quote") or {}).get("USD") or {}
                        r.log("    first row quote keys: %s" % sorted(q.keys()))
                        r.log("    top 60 by rank: %s" % " ".join("%s:%s" % (c.get("symbol"), ("%.1fB" % (((c.get("quote") or {}).get("USD") or {}).get("market_cap") or 0) / 1e9)) for c in data[:60]))
                    else:
                        r.log("    listings shape: %s" % (str(doc)[:300]))

        # ------------------------------------------------------------------ 6. spot currencies, futures, indexes
        r.section("6. The FMP price bank: spot currencies, futures and indexes, and how old each is")
        fman, fwhen = get("data/warm/fmp/manifest.json")
        if isinstance(fman, dict):
            r.log("FMP walker manifest written %s; top-level: %s" % (fwhen.isoformat(timespec="minutes"), {k: fman[k] for k in fman if not isinstance(fman[k], (list, dict))}))
        for lst in ("commodities-list", "forex-list", "cryptocurrency-list", "index-list"):
            doc, lw = get("data/warm/fmp/%s/latest.json.gz" % lst)
            rows_l = doc if isinstance(doc, list) else (doc or {}).get("rows") or (doc or {}).get("data") or []
            r.log("%s: %s rows, written %s; first: %s" % (lst, len(rows_l), lw.isoformat(timespec="minutes") if hasattr(lw, "isoformat") else lw, json.dumps(rows_l[:2])[:260]))

        def spot(sym):
            doc, w = get(FMP_PX + safe(sym) + ".json.gz")
            rows_s = doc if isinstance(doc, list) else (doc or {}).get("rows") or (doc or {}).get("historical") or (doc or {}).get("data") or []
            ds = sorted(str(x.get("date"))[:10] for x in rows_s if isinstance(x, dict) and x.get("date"))
            return sym, len(rows_s), (ds[0] if ds else None), (ds[-1] if ds else None), w, (sorted(rows_s[0].keys()) if rows_s and isinstance(rows_s[0], dict) else str(doc)[:80] if doc else None)
        for kind, names in SPOT.items():
            with ThreadPoolExecutor(max_workers=16) as pool:
                got = list(pool.map(spot, names.split()))
            held = [g for g in got if g[1]]
            r.log("%s: %d of %d banked" % (kind, len(held), len(got)))
            for g in held:
                r.log("    %-10s %5d rows %s .. %s (file written %s)" % (g[0], g[1], g[2], g[3], g[4].isoformat(timespec="minutes") if hasattr(g[4], "isoformat") else g[4]))
            if held:
                r.log("    keys of a row: %s" % held[0][5])
            r.log("    not banked: %s" % " ".join(g[0] for g in got if not g[1]))

        # ------------------------------------------------------------------ 7. the forming bars
        r.section("7. The forming-bar file (today's bar of every US stock and fund, 15 minutes delayed)")
        tm, tw = get("data/warm/polygon-full/today-manifest.json")
        r.log("today-manifest written %s: %s" % (tw.isoformat(timespec="minutes") if hasattr(tw, "isoformat") else tw, json.dumps(tm)[:500] if tm else None))
        td, tdw = get("data/warm/polygon-full/today.json.gz")
        if isinstance(td, dict):
            rws = td.get("rows") or {}
            r.log("today.json.gz: session %s, as_of %s, %d names; candidates with a forming bar: %d of %d; SPY %s" % (td.get("session"), td.get("as_of"), len(rws), sum(1 for t in want if t in rws), len(want), rws.get("SPY")))

        r.kv(instruments_live=len(ranked), candidates=len(want), coins_current=len(cur), funds_in_book=len(etfs), failed=len(fails))
        r.ok("read only: the live feed recomputed for %d instruments; %d candidate funds and %d coins looked up in the warehouse" % (len(ranked), len(want), len(syms)))


if __name__ == "__main__":
    main()
