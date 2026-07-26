"""
justhodl-tradingview v3.0 — CADENCE-AWARE METRIC VAULT (brain-constitutional).

v3.0 architecture (Khalid 2026-07-26): rate limits are respected by design —
every symbol carries a RELEASE CADENCE (daily/weekly/monthly/quarterly) and
the vault caches its own previous output, re-fetching a symbol ONLY when its
data could actually have updated. Monthly FRED series hit the API ~1 run in
27 instead of every run; NO_FREE_SOURCE retries monthly. This kills the 429
flap that faked ~30 symbols as unresolved.

Resolver ladder: alias kinds fred / yoy / yahoo / fleet / fleetsum / fleetany
(multi-candidate paths) / ecb (ECB Data Portal, free) / poly (Polygon
prev-close, paid key) / disc / none / meta -> suffix-strip retry
(-TVC/-FRED/-ECONOMICS) -> second-chance FRED (alnum) -> Yahoo ^ -> Yahoo
plain -> Polygon prev -> documented NO_FREE_SOURCE. FRED throttled 0.55s +
one retry on failure.

Registry: parsed live from data/brain.json [TV:*] tags + brain-text scan.
Output: data/tradingview.json
"""
import json
import os
import re
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import boto3

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
POLY_KEY = os.environ.get("POLYGON_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/tradingview.json"
MARKER = "tradingview-vault v3.0 CADENCE-AWARE"

s3 = boto3.client("s3")
_FRED_CALLS = {"n": 0}
_FLEET_CACHE = {}

EQ_EX = {"NASDAQ", "NYSE", "AMEX", "DUS", "MC", "TSE", "HKEX", "TWSE", "KRX"}
CAT_RULES = [
    ("credit", re.compile(r"^BAML|^HQMCB|FALN|^JNK$|^HYG$|^SJNK$|^BKLN$|^CLOZ$|^IBHY$|^HYEM$|^HYXU$|^FLTR$|^CWB$")),
    ("plumbing", re.compile(r"^RRP|^RPON|^WREPO|^WRESBAL|^WTREGEN|^WDTGAL|^TOTRESNS|^BOGMB|^CASACB|^WALCL|^WLRRA|^WORAL|^WRBWFRBL|^H41RES|^MMMF|^SOFR|^AMERIBOR|^CPFF|^DCPN|^DCPF|^RIFSPP|^TEDRATE|^DPCREDIT|^EFFR$|^IORB$|^WLCFL|^WLODLL")),
    ("rates", re.compile(r"^US\d{2}M?Y$|^DGS|^DTB|^TB4WK|^FEDFUNDS|^THREEF|^TNX$|INTR$|^GB\d{2}Y|^EU\d{2}Y|^DE02Y|^JP0|^CH0|^CN\d{2}Y|^IT10Y|^NO03Y|^SS03|^GDPNOW")),
    ("vol", re.compile(r"^MOVE$|^VIX|^VVIX$|^SKEW$|^VOLI$|^VX|^SDEX$|^TDEX$|^UVXY$|^SVXY$|^NFCI")),
    ("fx", re.compile(r"^DXY$|^DTWEX|^RBUSBIS|^TWEXP|^XAUUSD|^USDX$|^USD$|^XD[NE]$|^UUP$|^USDU$|^6J")),
    ("commodity", re.compile(r"^GOLD$|^SILVER$|^WTI$|^USOIL$|^COPPER$|^PLATINUM$|^PALLADIUM$|^P[A-Z]{3,5}USDM$|^PIOREC|^PRAWM|^BDI$|^CPER$|^UGA$|^GC\d|^HG\d|^CL\d")),
    ("macro", re.compile(r"YY$|^UNEMPLOY|^ICSA$|^HOUST|^USNFP|^USLEI|^SAHM|CFNAI|^STLPPM|^USALOL|^ONMLOL|PMI$|^JPLG$|TOT$|^BOTOT|^MABOT|^USM[0-2]$|^PPIACO|^GACDFS|^TCU$|^MCUMFN|^TEMPHELP|^MSACSR|^AISRSA|^USHMI|^USBCOI|^DRTSC|^CSCICP|^SPASTT|^MAN_PMI|CLI$")),
]

CADENCE = {"daily": 0, "weekly": 6, "monthly": 27, "quarterly": 85}
CAT_CADENCE = {"macro": "monthly", "credit": "daily", "plumbing": "daily",
               "rates": "daily", "vol": "daily", "fx": "daily",
               "commodity": "daily", "equity": "daily", "etf": "daily",
               "futures": "daily", "other": "weekly"}
CADENCE_OVERRIDE = {
    "WRESBAL": "weekly", "TOTRESNS": "monthly", "BOGMBBM": "monthly",
    "WALCL": "weekly", "H41RESH4ENWW": "weekly", "USM1": "monthly",
    "USM2": "monthly", "USM0": "monthly", "JPM3": "monthly",
    "INDPRO": "monthly", "UNEMPLOY": "monthly", "ICSA": "weekly",
}
GDP_RX = re.compile(r"GDPYY$|^USGFCF|CA$")

ALIASES = {
    "US01Y": "fred:DGS1", "US02Y": "fred:DGS2", "US03Y": "fred:DGS3",
    "US05Y": "fred:DGS5", "US10Y": "fred:DGS10", "US30Y": "fred:DGS30",
    "US01MY": "fred:DGS1MO", "US03MY": "fred:DGS3MO", "US06MY": "fred:DGS6MO",
    "TNX": "fred:DGS10", "VIX": "fred:VIXCLS", "WTI": "fred:DCOILWTICO",
    "USOIL": "fred:DCOILWTICO", "TEDRATE": "fred:TEDRATE",
    "US02MY": "none:no DGS2MO series", "SOFR30DAYAVG": "fred:SOFR30DAYAVG",
    "USRR": "fred:RRPONTSYD",
    "USIRYY": "yoy:CPIAUCSL", "JPIRYY": "yoy:JPNCPIALLMINMEI",
    "USGDPYY": "yoy:GDPC1", "CNIRYY": "yoy:CHNCPIALLMINMEI",
    "DEIRYY": "yoy:DEUCPIALLMINMEI", "EUIRYY": "yoy:CP0000EZ19M086NEST",
    "USMPRYY": "yoy:PPIACO", "USHPIYY": "yoy:CSUSHPINSA",
    "CNGDPYY": "yoy:CHNGDPNQDSMEI", "JPGDPYY": "yoy:JPNRGDPEXP",
    "EUGDPYY": "yoy:CLVMNACSCAB1GQEA19", "FIGDPYY": "yoy:CLVMNACSCAB1GQFI",
    "CLGDPYY": "yoy:CLVMNACSAB1GQCL", "CNPPIYY": "none:NBS series not on FRED",
    "JPPPIYY": "yoy:PITGCG01JPM661N",
    "USIJC": "fred:ICSA", "USJC": "fred:ICSA", "USCFNAI": "fred:CFNAI",
    "USLEI": "fred:USSLIND", "USNFP": "fred:PAYEMS", "USHST": "fred:HOUST",
    "USNHS": "fred:HSN1F", "USCBBS": "fred:WALCL", "USM0": "fred:BOGMBASE",
    "USM1": "fred:M1SL", "USM2": "fred:M2SL", "USMNO": "fred:AMTMNO",
    "USINBR": "fred:BUSINV", "USBLR": "fred:DRTSCILM",
    "USGFCF": "fred:GPDIC1", "USTOT": "none:BLS ToT not on FRED as level",
    "USFDI": "fred:ROWFDIQ027S", "USCF": "none:ambiguous TE composite",
    "CAUR": "fred:LRUNTTTTCAM156S", "CHUR": "fred:LMUNRRTTCHM156S",
    "EUINTR": "fred:ECBDFR", "USINTR": "fred:DFF",
    "JPINTR": "fred:IRSTCB01JPM156N", "CHINTR": "fred:IRSTCB01CHM156N",
    "CLINTR": "fred:IRSTCB01CLM156N",
    "CNINTR": "none:LPR not on FRED", "TWINTR": "none:CBC not on FRED",
    "CNCA": "fred:CHNBCABP6USD",
    "EUCA": "ecb:BP6/M.N.I8.W1.S1.S1.T.B.CA._Z._Z._Z.EUR._T._X.N",
    "JPM3": "fred:MABMM301JPM189S",
    "IT10Y": "fred:IRLTLT01ITM156N", "GB10Y": "fred:IRLTLT01GBM156N",
    "EU10Y": "fred:IRLTLT01DEM156N", "GB30Y": "none:no free UK 30Y series",
    "EU02Y-TVC": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    "EU03Y": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y",
    "EU30Y-TVC": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y",
    "DE02Y": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    "JP02Y": "none:JGB tenors need MOF/BOJ build", "JP03MY": "none:JGB build",
    "CH02Y": "none:SNB tenor build", "CH03Y": "none:SNB tenor build",
    "CN10Y": "none:ChinaBond licensed", "NO03Y": "none:Norges build",
    "SS03": "none:FTSE licensed",
    "USCLI": "fleetany:data/global-business-cycle.json:by_country.USA.cli_level|by_country.United States.cli_level|by_country.US.cli_level",
    "JPCLI": "fleetany:data/global-business-cycle.json:by_country.JPN.cli_level|by_country.Japan.cli_level|by_country.JP.cli_level",
    "DECLI": "fleetany:data/global-business-cycle.json:by_country.DEU.cli_level|by_country.Germany.cli_level|by_country.DE.cli_level",
    "FRCLI": "fleetany:data/global-business-cycle.json:by_country.FRA.cli_level|by_country.France.cli_level|by_country.FR.cli_level",
    "GBCLI": "fleetany:data/global-business-cycle.json:by_country.GBR.cli_level|by_country.United Kingdom.cli_level|by_country.GB.cli_level",
    "ITCLI": "fleetany:data/global-business-cycle.json:by_country.ITA.cli_level|by_country.Italy.cli_level|by_country.IT.cli_level",
    "KRCLI": "fleetany:data/global-business-cycle.json:by_country.KOR.cli_level|by_country.South Korea.cli_level|by_country.KR.cli_level",
    "CNCLI": "fleetany:data/global-business-cycle.json:by_country.CHN.cli_level|by_country.China.cli_level|by_country.CN.cli_level",
    "SPX": "yahoo:^GSPC", "NDX": "yahoo:^NDX", "RUT": "yahoo:^RUT",
    "IXIC": "yahoo:^IXIC", "RUA": "yahoo:^RUA", "MOVE": "yahoo:^MOVE",
    "VIX3M": "yahoo:^VIX3M", "VVIX": "yahoo:^VVIX", "SKEW": "yahoo:^SKEW",
    "DXY": "yahoo:DX-Y.NYB", "USDX": "yahoo:DX-Y.NYB", "NI225": "yahoo:^N225",
    "TOPIX": "yahoo:^TPX", "HSI": "yahoo:^HSI", "DAX": "yahoo:^GDAXI",
    "AEX": "yahoo:^AEX", "SX5E": "yahoo:^STOXX50E", "SXXP": "yahoo:^STOXX",
    "SENSEX": "yahoo:^BSESN", "XJO": "yahoo:^AXJO", "NZ50G": "yahoo:^NZ50",
    "KRX": "yahoo:^KS11", "KOSPI": "yahoo:^KS11", "MDAX": "yahoo:^MDAXI",
    "XAUUSD": "yahoo:GC=F", "EWT": "yahoo:EWT",
    "000300": "yahoo:000300.SS", "000001": "yahoo:000001.SS",
    "000039": "yahoo:000039.SZ", "2330": "yahoo:2330.TW",
    "700": "yahoo:0700.HK", "388": "yahoo:0388.HK", "8604": "yahoo:8604.T",
    "NOVO_B": "yahoo:NOVO-B.CO", "BARC": "yahoo:BARC.L", "AKZA": "yahoo:AKZA.AS",
    "W4500": "none:index not on free feeds",
    "CL1!": "yahoo:CL=F", "GC2!": "yahoo:GC=F", "HG1!": "yahoo:HG=F",
    "6J2!": "yahoo:6J=F", "ZF1!": "yahoo:ZF=F", "SR32!": "yahoo:SR3=F",
    "GOLD": "yahoo:GC=F", "SILVER": "yahoo:SI=F", "COPPER": "yahoo:HG=F",
    "PLATINUM": "yahoo:PL=F", "PALLADIUM": "yahoo:PA=F",
    "2USNOTE": "yahoo:ZT=F", "5USNOTE": "yahoo:ZF=F", "10USNOTE": "yahoo:ZN=F",
    "TOPIX1!": "yahoo:^TPX", "MME1!": "yahoo:EEM",
    "GE1!": "disc:CME eurodollar futures ceased Jun-2023 (SOFR transition)",
    "GE2!": "disc:CME eurodollar futures ceased Jun-2023 (SOFR transition)",
    "BTPBUND": "fleet:data/euro-fragmentation.json:countries.IT.spread_vs_bund_bp",
    "ES10Y-TVC": "fleetsum:data/euro-fragmentation.json:bund_benchmark_10y_pct:countries.ES.spread_vs_bund_bp",
    "FR10Y-TVC": "fleetsum:data/euro-fragmentation.json:bund_benchmark_10y_pct:countries.FR.spread_vs_bund_bp",
    "JPEXPYY": "fleet:data/asia-leads.json:korea_exports.yoy_pct",
    "JPLG": "none:BOJ stat-search build queued (endpoint probe in ops 3928)",
    "CLTOT": "none:TradingEconomics-paywalled (Chile ToT)",
    "PETOT": "none:TradingEconomics-paywalled (Peru ToT)",
    "BDI": "none:referenced in eurodollar-plumbing code, not exported (producer todo)",
    "TWMPMI": "none:S&P Global PMI licensed", "USMPMI": "none:S&P Global PMI licensed",
    "EUMPMI": "none:S&P Global PMI licensed", "JPMPMI": "none:S&P Global PMI licensed",
    "CHMPMI": "none:S&P Global PMI licensed", "WWMPMI": "none:S&P Global PMI licensed",
    "USCPMI": "none:S&P Global PMI licensed", "USNMPR": "none:S&P Global PMI licensed",
    "MAN_PMI": "none:ISM licensed",
    "UNTAGGED": "meta:not a metric — the no-tag note bucket",
}


def _dot(doc, path):
    cur = doc
    for part in path.split("."):
        cur = cur.get(part) if isinstance(cur, dict) else None
        if cur is None:
            return None
    return cur if isinstance(cur, (int, float)) else None


def fleet_value(key, path):
    if key not in _FLEET_CACHE:
        try:
            _FLEET_CACHE[key] = json.loads(
                s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        except Exception:
            _FLEET_CACHE[key] = {}
    v = _dot(_FLEET_CACHE[key], path)
    return None if v is None else {"value": v, "prev": None, "chg_pct": None,
                                   "asof": f"fleet:{key.split('/')[-1]}"}


def fred_latest(series_id):
    _FRED_CALLS["n"] += 1
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=3")
    for attempt in range(2):
        try:
            time.sleep(0.55)
            req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/3.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                obs = [o for o in json.loads(r.read()).get("observations", [])
                       if o.get("value") not in (None, "", ".")]
            if not obs:
                return None
            cur = float(obs[0]["value"])
            prev = float(obs[1]["value"]) if len(obs) > 1 else None
            return {"value": cur, "prev": prev, "asof": obs[0]["date"],
                    "chg_pct": round((cur / prev - 1) * 100, 3) if prev else None}
        except Exception:
            if attempt == 0:
                time.sleep(2.0)
    return None


def fred_yoy(series_id):
    _FRED_CALLS["n"] += 1
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=14")
    for attempt in range(2):
        try:
            time.sleep(0.55)
            req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/3.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                obs = [o for o in json.loads(r.read()).get("observations", [])
                       if o.get("value") not in (None, "", ".")]
            if len(obs) < 5:
                return None
            back = 12 if len(obs) >= 13 else 4
            cur, yr = float(obs[0]["value"]), float(obs[back]["value"])
            return {"value": round((cur / yr - 1) * 100, 2), "prev": None,
                    "chg_pct": None, "asof": obs[0]["date"] + " YoY"}
        except Exception:
            if attempt == 0:
                time.sleep(2.0)
    return None


def yahoo_quote(sym):
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.request.quote(sym)}?range=5d&interval=1d")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            res = json.loads(r.read())["chart"]["result"][0]
        closes = [c for c in res["indicators"]["quote"][0]["close"] if c is not None]
        if not closes:
            return None
        cur = closes[-1]
        prev = closes[-2] if len(closes) > 1 else None
        return {"value": round(cur, 4), "prev": round(prev, 4) if prev else None,
                "chg_pct": round((cur / prev - 1) * 100, 3) if prev else None,
                "asof": "yahoo_5d"}
    except Exception:
        return None


def ecb_latest(flow_key):
    url = (f"https://data-api.ecb.europa.eu/service/data/{flow_key}"
           f"?format=jsondata&lastNObservations=1")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/3.0",
                                                   "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read())
        series = d["dataSets"][0]["series"]
        obs = list(series.values())[0]["observations"]
        val = list(obs.values())[0][0]
        dates = d["structure"]["dimensions"]["observation"][0]["values"]
        asof = dates[-1]["id"] if dates else "ecb"
        return {"value": round(float(val), 4), "prev": None, "chg_pct": None,
                "asof": f"ecb:{asof}"}
    except Exception:
        return None


def poly_prev(sym):
    if not POLY_KEY:
        return None
    url = f"https://api.polygon.io/v2/aggs/ticker/{sym}/prev?apiKey={POLY_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/3.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            res = json.loads(r.read()).get("results") or []
        if not res:
            return None
        c, o = res[0].get("c"), res[0].get("o")
        return {"value": c, "prev": o,
                "chg_pct": round((c / o - 1) * 100, 3) if (c and o) else None,
                "asof": "polygon_prev"}
    except Exception:
        return None


def fmp_quotes(symbols):
    out = {}
    for i in range(0, len(symbols), 40):
        chunk = symbols[i:i + 40]
        url = (f"https://financialmodelingprep.com/stable/batch-quote?"
               f"symbols={','.join(chunk)}&apikey={FMP_KEY}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JH-TV-Vault/3.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                for q in json.loads(r.read()):
                    if isinstance(q, dict) and q.get("price") is not None:
                        out[q.get("symbol")] = {"value": q.get("price"),
                                                "prev": q.get("previousClose"),
                                                "chg_pct": q.get("changePercentage"),
                                                "asof": "live"}
        except Exception:
            pass
        time.sleep(0.25)
    return out


def get_brain():
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/brain.json")["Body"].read())


def build_registry(brain):
    reg = {}
    for n in brain.get("notes", []):
        txt = n.get("text") or ""
        m = re.match(r"\[TV:([A-Z0-9_:!\.\-]+)\]", txt)
        if not m:
            continue
        parts = m.group(1).split(":")
        ex = parts[0] if len(parts) > 1 else ""
        sym = parts[-1]
        r = reg.setdefault(sym, {"symbol": sym, "n_notes": 0, "exchanges": set(),
                                 "note_ids": [], "note_snippet": ""})
        r["n_notes"] += 1
        if ex:
            r["exchanges"].add(ex)
        if len(r["note_ids"]) < 4:
            r["note_ids"].append(n.get("id"))
        body = txt[len(m.group(0)):].strip()
        if len(body) > len(r["note_snippet"]):
            r["note_snippet"] = body[:400]
    for n in brain.get("notes", []):
        txt = n.get("text") or ""
        for ex, sym in re.findall(r"\b(FRED|ECONOMICS|AMEX|NASDAQ|NYSE|TVC|ICEUS):"
                                  r"([A-Z0-9_\.]{2,22})\b", txt):
            if sym in reg:
                continue
            reg[sym] = {"symbol": sym, "n_notes": 1, "exchanges": {ex},
                        "note_ids": [n.get("id")], "note_snippet": txt[:300],
                        "origin": "brain_text"}
    for r in reg.values():
        r["exchanges"] = sorted(r["exchanges"])
        r.setdefault("origin", "tv_tag")
    return reg


def categorize(sym, exchanges):
    if sym.endswith("!"):
        return "futures"
    for cat, rx in CAT_RULES:
        if rx.search(sym):
            return cat
    if "ECONOMICS" in exchanges:
        return "macro"
    if sym.isdigit():
        return "equity"
    if len(sym) <= 5 and sym.isalpha():
        return "equity"
    return "other"


def route(sym, exchanges, cat):
    if "FRED" in exchanges:
        return "fred"
    if sym.endswith("!"):
        return "unresolved_futures"
    if "ECONOMICS" in exchanges:
        return "unresolved_economics"
    if set(exchanges) & EQ_EX or cat in ("equity", "etf"):
        return "fmp"
    return "unresolved_tv_only"


def cadence_for(sym, cat):
    if sym in CADENCE_OVERRIDE:
        return CADENCE_OVERRIDE[sym]
    if GDP_RX.search(sym):
        return "quarterly"
    return CAT_CADENCE.get(cat, "weekly")


def resolve_alias(row, al):
    kind, _, tgt = al.partition(":")
    if kind == "fred":
        v = fred_latest(tgt)
        if v:
            row["source"] = f"fred_alias:{tgt}"
        return v, None
    if kind == "yoy":
        v = fred_yoy(tgt)
        if v:
            row["source"] = f"fred_yoy:{tgt}"
        return v, None
    if kind == "yahoo":
        v = yahoo_quote(tgt)
        if v:
            row["source"] = f"yahoo:{tgt}"
        return v, None
    if kind == "ecb":
        v = ecb_latest(tgt)
        if v:
            row["source"] = f"ecb:{tgt.split('/')[0]}"
        return v, None
    if kind == "poly":
        v = poly_prev(tgt)
        if v:
            row["source"] = f"polygon:{tgt}"
        return v, None
    if kind == "fleet":
        fk, _, fp = tgt.partition(":")
        v = fleet_value(fk, fp)
        if v:
            row["source"] = f"fleet:{fk}"
        return v, None
    if kind == "fleetsum":
        fk, base_p, add_p = tgt.split(":")
        b_, a_ = fleet_value(fk, base_p), fleet_value(fk, add_p)
        if b_ and a_:
            row["source"] = f"fleetsum:{fk}"
            return {"value": round(b_["value"] + a_["value"] / 100.0, 3),
                    "prev": None, "chg_pct": None,
                    "asof": f"fleet:{fk.split('/')[-1]} computed"}, None
        return None, None
    if kind == "fleetany":
        fk, _, paths = tgt.partition(":")
        for fp in paths.split("|"):
            v = fleet_value(fk, fp)
            if v:
                row["source"] = f"fleet:{fk}:{fp}"
                return v, None
        return None, None
    if kind == "disc":
        return None, ("DISCONTINUED", tgt)
    if kind == "none":
        return None, ("NO_FREE_SOURCE", tgt)
    if kind == "meta":
        return None, ("META", tgt)
    return None, None


def ladder(row):
    sym = row["symbol"]
    al = ALIASES.get(sym)
    if al:
        v, terminal = resolve_alias(row, al)
        if terminal:
            row["status"], row["resolution_note"] = terminal
            row["value"] = None
            return False
        if v:
            row.update(v)
            row["status"] = "LIVE"
            return True
    base = re.sub(r"-(TVC|FRED|ECONOMICS)$", "", sym)
    if base != sym:
        al2 = ALIASES.get(base)
        if al2:
            v, terminal = resolve_alias(row, al2)
            if terminal:
                row["status"], row["resolution_note"] = terminal
                row["value"] = None
                return False
            if v:
                row.update(v)
                row["status"] = "LIVE"
                row["source"] = (row.get("source") or "") + f" (base:{base})"
                return True
        if base.isalnum():
            v = fred_latest(base)
            if v:
                row.update(v)
                row["status"] = "LIVE"
                row["source"] = f"fred_2nd_chance:{base}"
                return True
    if not al and sym.isalnum() and 3 <= len(sym) <= 22 and not sym.endswith("!"):
        v = fred_latest(sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = "fred_2nd_chance"
            return True
    if not al and sym.isalpha() and len(sym) <= 6:
        v = yahoo_quote("^" + sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = f"yahoo:^{sym}"
            return True
    if not al and sym.isalpha() and 2 <= len(sym) <= 5:
        v = yahoo_quote(sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = f"yahoo:{sym}"
            return True
        v = poly_prev(sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = f"polygon:{sym}"
            return True
    row["status"] = "NO_FREE_SOURCE"
    row["value"] = None
    row.setdefault("resolution_note", "no free API found (TV/TradingEconomics only)")
    return False


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print(f"[tv-vault] {MARKER}")

    try:
        prev = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
        cache = {r["symbol"]: r for r in prev.get("symbols") or []}
    except Exception:
        cache = {}

    brain = get_brain()
    reg = build_registry(brain)
    rows, fmp_syms = [], []
    for sym, r in reg.items():
        ex = set(r["exchanges"])
        cat = categorize(sym, ex)
        row = dict(r)
        row["category"] = cat
        row["source"] = route(sym, ex, cat)
        row["cadence"] = cadence_for(sym, cat)
        rows.append(row)
        if row["source"] == "fmp" and sym not in ALIASES:
            fmp_syms.append(sym)

    fmp_vals = fmp_quotes(fmp_syms)
    n_live = n_cached = 0
    force = bool((event or {}).get("force"))
    for row in rows:
        sym = row["symbol"]
        c = cache.get(sym) or {}
        fetched_at = c.get("fetched_at")
        fresh_days = CADENCE.get(row["cadence"], 0)
        if not force and fetched_at:
            try:
                age_d = (now - datetime.fromisoformat(fetched_at)).total_seconds() / 86400
            except Exception:
                age_d = 999
            retry_gate = 27 if c.get("status") == "NO_FREE_SOURCE" else fresh_days
            if age_d < retry_gate and c.get("status") in ("LIVE", "NO_FREE_SOURCE",
                                                          "DISCONTINUED", "META"):
                for k in ("value", "prev", "chg_pct", "asof", "status", "source",
                          "resolution_note", "fetched_at"):
                    if k in c:
                        row[k] = c[k]
                row["cached"] = True
                if row["status"] == "LIVE":
                    n_live += 1
                n_cached += 1
                continue
        if row["source"] == "fmp" and sym in fmp_vals:
            row.update(fmp_vals[sym])
            row["status"] = "LIVE"
            n_live += 1
        else:
            if ladder(row):
                n_live += 1
        row["fetched_at"] = now.isoformat()

    rows.sort(key=lambda r: (-r["n_notes"], r["symbol"]))
    by_cat = defaultdict(list)
    by_status = defaultdict(int)
    for r in rows:
        by_cat[r["category"]].append(r["symbol"])
        by_status[r["status"]] += 1

    out = {
        "engine": "justhodl-tradingview",
        "version": "3.0",
        "marker": MARKER,
        "generated_at": now.isoformat(),
        "brain_constitution": "registry parsed live from data/brain.json [TV:*] tags — "
                              "self-updating; every symbol carries its note ids",
        "cadence_model": {"daily_refetch": "every run", "weekly": ">6d",
                          "monthly": ">27d", "quarterly": ">85d",
                          "no_free_source_retry": ">27d",
                          "rationale": "fetch only when the data can have updated — "
                                       "respects FRED 120/min + all provider limits"},
        "n_symbols": len(rows),
        "n_tv_notes": sum(r["n_notes"] for r in rows),
        "n_live": n_live,
        "n_cached_this_run": n_cached,
        "fred_calls_this_run": _FRED_CALLS["n"],
        "n_unresolved": by_status.get("NO_FREE_SOURCE", 0),
        "coverage_pct": round(n_live / max(1, len(rows)) * 100, 1),
        "status_counts": dict(by_status),
        "by_category_counts": {k: len(v) for k, v in sorted(by_cat.items())},
        "symbols": rows,
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="max-age=900")
    print(f"[tv-vault] DONE {out['elapsed_s']}s live={n_live}/{len(rows)} "
          f"cached={n_cached} fred_calls={_FRED_CALLS['n']}")
    return {"ok": True, "n_symbols": len(rows), "n_live": n_live,
            "n_cached": n_cached, "fred_calls": _FRED_CALLS["n"],
            "coverage_pct": out["coverage_pct"]}
