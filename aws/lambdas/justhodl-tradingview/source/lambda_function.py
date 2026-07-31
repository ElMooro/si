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
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

import boto3

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
POLY_KEY = os.environ.get("POLYGON_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/tradingview.json"
MARKER = "tradingview-vault v3.23.0 ops4167 mei"

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
GDP_RX = re.compile(r"GDPYY$|^USGFCF|CA$|GDG$")

ALIASES = {
    "US01Y": "fred:DGS1", "US02Y": "fred:DGS2", "US03Y": "fred:DGS3",
    "US05Y": "fred:DGS5", "US10Y": "fred:DGS10", "US30Y": "fred:DGS30",
    "US01MY": "fred:DGS1MO", "US03MY": "fred:DGS3MO", "US06MY": "fred:DGS6MO",
    "TNX": "fred:DGS10", "VIX": "fred:VIXCLS", "WTI": "fred:DCOILWTICO",
    "USOIL": "fred:DCOILWTICO", "TEDRATE": "fred:TEDRATE",
    "US02MY": "ust:2 Mo", "SOFR30DAYAVG": "fred:SOFR30DAYAVG",
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
    
    "IT10Y": "fred:IRLTLT01ITM156N", "GB10Y": "fred:IRLTLT01GBM156N",
    "EU10Y": "fred:IRLTLT01DEM156N", "GB30Y": "none:BoE IADB max par tenor = 20Y (IUDLNPY 5.27 live); no official 30Y series",
    "EU03Y": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y",
    "DE02Y": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y",
    # ops 3970 discovery: confirmed live at 3.2247 from the ECB Data Portal
    # (same proven YC adapter as EU03Y/DE02Y, 10Y tenor). EUBUND carries 4 of
    # his notes and was sitting NO_FREE_SOURCE.
    "EUBUND": "ecb:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
    "JP02Y": "mofjp:2Y", "JP03MY": "none:3M TB not in MOF JGB CSV — BOJ API next",
    # ops 4001-4002 expansion (Khalid: pull all important BOJ/MOF/NORGES/BCRP
    # data). Every alias below was LIVE-confirmed with a real value and, for
    # BCRP, a full-name check ("Reservas Internacionales Netas") — the
    # JPEXPYY same-measure/same-country bar. BOJ M-aggregates (JPMB/JPM2YY/
    # JPM3) deferred: codes found, getDataCode returns empty for them —
    # response-shape work, not guessed.
    "JP01Y": "mofjp:1Y", "JP05Y": "mofjp:5Y", "JP10Y": "mofjp:10Y",
    "JP20Y": "mofjp:20Y", "JP30Y": "mofjp:30Y",
    "NOINTR": "norges:IR:KPRA",
    "NO10Y": "norges:GOVT_GENERIC_RATES:10",
    "PEINTR": "bcrp:PD04722MM", "PENUSD": "bcrp:PN01207PM",
    "PEFER": "bcrp:PN00026MM",
    # ops 4009-4010 — BOJ family + six-country liquidity/risk/output layer.
    # BOJ M-aggregates: the "empty" of ops-4002 was a lowercase-regex probe
    # bug; RESULTSET.VALUES parses with the existing adapter (MonBase running
    # ~-13.7% YoY as of 202606 — the yen-QT signal). Tankan/call rate go
    # through the new LEVEL adapter (YoY would be silently wrong). FRED-MEI
    # aliases verify at RUNTIME (fredgraph is bot-walled to the runner; the
    # Lambda key resolves them daily). Duplicate keys later in this dict
    # OVERRIDE earlier ones — that is how JPM3 gets repointed to the BOJ.
    "JPMB": "boj:MD01:MABS1AN11",
    "JPM2YY": "boj:MD02:MAM1NAM2M2MO",
    "JPM3": "boj:MD02:MAM1NAM3M3MO",
    "JPTANKAN": "bojlvl:CO:TK99F1000601GCQ00000",
    "JPCALLR": "bojlvl:FM01:STRDCLUCON",
    "BRINTR": "bcb:432", "BRFER": "bcb:13621", "BRLUSD": "bcb:1",
    "FIIPYY": "yoy:FINPROINDMISMEI", "ESIPYY": "yoy:ESPPROINDMISMEI",
    "ITIPYY": "yoy:ITAPROINDMISMEI", "CHIPYY": "yoy:CHEPROINDMISMEI",
    "KRIPYY": "yoy:KORPROINDMISMEI", "BRIPYY": "yoy:BRAPROINDMISMEI",
    "IT10Y": "fred_alias:IRLTLT01ITM156N", "ES10Y": "fred_alias:IRLTLT01ESM156N",
    "FI10Y": "fred_alias:IRLTLT01FIM156N", "CH10Y": "fred_alias:IRLTLT01CHM156N",
    "KR10Y": "fred_alias:IRLTLT01KRM156N",
    "CHINTR": "fred_alias:IRSTCI01CHM156N",
    "KRINTR": "fred_alias:INTDSRKRM193N",
    "CH02Y": "snb:2 year", "CH03Y": "snb:3 year",
    "CN10Y": "none:ChinaBond licensed", "NO03Y": "norges:GOVT_GENERIC_RATES:3",
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
    # ops 3973 INTEGRITY FIX: JPEXPYY (JAPAN exports YoY) was wired to KOREA's
    # export YoY and resolved LIVE at 47.96 — one country's number published
    # under another country's symbol, carrying his Japan notes and voting in
    # the MACRO barometer. Repointed to Japan's own series in the same FRED
    # family Korea uses (XTEXVA01KRM667N -> XTEXVA01JPM667N).
    "JPEXPYY": "yoy:XTEXVA01JPM667N",
    # ops 3973: the fleet already held these — the Asia Arc (3582-3634) and
    # boj-detail landed them while the vault still called them NO_FREE_SOURCE.
    # Both are the SAME measure for the SAME country, which is the bar that
    # the JPEXPYY bug failed.
    "TWEXPYY": "fleet:data/asia-leads.json:taiwan_exports.yoy_pct",
    "JPIRYY": "fleet:data/boj-detail.json:inflation.cpi_yoy_pct",
    "TOPIX": "yahoo:^TPX",
    "JPLG": "boj:MD11:DLCLAADBLTTO|MD11:DLCLBADBLTTO",
    "CLTOT": "none:TradingEconomics-paywalled (Chile ToT)",
    "PETOT": "bcrp:PN38923BM",
    "BDI": "none:referenced in eurodollar-plumbing code, not exported (producer todo)",
    "TWMPMI": "none:S&P Global PMI licensed", "USMPMI": "none:S&P Global PMI licensed",
    "EUMPMI": "none:S&P Global PMI licensed", "JPMPMI": "none:S&P Global PMI licensed",
    "CHMPMI": "none:S&P Global PMI licensed", "WWMPMI": "none:S&P Global PMI licensed",
    "USCPMI": "none:S&P Global PMI licensed", "USNMPR": "none:S&P Global PMI licensed",
    "MAN_PMI": "none:ISM licensed",
    "USFER": "imf:M.US.RAF_USD", "EUFER": "imf:M.U2.RAF_USD",
    "JPFER": "imf:M.JP.RAF_USD", "CHFER": "imf:M.CH.RAF_USD",
    "ITGDG": "estat:gov_10dd_edpt1|na_item=GD&sector=S13&unit=PC_GDP|IT",
    "ESGDG": "estat:gov_10dd_edpt1|na_item=GD&sector=S13&unit=PC_GDP|ES",
    "EUGDG": "estat:gov_10dd_edpt1|na_item=GD&sector=S13&unit=PC_GDP|EA20|EA19",
    "GBGDG": "none:left Eurostat post-Brexit — ONS build queued",
    "JPGDG": "none:IMF new data.imf.org API discovery queued",
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
    # v3.13.1 — ops 4093 added the cftc adapter to resolve_direct only, which
    # is the REFETCH path used once a symbol already has a proven source. The
    # FIRST resolution of an alias comes through here, so every one of the 65
    # verified COT aliases fell straight through to "no source" and the gate
    # correctly reported zero cftc values despite perfect resolution upstream.
    # Two entry points, both must know the kind.
    if kind == "cftc":
        v = cftc_latest(tgt)
        if v:
            row["source"] = f"cftc:{tgt.split(':')[1]}"
            row["resolved_via"] = f"cftc:{tgt}"
        return v, None
    if kind == "fred":
        v = fred_latest(tgt)
        if v:
            row["source"] = f"fred_alias:{tgt}"
            row["resolved_via"] = f"fred:{tgt}"
        return v, None
    if kind == "yoy":
        v = fred_yoy(tgt)
        if v:
            row["source"] = f"fred_yoy:{tgt}"
            row["resolved_via"] = f"yoy:{tgt}"
        return v, None
    if kind == "yahoo":
        v = yahoo_quote(tgt)
        if v:
            row["source"] = f"yahoo:{tgt}"
            row["resolved_via"] = f"yahoo:{tgt}"
        return v, None
    if kind == "ecb":
        v = ecb_latest(tgt)
        if v:
            row["source"] = f"ecb:{tgt.split('/')[0]}"
            row["resolved_via"] = f"ecb:{tgt}"
        return v, None
    if kind == "poly":
        v = poly_prev(tgt)
        if v:
            row["source"] = f"polygon:{tgt}"
            row["resolved_via"] = f"poly:{tgt}"
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
    if kind == "ust":
        v = ust_latest(tgt)
        if v:
            row["source"] = "treasury.gov"
            row["resolved_via"] = f"ust:{tgt}"
        return v, None
    if kind == "estat":
        v = estat_latest(tgt)
        if v:
            row["source"] = "eurostat"
            row["resolved_via"] = f"estat:{tgt}"
        return v, None
    if kind == "norges":
        v = norges_latest(tgt)
        if v:
            row["source"] = "norges-bank"
            row["resolved_via"] = f"norges:{tgt}"
        return v, None
    if kind == "bcrp":
        v = bcrp_latest(tgt)
        if v:
            row["source"] = "bcrp-peru"
            row["resolved_via"] = f"bcrp:{tgt}"
        return v, None
    if kind == "mofjp":
        v = mofjp_latest(tgt)
        if v:
            row["source"] = "mof-japan"
            row["resolved_via"] = f"mofjp:{tgt}"
        return v, None
    if kind == "boe":
        v = boe_latest(tgt)
        if v:
            row["source"] = "bank-of-england"
            row["resolved_via"] = f"boe:{tgt}"
        return v, None
    if kind == "snb":
        v = snb_latest(tgt)
        if v:
            row["source"] = "snb"
            row["resolved_via"] = f"snb:{tgt}"
        return v, None
    if kind == "imf":
        v = imf_latest(tgt)
        if v:
            row["source"] = "imf"
            row["resolved_via"] = f"imf:{tgt}"
        return v, None
    if kind == "boj":
        v = boj_yoy(tgt)
        if v:
            row["source"] = "bank-of-japan"
            row["resolved_via"] = f"boj:{tgt}"
        return v, None
    if kind == "bojlvl":
        v = boj_level(tgt)
        if v:
            row["source"] = "bank-of-japan"
            row["resolved_via"] = f"bojlvl:{tgt}"
        return v, None
    if kind == "bcb":
        v = bcb_latest(tgt)
        if v:
            row["source"] = "bcb-brazil"
            row["resolved_via"] = f"bcb:{tgt}"
        return v, None
    if kind == "disc":
        return None, ("DISCONTINUED", tgt)
    if kind == "none":
        return None, ("NO_FREE_SOURCE", tgt)
    if kind == "meta":
        return None, ("META", tgt)
    return None, None


_UST_CACHE = {}


def ust_latest(col):
    """US Treasury daily par yield curve CSV — official gov source."""
    if "rows" not in _UST_CACHE:
        url = ("https://home.treasury.gov/resource-center/data-chart-center/"
               "interest-rates/daily-treasury-rates.csv/2026/all"
               "?type=daily_treasury_yield_curve&field_tdr_date_value=2026&_format=csv")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                lines = r.read().decode("utf-8", "ignore").strip().splitlines()
            hdr = [h.strip().strip('"') for h in lines[0].split(",")]
            first = [c.strip().strip('"') for c in lines[1].split(",")]
            _UST_CACHE["rows"] = (hdr, first)
        except Exception:
            _UST_CACHE["rows"] = None
    if not _UST_CACHE.get("rows"):
        return None
    hdr, first = _UST_CACHE["rows"]
    try:
        i = hdr.index(col)
        v = float(first[i])
        return {"value": v, "prev": None, "chg_pct": None,
                "asof": f"treasury.gov:{first[0]}"}
    except Exception:
        return None


def estat_latest(tgt):
    """Eurostat JSON API. tgt = dataset|params|geo1|geo2..."""
    parts = tgt.split("|")
    ds, params, geos = parts[0], parts[1], parts[2:]
    for geo in geos:
        url = (f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
               f"{ds}?format=JSON&lang=EN&freq=A&{params}&geo={geo}&lastTimePeriod=1")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            vals = d.get("value") or {}
            if vals:
                t_idx = (d.get("dimension", {}).get("time", {})
                         .get("category", {}).get("index", {}))
                t_lbl = sorted(t_idx.keys())[-1] if t_idx else "latest"
                return {"value": float(list(vals.values())[0]), "prev": None,
                        "chg_pct": None, "asof": f"eurostat:{geo}:{t_lbl}"}
        except Exception:
            continue
    return None


def norges_latest(tgt):
    """Norges Bank SDMX-JSON. tgt = flow:tenor_match (e.g. GOVT_GENERIC_RATES:3)."""
    flow, _, match = tgt.partition(":")
    url = (f"https://data.norges-bank.no/api/data/{flow}"
           f"?format=sdmx-json&lastNObservations=1")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            d = json.loads(r.read())
        data = d.get("data") or d
        series = data["dataSets"][0]["series"]
        struct = data.get("structure") or (data.get("structures") or [{}])[0]
        dims = struct["dimensions"]["series"]
        # find the tenor-ish dimension index and target value position
        want = None
        for di, dim in enumerate(dims):
            for vi, val in enumerate(dim.get("values") or []):
                idn = (str(val.get("id", "")) + " " + str(val.get("name", ""))).upper()
                if match.upper() + "Y" in idn.replace(" ", "") or                    (match in idn and "YEAR" in idn):
                    want = (di, vi)
                    break
            if want:
                break
        for skey, sval in series.items():
            pos = [int(x) for x in skey.split(":")]
            if want and pos[want[0]] != want[1]:
                continue
            obs = sval.get("observations") or {}
            if obs:
                v = list(obs.values())[0][0]
                return {"value": round(float(v), 4), "prev": None, "chg_pct": None,
                        "asof": "norges-bank"}
    except Exception:
        pass
    return None


_MOF_CACHE = {}


def mofjp_latest(col):
    """Japan MOF official JGB par-yield CSV. mof.go.jp blocks AWS IPs (runner
    fetched fine, Lambda 3942 could not) -> direct first, then the /gov edge
    proxy (Cloudflare IPs pass)."""
    if "rows" not in _MOF_CACHE:
        base = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
        urls = [base,
                "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
                + urllib.request.quote(base, safe="")]
        _MOF_CACHE["rows"] = None
        _MOF_CACHE["diag"] = []
        for url in urls:
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=25) as r:
                    body = r.read()
                lines = [l for l in body.decode("utf-8", "ignore").splitlines()
                         if l.strip()]
                if len(lines) < 3 or "Date" not in lines[1]:
                    _MOF_CACHE["diag"].append(
                        f"{url[:60]} -> {len(body)}b noDate "
                        f"head={body[:80].decode('utf-8','ignore')!r}")
                    continue
                hdr = [h.strip() for h in lines[1].split(",")]
                data_rows = [[c.strip() for c in l.split(",")] for l in lines[2:]]
                _MOF_CACHE["rows"] = (hdr, data_rows)
                _MOF_CACHE["diag"].append(f"{url[:60]} -> OK {len(lines)} lines")
                break
            except Exception as e:
                _MOF_CACHE["diag"].append(f"{url[:60]} -> EXC {type(e).__name__}: {str(e)[:90]}")
                continue
    if not _MOF_CACHE.get("rows"):
        return None
    hdr, rows = _MOF_CACHE["rows"]
    try:
        i = hdr.index(col)
        for last in reversed(rows):
            if re.match(r"^\d{4}/", last[0]) and len(last) > i and last[i] not in ("", "-"):
                return {"value": float(last[i]), "prev": None, "chg_pct": None,
                        "asof": f"mof.go.jp:{last[0]}"}
    except Exception:
        pass
    return None


def boe_latest(series_code):
    """Bank of England IADB CSV API (the /iadb/ path returns pure CSV)."""
    url = ("https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
           f"?csv.x=yes&SeriesCodes={series_code}&CSVF=TN&UsingCodes=Y"
           "&Datefrom=01/Jan/2026")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            lines = [l for l in r.read().decode("utf-8", "ignore").splitlines()
                     if l.strip()]
        if len(lines) < 2 or "DATE" not in lines[0].upper():
            return None
        last = lines[-1].split(",")
        return {"value": float(last[1]), "prev": None, "chg_pct": None,
                "asof": f"boe:{last[0]}"}
    except Exception:
        return None


def bcb_latest(code):
    """Banco Central do Brasil SGS open API (no key). Selic=432, NIR=13621,
    USD/BRL=1 — code-canonical, value sanity-gated in the verifier."""
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}"
           f"/dados/ultimos/2?formato=json")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            d = json.loads(r.read())
        xs = [float(str(x.get("valor")).replace(",", ".")) for x in d
              if x.get("valor") not in (None, "")]
        if xs:
            v = xs[-1]
            pv = xs[-2] if len(xs) > 1 else None
            return {"value": round(v, 4), "prev": pv,
                    "chg_pct": round((v / pv - 1) * 100, 2) if pv else None,
                    "asof": str(d[-1].get("data"))}
    except Exception:
        pass
    return None


def bcrp_latest(code):
    """Banco Central de Reserva del Peru official series API (no key)."""
    yr = datetime.now(timezone.utc).year
    url = (f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/"
           f"{code}/json/{yr-1}-1/{yr}-12")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            d = json.loads(r.read())
        periods = d.get("periods") or []
        if not periods:
            return None
        last = periods[-1]
        v = float(str(last.get("values", [None])[0]).replace(",", ""))
        return {"value": round(v, 3), "prev": None, "chg_pct": None,
                "asof": f"bcrp:{last.get('name')}"}
    except Exception:
        return None


def snb_latest(match):
    """SNB data portal — rendoblid DAILY Confederation yield cube, windowed
    (rendoblim proven stale at 2025-07 in ops 3946)."""
    frm = (datetime.now(timezone.utc).date().replace(day=1)).isoformat()
    url = f"https://data.snb.ch/api/cube/rendoblid/data/json/en?fromDate={frm}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=25) as r:
            d = json.loads(r.read())
        for ts in d.get("timeseries") or []:
            dim = " ".join(str(h.get("dimItem", "")) for h in ts.get("header") or [])
            if match.lower() in dim.lower():
                vals = ts.get("values") or []
                if not vals:
                    continue
                def _dv(x):
                    if isinstance(x, dict):
                        return str(x.get("date")), x.get("value")
                    return str(x[0]), x[-1]
                dt, v = max((_dv(x) for x in vals), key=lambda t: t[0])
                return {"value": round(float(v), 3), "prev": None, "chg_pct": None,
                        "asof": f"snb:{dt}"}
    except Exception:
        pass
    return None


def imf_latest(key):
    """IMF official SDMX 2.1 API (api.imf.org) — IRFCL reserves etc."""
    base = (f"https://api.imf.org/external/sdmx/2.1/data/IRFCL/{key}"
            f"?lastNObservations=1")
    xml = None
    for url in (base,
                "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
                + urllib.request.quote(base, safe="")):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                xml = r.read().decode("utf-8", "ignore")
            if "OBS_VALUE" in xml:
                break
        except Exception:
            xml = None
    try:
        if xml is None:
            return None
        vals = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', xml)
        tps = re.findall(r'TIME_PERIOD="([^"]+)"', xml)
        if not vals:
            return None
        return {"value": round(float(vals[-1]), 2), "prev": None, "chg_pct": None,
                "asof": f"imf:{tps[-1] if tps else 'latest'}"}
    except Exception:
        return None


FAM_RX = re.compile(r"^(?:ECONOMICS:)?([A-Z]{2})(INTR|FER|GDPYY|IRYY|UR|LG|CBBS|M0|BM|GDG|BOT|DIR|LIR|TOT|CA|RSYY|CIR|INBR|UP|MPRYY)$")   # ops4131: admission stores BARE
_FAM = {}


def _fam_fetch(url):
    """ops4116: 8s fast-fail. Three 900s timeouts (Max Mem 134MB) proved
    the cost was blocking I/O inside the admission loop. Proxy fallback
    only where GOV_ALLOW permits (api.imf.org)."""
    urls = [url]
    if "api.imf.org" in url:
        urls.append("https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
                    + urllib.request.quote(url, safe=""))
    for u in urls:
        try:
            req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception:
            continue
    return ""


def _sdmx_map(t, alen, conv=None):
    d = {}
    for blk in re.split(r"<Series[ >]", t)[1:]:
        a2 = re.search(r'(?:REF_AREA|COUNTRY)="([A-Z0-9]{%d})"' % alen, blk)
        v = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', blk)
        tp = re.findall(r'TIME_PERIOD="([^"]+)"', blk)
        if a2 and v:
            try:
                d[a2.group(1)] = ((conv or float)(v[-1]),
                                  tp[-1] if tp else "latest")
            except Exception:
                pass
    return d


def _families():
    """ops4112 FAMILY ADAPTERS: one bulk call per family per run.
    INTR=BIS CBPOL (daily, BR/PE exact-matched fleet in discovery 4111);
    FER=IMF IRFCL RAF_USD bulk (ISO3) with WB fallback normalized to USD
    mn; GDPYY/IRYY/UR=World Bank mrnev (annual, honest asof)."""
    if _FAM:
        return _FAM
    # ops4121: S3-FIRST — the families-feed lambda pre-computes on its own
    # clock; the vault pays one 1s read instead of five network pulls.
    try:
        _doc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="data/families.json")["Body"].read())
        _f = _doc.get("families") or {}
        if _f.get("INTR"):
            for _k, _v in _f.items():
                _FAM[_k] = {c: (r[0], r[1]) for c, r in (_v or {}).items()}
            print("[tv-vault] families from feed:",
                  {k: len(v) for k, v in _FAM.items()})
            return _FAM
    except Exception:
        pass
    _fb0 = time.time()

    def _left():
        return 60 - (time.time() - _fb0)
    iso23 = {}
    for fam, code in (("GDPYY", "NY.GDP.MKTP.KD.ZG"),
                      ("IRYY", "FP.CPI.TOTL.ZG"),
                      ("UR", "SL.UEM.TOTL.ZS"),
                      ("FERWB", "FI.RES.TOTL.CD")):
        if _left() < 10:
            _FAM.setdefault(fam, {})
            continue
        t = _fam_fetch("https://api.worldbank.org/v2/country/all/indicator/"
                       f"{code}?format=json&mrnev=1&per_page=400")
        try:
            rows = json.loads(t)[1] or []
        except Exception:
            rows = []
        d = {}
        for r in rows:
            v = r.get("value")
            c2 = (r.get("country") or {}).get("id") or ""
            if v is None or len(c2) != 2:
                continue
            vv = float(v)
            if fam == "FERWB":
                vv = round(vv / 1e6, 1)   # USD -> USD millions (IRFCL unit)
            d[c2.upper()] = (round(vv, 2), "wb:%s" % r.get("date"))
            c3 = r.get("countryiso3code")
            if c3:
                iso23[c2.upper()] = c3
        _FAM[fam] = d
    t = "" if _left() < 10 else _fam_fetch(
        "https://stats.bis.org/api/v1/data/WS_CBPOL/D../all"
        "?lastNObservations=1")
    _FAM["INTR"] = {k: (v[0], "bis:" + v[1])
                    for k, v in _sdmx_map(t, 2).items()}
    inv = {v: k for k, v in iso23.items()}
    t = "" if _left() < 10 else _fam_fetch(
        "https://api.imf.org/external/sdmx/2.1/data/IRFCL/"
        "M..RAF_USD?lastNObservations=1")
    d3 = {}
    for a3, (vv, tp) in _sdmx_map(t, 3).items():
        if a3 in inv:
            d3[inv[a3]] = (round(vv, 1), "imf:" + tp)
    _FAM["FER"] = d3
    try:
        print("[tv-vault] families:",
              {k: len(v) for k, v in _FAM.items()},
              "in %.1fs" % (time.time() - _fb0))
    except Exception:
        pass
    return _FAM


_FV = {}


def _fleet_prices():
    """ops4133 FLEET-PRICE JOIN — the fleet already pays for these prices
    (finviz-universe, ~11.3k tickers). One S3 read, dict-hits forever."""
    if _FV:
        return _FV
    try:
        d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                     Key="data/finviz-universe.json")["Body"].read())
        _FV["by"] = d.get("by_ticker") or {}
        _FV["asof"] = str(d.get("generated_at") or "")[:10]
        print("[tv-vault] fleet-prices n=%d asof=%s"
              % (len(_FV["by"]), _FV["asof"]))
    except Exception as e:
        _FV["by"] = {}
        _FV["asof"] = ""
        print("[tv-vault] fleet-prices load failed:", type(e).__name__)
    return _FV


_SF = {}


def _symfeed_try(row):
    """ops4136: intl/FX/index prices from the symbol-feed (yahoo chart)."""
    if not row.get("from_watchlist"):
        return None
    if not _SF:
        try:
            d = json.loads(s3.get_object(
                Bucket=S3_BUCKET, Key="data/symbol-feed.json")["Body"].read())
            _SF["p"] = d.get("prices") or {}
            print("[tv-vault] symbol-feed n=%d" % len(_SF["p"]))
        except Exception:
            _SF["p"] = {}
    r = (_SF.get("p") or {}).get(str(row.get("symbol") or ""))
    if not isinstance(r, dict) or "value" not in r:
        return None
    return {"status": "LIVE", "value": r["value"],
            "source": "yahoo:" + str(r.get("ysym", "")),
            "adapter": "feed:symbol", "asof": r.get("asof")}


_COT = {}


def _cotfeed_try(row):
    """ops4153: CFTC primary via cot-feed for {code}_{F|FO}_{FIELD}."""
    if not row.get("from_watchlist"):
        return None
    sym = str(row.get("symbol") or "")
    if not re.match(r"^\d{6}_(FO|F)_", sym):
        return None
    if not _COT:
        try:
            d2 = json.loads(s3.get_object(
                Bucket=S3_BUCKET, Key="data/cot-feed.json")["Body"].read())
            _COT["p"] = d2.get("prices") or {}
            print("[tv-vault] cot-feed n=%d" % len(_COT["p"]))
        except Exception:
            _COT["p"] = {}
    r = (_COT.get("p") or {}).get(sym)
    if not isinstance(r, dict) or "value" not in r:
        return None
    return {"status": "LIVE", "value": r["value"],
            "source": "cftc:" + str(r.get("ds", "")) + " "
            + str(r.get("col", ""))[:40],
            "adapter": "feed:cot", "asof": r.get("asof")}


def _fleet_try(row):
    if not row.get("from_watchlist"):
        return None
    F = _fleet_prices()
    fvr = (F.get("by") or {}).get(str(row.get("symbol") or ""))
    if not isinstance(fvr, dict):
        return None
    pv = None
    for k in ("price", "close", "last", "prev_close"):
        try:
            pv = float(fvr.get(k))
            break
        except Exception:
            continue
    if pv is None or pv <= 0:
        return None
    out = {"status": "LIVE", "value": round(pv, 4),
           "source": "fleet:finviz", "adapter": "fleet:finviz",
           "asof": F.get("asof") or "finviz"}
    try:
        out["chg_pct"] = round(float(fvr.get("change")), 2)
    except Exception:
        pass
    return out


def _dict_try(row):
    """ops4154: the cheap-dictionary chain — every zero-cost resolver in
    one place, so thaw and else-path can never skip a feed again."""
    return (_family_try(row) or _fleet_try(row)
            or _symfeed_try(row) or _cotfeed_try(row))


def _family_try(row):
    m = FAM_RX.match(str(row.get("symbol") or ""))
    if not m:
        return None
    cc, f = m.group(1), m.group(2)
    F = _families()
    src = {"RSYY": "oecd:MEI SLRTCR03", "CIR": "oecd:MEI CPGRLE01", "INBR": "oecd:MEI IR3TIB01", "UP": "oecd:MEI LFHUTTTT", "MPRYY": "oecd:MEI PITGND01", "GDG": "wb:GC.DOD.TOTL.GD.ZS", "BOT": "wb:BN.GSR.GNFS.CD",
           "DIR": "wb:FR.INR.DPST", "LIR": "wb:FR.INR.LEND",
           "TOT": "wb:TT.PRI.MRCH.XD.WD", "CA": "wb:BN.CAB.XOKA.CD",
           "BM": "imf:MFS_DC BM", "LG": "imf:MFS_DC", "CBBS": "imf:MFS_CBS TA", "M0": "imf:MFS_CBS MB",
           "INTR": "bis:CBPOL", "FER": "imf:IRFCL RAF_USD",
           "GDPYY": "wb:NY.GDP.MKTP.KD.ZG", "IRYY": "wb:FP.CPI.TOTL.ZG",
           "UR": "wb:SL.UEM.TOTL.ZS"}[f]
    hit = (F.get(f) or {}).get(cc)
    if not hit and f == "FER":
        hit = (F.get("FERWB") or {}).get(cc)
        if hit:
            src = "wb:FI.RES.TOTL.CD"
    if not hit:
        return None
    val, asof = hit
    return {"status": "LIVE", "value": val,
            "source": src + " (family)", "asof": asof,
            "adapter": "family:" + f}


def boj_yoy(tgt):
    """Bank of Japan official API (launched 2026-02): getDataCode on the
    loans db, YoY computed from avg-amounts-outstanding levels (last
    non-null month vs 12 months prior) — JPLG, the flash-warning metric
    [tv-b3ec3933837d5155]. tgt = 'DB:CODE|DB:CODE' fallback list."""
    now = datetime.now(timezone.utc)
    end = now.year * 100 + now.month
    sy, sm = now.year - 1, now.month - 3
    if sm < 1:
        sy, sm = sy - 1, sm + 12
    start = sy * 100 + sm
    for pair in tgt.split("|"):
        db, _, code = pair.partition(":")
        url = (f"https://www.stat-search.boj.or.jp/api/v1/getDataCode"
               f"?format=json&lang=en&db={db}&startDate={start}&endDate={end}"
               f"&code={code}")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0", "Accept-Encoding": "identity"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            rs = (d.get("RESULTSET") or [{}])[0]
            vals = ((rs.get("VALUES") or {}).get("VALUES")) or []
            dates = ((rs.get("VALUES") or {}).get("SURVEY_DATES")) or []
            i = len(vals) - 1
            while i >= 12 and vals[i] in (None, ""):
                i -= 1
            if i < 12 or vals[i] in (None, "") or vals[i - 12] in (None, ""):
                continue
            yoy = round((float(vals[i]) / float(vals[i - 12]) - 1) * 100, 2)
            prev = None
            if i >= 13 and vals[i - 1] not in (None, "") and vals[i - 13] not in (None, ""):
                prev = round((float(vals[i - 1]) / float(vals[i - 13]) - 1) * 100, 2)
            return {"value": yoy, "prev": prev,
                    "chg_pct": round(yoy - prev, 2) if prev is not None else None,
                    "asof": f"boj:{dates[i] if i < len(dates) else '?'} YoY"}
        except Exception:
            continue
    return None

def boj_level(tgt):
    """Bank of Japan official API (launched 2026-02): getDataCode on the
    loans db, LEVEL series (Tankan DI, call rate) — last non-null value; the
    YoY adapter would be silently wrong for these. Same RESULTSET shape."""
    now = datetime.now(timezone.utc)
    end = now.year * 100 + now.month
    sy, sm = now.year - 1, now.month - 3
    if sm < 1:
        sy, sm = sy - 1, sm + 12
    start = (now.year - 2) * 100 + 1   # 2y window: CO Tankan is QUARTERLY
    for pair in tgt.split("|"):
        db, _, code = pair.partition(":")
        url = (f"https://www.stat-search.boj.or.jp/api/v1/getDataCode"
               f"?format=json&lang=en&db={db}&startDate={start}&endDate={end}"
               f"&code={code}")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0", "Accept-Encoding": "identity"})
            with urllib.request.urlopen(req, timeout=25) as r:
                d = json.loads(r.read())
            rs = (d.get("RESULTSET") or [{}])[0]
            vals = ((rs.get("VALUES") or {}).get("VALUES")) or []
            dates = ((rs.get("VALUES") or {}).get("SURVEY_DATES")) or []
            xs = [(dates[j] if j < len(dates) else None, float(v))
                  for j, v in enumerate(vals) if v is not None]
            if xs:
                dt, v = xs[-1]
                pv = xs[-2][1] if len(xs) > 1 else None
                chg = round((v / pv - 1) * 100, 2) if pv else None
                return {"value": round(v, 4), "prev": pv, "chg_pct": chg,
                        "asof": str(dt)}
        except Exception:
            continue
    return None


# ── CFTC adapter (ops 4093) ───────────────────────────────────────────
# ops 4091 found the fleet's cftc-all-cache holds 29 short-name contracts
# and shares ZERO keys with Khalid's 340 COT/COT3 tickers, which are
# 6-digit CFTC market codes. ops 4092 probed the CFTC's own Socrata
# publication and confirmed the TFF datasets carry exactly the trader
# taxonomy those tickers encode: dealer / asset_mgr / lev_money /
# other_rept, each with long, short and spread columns.
#
# Alias form: cftc:<dataset_id>:<market_code>:<column>
# The column is chosen and VERIFIED by the resolver against a real row
# before the alias is ever written, so this adapter only ever reads a
# column already proven to exist and to carry a value.
CFTC_BASE = "https://publicreporting.cftc.gov/resource/{}.json"


def cftc_latest(tgt):
    """tgt = '<dataset_id>:<market_code>:<column>' -> (value, asof)."""
    try:
        did, code, col = tgt.split(":", 2)
    except ValueError:
        return None
    url = (CFTC_BASE.format(did) + "?" + urllib.parse.urlencode({
        "cftc_contract_market_code": code,
        "$select": f"report_date_as_yyyy_mm_dd,{col}",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1}))
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-vault/3.13", "Accept": "application/json"})
        rows = json.loads(urllib.request.urlopen(req, timeout=25).read())
    except Exception as e:
        print(f"[cftc] {tgt}: {str(e)[:80]}")
        return None
    if not rows:
        return None
    r = rows[0]
    v = r.get(col)
    if v in (None, ""):
        return None
    try:
        v = float(v)
    except Exception:
        return None
    asof = str(r.get("report_date_as_yyyy_mm_dd") or "")[:10]
    return v, asof


def resolve_direct(rv):
    """Source memory: refetch straight from the proven resolver."""
    kind, _, tgt = (rv or "").partition(":")
    if kind == "cftc":
        return cftc_latest(tgt)
    if kind == "fred":
        return fred_latest(tgt)
    if kind == "yoy":
        return fred_yoy(tgt)
    if kind == "yahoo":
        return yahoo_quote(tgt)
    if kind == "ecb":
        return ecb_latest(tgt)
    if kind == "poly":
        return poly_prev(tgt)
    if kind == "ust":
        return ust_latest(tgt)
    if kind == "estat":
        return estat_latest(tgt)
    if kind == "norges":
        return norges_latest(tgt)
    if kind == "bcrp":
        return bcrp_latest(tgt)
    if kind == "mofjp":
        return mofjp_latest(tgt)
    if kind == "boe":
        return boe_latest(tgt)
    if kind == "snb":
        return snb_latest(tgt)
    if kind == "imf":
        return imf_latest(tgt)
    if kind == "boj":
        return boj_yoy(tgt)
    return None


# v3.11.0 — GENERATED ALIAS LAYER. ops 4084 found only 43.6% of the 10,319
# imported watchlist tickers had any fetch route, while the mechanism to fix
# it already existed here: the curated ALIASES dict. justhodl-symbol-resolver
# generates the rest into data/symbol-aliases.json (FRED ids VERIFIED against
# the FRED API before emission). Loaded once per invocation and consulted
# ONLY after the curated dict, so a hand-made decision always outranks a
# generated one.
MAX_EXPAND = int(os.environ.get("MAX_EXPAND", "250"))
RESOLVE_BUDGET = int(os.environ.get("RESOLVE_BUDGET", "150"))
FMP_CAP = int(os.environ.get("FMP_CAP", "400"))
_GEN_ALIASES = None


def gen_aliases():
    global _GEN_ALIASES
    if _GEN_ALIASES is None:
        try:
            doc = json.loads(s3.get_object(
                Bucket=S3_BUCKET, Key="data/symbol-aliases.json")["Body"].read())
            _GEN_ALIASES = doc.get("aliases") or {}
            print(f"[vault] generated aliases loaded: {len(_GEN_ALIASES)}")
        except Exception as e:
            print(f"[vault] no generated aliases ({str(e)[:60]})")
            _GEN_ALIASES = {}
    return _GEN_ALIASES


def ladder(row):
    sym = row["symbol"]
    al = ALIASES.get(sym) or gen_aliases().get(sym)
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
            row["resolved_via"] = f"fred:{sym}"
            return True
    if not al and sym.isalpha() and len(sym) <= 6:
        v = yahoo_quote("^" + sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = f"yahoo:^{sym}"
            row["resolved_via"] = f"yahoo:^{sym}"
            return True
    if not al and sym.isalpha() and 2 <= len(sym) <= 5:
        v = yahoo_quote(sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = f"yahoo:{sym}"
            row["resolved_via"] = f"yahoo:{sym}"
            return True
        v = poly_prev(sym)
        if v:
            row.update(v)
            row["status"] = "LIVE"
            row["source"] = f"polygon:{sym}"
            row["resolved_via"] = f"poly:{sym}"
            return True
    row["status"] = "NO_FREE_SOURCE"
    row["value"] = None
    row.setdefault("resolution_note", "no free API found (TV/TradingEconomics only)")
    return False


def lambda_handler(event, context):
    import time as _tm
    _T0 = _tm.time()

    def PH(tag):
        try:
            print("[tv-vault][phase] %s t+%.1fs" % (tag, _tm.time() - _T0))
        except Exception:
            pass
    PH("start")
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print(f"[tv-vault] {MARKER}")

    try:
        prev = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
        cache = {r["symbol"]: r for r in prev.get("symbols") or []}
    except Exception:
        cache = {}

    brain = get_brain()
    PH("brain-loaded")
    reg = build_registry(brain)
    PH("registry n=%d" % len(reg))
    # ops4006 alias-union: ALIASES define coverage too. reg is notes-derived,
    # so a wired official source sat DORMANT whenever the notes never mention
    # the ticker — the ten CB-expansion symbols proved it (v3.9 marker live,
    # symbols absent for a full run). Stub rows carry n_notes=0 honestly.
    for _s in ALIASES:
        if _s not in reg:
            reg[_s] = {"symbol": _s, "n_notes": 0, "exchanges": set(),
                       "note_ids": [], "note_snippet": ""}
    # ops4086 UNIVERSE EXPANSION — the other half of ops4085.
    # ops4085 taught the vault to RESOLVE generated aliases, but the union
    # above only admitted the curated dict, so a FRED ticker could be
    # perfectly aliased and still never walked: n_symbols stayed at 591
    # while 354 verified aliases sat unused. Resolution without admission
    # is a no-op, and it looked like a success.
    #
    # BOUNDED on purpose: ops4084 counted 10,319 imported tickers, and
    # admitting a four-figure block in one run would blow the 900s budget
    # and rewrite every cadence gate at once. New symbols are admitted
    # MAX_EXPAND per run and accrete, exactly like the resolver's own
    # ledger — the vault reaches full coverage over days, never in a
    # single unreviewable jump.
    # v3.12.1 ACCRETION FIX. v3.12.0 took sorted(_gen)[:MAX_EXPAND], which
    # selects the SAME alphabetical 250 on every run: the backlog could
    # never drain and the vault would have sat at 841 rows forever while
    # reporting a healthy expansion. The gate in ops 4087 caught it.
    # Correct rule: symbols already carried in the previous artifact are
    # re-admitted for FREE (they are cadence-cached, no new fetch), and the
    # MAX_EXPAND budget is spent only on genuinely UNSEEN symbols. That is
    # what makes the backlog actually shrink run over run.
    # ── v3.14.0 FULL UNIVERSE (Khalid: "make sure every single tradingview
    # indicator is here"). Until now the vault's registry came only from
    # brain-tagged notes plus aliases, so it carried ~1,358 of the 10,319
    # symbols he actually imported: clicking a category showed a fraction
    # of his watchlists. Every imported symbol is now admitted as a ROW so
    # the page mirrors TradingView, with its exchange preserved.
    #
    # Resolution stays BOUNDED. Admitting 10,319 rows is cheap; trying to
    # price them all in one 900s run is not. Rows we have not attempted
    # yet carry status PENDING_RESOLUTION — an honest "not looked at yet",
    # never a fake zero and never a claim that no source exists.
    PH("pre-admission")
    try:
        _wl = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                       Key="data/tv-watchlists.json")["Body"].read())
        _seen_wl = 0
        for _l in (_wl.get("watchlists") or _wl.get("lists") or []):
            for _x in (_l.get("symbols") or []):
                _x = str(_x).strip()
                if not _x:
                    continue
                _ex, _, _bare = _x.partition(":")
                if not _bare:
                    _ex, _bare = "", _x
                _bare = _bare.upper()
                if _bare in reg:
                    if _ex:
                        _e = reg[_bare].get("exchanges")
                        if isinstance(_e, (set, list)):
                            reg[_bare]["exchanges"] = sorted(set(_e) | {_ex.upper()})
                    continue
                reg[_bare] = {"symbol": _bare, "n_notes": 0,
                              "exchanges": {_ex.upper()} if _ex else set(),
                              "note_ids": [], "note_snippet": "",
                              "from_watchlist": True}
                _seen_wl += 1
        print(f"[vault] full universe: +{_seen_wl} watchlist symbols admitted")
    except Exception as _e:
        print(f"[vault] watchlist admission skipped: {str(_e)[:80]}")

    _gen = gen_aliases()
    _seen = set(cache)
    _readmit = [x for x in _gen if x not in reg and x in _seen]
    _fresh = [x for x in sorted(_gen) if x not in reg and x not in _seen]
    _admit = _readmit + _fresh[:MAX_EXPAND]
    print(f"[vault] expansion: {len(_gen)} aliases · re-admit {len(_readmit)} "
          f"cached · {len(_fresh)} unseen · spending budget on "
          f"{len(_fresh[:MAX_EXPAND])}")
    for _s in _admit:
        reg[_s] = {"symbol": _s, "n_notes": 0, "exchanges": set(),
                   "note_ids": [], "note_snippet": ""}

    # v3.13.2 — THE EMPTY-TABLE BUG. build_registry() sorts exchanges into a
    # list, but every row admitted AFTER it (the curated ALIASES union and the
    # generated-alias expansion) kept a raw Python set(). json.dumps(default=str)
    # silently renders set() as the STRING "set()", and tradingview.html does
    # (r.exchanges||[]).join(',') on it -> TypeError -> the .map() throws ->
    # the ENTIRE table body renders empty. One malformed row blanked all 1,358.
    # Normalising here covers every admission path, present and future.
    for _r in reg.values():
        _e = _r.get("exchanges")
        _r["exchanges"] = sorted(_e) if isinstance(_e, (set, list, tuple)) else []
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
    # v3.14.0: the universe now includes thousands of listed names. Cap the
    # quote batch so one run cannot turn into a multi-thousand-symbol call.
    if len(fmp_syms) > FMP_CAP:
        _known = [x for x in fmp_syms if x in cache]
        _new = [x for x in fmp_syms if x not in cache]
        fmp_syms = (_known + _new)[:FMP_CAP]
        print(f"[vault] fmp batch capped to {len(fmp_syms)}")

    fmp_vals = fmp_quotes(fmp_syms)

    PH("fmp-bulk-done n=%d" % len(fmp_vals))
    n_live = n_cached = n_pending = 0
    attempted = 0
    out_of_time = False
    try:
        _families()   # ops4117 pre-flight: hard 60s cap, before the heavy loops
    except Exception:
        pass
    PH("families-preflight-done")
    try:
        _fleet_prices()
    except Exception:
        pass
    PH("fleet-prices-preflight-done")
    force = bool((event or {}).get("force"))
    try:
        _age_h = (datetime.now(timezone.utc)
                  - datetime.fromisoformat(str(prev.get("generated_at")))
                  ).total_seconds() / 3600
    except Exception:
        _age_h = 999.0
    CACHE_FRESH = _age_h < 26
    REV_SLOT = int(time.time() // 86400) % 12
    _rev_spent = 0.0
    LADDER_WALL_S = 480.0   # ops4133: headroom banked at 370s runs
    _ladder_spent = 0.0
    _ladder_n = 0
    _ri = 0
    PH("cache age_h=%.1f fresh=%s slot=%d" % (_age_h, CACHE_FRESH, REV_SLOT))
    PH("main-loop-start")
    for row in rows:
        sym = row["symbol"]
        _ri += 1
        if _ri % 500 == 0:
            PH("row %d/%d live=%d lad=%d(%.0fs) rev=%.0fs"
               % (_ri, len(rows), n_live, _ladder_n,
                  _ladder_spent, _rev_spent))
        c = cache.get(sym) or {}
        fetched_at = c.get("fetched_at")
        fresh_days = CADENCE.get(row["cadence"], 0)
        if not force and fetched_at:
            try:
                age_d = (now - datetime.fromisoformat(fetched_at)).total_seconds() / 86400
            except Exception:
                age_d = 999
            retry_gate = (fresh_days if c.get("resolved_via")
                          else (27 if c.get("status") == "NO_FREE_SOURCE" else fresh_days))
            if age_d < retry_gate and c.get("status") in ("LIVE", "NO_FREE_SOURCE",
                                                          "DISCONTINUED", "META"):
                for k in ("value", "prev", "chg_pct", "asof", "status", "source",
                          "resolution_note", "fetched_at",
                          "adapter", "resolved_via"):
                    if k in c:
                        row[k] = c[k]
                row["cached"] = True
                if row["status"] == "NO_FREE_SOURCE":
                    # ops4154 THAW GENERALIZED: any cheap dict (family,
                    # fleet, symbol-feed, cot-feed) can thaw a frozen row.
                    _fh = _dict_try(row)
                    if _fh:
                        row.update(_fh)
                if row["status"] == "LIVE":
                    n_live += 1
                n_cached += 1
                continue
        # v3.14.0 BOUNDED RESOLUTION. A symbol we have never attempted and
        # have no budget left for is marked PENDING_RESOLUTION — an honest
        # "not looked at yet". It is deliberately NOT NO_FREE_SOURCE, which
        # would be a claim we have not earned and would also trip the
        # 27-day retry gate and freeze the symbol out for a month.
        # v3.14.1 WALL-CLOCK GUARD. ops 4098 timed out at 917s: a COUNT
        # budget cannot bound wall time, because each ladder attempt makes
        # several network calls of unknown latency. The only safe bound is
        # the clock itself — stop attempting with headroom to spare and
        # write the artifact, rather than dying with nothing written.
        if context is not None:
            try:
                if context.get_remaining_time_in_millis() < 180000:
                    out_of_time = True
            except Exception:
                pass
        if (out_of_time or attempted >= RESOLVE_BUDGET) and not c \
                and row.get("from_watchlist"):
            _famhit = _family_try(row)
            if _famhit:
                row.update(_famhit)
            else:
                row["status"] = "PENDING_RESOLUTION"
                row["resolution_note"] = "queued — not yet attempted this run"
                n_pending += 1
            continue
        if not c:
            attempted += 1
            if attempted <= 12 or attempted % 25 == 0:
                PH("iter %d sym=%s via=%s" % (attempted, sym,
                                              c.get("resolved_via")))
        if row["source"] == "fmp" and sym in fmp_vals:
            row.update(fmp_vals[sym])
            row["status"] = "LIVE"
            row["resolved_via"] = "fmp"
            n_live += 1
        else:
            rv = c.get("resolved_via")
            # ops4126 TRUST-THE-CACHE — the measured 900s hog: <10 network
            # re-verifications completed in 895s (~90s+/symbol). Fresh
            # cache (<26h) passes through untouched; a rotating slice
            # (attempted %% 12 == daily slot) still re-verifies, under a
            # 120s wall budget, so staleness can't hide and a hanging
            # adapter names itself in slow-prints instead of killing runs.
            _cok = (rv and rv != "fmp" and c.get("status") == "LIVE"
                    and not force)
            _rev = (_cok and ((not CACHE_FRESH)
                              or (attempted % 12 == REV_SLOT))
                    and _rev_spent < 120.0)
            v = None
            if _cok and not _rev:
                v = {k: c[k] for k in ("value", "prev", "chg_pct", "asof")
                     if k in c}
                row["resolution_note"] = ("cache<26h" if CACHE_FRESH
                                          else "cache-passthrough (rev-budget)")
            elif _cok:
                _rt = time.time()
                v = resolve_direct(rv)
                _dt = time.time() - _rt
                _rev_spent += _dt
                if _dt > 3:
                    PH("slow sym=%s via=%s %.1fs" % (sym, rv, _dt))
                if not v:
                    # ops4131: a failed re-verify must never cost a LIVE
                    # row — restore the cache, note it, move on.
                    v = {k: c[k] for k in ("value", "prev", "chg_pct",
                                           "asof") if k in c}
                    row["resolution_note"] = "cache kept (rev miss)"
            if v:
                row.update(v)
                row["status"] = "LIVE"
                row["source"] = c.get("source")
                row["resolved_via"] = rv
                n_live += 1
            else:
                # ops4129 THE HOG, finally named: ~8,500 cached-PENDING
                # rows fail the cadence gate (PENDING not in the list),
                # dodge the wall-clock guard (its "and not c" died the
                # moment every row had cache), and each called the full
                # network ladder — unguarded, every run, since 21:55.
                # Order now: family dict-hit first, then ladder under a
                # 240s wall budget, then HONEST deferral keeping prior
                # state. Write becomes guaranteed by arithmetic.
                _famhit = _family_try(row)
                _fvhit = None if _famhit else _fleet_try(row)
                if _famhit:
                    row.update(_famhit)
                    n_live += 1
                elif _fvhit:
                    row.update(_fvhit)
                    n_live += 1
                elif _symfeed_try(row):
                    row.update(_symfeed_try(row))
                    n_live += 1
                elif _cotfeed_try(row):
                    row.update(_cotfeed_try(row))
                    n_live += 1
                elif (not out_of_time) and _ladder_spent < LADDER_WALL_S:
                    _lt = time.time()
                    if ladder(row):
                        n_live += 1
                    _ladder_spent += time.time() - _lt
                    _ladder_n += 1
                    if _ladder_n <= 12 or _ladder_n % 50 == 0:
                        PH("ladder %d sym=%s %.1fs spent=%.0fs"
                           % (_ladder_n, sym,
                              time.time() - _lt, _ladder_spent))
                else:
                    if c:
                        for k in ("value", "prev", "chg_pct", "asof",
                                  "status", "source"):
                            if k in c:
                                row[k] = c[k]
                        if row.get("status") == "LIVE":
                            n_live += 1
                    else:
                        row["status"] = "PENDING_RESOLUTION"
                    row["resolution_note"] = "deferred (ladder wall budget)"
        row["fetched_at"] = now.isoformat()

    rows.sort(key=lambda r: (-r["n_notes"], r["symbol"]))
    by_cat = defaultdict(list)
    by_status = defaultdict(int)
    for r in rows:
        by_cat[r["category"]].append(r["symbol"])
        by_status[r["status"]] += 1

    out = {
        "engine": "justhodl-tradingview",
        "version": "3.2",
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
        "n_pending": by_status.get("PENDING_RESOLUTION", 0),
        "n_from_watchlist": sum(1 for r in rows if r.get("from_watchlist")),
        "coverage_pct": round(n_live / max(1, len(rows)) * 100, 1),
        "status_counts": dict(by_status),
        "by_category_counts": {k: len(v) for k, v in sorted(by_cat.items())},
        "symbols": rows,
        "debug_mof": _MOF_CACHE.get("diag"),
        "elapsed_s": round(time.time() - t0, 1),
    }
    PH("pre-write")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str),
                  ContentType="application/json", CacheControl="max-age=900")
    print(f"[tv-vault] DONE {out['elapsed_s']}s live={n_live}/{len(rows)} "
          f"cached={n_cached} fred_calls={_FRED_CALLS['n']}")
    return {"ok": True, "n_symbols": len(rows), "n_live": n_live,
            "n_cached": n_cached, "fred_calls": _FRED_CALLS["n"],
            "coverage_pct": out["coverage_pct"]}
