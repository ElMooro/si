"""aws/shared/series_source.py — universal FREE-source series resolver.

Khalid's TradingView symbols are mostly public data behind proprietary
codes. This module maps them to free sources with DEEP history (1990+,
often 1960+), and fetches them.

Sources, in priority order:
  FRED      — US + OECD macro. Deep (1950s+), keyed, reliable.
              Also used as an auto-mapper via its /series/search endpoint.
  STOOQ     — equities, indices, FX, commodities as CSV, no key, back to
              the 1990s (Polygon only reaches ~5y, which is why the first
              thesis study was history-starved).
  DBNOMICS  — OECD/IMF/Eurostat/BIS/World Bank aggregator, no key.
  COINGECKO — crypto, 2013+.

ECONOMICS:* codes are {ISO2}{INDICATOR} (JPGDPYY = Japan GDP YoY). The
curated templates below cover the common indicators against FRED's OECD
mirrors; anything else falls back to a FRED search, cached with its
title + confidence so every mapping stays auditable.
"""

import csv
import io
import json
import os
import re
import urllib.parse
import urllib.request

FRED_FALLBACK = "2f057499936072679d8843d7fce99989"
FRED_KEY = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
            or FRED_FALLBACK)

ISO2_ISO3 = {
    "US": "USA", "EU": "EA19", "JP": "JPN", "CN": "CHN", "GB": "GBR",
    "DE": "DEU", "FR": "FRA", "IT": "ITA", "ES": "ESP", "CA": "CAN",
    "AU": "AUS", "IN": "IND", "BR": "BRA", "RU": "RUS", "KR": "KOR",
    "MX": "MEX", "ZA": "ZAF", "TR": "TUR", "CH": "CHE", "SE": "SWE",
    "NO": "NOR", "DK": "DNK", "FI": "FIN", "NL": "NLD", "BE": "BEL",
    "AT": "AUT", "PL": "POL", "PT": "PRT", "GR": "GRC", "IE": "IRL",
    "NZ": "NZL", "ID": "IDN", "TH": "THA", "MY": "MYS", "PH": "PHL",
    "VN": "VNM", "AR": "ARG", "CL": "CHL", "CO": "COL", "PE": "PER",
    "SA": "SAU", "IL": "ISR", "HK": "HKG", "SG": "SGP", "TW": "TWN",
    "CZ": "CZE", "HU": "HUN", "RO": "ROU", "SZ": "CHE", "SY": "SYR",
    # ops 3185: the ECONOMICS gap was COUNTRIES, not indicators — his lists
    # span 190+ countries and the map only held ~50.
    "AE": "ARE", "AL": "ALB", "AO": "AGO", "BD": "BGD", "BG": "BGR",
    "BH": "BHR", "BJ": "BEN", "BO": "BOL", "BW": "BWA", "BY": "BLR",
    "CD": "COD", "CI": "CIV", "CM": "CMR", "CR": "CRI", "CY": "CYP",
    "DO": "DOM", "DZ": "DZA", "EC": "ECU", "EE": "EST", "EG": "EGY",
    "ET": "ETH", "GA": "GAB", "GE": "GEO", "GH": "GHA", "GQ": "GNQ",
    "GT": "GTM", "HN": "HND", "HR": "HRV", "IQ": "IRQ", "IR": "IRN",
    "IS": "ISL", "JM": "JAM", "JO": "JOR", "KE": "KEN", "KH": "KHM",
    "KM": "COM", "KW": "KWT", "KZ": "KAZ", "LB": "LBN", "LK": "LKA",
    "LR": "LBR", "LT": "LTU", "LU": "LUX", "LV": "LVA", "LY": "LBY",
    "MA": "MAR", "MD": "MDA", "ME": "MNE", "MK": "MKD", "MM": "MMR",
    "MN": "MNG", "MT": "MLT", "MU": "MUS", "MZ": "MOZ", "NA": "NAM",
    "NG": "NGA", "NP": "NPL", "OM": "OMN", "PA": "PAN", "PK": "PAK",
    "PY": "PRY", "QA": "QAT", "RS": "SRB", "RW": "RWA", "SD": "SDN",
    "SI": "SVN", "SK": "SVK", "SN": "SEN", "SS": "SSD", "TN": "TUN",
    "TZ": "TZA", "UA": "UKR", "UG": "UGA", "UY": "URY", "UZ": "UZB",
    "VE": "VEN", "YE": "YEM", "ZM": "ZMB", "ZW": "ZWE", "AZ": "AZE",
    "AM": "ARM", "BA": "BIH", "BN": "BRN", "BF": "BFA", "BI": "BDI",
    "MW": "MWI", "ML": "MLI", "NE": "NER", "TD": "TCD", "TG": "TGO",
    "SL": "SLE", "SO": "SOM", "MG": "MDG", "LA": "LAO", "PG": "PNG",
    "FJ": "FJI", "TT": "TTO", "SV": "SLV", "NI": "NIC", "CU": "CUB",
}

# indicator suffix → FRED series template (OECD Main Economic Indicators
# mirrors carry 1960+ history for most members)
FRED_TEMPLATES = {
    # NOTE: ops 3167 proved two of these ids were WRONG (returned nothing).
    # validate_template() now test-fetches every template before use, and
    # the mapper falls back to FRED search for any that fail.
    "GDPYY": "NAEXKP01{i3}Q657S",       # real GDP growth YoY
    "IRYY": "CPALTT01{i3}M657N",        # CPI YoY (657N = same period prev yr)
    "CPI": "CPALTT01{i3}M661N",
    "INTR": "IR3TIB01{i3}M156N",        # short rate
    "IR": "IRLTLT01{i3}M156N",          # long rate (10y)
    "UR": "LRHUTTTT{i3}M156S",          # unemployment
    "M2": "MABMM301{i3}M189S",          # broad money
    "M3": "MABMM301{i3}M189S",
    "CLI": "{i3}LOLITOAASTSAM",         # composite leading indicator
    "BCOI": "BSCICP03{i3}M665S",        # business confidence
    "CCI": "CSCICP03{i3}M665S",         # consumer confidence
    "PROD": "PRINTO01{i3}M657S",        # industrial production
    "EXP": "XTEXVA01{i3}M667S",         # exports
    "IMP": "XTIMVA01{i3}M667S",         # imports
    "BOT": "XTNTVA01{i3}M667S",         # trade balance
    "SP": "SPASTT01{i3}M657N",          # share prices
    "HOU": "HSN1F",
}

# US-specific and market codes that map cleanly
DIRECT = {
    "ECONOMICS:USCBBS": ("FRED", "WALCL"),
    "ECONOMICS:USBBS": ("FRED", "WALCL"),
    "ECONOMICS:EUCBBS": ("FRED", "ECBASSETSW"),
    "ECONOMICS:JPCBBS": ("FRED", "JPNASSETS"),
    "ECONOMICS:USINTR": ("FRED", "FEDFUNDS"),
    "ECONOMICS:USM2": ("FRED", "M2SL"),
    "ECONOMICS:USINBR": ("FRED", "TOTRESNS"),
    "ECONOMICS:USNFP": ("FRED", "PAYEMS"),
    "ECONOMICS:USRRP": ("FRED", "RRPONTSYD"),
    "ECONOMICS:USUR": ("FRED", "UNRATE"),
    "TVC:US02Y": ("FRED", "DGS2"), "TVC:US03MY": ("FRED", "DTB3"),
    "TVC:US10Y": ("FRED", "DGS10"), "TVC:US30Y": ("FRED", "DGS30"),
    "TVC:US05Y": ("FRED", "DGS5"), "TVC:US01Y": ("FRED", "DGS1"),
    "TVC:US03Y": ("FRED", "DGS3"), "TVC:US07Y": ("FRED", "DGS7"),
    "TVC:US06MY": ("FRED", "DGS6MO"), "TVC:US01MY": ("FRED", "DGS1MO"),
    "TVC:VIX": ("FRED", "VIXCLS"),          # 1990+
    "TVC:DXY": ("MARKET", "DX-Y.NYB"),      # 1990+ (FRED DTWEXBGS starts 2006)
    "TVC:GOLD": ("MARKET", "GC=F"),
    "TVC:SILVER": ("MARKET", "SI=F"),
    "TVC:USOIL": ("FRED", "DCOILWTICO"),
    "TVC:UKOIL": ("FRED", "DCOILBRENTEU"),
    "TVC:SPX": ("MARKET", "^GSPC"), "TVC:NDX": ("MARKET", "^NDX"),
    "TVC:DJI": ("MARKET", "^DJI"), "TVC:NI225": ("MARKET", "^N225"),
    "TVC:DAX": ("MARKET", "^GDAXI"), "TVC:UKX": ("MARKET", "^FTSE"),
    "TVC:HSI": ("MARKET", "^HSI"), "TVC:SHCOMP": ("MARKET", "000001.SS"),
    "TVC:MOVE": ("MARKET", "^MOVE"),
    "TVC:DE10Y": ("FRED", "IRLTLT01DEM156N"),
    "TVC:JP10Y": ("FRED", "IRLTLT01JPM156N"),
    "TVC:GB10Y": ("FRED", "IRLTLT01GBM156N"),
    "TVC:IT10Y": ("FRED", "IRLTLT01ITM156N"),
    "TVC:FR10Y": ("FRED", "IRLTLT01FRM156N"),
    "CRYPTOCAP:TOTAL": ("COINGECKO", "total"),
    "CRYPTOCAP:BTC.D": ("COINGECKO", "btc_dominance"),
    "INDEX:BTCUSD": ("COINGECKO", "bitcoin"),
}

# ops 3177: TradingView's ECONOMICS:{ISO2}{IND} codes are public data behind
# proprietary names. World Bank (free, no key, 1960+, ~200 countries) carries
# the ones FRED's OECD mirrors miss — and these codes REPEAT across Khalid's
# lists, so each mapping activates several dormant engines at once.
ECON_WB = {
    "GDPYY": "NY.GDP.MKTP.KD.ZG",     # real GDP growth  (168 in his universe)
    "GDP": "NY.GDP.MKTP.CD",
    "GDG": "GC.DOD.TOTL.GD.ZS",       # govt debt / GDP   (164)
    "BOT": "NE.RSB.GNFS.CD",          # trade balance     (186)
    "FER": "FI.RES.TOTL.CD",          # FX reserves       (121)
    "DIR": "FR.INR.DPST",             # deposit rate      (150)
    "LIR": "FR.INR.LEND",             # lending rate
    "IRYY": "FP.CPI.TOTL.ZG",         # CPI YoY
    "CPI": "FP.CPI.TOTL.ZG",
    "FI": "FP.CPI.TOTL.ZG",           # food inflation → CPI proxy (174)
    "UR": "SL.UEM.TOTL.ZS",           # unemployment
    "CS": "NE.CON.PRVT.KD.ZG",        # consumer spending (30)
    "CAG": "BN.CAB.XOKA.GD.ZS",       # current account / GDP (28)
    "EXP": "NE.EXP.GNFS.CD",
    "IMP": "NE.IMP.GNFS.CD",
    "MS": "FM.LBL.BMNY.GD.ZS",        # broad money / GDP
    "FDI": "BX.KLT.DINV.WD.GD.ZS",
    "POP": "SP.POP.TOTL",
    "GS": "NY.GNS.ICTR.ZS",           # gross savings
    "MIW": "NY.GDP.PCAP.CD",          # income proxy
    "IP": "NV.IND.TOTL.KD.ZG",        # industrial production growth
    "IPRI": "TM.VAL.MRCH.XD.WD",      # import price index
    "EPRI": "TX.VAL.MRCH.XD.WD",      # export price index
    "MIN": "NY.GDP.MINR.RT.ZS",
    # ops 3185: the remaining high-count codes in his universe
    "GDPQQ": "NY.GDP.MKTP.KD.ZG",     # GDP QoQ → annual growth proxy
    "BR": "FR.INR.LEND",              # bank rate
    "CPR": "FR.INR.RINR",             # corporate/real rate proxy
    "IC": "IC.BUS.EASE.XQ",           # business environment
    "CU": "NV.IND.MANF.ZS",           # capacity/industry share proxy
    "FO": "BX.KLT.DINV.CD.WD",        # foreign investment inflow
    "MTO": "TM.VAL.MRCH.CD.WT",       # merchandise imports
    "PSC": "FS.AST.PRVT.GD.ZS",       # private sector credit / GDP
    "CCR": "FS.AST.DOMS.GD.ZS",       # domestic credit / GDP
    "GS2": "NY.GNS.ICTR.ZS",
    "INBR": "FR.INR.LEND",            # interbank → lending rate proxy (WB)
    "BCOI": "IC.BUS.EASE.XQ",         # business climate (non-OECD fallback)
    "CBBS": "FM.LBL.BMNY.CN",         # broad money as a CB-balance proxy
    "NO": "NV.IND.MANF.KD.ZG",        # new orders → manufacturing growth
}

# TradingView ISO2 quirks → World Bank ISO2
TV_WB = {"EU": "EMU", "SZ": "CH", "UK": "GB", "SY": "SY", "SP": "ES",
         "GE": "DE", "SW": "SE", "SF": "ZA", "KS": "KR", "CI": "CL"}

# continuous futures contracts (TradingView's "1!" convention)
FUT = {"CL": "CL=F", "NG": "NG=F", "GC": "GC=F", "SI": "SI=F", "HG": "HG=F",
       "ZC": "ZC=F", "ZS": "ZS=F", "ZW": "ZW=F", "ZB": "ZB=F", "ZN": "ZN=F",
       "ES": "ES=F", "NQ": "NQ=F", "RB": "RB=F", "HO": "HO=F", "KC": "KC=F",
       "CT": "CT=F", "SB": "SB=F", "CC": "CC=F", "LE": "LE=F", "PL": "PL=F",
       # ops 3189: remaining CME/CBOT roots Yahoo genuinely carries
       "ZF": "ZF=F", "ZT": "ZT=F", "ZM": "ZM=F", "ZL": "ZL=F",
       "HE": "HE=F", "GF": "GF=F", "PA": "PA=F", "YM": "YM=F",
       "RTY": "RTY=F", "KE": "KE=F", "ZO": "ZO=F", "ZR": "ZR=F"}
FUT_EX = {"NYMEX", "COMEX", "CBOT", "CME", "ICEUS", "MATBAROFEX", "NYBOT"}

# ── free on-chain + CFTC (ops 3189) ─────────────────────────────────
# GLASSNODE / INTOTHEBLOCK watchlist tiles are vendor views of metrics the
# Coin Metrics COMMUNITY API serves free (no key, daily, 2010+). COT3 tiles
# embed the CFTC contract-market code — publicreporting.cftc.gov serves the
# full weekly history free. Both are probe-gated by the mapping ops before
# an entry is allowed to count toward coverage.
CM_ASSETS = {"BTC": "btc", "ETH": "eth", "LTC": "ltc", "XRP": "xrp",
             "ADA": "ada", "DOGE": "doge", "BCH": "bch", "XLM": "xlm",
             "DOT": "dot", "SOL": "sol", "LINK": "link", "XMR": "xmr",
             "ZEC": "zec", "ETC": "etc", "BSV": "bsv", "DASH": "dash",
             "ALGO": "algo", "XTZ": "xtz", "EOS": "eos", "TRX": "trx",
             "AVAX": "avax", "MATIC": "matic", "UNI": "uni", "AAVE": "aave"}
CM_METRICS = {  # substring of the TV tile name → community metric id
    "ACTIVEADDRESS": "AdrActCnt", "ADDRESSESACTIVE": "AdrActCnt",
    "TRANSACTIONCOUNT": "TxCnt", "TXCOUNT": "TxCnt", "TRANSACTION": "TxCnt",
    "TRANSFERVOLUME": "TxTfrValAdjUSD", "TXVOLUME": "TxTfrValAdjUSD",
    "TOTALFEES": "FeeTotUSD", "AVERAGEFEE": "FeeMeanUSD",
    "MEANFEE": "FeeMeanUSD", "FEE": "FeeTotUSD",
    "REALIZEDCAP": "CapRealUSD", "MARKETCAP": "CapMrktCurUSD",
    "MVRV": "CapMVRVCur", "NVT": "NVTAdj", "VELOCITY": "VelCur1yr",
    "HASHRATE": "HashRate", "DIFFICULTY": "DiffMean",
    "MINERREVENUE": "RevUSD", "REVENUE": "RevUSD", "ISSUANCE": "IssTotUSD",
    "BLOCKCOUNT": "BlkCnt", "SUPPLY": "SplyCur", "CIRCULATING": "SplyCur",
    "PRICE": "PriceUSD"}
CM_METRIC_KEYS = sorted(CM_METRICS, key=len, reverse=True)

# TVC world indices → Yahoo (free, deep history)
TVC_INDEX = {
    "AEX": "^AEX", "CAC": "^FCHI", "CAC40": "^FCHI", "IBEX": "^IBEX",
    "IBEX35": "^IBEX", "SMI": "^SSMI", "SX5E": "^STOXX50E",
    "STOXX50E": "^STOXX50E", "SX7E": "^SX7E", "ASX": "^AXJO",
    "XJO": "^AXJO", "TSX": "^GSPTSE", "SPTSX": "^GSPTSE",
    "IBOV": "^BVSP", "BVSP": "^BVSP", "KOSPI": "^KS11", "KS11": "^KS11",
    "SENSEX": "^BSESN", "NIFTY": "^NSEI", "TAIEX": "^TWII",
    "STI": "^STI", "PSI": "^PSI20", "OMXS30": "^OMX",
    "MIB": "FTSEMIB.MI", "FTSEMIB": "FTSEMIB.MI", "IMOEX": "IMOEX.ME",
    "RUT": "^RUT", "RUI": "^RUI", "SPX": "^GSPC", "NDX": "^NDX",
    "DJI": "^DJI", "NI225": "^N225", "DAX": "^GDAXI", "UKX": "^FTSE",
    "HSI": "^HSI", "VIX": "^VIX", "TNX": "^TNX",
}
# TVC bond-yield codes: {ISO2}{TENOR}Y → OECD mirrors on FRED (1990+)
TVC_YIELD = re.compile(r"^([A-Z]{2})(\d{2})(M?)Y$")

# OECD MEI mirrors on FRED only exist for OECD members. ops 3185 caught
# me routing Zimbabwe to BSCICP03ZWEM665S — a series that does not exist.
# Non-members go to the World Bank, which actually covers them.
# ops 3186: foreign listings are mostly FREE on Yahoo via exchange suffixes.
# Probe before paying: LSE:VOD -> VOD.L, SWB:BMW -> BMW.DE, SSE:600519 ->
# 600519.SS. Only what genuinely misses is worth a vendor line item.
YAHOO_SUFFIX = {
    "LSE": ".L", "LSIN": ".L", "AIM": ".L",
    "SWB": ".DE", "XETR": ".DE", "FWB": ".DE", "TRADEGATE": ".DE",
    "GETTEX": ".DE", "BER": ".BE", "MUN": ".MU", "DUS": ".DU",
    "EURONEXT": ".AS", "AMS": ".AS", "EPA": ".PA", "EBR": ".BR",
    "ELI": ".LS", "MIL": ".MI", "BIT": ".MI", "BME": ".MC",
    "OMXSTO": ".ST", "OMXHEX": ".HE", "OMXCOP": ".CO", "OSL": ".OL",
    "SIX": ".SW", "SWX": ".SW", "VIE": ".VI", "WSE": ".WA",
    "SSE": ".SS", "SZSE": ".SZ", "HKEX": ".HK", "TSE": ".T",
    "TSX": ".TO", "TSXV": ".V", "ASX": ".AX", "NZX": ".NZ",
    "NSE": ".NS", "BSE": ".BO", "KRX": ".KS", "TWSE": ".TW",
    "SGX": ".SI", "IDX": ".JK", "BMV": ".MX", "BVMF": ".SA",
    "JSE": ".JO", "TASE": ".TA",
}

OECD_MEMBERS = {
    "US", "GB", "DE", "FR", "IT", "ES", "JP", "CA", "AU", "NZ", "KR",
    "MX", "CL", "CO", "CR", "TR", "IL", "CH", "SE", "NO", "DK", "FI",
    "IS", "IE", "NL", "BE", "AT", "PT", "GR", "LU", "PL", "CZ", "HU",
    "SK", "SI", "EE", "LV", "LT", "EU",
}

EQ_EX = {"NASDAQ", "NYSE", "AMEX", "ARCA", "BATS", "CBOE", "OTC"}

# ── ECONOMICS residual ladders (ops 3190) ───────────────────────────
# TV econ families FRED/OECD/World-Bank do NOT cover map to IMF IFS (and
# OECD MEI) on DBnomics. Each family is a LADDER of candidate series-id
# templates: map_symbol emits rung 0; the mapping ops probes it with a real
# fetch and climbs the ladder on a dry hit. Nothing counts unprobed.
ECON_DBN = {
    "CBBS":  ["IMF/IFS/M.{i2}.FASAF_XDC",      # central bank assets
              "IMF/IFS/Q.{i2}.FASAF_XDC",
              "IMF/IFS/A.{i2}.FASAF_XDC"],
    "INBR":  ["IMF/IFS/M.{i2}.FIMM_PA",        # money-market / interbank
              "IMF/IFS/Q.{i2}.FIMM_PA"],
    "BR":    ["IMF/IFS/M.{i2}.FPOLM_PA",       # policy rate
              "IMF/IFS/M.{i2}.FIDR_PA"],
    "GDPQQ": ["IMF/IFS/Q.{i2}.NGDP_R_SA_XDC",  # real GDP level, SA
              "IMF/IFS/Q.{i2}.NGDP_SA_XDC"],
    "BCOI":  ["OECD/MEI/{i3}.BSCICP03.IXNSA.M",
              "OECD/MEI/{i3}.BSCICP02.IXNSA.M"],
    "NO":    ["OECD/MEI/{i3}.PRMNTO01.IXOBSA.M"],
    "FO":    ["OECD/MEI/{i3}.PRMNTO01.IXOBSA.M"],
    "IC":    ["IMF/IFS/M.{i2}.AIP_IX",          # industrial production
              "OECD/MEI/{i3}.PRINTO01.IXOBSA.M"],
    "CU":    ["OECD/MEI/{i3}.BSCURT02.STSA.Q"],
    "CPR":   ["IMF/IFS/M.{i2}.PCPI_IX",         # CPI level
              "IMF/IFS/Q.{i2}.PCPI_IX"],
    "MTO":   ["IMF/IFS/M.{i2}.TXG_FOB_USD",     # exports (trade)
              "IMF/IFS/M.{i2}.TMG_CIF_USD"],
}
FX_EX = {"FX", "OANDA", "FOREXCOM", "FX_IDC", "SAXO"}
OPS_RE = re.compile(r"[+\-*/()]")
NUM_RE = re.compile(r"^[\d.]+$")


def _http(url, timeout=25, raw=False):
    req = urllib.request.Request(url, headers={"User-Agent": "jh-series/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        b = r.read()
    return b.decode("utf-8", "replace") if raw else json.loads(b.decode())


# ── mapping ──────────────────────────────────────────────────────────
def map_symbol(sym, fred_search=None):
    """→ (source, id, confidence, note). fred_search: callable for fallback."""
    s = str(sym).strip().upper()
    if not s:
        return None, None, 0, "empty"
    if OPS_RE.search(s):
        return "FORMULA", s, 1.0, "arithmetic over members"
    if s in DIRECT:
        src, sid = DIRECT[s]
        return src, sid, 1.0, "curated"
    if ":" not in s:
        return "MARKET", s, 0.9, "bare ticker → US equity"
    ex, t = s.split(":", 1)
    if ex == "FRED":
        return "FRED", t, 1.0, "native"
    suf = YAHOO_SUFFIX.get(ex)
    if suf:
        return "MARKET", f"{t}{suf}", 0.75, f"{ex} → Yahoo {suf}"
    if ex in FUT_EX:
        root = re.sub(r"\d*!$", "", t)
        y = FUT.get(root)
        if y:
            return "MARKET", y, 0.75, f"continuous future {root}"
    if ex in EQ_EX:
        return "MARKET", t, 0.9, "US listing"
    if ex in FX_EX:
        return "MARKET", f"{t}=X", 0.7, "fx pair"
    if ex == "ECONOMICS":
        m = re.match(r"^([A-Z]{2})([A-Z0-9]+)$", t)
        if m:
            i2, ind = m.groups()
            i3 = ISO2_ISO3.get(i2)
            tpl = FRED_TEMPLATES.get(ind)
            if i3 and tpl and i2 in OECD_MEMBERS:   # OECD monthly (best freq)
                return "FRED", tpl.format(i3=i3), 0.85, f"OECD template {ind}"
            wb = ECON_WB.get(ind)
            if wb:                               # World Bank annual, 1960+
                iso2 = TV_WB.get(i2, i2)
                return ("WORLDBANK", f"{iso2}|{wb}", 0.8,
                        f"world-bank {ind}")
            if ind in ECON_DBN:                  # IMF/OECD ladder (ops 3190)
                sid = ECON_DBN[ind][0].format(i2=i2, i3=i3 or i2)
                return ("DBNOMICS", sid, 0.6,
                        f"econ ladder {ind} rung0 (probe-gated)")
        if fred_search:
            hit = fred_search(t)
            if hit:
                return "FRED", hit[0], 0.5, f"fred-search: {hit[1][:60]}"
        return None, None, 0, "econ_unmapped"
    if ex == "TVC":
        # world index?
        y = TVC_INDEX.get(t)
        if y:
            return "MARKET", y, 0.9, "TVC world index"
        # global bond yield? 10Y → OECD long rate; short tenors → 3M interbank
        m = TVC_YIELD.match(t)
        if m:
            i2, tenor, mo = m.groups()
            i3 = ISO2_ISO3.get(i2)
            if i3 and i2 in OECD_MEMBERS:
                if mo or int(tenor) <= 2:
                    return ("FRED", f"IR3TIB01{i3}M156N", 0.7,
                            f"{i2} short rate (3M interbank proxy)")
                return ("FRED", f"IRLTLT01{i3}M156N", 0.85,
                        f"{i2} long-term govt bond yield")
        if fred_search:
            hit = fred_search(t)
            if hit:
                return "FRED", hit[0], 0.5, f"fred-search: {hit[1][:60]}"
        return None, None, 0, "tvc_unmapped"
    if ex == "INDEX":
        y = TVC_INDEX.get(t)
        if y:
            return "MARKET", y, 0.85, "index → Yahoo"
        return "MARKET", f"^{t}", 0.5, "index (best-effort Yahoo)"
    if ex == "USI":
        # US market internals — we COMPUTE these from the Polygon grouped
        # feed Khalid already pays for (ops 3185). No vendor needed.
        code = re.sub(r"\.(US|NY|NQ)$", "", t)
        m = {"ADV": "ADVANCERS", "ADVN": "ADVANCERS", "ADVQ": "ADVANCERS",
             "DECL": "DECLINERS", "DECN": "DECLINERS", "DECQ": "DECLINERS",
             "UNCH": "UNCHANGED", "ADVDEC": "ADVDEC_LINE",
             "UVOL": "UP_VOLUME", "DVOL": "DOWN_VOLUME",
             "TRIN": "TRIN", "TICK": "TICK",
             "HIGH": "NEW_HIGHS", "LOW": "NEW_LOWS",
             "NEWHI": "NEW_HIGHS", "NEWLO": "NEW_LOWS",
             "PCTABOVE50MA": "PCT_ABOVE_50DMA",
             "PCTABOVE200MA": "PCT_ABOVE_200DMA",
             "ACTV": "ADVANCERS", "BASPRD": "ADVDEC_LINE"}.get(code)
        if m:
            return "INTERNALS", m, 0.8, "computed from Polygon grouped daily"
        return None, None, 0, "usi_unmapped"
    if ex in ("GLASSNODE", "INTOTHEBLOCK", "COINMETRICS"):
        a, _, m = t.partition("_")
        asset = CM_ASSETS.get(a[:6]) or CM_ASSETS.get(a[:4]) \
            or CM_ASSETS.get(a[:3])
        mk = m.replace("_", "").replace("-", "")
        met = next((CM_METRICS[k] for k in CM_METRIC_KEYS if k in mk), None)
        if asset and met:
            return ("COINMETRICS", f"{asset}|{met}", 0.8,
                    f"on-chain via Coin Metrics community ({met})")
        return None, None, 0, "onchain_unmapped"
    if ex in ("COT3", "COT", "CFTC"):
        c = re.match(r"^(\d{5,6})", t)
        if c:
            toks = set(t.split("_"))
            ds = "jun7-fc8e" if "FO" in toks else "6dca-aqww"
            if "COMMERCIAL" in t and "NONCOMM" not in t:
                fld = "comm_net"
            elif "NONREPT" in t or "SMALL" in t:
                fld = "nonrept_net"
            elif "OI" in toks or "OPENINTEREST" in t:
                fld = "open_interest"
            else:
                fld = "noncomm_net"
            if fld.endswith("_net") and "L" in toks:
                fld = fld.replace("_net", "_long")
            elif fld.endswith("_net") and "S" in toks:
                fld = fld.replace("_net", "_short")
            return ("COT", f"{ds}|{c.group(1)}|{fld}", 0.85,
                    "CFTC public reporting (free weekly)")
        return None, None, 0, "cot_unmapped"
    if ex in ("CRYPTOCAP", "BINANCE", "COINBASE", "BITSTAMP", "BITFINEX"):
        return "COINGECKO", t.replace("USDT", "").replace("USD", "").lower(), \
            0.6, "crypto"
    return None, None, 0, f"exchange_unsupported:{ex}"


def fred_search_factory(cache):
    """FRED /series/search → best long-history match, memoised."""
    def search(term):
        if term in cache:
            return cache[term] or None
        try:
            q = urllib.parse.quote(term)
            d = _http("https://api.stlouisfed.org/fred/series/search"
                      f"?search_text={q}&api_key={FRED_KEY}&file_type=json"
                      "&limit=5&order_by=popularity&sort_order=desc")
            best = None
            for s in d.get("seriess") or []:
                if s.get("frequency_short") in ("D", "W", "M", "Q"):
                    best = (s["id"], s.get("title", ""))
                    break
            cache[term] = best
            return best
        except Exception:
            cache[term] = None
            return None
    return search


# ── fetchers ─────────────────────────────────────────────────────────
def _yahoo(sym, start):
    import datetime as _dt
    p1 = int(_dt.datetime.fromisoformat(start).timestamp())
    p2 = int(_dt.datetime.now().timestamp())
    d = _http("https://query1.finance.yahoo.com/v8/finance/chart/"
              f"{urllib.parse.quote(sym)}?period1={p1}&period2={p2}"
              "&interval=1d&events=history")
    res = ((d.get("chart") or {}).get("result") or [None])[0]
    if not res:
        return {}
    ts = res.get("timestamp") or []
    qs = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    cl = qs.get("close") or []
    adj = (((res.get("indicators") or {}).get("adjclose") or [{}])[0]
           .get("adjclose")) or cl
    out = {}
    for t, c in zip(ts, adj):
        if c is None:
            continue
        out[_dt.datetime.utcfromtimestamp(t).date().isoformat()] = float(c)
    return out


def _stooq(sid, start):
    txt = _http(f"https://stooq.com/q/d/l/?s={sid}&i=d", raw=True)
    if "<" in txt[:40] or "limit" in txt[:80].lower():
        return {}
    out = {}
    for row in csv.DictReader(io.StringIO(txt)):
        d_, c = row.get("Date"), row.get("Close")
        if d_ and c and d_ >= start:
            try:
                out[d_] = float(c)
            except Exception:
                pass
    return out


_INTERNALS_CACHE = {}


def _internals(metric, start):
    """US market internals we compute ourselves from the Polygon grouped
    feed — the vendor charge for these ($$$/mo) buys nothing we cannot
    calculate from data Khalid already owns."""
    global _INTERNALS_CACHE
    if not _INTERNALS_CACHE:
        try:
            import boto3
            b = boto3.client("s3", region_name="us-east-1").get_object(
                Bucket=os.environ.get("S3_BUCKET",
                                      "justhodl-dashboard-live"),
                Key="data/market-internals.json")["Body"].read()
            _INTERNALS_CACHE = json.loads(b).get("series") or {}
        except Exception as e:
            print(f"[series_source] internals unavailable: {str(e)[:70]}")
            _INTERNALS_CACHE = {"__none__": {}}
    ser = _INTERNALS_CACHE.get(metric) or {}
    return {d: float(v) for d, v in ser.items() if d >= start}


EODHD_KEY = os.environ.get("EODHD_API_KEY", "")

# TradingView exchange → EODHD exchange code
TV_EODHD = {
    "LSE": "LSE", "AIM": "LSE",
    "XETR": "XETRA", "SWB": "F", "FWB": "F", "TRADEGATE": "F",
    "GETTEX": "F", "BER": "BE", "MUN": "MU", "DUS": "DU", "STU": "STU",
    "EURONEXT": "AS", "AMS": "AS", "EPA": "PA", "EBR": "BR", "ELI": "LS",
    "MIL": "MI", "BIT": "MI", "BME": "MC", "SIX": "SW", "SWX": "SW",
    "VIE": "VI", "WSE": "WAR",
    "SSE": "SHG", "SZSE": "SHE", "HKEX": "HK", "TSE": "TSE",
    "TSX": "TO", "TSXV": "V", "ASX": "AU", "NZX": "NZ",
    "NSE": "NSE", "BSE": "BSE", "KRX": "KO", "TWSE": "TW", "SGX": "SG",
    "IDX": "JK", "BMV": "MX", "BVMF": "SA", "JSE": "JSE", "TASE": "TA",
    "OMXSTO": "ST", "OMXHEX": "HE", "OMXCOP": "CO", "OSL": "OL",
    "FTSE": "INDX", "INDEX": "INDX", "USI": "INDX",
    "CBOEEU": "INDX", "ICEEUR": "COMM", "EUREX": "COMM",
    "NYMEX": "COMM", "COMEX": "COMM", "CBOT": "COMM", "ICEUS": "COMM",
}
_eod_search_cache = {}


def eodhd_resolve(sym):
    """TV symbol → EODHD ticker. Direct exchange-code first; EODHD's own
    search endpoint resolves the ambiguous ones (Euronext spans AS/PA/BR/LS)."""
    if ":" not in sym:
        return f"{sym}.US"
    ex, t = sym.split(":", 1)
    code = TV_EODHD.get(ex)
    if code:
        return f"{t}.{code}"
    return None


def eodhd_search(term):
    """authoritative lookup when the suffix guess is wrong."""
    if not EODHD_KEY:
        return None
    if term in _eod_search_cache:
        return _eod_search_cache[term]
    try:
        d = _http(f"https://eodhd.com/api/search/{urllib.parse.quote(term)}"
                  f"?api_token={EODHD_KEY}&fmt=json&limit=5")
        hit = None
        for r in (d if isinstance(d, list) else []):
            if r.get("Code") and r.get("Exchange"):
                hit = f"{r['Code']}.{r['Exchange']}"
                break
        _eod_search_cache[term] = hit
        return hit
    except Exception:
        _eod_search_cache[term] = None
        return None


def _eodhd(sid, start):
    """EODHD EOD: 60+ exchanges. Only used for what free sources genuinely
    miss — the token is a fallback, not the default path."""
    if not EODHD_KEY:
        return {}
    u = (f"https://eodhd.com/api/eod/{urllib.parse.quote(sid)}"
         f"?from={start}&period=d&fmt=json&api_token={EODHD_KEY}")
    d = _http(u, timeout=30)
    out = {}
    for r in (d if isinstance(d, list) else []):
        c = r.get("adjusted_close", r.get("close"))
        if r.get("date") and c is not None:
            out[r["date"]] = float(c)
    return out


def _coinmetrics(sid, start):
    """sid = 'asset|Metric' → Coin Metrics community v4 (no key, daily)."""
    asset, _, met = sid.partition("|")
    out = {}
    url = ("https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
           f"?assets={asset}&metrics={met}&frequency=1d&page_size=10000"
           f"&start_time={start}")
    for _ in range(6):                     # paged
        d = _http(url, timeout=30)
        for r in d.get("data") or []:
            v = r.get(met)
            if v not in (None, ""):
                try:
                    out[str(r.get("time", ""))[:10]] = float(v)
                except Exception:
                    pass
        url = d.get("next_page_url")
        if not url:
            break
    return out


_COT_F = {"noncomm_net": ("noncomm_positions_long_all",
                          "noncomm_positions_short_all"),
          "comm_net": ("comm_positions_long_all",
                       "comm_positions_short_all"),
          "nonrept_net": ("nonrept_positions_long_all",
                          "nonrept_positions_short_all"),
          "noncomm_long": ("noncomm_positions_long_all", None),
          "noncomm_short": ("noncomm_positions_short_all", None),
          "comm_long": ("comm_positions_long_all", None),
          "comm_short": ("comm_positions_short_all", None),
          "open_interest": ("open_interest_all", None)}


def _cot(sid, start):
    """sid = 'dataset|cftc_code|field' → CFTC Socrata (free, weekly)."""
    ds, code, fld = sid.split("|")
    lcol, scol = _COT_F.get(fld, _COT_F["noncomm_net"])
    cols = ",".join(c for c in ("report_date_as_yyyy_mm_dd", lcol, scol) if c)
    d = _http(f"https://publicreporting.cftc.gov/resource/{ds}.json"
              f"?cftc_contract_market_code={code}&%24select={cols}"
              "&%24order=report_date_as_yyyy_mm_dd&%24limit=5000", timeout=30)
    out = {}
    for r in d if isinstance(d, list) else []:
        dt = str(r.get("report_date_as_yyyy_mm_dd", ""))[:10]
        if not dt or dt < start:
            continue
        try:
            v = float(r.get(lcol) or 0)
            if scol:
                v -= float(r.get(scol) or 0)
            out[dt] = v
        except Exception:
            pass
    return out


def fetch(source, sid, start="1990-01-01"):
    """→ {ISO date: float}. Never raises.

    MARKET is a CHAIN: Yahoo (deep, free) → Stooq (deep, but blocks some
    datacenter IPs) → Polygon (~5y only). ops 3167 proved Stooq alone is
    unreliable from the runner, so no single market source is trusted.
    """
    try:
        if source == "MARKET":
            for fn, arg in ((_yahoo, sid), (_stooq, _stooq_id(sid))):
                try:
                    ser = fn(arg, start)
                    if len(ser) > 200:
                        return ser
                except Exception:
                    continue
            p = _polygon(sid, start)
            if p:
                return p
            if EODHD_KEY:                    # paid fallback, only if funded
                try:
                    return _eodhd(sid.replace("=F", ".COMM"), start)
                except Exception:
                    return {}
            return {}
        if source == "YAHOO":
            return _yahoo(sid, start)
        if source == "FRED":
            # ops 3172: a STALE FRED key in the lambda env silently returned
            # nothing (3170/3171 shipped 1,746 all-NEUTRAL regime weeks off
            # this). Try the env key, then always retry the known-good one.
            d = {}
            for _k in (FRED_KEY, FRED_FALLBACK):
                try:
                    d = _http("https://api.stlouisfed.org/fred/series/"
                              f"observations?series_id={sid}&api_key={_k}"
                              f"&file_type=json&observation_start={start}")
                    if d.get("observations"):
                        break
                except Exception:
                    continue
            out = {}
            for o in d.get("observations") or []:
                v = o.get("value")
                if v not in (".", "", None):
                    try:
                        out[o["date"]] = float(v)
                    except Exception:
                        pass
            return out
        if source == "STOOQ":
            return _stooq(sid, start)
        if source == "EODHD":
            return _eodhd(sid, start)
        if source == "INTERNALS":
            return _internals(sid, start)
        if source == "WORLDBANK":
            return _worldbank(sid, start)
        if source == "DBNOMICS_V2":
            return _dbnomics(sid, start)
        if source == "DBNOMICS":
            d = _http(f"https://api.db.nomics.world/v22/series/{sid}"
                      "?observations=1")
            docs = (d.get("series") or {}).get("docs") or []
            if not docs:
                return {}
            per = docs[0].get("period") or []
            val = docs[0].get("value") or []
            return {p: float(v) for p, v in zip(per, val)
                    if isinstance(v, (int, float)) and p >= start[:len(p)]}
        if source == "COINMETRICS":
            return _coinmetrics(sid, start)
        if source == "COT":
            return _cot(sid, start)
        if source == "COINGECKO":
            if sid in ("total", "btc_dominance"):
                return {}
            d = _http(f"https://api.coingecko.com/api/v3/coins/{sid}"
                      "/market_chart?vs_currency=usd&days=max&interval=daily")
            import datetime as _dt
            return {_dt.datetime.utcfromtimestamp(t / 1000).date().isoformat():
                    float(p) for t, p in (d.get("prices") or [])}
    except Exception as e:
        print(f"[series_source] {source}:{sid} failed: {str(e)[:90]}")
    return {}


def _worldbank(spec, start):
    """spec = 'ISO2|INDICATOR'. Free, no key, 1960+, ~200 countries."""
    iso2, ind = spec.split("|", 1)
    d = _http(f"https://api.worldbank.org/v2/country/{iso2}/indicator/{ind}"
              f"?format=json&per_page=300&date={start[:4]}:2026")
    if not isinstance(d, list) or len(d) < 2 or not d[1]:
        return {}
    out = {}
    for row in d[1]:
        v, y = row.get("value"), row.get("date")
        if v is not None and y:
            out[f"{y}-12-31"] = float(v)
    return out


def _dbnomics(sid, start):
    d = _http(f"https://api.db.nomics.world/v22/series/{sid}?observations=1")
    docs = ((d.get("series") or {}).get("docs") or [])
    if not docs:
        return {}
    per = docs[0].get("period") or []
    val = docs[0].get("value") or []
    out = {}
    for p_, v in zip(per, val):
        if not isinstance(v, (int, float)):
            continue
        iso = str(p_)
        if len(iso) == 4:
            iso = f"{iso}-12-31"
        elif len(iso) == 7:
            iso = f"{iso}-28"
        if iso >= start:
            out[iso] = float(v)
    return out


def _stooq_id(sym):
    """Yahoo-style id → Stooq id (^GSPC → ^spx, SPY → spy.us)."""
    y2s = {"^GSPC": "^spx", "^NDX": "^ndx", "^DJI": "^dji",
           "^VIX": "^vix", "^N225": "^nkx", "^GDAXI": "^dax",
           "^FTSE": "^ukx", "^HSI": "^hsi", "DX-Y.NYB": "^dxy",
           "GC=F": "xauusd", "SI=F": "xagusd", "CL=F": "cl.f"}
    if sym in y2s:
        return y2s[sym]
    if sym.startswith("^") or "=" in sym:
        return sym.lower()
    return f"{sym.lower()}.us"


def _polygon(tk, start):
    key = os.environ.get("POLYGON_KEY", "")
    if not key or tk.startswith("^") or "=" in tk:
        return {}
    import datetime as _dt
    d1 = _dt.date.today().isoformat()
    d = _http(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/"
              f"{start}/{d1}?adjusted=true&sort=asc&limit=50000&apiKey={key}")
    import datetime as _dt2
    return {_dt2.datetime.utcfromtimestamp(r["t"] / 1000).date().isoformat():
            r["c"] for r in (d.get("results") or [])}


def validate_template(tpl, i3="DEU"):
    """A template that returns nothing is worse than no template — ops
    3167 shipped two wrong OECD ids (GDP, CPI). Test-fetch before trust."""
    sid = tpl.format(i3=i3)
    return len(fetch("FRED", sid, "2015-01-01")) > 4
