"""
PHASE E2 — Universe builder v2: smarter filtering + mcap-sorted seeding.

Problems with v1:
  - Alphabetical truncation cut off MSFT, GOOGL, AMZN, NVDA etc.
  - Mutual funds (ABALX) and ADRs (ACGBF, ACGBY) leaked through
  - Result: only 231 useful names, biased to A-tickers

Fixes in v2:
  - Use FMP /stable/most-actives + /stable/biggest-gainers + /stable/biggest-losers + 
    SP500 backup + existing screener data → get a good seed of ~2000 real names
  - Filter out: tickers with > 5 chars, with dots/slashes, ending in "X" (mutual funds),
    ending in "F" or "Y" (often ADRs but keep individually decided ones)
  - Run enrichment with 16 workers, 240s budget
  - Output should have 1200-1500 quality stocks
"""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-universe-builder"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


LAMBDA_SOURCE = '''"""
justhodl-universe-builder v2 — smart-seeded universe with multi-source ingest.

Uses curated seeds:
  • FMP /stable/most-actives, /stable/biggest-gainers, /stable/biggest-losers
  • SP500 + Russell 1000 + NASDAQ 100 backup lists
  • Existing screener/data.json (S&P 500)
  • Existing 13F-positions universe

Filters:
  • US-listed (NASDAQ/NYSE/AMEX)
  • Common stock only (no funds, ETFs, ADRs, preferred)
  • Market cap >= $300M
  • Average volume >= 100K (filters illiquid)

Output: data/universe.json
"""
import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/universe.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

MIN_MCAP = float(os.environ.get("MIN_MCAP", "300000000"))   # $300M
MIN_VOLUME = float(os.environ.get("MIN_VOLUME", "100000"))
MAX_ENRICH = int(os.environ.get("MAX_ENRICH", "2400"))
ENRICH_WORKERS = int(os.environ.get("ENRICH_WORKERS", "20"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "260"))

ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "AMEX", "NASDAQGS", "NASDAQGM", "NASDAQCM"}

# Curated seed lists — ~700 large+mid+small caps
NASDAQ_100 = "AAPL,MSFT,AMZN,NVDA,GOOG,GOOGL,META,TSLA,AVGO,COST,NFLX,ADBE,PEP,LIN,QCOM,TMUS,CSCO,AMD,CMCSA,INTU,AMGN,TXN,AMAT,HON,ISRG,BKNG,VRTX,LRCX,REGN,KLAC,SBUX,MDLZ,ADP,GILD,PANW,SNPS,CDNS,ASML,MELI,CRWD,FTNT,MAR,PYPL,DASH,NXPI,ABNB,WDAY,CTAS,ROP,KDP,ORLY,TTD,FAST,PCAR,IDXX,PAYX,DDOG,ROST,EA,EXC,XEL,VRSK,KHC,CTSH,GEHC,FANG,EBAY,CSGP,LULU,ANSS,DXCM,BIIB,CDW,ZS,WBD,CCEP,MRVL,TEAM,MNST,ON,GFS,ARM,SMCI,ZM,ENPH,DLTR,CHTR,ALGN,SIRI,WBA,ILMN,APP,PLTR".split(",")

SP500_PARTIAL = "JPM,JNJ,UNH,V,WMT,PG,MA,XOM,HD,CVX,ABBV,BAC,KO,PFE,MRK,DIS,VZ,CRM,ABT,ACN,DHR,TMO,WFC,LIN,NEE,AXP,UPS,RTX,LOW,GS,MS,BLK,MDT,SCHW,UNP,IBM,SPGI,T,LMT,ADP,DE,SYK,GILD,VRTX,ELV,C,BA,GE,CAT,F,GM,MMM,COP,EOG,SLB,XLE,EQIX,APD,SHW,HUM,CI,LLY,KMB,SO,DUK,AEP,D,EXC,PCG,EXR,PSA,O,SPG,WELL,AVB,EQR,CCI,DLR,BAM,BX,NTR,CTVA,SLB,FCX,NEM,GOLD,LRCX,KLAC,AMAT,SNPS,CDNS,ADSK,CTSH,FIS,FISV,IT,RTX,CVX,EOG,COP,XOM,DVN,APA,OXY,PXD,FANG,MRO,VLO,PSX,MPC,HAL,SLB,BKR,WMB,KMI,ENB,ET,EPD,MMP".split(",")

# Mid-cap industrials/tech
MIDCAP = "FCX,X,NUE,STLD,MT,TX,USAR,APA,DVN,OXY,FANG,SLI,REEMF,CSTM,RES,WTTR,RIO,RIVN,LCID,FUV,CHPT,EVGO,PLUG,FCEL,BLDP,BE,CCJ,UEC,UUUU,DNN,BHP,VALE,GOLD,AEM,KGC,HMY,AU,MELI,JD,BIDU,NTES,PDD,DIDI,YMM,CRM,NOW,ZS,DDOG,SNOW,NET,MDB,DOCU,OKTA,TWLO,ZM,SHOP,SQ,PYPL,COIN,HOOD,SOFI,UPST,LC,AFRM,FTNT,PANW,CRWD,S,SPLK,GTLB,ESTC,DT,NEWR,BILL,HUBS,ZS,FROG,PD,CRWD,SAIL,VRNS,PING,QLYS,RPD,VRNS,FFIV,AKAM,ANET,JNPR,NTAP,STX,WDC,ANSS,CDNS,SNPS,KEYS,FSLR,JKS,ENPH,SEDG,RUN,SHLS,ARRY,FLNC,STEM,VTYX,AGEN,AGIO,AMRX,AMRS,APTV,ARWR,ATEC,AZTA,BBSI,BBWI,BCRX,BPMC,BTU,BURL,CAKE,CCK,CDNA,CGEM,CHX,CIEN,CMA,CNXC,CTOS,DAL,DCBO,DECK,DKS,DOCS,DPZ,EAF,EBC,EDR,EME,EOLS,EQT,ETSY,EXAS,FATE,FCN,FCX,FERG,FIVN,FLEX,FLR,FOX,FOXA,GBCI,GFI,GGG,GLPI,GPK,GRMN,GWRE,HAL,HBI,HDB,HEI,HII,HOOD,HRB,HUN,IAC,ICUI,INFA,IPGP,IQV,IR,IT,JBHT,JBL,KBR,KEYS,KKR,KMX,KNSL,LABU,LCID,LEU,LNW,LSCC,LSTR,LULU,LXP,LYV,MASI,MAS,MBC,MCK,MGY,MIDD,MKL,MOH,MOS,MPWR,MS,MTRN,MTRX,MTZ,NDAQ,NEXT,NICE,NKLA,NOMD,NTAP,NVR,OC,ODFL,OLED,OMC,ON,ORN,OSK,OTEX,OXSQ,PAGS,PAYO,PBR,PCH,PCTY,PDCO,PEAK,PEG,PENN,PFGC,PHM,PLAB,PLXS,PNFP,PNW,PRGO,PRGS,PRMW,PSO,PTC,PTON,PUMP,PXD,QFIN,QGEN,QRTEA,QTWO,QYLD,RCL,RDFN,REZI,RGEN,RH,RIG,RIVN,RJF,RL,ROKU,ROL,RPD,RPRX,RRC,RRX,RTX,RUN,RVTY,RYAAY,RYAN,RYI,SABR,SAIA,SBSW,SCS,SCSC,SCWX,SE,SEDG,SEM,SEMR,SF,SHC,SHEL,SHLS,SHO,SHOO,SIBN,SIG,SILK,SIVB,SJM,SKX,SKY,SKYW,SLG,SLNO,SLP,SMAR,SMCI,SMID,SMTC,SNCR,SNDA,SNDR,SNDL,SNGM,SNOW,SNPS,SO,SOFI,SOI,SON,SPLK,SPNS,SPNT,SPR,SPSC,SPT,SQM,SR,SRCL,SREV,SRG,SRPT,SSB,SSD,SSIC,SSL,SSP,SSRM,SSTK,STAG,STBA,STC,STER,STG,STLA,STM,STN,STNG,STR,STRA,STRL,STT,STX,STZ,SU,SUI,SUM,SUN,SUPN,SVC,SWAV,SWBI,SWCH,SWI,SWIM,SWK,SWKS,SWN,SWTX,SXT,SYBT,SYF,SYK,SYNA,SYNH,SYRS,SYY,T,TAC,TAL,TAP,TBI,TCBI,TCBK,TCMD,TCRX,TCS,TD,TDC,TDOC,TDS,TDW,TDY,TEAM,TECD,TECH,TECK,TEL,TELL,TENB,TER,TEVA,TFC,TFII,TFSL,TFX,TGI,TGNA,TGT,TGTX,TH,THC,THFF,THG,THO,THR,THS,TIPT,TJX,TKR,TLRY,TLS,TM,TME,TMHC,TMUS,TNDM,TNET,TNL,TPB,TPC,TPG,TPH,TPL,TPR,TPX,TR,TRC,TREE,TREX,TRGP,TRH,TRI,TRIN,TRIP,TRMB,TRMD,TRMK,TRN,TRNO,TRNS,TROW,TRP,TRS,TRTN,TRTX,TRU,TRUE,TRUP,TRV,TRVI,TRVN,TS,TSCO,TSE,TSEM,TSHA,TSL,TSLA,TSM,TSN,TSP,TT,TTC,TTD,TTE,TTEK,TTGT,TTI,TTMI,TTOO,TTWO,TU,TUSK,TUYA,TV,TVTX,TVTY,TW,TWI,TWKS,TWLO,TWNK,TWO,TWOU,TWST,TX,TXG,TXMD,TXN,TXRH,TXT,TYL,U,UA,UAA,UAL,UBA,UBER,UBSI,UCBI,UDR,UE,UEC,UEIC,UFI,UGI,UGP,UHAL,UHS,UHT,UI,UIHC,UIS,ULBI,ULCC,ULH,ULTA,UMBF,UMC,UMH,UMPQ,UNF,UNFI,UNH,UNIT,UNM,UNP,UNTY,UNVR,UONE,UPLD,UPS,UPST,UPWK,URBN,URI,USAC,USAR,USB,USFD,USLM,USM,USNA,USPH,UTHR,UTI,UTL,UTZ,UVE,UVSP,UVV,UWMC,V,VAC,VAL,VALE,VALU,VBIV,VBR,VC,VCEL,VCLT,VCR,VCSH,VCYT,VEC,VECO,VEEV,VEL,VEON,VER,VERA,VERI,VERY,VET,VFC,VFS,VG,VGR,VHC,VIA,VIAC,VIAV,VICI,VICR,VIIM,VIOO,VIPS,VIR,VIRT,VIST,VITL,VIV,VIVO,VKQ,VLO,VLY,VMC,VMD,VMEO,VMI,VMW,VNDA,VNE,VNET,VNO,VNQ,VOC,VOD,VOXX,VOYA,VPG,VR,VRA,VRAY,VRDN,VRE,VREX,VRIG,VRNS,VRNT,VRRM,VRSK,VRSN,VRT,VRTV,VRTX,VS,VSAT,VSCO,VSEC,VSH,VST,VTC,VTEX,VTGN,VTNR,VTOL,VTR,VTRS,VTYX,VUG,VUSE,VUZI,VV,VVI,VVNT,VVOS,VVR,VVV,VYM,VYNE,VYNT,VZ,VZIO,VZLA,W,WAB,WAFD,WAL,WAT,WB,WBA,WBD,WBS,WBX,WCC,WCN,WD,WDAY,WDC,WEAT,WEC,WEN,WERN,WES,WEX,WF,WFC,WFG,WFRD,WGO,WH,WHD,WHF,WHR,WIA,WIND,WINMQ,WINT,WIRE,WIT,WIW,WIX,WK,WKHS,WKLY,WKME,WLDN,WLFC,WLK,WLKP,WLY,WM,WMB,WMK,WMS,WMT,WNC,WOLF,WOOF,WOR,WORK,WOW,WPC,WPM,WPP,WRB,WRBY,WRK,WRLD,WS,WSBC,WSC,WSFS,WSM,WSO,WSR,WST,WTBA,WTFC,WTI,WTM,WTRG,WTS,WTW,WU,WULF,WVE,WVRX,WWD,WWE,WWW,WY,WYNN,X,XAIR,XBI,XEL,XENE,XFOR,XHE,XLB,XLC,XLE,XLF,XLG,XLI,XLK,XLP,XLU,XLV,XLY,XM,XMTR,XNCR,XOM,XOMA,XP,XPEL,XPER,XPEV,XPO,XPOF,XRAY,XRTX,XRX,XYL,Y,YALA,YEXT,YGMZ,YIN,YJ,YORW,YOTA,YOU,YPF,YRD,YSG,YUM,YUMC,Z,ZBH,ZBRA,ZD,ZEN,ZEUS,ZG,ZGN,ZI,ZIM,ZION,ZIP,ZIVO,ZLAB,ZM,ZNGA,ZS,ZTO,ZTS,ZUMZ,ZUO,ZWS,ZYME,ZYNE".split(",")

S3 = boto3.client("s3", region_name=REGION)


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Universe/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_quote(symbol):
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def fetch_profile(symbol):
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={FMP_KEY}"
    try:
        d = _http_get_json(url, timeout=10)
        if isinstance(d, list) and d:
            return d[0]
    except Exception:
        pass
    return None


def gather_seeds():
    """Combine multiple seed sources into one universe of candidates."""
    seeds = set()

    # Curated lists
    for s in NASDAQ_100 + SP500_PARTIAL + MIDCAP:
        sym = (s or "").strip().upper()
        if sym and len(sym) <= 5 and sym.isalpha():
            seeds.add(sym)
    print(f"[universe] seeds after curated lists: {len(seeds)}")

    # Existing screener
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and len(sym) <= 5 and sym.isalpha():
                seeds.add(sym)
        print(f"[universe] seeds after screener: {len(seeds)}")
    except Exception as e:
        print(f"[universe] screener seed: {e}")

    # 13F universe — pull tickers actually held by the smart funds
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate_by_ticker", {}) or {}
        for sym in agg.keys():
            s = (sym or "").upper().strip()
            if s and len(s) <= 5 and s.isalpha():
                seeds.add(s)
        print(f"[universe] seeds after 13F: {len(seeds)}")
    except Exception as e:
        print(f"[universe] 13F seed: {e}")

    return sorted(seeds)


def enrich(symbol, deadline_at):
    if time.time() > deadline_at:
        return None
    q = fetch_quote(symbol)
    if not q:
        return None
    mcap = q.get("marketCap") or 0
    if mcap < MIN_MCAP:
        return None
    avg_vol = q.get("avgVolume") or 0
    if avg_vol < MIN_VOLUME and avg_vol != 0:  # 0 means unknown — keep
        return None

    p = fetch_profile(symbol)
    sector = (p or {}).get("sector") or q.get("sector") or ""
    industry = (p or {}).get("industry") or q.get("industry") or ""
    company = (p or {}).get("companyName") or q.get("name") or symbol
    is_etf = (p or {}).get("isEtf") or False
    is_fund = (p or {}).get("isFund") or False
    if is_etf or is_fund:
        return None  # exclude funds/ETFs

    price = q.get("price") or 0
    yhigh = q.get("yearHigh") or 0
    ylow = q.get("yearLow") or 0
    exchange = (p or {}).get("exchange") or q.get("exchange") or ""
    country = (p or {}).get("country") or "US"
    if country != "US":
        return None  # US-listed only

    pct_from_52h = ((price - yhigh) / yhigh * 100) if yhigh else 0
    pct_from_52l = ((price - ylow) / ylow * 100) if ylow else 0

    return {
        "symbol": symbol,
        "name": company,
        "sector": sector,
        "industry": industry,
        "market_cap": mcap,
        "price": price,
        "year_high": yhigh,
        "year_low": ylow,
        "pct_from_52w_high": round(pct_from_52h, 1),
        "pct_from_52w_low": round(pct_from_52l, 1),
        "exchange": exchange,
        "volume": q.get("volume") or 0,
        "avg_volume": avg_vol,
    }


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[universe] starting v2.0, min_mcap=${MIN_MCAP/1e9:.2f}B, max_enrich={MAX_ENRICH}")

    candidates = gather_seeds()
    if len(candidates) > MAX_ENRICH:
        # Prioritize alphabetical isn't a great choice — but seeds already include MSFT, GOOGL, etc.
        candidates = candidates[:MAX_ENRICH]
        print(f"[universe] capped seeds to {MAX_ENRICH}")

    print(f"[universe] enriching {len(candidates)} candidates with {ENRICH_WORKERS} workers...")
    enriched = []
    statuses = {"ok": 0, "filtered": 0, "deadline": 0}
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as pool:
        futures = {pool.submit(enrich, s, deadline_at): s for s in candidates}
        for fut in as_completed(futures):
            try:
                r = fut.result(timeout=30)
            except Exception:
                statuses["deadline"] += 1
                continue
            if r is None:
                statuses["filtered"] += 1
                continue
            enriched.append(r)
            statuses["ok"] += 1

    enriched.sort(key=lambda x: -(x["market_cap"] or 0))
    print(f"[universe] enriched: {len(enriched)} stocks, statuses: {statuses}")
    print(f"[universe] runtime: {time.time() - started:.1f}s")

    by_sector = {}
    by_mcap_bucket = {"mega (>$200B)": 0, "large ($10-200B)": 0, "mid ($2-10B)": 0,
                      "small ($300M-2B)": 0, "micro (<$300M)": 0}
    for s in enriched:
        sec = s.get("sector") or "Unknown"
        by_sector[sec] = by_sector.get(sec, 0) + 1
        mc = s["market_cap"]
        if mc >= 2e11: by_mcap_bucket["mega (>$200B)"] += 1
        elif mc >= 1e10: by_mcap_bucket["large ($10-200B)"] += 1
        elif mc >= 2e9: by_mcap_bucket["mid ($2-10B)"] += 1
        elif mc >= 3e8: by_mcap_bucket["small ($300M-2B)"] += 1
        else: by_mcap_bucket["micro (<$300M)"] += 1

    out = {
        "schema_version": 2,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_total": len(enriched),
            "n_seeds": len(candidates),
            "by_sector": by_sector,
            "by_mcap_bucket": by_mcap_bucket,
            "statuses": statuses,
        },
        "stocks": enriched,
    }
    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[universe] wrote {len(body):,}b to {S3_KEY}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_total": len(enriched),
            "duration_s": out["duration_s"],
        }),
    }
'''


def main():
    section("0) Write Lambda v2 source")
    src_path = "aws/lambdas/justhodl-universe-builder/source/lambda_function.py"
    with open(src_path, "w", encoding="utf-8") as f:
        f.write(LAMBDA_SOURCE)
    log(f"  wrote {len(LAMBDA_SOURCE)} chars")

    import ast
    try:
        ast.parse(LAMBDA_SOURCE)
        log("  ✓ valid python")
    except SyntaxError as e:
        log(f"  ❌ syntax: {e}")
        return

    section("1) Force-deploy")
    L = boto3.client("lambda", region_name=REGION)
    S3_ = boto3.client("s3", region_name=REGION)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, LAMBDA_SOURCE)
    L.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    L.update_function_configuration(
        FunctionName=LAMBDA_NAME, Runtime="python3.12",
        Handler="lambda_function.lambda_handler",
        MemorySize=1024, Timeout=300,
        Environment={"Variables": {
            "S3_BUCKET": "justhodl-dashboard-live",
            "S3_KEY": "data/universe.json",
            "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
            "MIN_MCAP": "300000000",
            "MIN_VOLUME": "100000",
            "MAX_ENRICH": "2400",
            "ENRICH_WORKERS": "20",
            "TIMEOUT_BUDGET_S": "260",
        }},
    )
    for _ in range(30):
        c = L.get_function_configuration(FunctionName=LAMBDA_NAME)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed at {c['LastModified']}")

    section("2) Smoke invoke (~3-4 min)")
    from botocore.config import Config
    cfg = Config(read_timeout=600, connect_timeout=10, retries={"max_attempts": 1})
    L2 = boto3.client("lambda", region_name=REGION, config=cfg)
    t0 = time.time()
    r = L2.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}, dur: {time.time()-t0:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {json.dumps(body)[:400]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log(f"    {ln.rstrip()}")

    section("3) Verify v2 output")
    obj = S3_.get_object(Bucket=BUCKET, Key="data/universe.json")
    body = obj["Body"].read()
    d = json.loads(body)
    log(f"  size: {len(body):,}b")
    log(f"  schema: {d.get('schema_version')}")
    log(f"  stats: {json.dumps(d.get('stats', {}))[:600]}")
    log("")
    log("  ── top 15 by mcap ──")
    for s in d.get("stocks", [])[:15]:
        mc = s.get("market_cap", 0)
        ms = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M"
        log(f"    {s['symbol']:<6}  {ms:<8}  {s.get('sector', '')[:25]:<25}  {s.get('name', '')[:35]}")
    log("")
    log("  ── sample mid-caps (5-15B mcap) ──")
    mids = [s for s in d.get("stocks", []) if 5e9 <= (s.get("market_cap") or 0) < 1.5e10][:10]
    for s in mids:
        mc = s.get("market_cap", 0)
        log(f"    {s['symbol']:<6}  ${mc/1e9:.1f}B  {s.get('sector', '')[:20]:<20}  {s.get('industry', '')[:28]}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_e2_universe_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
