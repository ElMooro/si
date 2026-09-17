"""justhodl-ecb-history — full 1997→now history for the key ECB series, so tiles
become clickable charts (the audit's one real, valuable gap).

VERIFIED from AWS: ECB is NOT WAF-blocking (Mozilla UA returns 200), and
csvdata + startPeriod=1997-01-01 returns full history (CISS ~7,685 daily rows
back to ~1999, ILM ~1,432 weekly rows back to 1998). The existing ecb-detail
engine only stores today's point values (2KB, no history). This adds the history.

Per series → data/ecb-hist/<id>.json: {id, label, freq, points:[[date,value]...],
latest, min, max, percentile, z}. SCHEDULE: weekdays 06:00 UTC. Units and dates validated before publication.
"""
import json, time, ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from ecb_measurements import METHOD, parse_csv, summarize, yoy_series
import urllib.request
from io import StringIO
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
BASE = "https://data-api.ecb.europa.eu/service/data/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/vnd.sdmx.data+json;version=1.0.0-wd, text/csv;q=0.9, */*;q=0.5",
    "Accept-Language": "en-US,en;q=0.9", "Accept-Encoding": "gzip, deflate",
}
s3 = boto3.client("s3", region_name=REGION)
_ctx = ssl.create_default_context()

# (flow/series_key, id, human label) — the high-signal liquidity/stress series
SERIES = [
    ("ILM/W.U2.C.T000000.Z5.Z01", "total_assets", "Eurosystem total assets (EUR bn, stock)"),
    ("ICP/M.U2.N.000000.4.ANR", "hicp_headline", "Euro-area HICP annual inflation (%)"),
    ("ILM/W.U2.C.A030000.U2.Z06", "ilm_usd_claims", "Claims on EA residents in foreign currency (EUR bn, stock)"),
    ("ILM/W.U2.C.A020000.U4.Z06", "fx_claims_nonea", "Claims on non-EA residents in foreign currency (EUR bn, stock)"),
    ("ILM/W.U2.C.L060000.U4.EUR", "ilm_eur_to_nonres", "EUR liabilities to non-residents (EUR bn, stock)"),
    ("ILM/W.U2.C.A050000.U2.EUR", "ilm_mp_lending", "Monetary policy lending to banks (€bn)"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX", "ciss_ea", "CISS — Euro Area systemic stress composite"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_FIN.CON", "ciss_fi", "CISS — financial intermediaries sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_BMN.CON", "ciss_bo", "CISS — bond market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_FXN.CON", "ciss_fx", "CISS — FX market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_EMN.CON", "ciss_eq", "CISS — equity market sub-index"),
    ("CISS/D.U2.Z0Z.4F.EC.SS_MMN.CON", "ciss_mm", "CISS — money market sub-index"),
    ("LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T", "unemployment_ea", "Unemployment rate — Euro Area (%, ages 15-74)"),
    ("STS/M.I9.Y.PROD.NS0010.4.000", "indprod_total", "Industrial production — total incl. construction (index, WDA+SA)"),
    ("STS/M.I9.Y.PROD.NS0020.4.000", "indprod_core", "Industrial production — excl. construction / core industry (index, WDA+SA)"),
    ("EXR/D.USD.EUR.SP00.A", "eurusd", "EUR/USD reference rate — dollar-strength / funding gauge"),
    # ── Unemployment (LFSI): euro area total + youth + member states ──
    ("LFSI/M.I9.S.UNEHRT.TOTAL0.15_24.T", "unemp_ea_youth", "Unemployment rate — Euro Area youth <25 (%)"),
    ("LFSI/M.DE.S.UNEHRT.TOTAL0.15_74.T", "unemp_de", "Unemployment rate — Germany (%)"),
    ("LFSI/M.FR.S.UNEHRT.TOTAL0.15_74.T", "unemp_fr", "Unemployment rate — France (%)"),
    ("LFSI/M.IT.S.UNEHRT.TOTAL0.15_74.T", "unemp_it", "Unemployment rate — Italy (%)"),
    ("LFSI/M.ES.S.UNEHRT.TOTAL0.15_74.T", "unemp_es", "Unemployment rate — Spain (%)"),
    ("LFSI/M.NL.S.UNEHRT.TOTAL0.15_74.T", "unemp_nl", "Unemployment rate — Netherlands (%)"),
    ("LFSI/M.GR.S.UNEHRT.TOTAL0.15_74.T", "unemp_gr", "Unemployment rate — Greece (%)"),
    ("LFSI/M.PT.S.UNEHRT.TOTAL0.15_74.T", "unemp_pt", "Unemployment rate — Portugal (%)"),
    ("LFSI/M.IE.S.UNEHRT.TOTAL0.15_74.T", "unemp_ie", "Unemployment rate — Ireland (%)"),
    ("LFSI/M.AT.S.UNEHRT.TOTAL0.15_74.T", "unemp_at", "Unemployment rate — Austria (%)"),
    ("LFSI/M.BE.S.UNEHRT.TOTAL0.15_74.T", "unemp_be", "Unemployment rate — Belgium (%)"),
    ("LFSI/M.FI.S.UNEHRT.TOTAL0.15_74.T", "unemp_fi", "Unemployment rate — Finland (%)"),
    ("LFSI/M.BG.S.UNEHRT.TOTAL0.15_74.T", "unemp_bg", "Unemployment rate — Bulgaria (%)"),
    ("LFSI/M.HR.S.UNEHRT.TOTAL0.15_74.T", "unemp_hr", "Unemployment rate — Croatia (%)"),
    ("LFSI/M.CY.S.UNEHRT.TOTAL0.15_74.T", "unemp_cy", "Unemployment rate — Cyprus (%)"),
    ("LFSI/M.CZ.S.UNEHRT.TOTAL0.15_74.T", "unemp_cz", "Unemployment rate — Czechia (%)"),
    ("LFSI/M.DK.S.UNEHRT.TOTAL0.15_74.T", "unemp_dk", "Unemployment rate — Denmark (%)"),
    ("LFSI/M.EE.S.UNEHRT.TOTAL0.15_74.T", "unemp_ee", "Unemployment rate — Estonia (%)"),
    ("LFSI/M.HU.S.UNEHRT.TOTAL0.15_74.T", "unemp_hu", "Unemployment rate — Hungary (%)"),
    ("LFSI/M.LV.S.UNEHRT.TOTAL0.15_74.T", "unemp_lv", "Unemployment rate — Latvia (%)"),
    ("LFSI/M.LT.S.UNEHRT.TOTAL0.15_74.T", "unemp_lt", "Unemployment rate — Lithuania (%)"),
    ("LFSI/M.LU.S.UNEHRT.TOTAL0.15_74.T", "unemp_lu", "Unemployment rate — Luxembourg (%)"),
    ("LFSI/M.MT.S.UNEHRT.TOTAL0.15_74.T", "unemp_mt", "Unemployment rate — Malta (%)"),
    ("LFSI/M.PL.S.UNEHRT.TOTAL0.15_74.T", "unemp_pl", "Unemployment rate — Poland (%)"),
    ("LFSI/M.RO.S.UNEHRT.TOTAL0.15_74.T", "unemp_ro", "Unemployment rate — Romania (%)"),
    ("LFSI/M.SK.S.UNEHRT.TOTAL0.15_74.T", "unemp_sk", "Unemployment rate — Slovakia (%)"),
    ("LFSI/M.SI.S.UNEHRT.TOTAL0.15_74.T", "unemp_si", "Unemployment rate — Slovenia (%)"),
    ("LFSI/M.SE.S.UNEHRT.TOTAL0.15_74.T", "unemp_se", "Unemployment rate — Sweden (%)"),
    # ── Industrial production by Main Industrial Grouping (STS PROD) + turnover ──
    ("STS/M.I9.Y.PROD.NS0040.4.000", "indprod_intermediate", "Industrial production — intermediate goods (index)"),
    ("STS/M.I9.Y.PROD.NS0050.4.000", "indprod_capital", "Industrial production — capital goods (index)"),
    ("STS/M.I9.Y.PROD.NS0060.4.000", "indprod_durable", "Industrial production — durable consumer goods (index)"),
    ("STS/M.I9.Y.PROD.NS0070.4.000", "indprod_nondurable", "Industrial production — non-durable consumer goods (index)"),
    ("STS/M.I9.Y.PROD.NS0080.4.000", "indprod_energy", "Industrial production — energy (index)"),
    ("STS/M.I9.Y.TOVT.NS0020.4.000", "manuf_turnover", "Industry turnover — total excluding construction (index)"),
    ("STS/M.I9.Y.TOVT.NS0040.4.000", "retail_turnover", "Industry turnover — intermediate goods (index)"),
    # ── Money, growth, credit cost, FX crosses ──
    ("BSI/M.U2.Y.V.M10.X.I.U2.2300.Z01.A", "m1_growth", "M1 narrow money — annual growth (%) — real-economy LEAD"),
    ("MNA/Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.GY", "gdp_yoy", "Euro-area real GDP — annual growth (%)"),
    ("MIR/M.U2.B.A2A.A.R.A.2240.EUR.N", "bank_rate_nfc", "Bank lending rate to corporations (%, new business) — policy pass-through"),
    ("EXR/D.CNY.EUR.SP00.A", "eurcny", "EUR/CNY reference rate"),
    ("EXR/D.JPY.EUR.SP00.A", "eurjpy", "EUR/JPY reference rate"),
]


def fetch_csv(flow_key):
    url = BASE + flow_key + "?format=csvdata&startPeriod=1997-01-01"
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            raw = urllib.request.urlopen(req, timeout=25, context=_ctx).read()
            # handle gzip
            if raw[:2] == b"\x1f\x8b":
                import gzip; raw = gzip.decompress(raw)
            text = raw.decode("utf-8", "replace")
            return text
        except Exception as e:
            if attempt < 1: time.sleep(1.5)
            else: print(f"[ecb-hist] {flow_key} err: {str(e)[:70]}")
    return None


# ── External + derived history: Eurostat EA confidence suite, production YoY, real M1 ──
EUROSTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bssi_m_r2"
_UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
EUROSTAT_CONF = [
    ("conf_esi",          "Euro-area Economic Sentiment Indicator (ESI, long-run avg=100)", "BS-ESI-I"),
    ("conf_industrial",   "Euro-area industrial confidence (balance, %)",                   "BS-ICI-BAL"),
    ("conf_services",     "Euro-area services confidence (balance, %)",                     "BS-SCI-BAL"),
    ("conf_consumer",     "Euro-area consumer confidence (balance, %)",                     "BS-CSMCI-BAL"),
    ("conf_retail",       "Euro-area retail trade confidence (balance, %)",                 "BS-RCI-BAL"),
    ("conf_construction", "Euro-area construction confidence (balance, %)",                 "BS-CCI-BAL"),
]
PROD_YOY = {  # base index id -> YoY label
    "indprod_total":        "Industrial production — total incl. construction · YoY (%)",
    "indprod_core":         "Industrial production excluding construction · YoY (%)",
    "indprod_intermediate": "Industrial production — intermediate goods · YoY (%)",
    "indprod_capital":      "Industrial production — capital goods · YoY (%)",
    "indprod_durable":      "Industrial production — consumer durables · YoY (%)",
    "indprod_nondurable":   "Industrial production — consumer non-durables · YoY (%)",
    "indprod_energy":       "Industrial production — energy · YoY (%)",
}
CAPTURE = set(PROD_YOY) | {"m1_growth", "hicp_headline"}


def fetch_eurostat(indic, geo="EA20"):
    url = "%s?format=JSON&lang=EN&geo=%s&indic=%s&s_adj=SA" % (EUROSTAT, geo, indic)
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=_UA), timeout=25) as r:
            j = json.loads(r.read().decode("utf-8", "ignore"))
        if any(size != 1 for dim,size in zip(j["id"],j["size"]) if dim != "time"):
            raise ValueError("ambiguous Eurostat series dimensions")
        idx = j["dimension"]["time"]["category"]["index"]; vals = j["value"]
        inv = {p: per for per, p in idx.items()}
        out = []
        for p in sorted(int(k) for k in vals.keys()):
            v = vals.get(str(p))
            if v is not None:
                out.append([inv[p], round(float(v), 2)])
        return out
    except Exception as e:
        print("eurostat %s: %s" % (indic, e)); return []


def write_series(sid,label,pts,metadata,source):
    doc=summarize(sid,label,pts,metadata,source)
    s3.put_object(Bucket=BUCKET,Key=f'data/ecb-hist/{sid}.json',Body=json.dumps(doc,allow_nan=False).encode(),
                  ContentType='application/json',CacheControl='public, max-age=3600')
    return {k:v for k,v in doc.items() if k!='points'}


def lambda_handler(event=None,context=None):
    started=time.monotonic();manifest=[];captured={};errors=[]
    def fetch_series(entry):
        flow,sid,label=entry
        text=fetch_csv(flow)
        if not text:raise ValueError('provider_unavailable')
        points,metadata=parse_csv(text,flow)
        return points,metadata
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures={pool.submit(fetch_series,entry):entry for entry in SERIES}
        for future in as_completed(futures):
            flow,sid,label=futures[future]
            try:
                points,metadata=future.result()
                manifest.append(write_series(sid,label,points,metadata,flow))
                if sid in CAPTURE:captured[sid]=points
            except Exception as exc:
                errors.append({'id':sid,'error':type(exc).__name__})
                manifest.append({'id':sid,'label':label,'latest':None,'methodology_version':METHOD,
                                 'quality':{'status':'unavailable','reason':type(exc).__name__},'flow_key':flow,
                                 'execution_eligible':False,'call':None})
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures={pool.submit(fetch_eurostat,ic):(sid,label) for sid,label,ic in EUROSTAT_CONF}
        for future in as_completed(futures):
            sid,label=futures[future]
            try:
                points=future.result()
                metadata={'frequency':'monthly','unit':'index_long_run_100' if sid=='conf_esi' else 'survey_balance_pct',
                          'source':'Eurostat ei_bssi_m_r2','source_unit_multipliers':[0]}
                manifest.append(write_series(sid,label,points,metadata,'https://ec.europa.eu/eurostat/databrowser/view/ei_bssi_m_r2/default/table'))
            except Exception as exc:
                errors.append({'id':sid,'error':type(exc).__name__})
                manifest.append({'id':sid,'label':label,'latest':None,'quality':{'status':'unavailable'},'methodology_version':METHOD})
    for base,label in PROD_YOY.items():
        points=yoy_series(captured.get(base,[]))
        if points:
            manifest.append(write_series(base+'_yoy',label,points,{'frequency':'monthly','unit':'percent_yoy','derived_from':base},
                                         'Exact same calendar month one year earlier'))
    m1=captured.get('m1_growth',[]);hicp={d[:7]:v for d,v in captured.get('hicp_headline',[])}
    real=[[d,round(((1+v/100)/(1+hicp[d[:7]]/100)-1)*100,4)] for d,v in m1 if d[:7] in hicp and hicp[d[:7]]>-100]
    if real:
        manifest.append(write_series('real_m1_growth','Real M1 annual growth, HICP-deflated (%)',real,
                                     {'frequency':'monthly','unit':'percent_yoy','derived_from':['m1_growth','hicp_headline']},
                                     '(1 + nominal M1 growth) / (1 + HICP inflation) - 1, matched calendar months'))
    # Preserve discoverability of sibling archives, but do not silently certify
    # old numbers whose units and scale were never recorded.
    seen={row['id'] for row in manifest}
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET,Prefix='data/ecb-hist/'):
        for item in page.get('Contents',[]):
            key=item['Key'];sid=key.rsplit('/',1)[-1].removesuffix('.json')
            if not key.endswith('.json') or sid.startswith('_') or sid in seen:continue
            try:
                doc=json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
                if doc.get('methodology_version')==METHOD and doc.get('source_metadata') and doc.get('points'):
                    current=summarize(sid,doc.get('label',sid),doc['points'],doc['source_metadata'],doc.get('flow_key',''))
                    current.pop('points',None);current['cached_archive']=True
                    # Keep the archive publication time, not the manifest's new time.
                    current['generated_at']=doc.get('generated_at');manifest.append(current)
                else:
                    manifest.append({'id':sid,'label':doc.get('label',sid),'latest':None,'unit':'unvalidated',
                                     'quality':{'status':'unvalidated','reason':'legacy archive lacks unit metadata'},
                                     'first_date':doc.get('first_date'),'latest_date':doc.get('latest_date'),
                                     'n_points':doc.get('n_points'),'freq':doc.get('freq'),'execution_eligible':False})
                seen.add(sid)
            except Exception as exc:errors.append({'id':sid,'error':type(exc).__name__})
    manifest.sort(key=lambda row:row['id'])
    fresh=sum(row.get('quality',{}).get('status')=='fresh' for row in manifest)
    out={'generated_at':datetime.now(timezone.utc).isoformat(),'methodology_version':METHOD,'series':manifest,'n':len(manifest),
         'quality':{'status':'partial' if errors or fresh<len(manifest) else 'fresh','fresh_series':fresh,'total_series':len(manifest)},
         'errors':errors,'duration_s':round(time.monotonic()-started,1),'call':None,'execution_eligible':False}
    s3.put_object(Bucket=BUCKET,Key='data/ecb-hist/_manifest.json',Body=json.dumps(out,allow_nan=False).encode(),
                  ContentType='application/json',CacheControl='public, max-age=3600')
    return {'statusCode':200,'body':json.dumps({'series':len(manifest),'fresh':fresh,'errors':len(errors)})}
