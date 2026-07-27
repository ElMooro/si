"""justhodl-gov-sources — the GOVERNMENT DATA SOURCES REGISTRY.
Every official agency the platform pulls from: full API spec (endpoints,
params, worked examples), the hard-won quirks/lessons, an INDEPENDENT live
probe of each agency's flagship series (not reading the vault — exercising
the endpoint), and a join showing which vault symbols each source serves.
Output: data/gov-sources.json -> /gov-sources.html.
Doctrine: this file IS the institutional memory of ops 3936-3955's
discovery arc — future engines read the spec here instead of re-deriving.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone

import boto3

MARKER = "gov-sources v1.0 REGISTRY"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/gov-sources.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}
s3 = boto3.client("s3")


def _get(url, timeout=25, headers=None):
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ── flagship probes (each exercises the agency endpoint directly) ──

def p_treasury():
    yr = datetime.now(timezone.utc).year
    url = ("https://home.treasury.gov/resource-center/data-chart-center/"
           "interest-rates/daily-treasury-rates.csv/" + str(yr) +
           "/all?type=daily_treasury_yield_curve&field_tdr_date_value=" +
           str(yr) + "&page&_format=csv")
    lines = [l for l in _get(url, 30).decode("utf-8", "ignore").splitlines() if l.strip()]
    hdr = [h.strip().strip('"') for h in lines[0].split(",")]
    row = [c.strip().strip('"') for c in lines[1].split(",")]
    i = hdr.index("2 Mo")
    return [{"name": "US 2M par yield", "value": float(row[i]), "unit": "%",
             "asof": row[0], "ok": True}]


def p_eurostat():
    out = []
    for geo in ("IT", "ES"):
        url = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/"
               "data/gov_10dd_edpt1?format=JSON&na_item=GD&sector=S13"
               "&unit=PC_GDP&geo=" + geo + "&lastTimePeriod=1")
        d = json.loads(_get(url, 30))
        v = list((d.get("value") or {}).values())
        t = list(((d.get("dimension") or {}).get("time") or {})
                 .get("category", {}).get("index", {}).keys())
        out.append({"name": geo + " gov debt/GDP", "value": v[0] if v else None,
                    "unit": "% GDP", "asof": t[-1] if t else None, "ok": bool(v)})
    return out


def p_norges():
    url = ("https://data.norges-bank.no/api/data/GOVT_GENERIC_RATES"
           "?format=sdmx-json&lastNObservations=1")
    d = json.loads(_get(url, 25))
    data = d.get("data") or d
    dims = ((data.get("structure") or {}).get("dimensions") or {}).get("series") or []
    want = None
    for di, dim in enumerate(dims):
        for vi, val in enumerate(dim.get("values") or []):
            if "3Y" in (str(val.get("id", "")) + str(val.get("name", ""))).replace(" ", ""):
                want = (di, vi)
                break
        if want:
            break
    v = None
    for skey, sval in ((data.get("dataSets") or [{}])[0].get("series") or {}).items():
        pos = [int(x) for x in skey.split(":")]
        if want and pos[want[0]] == want[1]:
            obs = sval.get("observations") or {}
            v = float(list(obs.values())[0][0])
            break
    return [{"name": "Norway 3Y govt", "value": v, "unit": "%", "asof": "latest", "ok": v is not None}]


def p_bcrp():
    yr = datetime.now(timezone.utc).year
    url = ("https://estadisticas.bcrp.gob.pe/estadisticas/series/api/"
           "PN38923BM/json/" + str(yr - 1) + "-1/" + str(yr) + "-12")
    d = json.loads(_get(url, 25))
    last = (d.get("periods") or [{}])[-1]
    v = float(str(last.get("values", [None])[0]).replace(",", ""))
    return [{"name": "Peru terms of trade (2007=100)", "value": round(v, 2),
             "unit": "index", "asof": last.get("name"), "ok": True}]


def p_ecb():
    url = ("https://data-api.ecb.europa.eu/service/data/YC/"
           "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y?lastNObservations=1&format=jsondata")
    d = json.loads(_get(url, 25))
    ser = ((d.get("dataSets") or [{}])[0].get("series") or {})
    v = None
    for sv in ser.values():
        obs = sv.get("observations") or {}
        v = float(list(obs.values())[0][0])
        break
    return [{"name": "Euro area 3Y AAA yield", "value": round(v, 3) if v else None,
             "unit": "%", "asof": "latest", "ok": v is not None}]


def p_mofjp():
    url = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
    lines = [l for l in _get(url, 25).decode("utf-8", "ignore").splitlines() if l.strip()]
    hdr = [h.strip() for h in lines[1].split(",")]
    rows = [[c.strip() for c in l.split(",")] for l in lines[2:]]
    i2, i10 = hdr.index("2Y"), hdr.index("10Y")
    out = []
    for last in reversed(rows):
        if last[0][:4].isdigit() and "/" in last[0]:
            out = [{"name": "JGB 2Y par", "value": float(last[i2]), "unit": "%",
                    "asof": last[0], "ok": True},
                   {"name": "JGB 10Y par", "value": float(last[i10]), "unit": "%",
                    "asof": last[0], "ok": True}]
            break
    return out


def p_boe():
    url = ("https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
           "?csv.x=yes&SeriesCodes=IUDSOIA&CSVF=TN&UsingCodes=Y&Datefrom=01/Jan/2026")
    lines = [l for l in _get(url, 25).decode("utf-8", "ignore").splitlines() if l.strip()]
    last = lines[-1].split(",")
    return [{"name": "SONIA o/n", "value": float(last[1]), "unit": "%",
             "asof": last[0], "ok": True}]


def p_boj():
    now = datetime.now(timezone.utc)
    end = now.year * 100 + now.month
    sy, sm = now.year - 1, now.month - 3
    if sm < 1:
        sy, sm = sy - 1, sm + 12
    url = ("https://www.stat-search.boj.or.jp/api/v1/getDataCode?format=json"
           "&lang=en&db=MD11&startDate=" + str(sy * 100 + sm) + "&endDate=" +
           str(end) + "&code=DLCLAADBLTTO")
    d = json.loads(_get(url, 25, {"Accept-Encoding": "identity"}))
    rs = (d.get("RESULTSET") or [{}])[0]
    vals = ((rs.get("VALUES") or {}).get("VALUES")) or []
    dates = ((rs.get("VALUES") or {}).get("SURVEY_DATES")) or []
    i = len(vals) - 1
    while i >= 12 and vals[i] in (None, ""):
        i -= 1
    yoy = round((float(vals[i]) / float(vals[i - 12]) - 1) * 100, 2)
    return [{"name": "Bank lending YoY (JPLG)", "value": yoy, "unit": "%",
             "asof": str(dates[i]), "ok": True}]


def p_snb():
    try:
        body = _get("https://data.snb.ch/api/cube/rendoblid/data/json/en"
                    "?fromDate=" + datetime.now(timezone.utc).date().replace(day=1).isoformat(), 25)
        ok = len(body) > 1000
        return [{"name": "rendoblid daily cube reachable", "value": len(body),
                 "unit": "bytes", "asof": "now", "ok": ok,
                 "note": "endpoint LIVE; windowed-shape parse still OPEN (CH02Y/CH03Y NFS)"}]
    except Exception as e:
        return [{"name": "rendoblid daily cube", "value": None, "unit": "",
                 "asof": None, "ok": False, "note": str(e)[:80]}]


def p_imf():
    body = _get("https://api.imf.org/external/sdmx/2.1/dataflow", 30)
    ok = b"IRFCL" in body
    return [{"name": "sdmx 2.1 dataflow (IRFCL present)", "value": len(body),
             "unit": "bytes", "asof": "now", "ok": ok,
             "note": "API LIVE; exact USA series key pending (USFER family queued)"}]


def p_fred():
    key = os.environ.get("FRED_KEY", "")
    url = ("https://api.stlouisfed.org/fred/series/observations?series_id=DGS10"
           "&api_key=" + key + "&file_type=json&sort_order=desc&limit=1")
    d = json.loads(_get(url, 20))
    o = (d.get("observations") or [{}])[0]
    return [{"name": "US 10Y (DGS10)", "value": float(o.get("value")), "unit": "%",
             "asof": o.get("date"), "ok": True}]


AGENCIES = [
    {"id": "boj", "name": "Bank of Japan", "country": "Japan", "flag": "🇯🇵",
     "auth": "none", "probe": p_boj,
     "spec": {"base": "https://www.stat-search.boj.or.jp/api/v1/",
              "endpoints": ["getDataCode (values by series code)",
                            "getDataLayer (statistics tree; needs layer param spec)",
                            "getMetadata?db=X (FULL series list per db)"],
              "params": "format=json&lang=en&db=..&startDate=YYYYMM&endDate=YYYYMM&code=A,B",
              "example": "getDataCode?format=json&lang=en&db=MD11&startDate=202505&endDate=202606&code=DLCLAADBLTTO",
              "db_table": "MD11 deposits/loans&bills (792 series) · MD01 mon base · MD02 money stock · MD12 by prefecture · LA01-03 loans by sector/BOJ/other · IR01-04 rates · FM01 call rate · CO Tankan (TK codes)"},
     "quirks": ["Launched 2026-02; send Accept-Encoding: identity",
                "JPLG = MD11:DLCLAADBLTTO (avg amounts, banks+trust+overseas); fallback DLCLBADBLTTO",
                "Latest month slot is null until published — walk back to last non-null; YoY = last vs -12",
                "API base lives only in the PDF manuals' text layer (pypdf, not regex)"],
     "how_to_add": "getMetadata?db=<DB> lists every series+code; then alias 'boj:DB:CODE' in the vault"},
    {"id": "mof_japan", "name": "Japan Ministry of Finance", "country": "Japan", "flag": "🇯🇵",
     "auth": "none", "probe": p_mofjp,
     "spec": {"base": "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/",
              "endpoints": ["jgbcme.csv (current month, all JGB par tenors 1Y-40Y)",
                            "historical/jgbcme_all.csv (full history)"],
              "params": "plain CSV; header row 2 = Date,1Y,2Y,...,40Y",
              "example": "jgbcme.csv -> row-walk to last ^YYYY/ line -> column '2Y'"},
     "quirks": ["LAST CSV LINE IS A FOOTER — float() on it fails; walk back to the last date row",
                "'AWS blocks MOF' theory was FALSIFIED (debug_mof): direct fetch works from Lambda"],
     "how_to_add": "alias 'mofjp:<TENOR>' in the vault (any header column)"},
    {"id": "us_treasury", "name": "US Treasury", "country": "USA", "flag": "🇺🇸",
     "auth": "none", "probe": p_treasury,
     "spec": {"base": "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/",
              "endpoints": ["daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve"],
              "params": "&field_tdr_date_value={year}&page&_format=csv",
              "example": "columns: Date, 1 Mo, 2 Mo, 3 Mo ... 30 Yr (row 1 = latest)"},
     "quirks": ["Header names have spaces ('2 Mo'); first data row is the newest"],
     "how_to_add": "alias 'ust:<column name>' in the vault"},
    {"id": "eurostat", "name": "Eurostat", "country": "EU", "flag": "🇪🇺",
     "auth": "none", "probe": p_eurostat,
     "spec": {"base": "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/",
              "endpoints": ["{dataset}?format=JSON&...&geo=..&lastTimePeriod=1"],
              "params": "gov debt: gov_10dd_edpt1?na_item=GD&sector=S13&unit=PC_GDP",
              "example": "gov_10dd_edpt1?format=JSON&na_item=GD&sector=S13&unit=PC_GDP&geo=IT&lastTimePeriod=1"},
     "quirks": ["Euro-area aggregate: try geo=EA20 then EA19", "JSON-stat: value dict keyed by index"],
     "how_to_add": "alias 'estat:<geo>' (debt/GDP) or extend estat_latest for other datasets"},
    {"id": "norges", "name": "Norges Bank", "country": "Norway", "flag": "🇳🇴",
     "auth": "none", "probe": p_norges,
     "spec": {"base": "https://data.norges-bank.no/api/data/",
              "endpoints": ["GOVT_GENERIC_RATES?format=sdmx-json&lastNObservations=1"],
              "params": "SDMX-JSON; dims FREQ / TENOR / INSTRUMENT_TYPE; series key 'f:t:i'",
              "example": "TENOR values: 3M 6M 12M 3Y 5Y 7Y 10Y"},
     "quirks": ["data.structure is SINGULAR here (ECB-style), not structures[]"],
     "how_to_add": "alias 'norges:GOVT_GENERIC_RATES:<tenor digit(s)>' in the vault"},
    {"id": "bcrp", "name": "BCRP (Central Reserve Bank of Peru)", "country": "Peru", "flag": "🇵🇪",
     "auth": "none", "probe": p_bcrp,
     "spec": {"base": "https://estadisticas.bcrp.gob.pe/estadisticas/series/",
              "endpoints": ["api/{code}/json/{from}/{to} (dates as YYYY-M)",
                            "metadata (full catalog, ';'-separated CSV)"],
              "params": "keyless; response config.title confirms the series",
              "example": "api/PN38923BM/json/2025-1/2026-12 -> Peru ToT index (2007=100)"},
     "quirks": ["Identify codes by TITLE from the catalog, never by adjacency",
                "PN38923BM = ToT index · PN38926BM = var% 12m"],
     "how_to_add": "grep the metadata catalog by name; alias 'bcrp:<code>' in the vault"},
    {"id": "ecb", "name": "European Central Bank", "country": "EU", "flag": "🇪🇺",
     "auth": "none", "probe": p_ecb,
     "spec": {"base": "https://data-api.ecb.europa.eu/service/data/",
              "endpoints": ["{flow}/{key}?lastNObservations=1&format=jsondata"],
              "params": "YC curve: B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{tenor}; BP6 for current account",
              "example": "YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3Y"},
     "quirks": ["structure SINGULAR; observations dict values are [value, ...] lists"],
     "how_to_add": "find the key in ECB Data Portal UI; alias 'ecb:<flow>/<key>'"},
    {"id": "boe", "name": "Bank of England", "country": "UK", "flag": "🇬🇧",
     "auth": "none", "probe": p_boe,
     "spec": {"base": "https://www.bankofengland.co.uk/boeapps/iadb/",
              "endpoints": ["fromshowcolumns.asp?csv.x=yes&SeriesCodes=..&CSVF=TN&UsingCodes=Y&Datefrom=DD/Mon/YYYY"],
              "params": "pure CSV: DATE,<CODE> rows",
              "example": "IUDSOIA=SONIA · gilt pars: IUDSNPY 5Y / IUDMNPY 10Y / IUDLNPY 20Y (max tenor)"},
     "quirks": ["/boeapps/database/ path returns HTML — only /boeapps/iadb/ serves CSV",
                "No 30Y par series exists; 20Y IUDLNPY is the long end"],
     "how_to_add": "alias 'boe:<SERIES_CODE>' in the vault"},
    {"id": "snb", "name": "Swiss National Bank", "country": "Switzerland", "flag": "🇨🇭",
     "auth": "none", "probe": p_snb,
     "spec": {"base": "https://data.snb.ch/api/cube/",
              "endpoints": ["{cube}/data/json/en (?fromDate=YYYY-MM-DD windows it)"],
              "params": "cubes: rendoblid (daily Confederation yields) · rendoblim (monthly, STALE 2025-07)",
              "example": "rendoblid/data/json/en?fromDate=2026-07-01"},
     "quirks": ["rendoblim last value 2025-07 — use rendoblid windowed",
                "OPEN: windowed-shape parse bug (fails on runner too) — CH02Y/CH03Y NFS until fixed"],
     "how_to_add": "fix snb_latest for the windowed shape, then alias 'snb:<n> year'"},
    {"id": "imf", "name": "IMF (new SDMX API)", "country": "Global", "flag": "🌐",
     "auth": "none", "probe": p_imf,
     "spec": {"base": "https://api.imf.org/external/sdmx/2.1/",
              "endpoints": ["dataflow (all flows)", "data/{flow}/{key}?lastNObservations=1",
                            "datastructure/IMF.STA/DSD_IRFCL_PUB"],
              "params": "key vocab: COUNTRY(ISO3).INDICATOR(IRFCLDT1_IRFCL32_USD style).SECTOR(S1XS1311).FREQ(M)",
              "example": "data/IRFCL?lastNObservations=1&startPeriod=2026 (unkeyed reveals Series attrs)"},
     "quirks": ["Legacy dataservices DNS is DEAD; data.imf.org/api 403s",
                "Empty dataset = 200 + header, zero <Obs> — ALWAYS check for observations",
                "OPEN: exact USA reserves key (USFER family) — unkeyed sample stopped at A-countries"],
     "how_to_add": "sample the unkeyed pull for the country's Series attrs; key = dotted attr values"},
    {"id": "fred", "name": "FRED (St. Louis Fed)", "country": "USA", "flag": "🇺🇸",
     "auth": "api key (env FRED_KEY)", "probe": p_fred,
     "spec": {"base": "https://api.stlouisfed.org/fred/",
              "endpoints": ["series/observations?series_id=..&api_key=..&file_type=json"],
              "params": "sort_order=desc&limit=1 for latest",
              "example": "the fleet workhorse — throttle 0.55s between calls (429s otherwise)"},
     "quirks": ["Vault uses source-memory to hold FRED calls to ~104/run"],
     "how_to_add": "vault suffix-strip + isalnum 2nd-chance resolves most FRED ids automatically"},
    {"id": "bcch", "name": "Banco Central de Chile", "country": "Chile", "flag": "🇨🇱",
     "auth": "FREE ACCOUNT REQUIRED (si3.bcentral.cl)", "probe": None,
     "status_override": "BLOCKED_ACCOUNT",
     "spec": {"base": "https://si3.bcentral.cl/SieteRestWS/",
              "endpoints": ["SieteRestWS.ashx?user=..&pass=..&function=GetSeries&timeseries=..&firstdate=..&lastdate=.."],
              "params": "credentials embedded per call",
              "example": "unlocks CLTOT / CLBOT first-party once Khalid registers"},
     "quirks": ["The only blocker is the account — a 2-minute Khalid action"],
     "how_to_add": "register, store creds as Lambda env, add 'bcch:' adapter kind"},
    {"id": "gov_proxy", "name": "Geo-blocked Asia (via /gov edge proxy)", "country": "KR/TW/CN/HK", "flag": "🌏",
     "auth": "none (Cloudflare edge)", "probe": None, "status_override": "SPEC_ONLY",
     "spec": {"base": "https://justhodl-data-proxy.raafouis.workers.dev/gov?u={url-encoded}",
              "endpoints": ["GET-only, hostname-allowlisted, size-capped edge fetch"],
              "params": "GOV_ALLOW: customs.go.kr · moea.gov.tw · pbc.gov.cn · stats.gov.cn · cad.gov.hk · hongkongairport.com · mof.go.jp · stat-search.boj.or.jp · api.imf.org",
              "example": "owner engines: korea-chip-flash, taiwan-orders, china-tsf, air-cargo"},
     "quirks": ["These agencies tarpit/403 AWS-Lambda IPs; Cloudflare edge IPs pass",
                "MOF turned out NOT to need it — kept as harmless fallback"],
     "how_to_add": "add hostname to GOV_ALLOW in cloudflare/workers/justhodl-data-proxy, deploy on push"},
]


def lambda_handler(event, context):
    t0 = time.time()
    # vault join: which symbols each agency serves
    by_src = {}
    try:
        vault = json.loads(s3.get_object(Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        for r in vault.get("symbols") or []:
            rv = str(r.get("resolved_via") or "")
            src = str(r.get("source") or "")
            for aid, pfx in (("boj", "boj:"), ("mof_japan", "mofjp:"), ("us_treasury", "ust:"),
                             ("eurostat", "estat:"), ("norges", "norges:"), ("bcrp", "bcrp:"),
                             ("ecb", "ecb:"), ("boe", "boe:"), ("snb", "snb:"), ("imf", "imf:"),
                             ("fred", "fred:")):
                if rv.startswith(pfx) or pfx.rstrip(":") in src:
                    by_src.setdefault(aid, []).append(r.get("symbol"))
                    break
    except Exception:
        pass

    agencies = []
    n_live = n_deg = 0
    for a in AGENCIES:
        row = {k: a[k] for k in ("id", "name", "country", "flag", "auth", "spec", "quirks", "how_to_add")}
        row["vault_symbols"] = sorted(set(by_src.get(a["id"], [])))[:40]
        row["vault_count"] = len(set(by_src.get(a["id"], [])))
        if a.get("probe") is None:
            row["status"] = a.get("status_override", "SPEC_ONLY")
            row["probes"] = []
            row["latency_ms"] = None
        else:
            t1 = time.time()
            try:
                probes = a["probe"]()
                row["probes"] = probes
                oks = [p.get("ok") for p in probes]
                row["status"] = "LIVE" if all(oks) else ("DEGRADED" if any(oks) else "DOWN")
            except Exception as e:
                row["probes"] = [{"name": "probe", "value": None, "unit": "",
                                  "asof": None, "ok": False, "note": str(e)[:100]}]
                row["status"] = "DOWN"
            row["latency_ms"] = int((time.time() - t1) * 1000)
        if row["status"] == "LIVE":
            n_live += 1
        elif row["status"] == "DEGRADED":
            n_deg += 1
        agencies.append(row)

    out = {"marker": MARKER,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "n_agencies": len(agencies), "n_live": n_live, "n_degraded": n_deg,
           "n_spec_only": sum(1 for a in agencies if a["status"] in ("SPEC_ONLY", "BLOCKED_ACCOUNT")),
           "agencies": agencies,
           "elapsed_s": round(time.time() - t0, 1)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json", CacheControl="max-age=120")
    return {"statusCode": 200, "n_live": n_live}
