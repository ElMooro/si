"""justhodl-cds-desk — sovereign + U.S. corporate single-name CDS and the major indices, measured from the
DTCC public price-dissemination files (SEC security-based swap and CFTC swap data repositories).

Source files (free, public, Dodd-Frank real-time dissemination):
    https://kgc0418-tdw-data-0.s3.amazonaws.com/sec/eod/SEC_CUMULATIVE_CREDITS_YYYY_MM_DD.zip
    https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/CFTC_CUMULATIVE_CREDITS_YYYY_MM_DD.zip
Each file holds the prints disseminated on that calendar day; it is posted shortly after midnight UTC.  A print
executed on day D is normally in file D, late reports land in file D+1.  Each run therefore finalises execution
date D-1 from files {D-1, D} and writes execution date D preliminarily from file D alone.

All measurement logic lives in aws/shared/cds_desk.py (unit-tested, cloud-free).  This module only does I/O:
download, bank persistence, packet publication.  Nothing here forecasts, calls or sizes anything.

Actions
    {"action": "daily"}                               latest available file date (walks back <= 5 days)
    {"action": "backfill", "start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}   sequential file dates, bank carried
    {"action": "rebuild", "start": ..., "end": ...}   same, but starts from an empty bank
    any of them with "dry_run": true                  the same work in memory; nothing is written anywhere

Since 2.0 (Khalid 2026-11-25: audit and improve; more companies, every asset class, sovereigns, listed companies worldwide):
    * every new trade of the file is counted by product (cds_desk.print_record / census_day), and the day's prints are kept
      in the warehouse (data/warm/cds/prints/<file day>.json.gz, no identifier of the underlying) so the desk can be
      measured again without asking DTCC;
    * a name is tied to its listed company and its own bonds from the credit board's document, to its home listing from
      the FMP bank's company profiles, and a sovereign to the Global Sovereign desk's reference quote: all three are the
      warehouse's own documents, read from S3;
    * the prints behind every single-name level of the last 30 days are published (data/cds-desk-tape.json).
"""
import gzip
import io
import json
import os
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

import cds_desk
import cds_long_context
import cds_risk_layers

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PACKET_KEY = os.environ.get("PACKET_KEY", "data/cds-desk.json")
BANK_KEY = os.environ.get("BANK_KEY", "data/warm/cds/bank.json.gz")
HISTORY_KEY = os.environ.get("HISTORY_KEY", "data/cds-desk-history.json")
TAPE_KEY = os.environ.get("TAPE_KEY", "data/cds-desk-tape.json")
PRINTS_PREFIX = "data/warm/cds/prints/"
CBE_KEY = "data/credit-before-equity.json"        # the credit board: a CDS name's listed company and its own bonds
SOV_REF_KEY = "data/global-sovereign.json"        # the Global Sovereign desk: a reference CDS quote per country
PROFILE_PREFIX = "data/warm/fmp-bank/profile-bulk/"   # the FMP bank: listed companies worldwide
LISTED_MAX_AGE_DAYS = 7
TAPE_DAYS = 30
TAPE_MAX_PER_NAME = 60
TRADE_COLS = ("id", "source", "kind", "date", "expiry", "tenor", "bucket", "upi", "name_raw", "ccy", "notional", "capped", "coupon_bp", "upfront", "cleared",
              "platform", "package", "index_family", "quoted_bp", "quoted_raw", "price", "coupon_how", "rec")
RECORD_COLS = ("date", "cls", "fam", "ccy", "notional", "capped", "how", "coupon_bp", "expiry", "points", "strike", "strike_kind")
DRY = False            # set by the handler for {"dry_run": true}: every write is skipped
LAST = {}              # what the last publish built (packet, history, tape) - read by the dry run and by ops
DAY_RECORDS = {}       # file day -> census records of that file (every new trade, by product)
LONG_SRC_PREFIX = "data/warm/cds/long-src/"
LONG_SOURCES = {
    # free, keyless, full-history credit records (see cds_long_context docstring)
    "BAA10Y": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAA10Y",
    "AAA10Y": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=AAA10Y",
    "BAMLH0A0HYM2": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2",
    "BAMLC0A0CM": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLC0A0CM",
    "ofr_fsi": "https://www.financialresearch.gov/financial-stress-index/data/fsi.csv",
    "ebp": "https://www.federalreserve.gov/econres/notes/feds-notes/ebp_csv.csv",
    # ECB SovCISS: daily sovereign-stress composite per euro-area state (SOV_CIN) and GDP-weighted euro area (SOV_GDPWN)
    "ecb_sovciss": "https://data-api.ecb.europa.eu/service/data/CISS/D..Z0Z.4F.EC.SOV_CIN.IDX?format=csvdata&startPeriod=2000-01-01&detail=dataonly",
    "ecb_sovciss_ea": "https://data-api.ecb.europa.eu/service/data/CISS/D.U2.Z0Z.4F.EC.SOV_GDPWN.IDX?format=csvdata&startPeriod=2000-01-01&detail=dataonly",
    # ECB new CISS: daily systemic financial stress for the euro area, US, UK, China and eight euro states (same feed the
    # justhodl-systemic-stress engine reads with lastNObservations; here the full record so 2008 is on the axis)
    "ecb_ciss": "https://data-api.ecb.europa.eu/service/data/CISS/D..Z0Z.4F.EC.SS_CIN.IDX?format=csvdata&startPeriod=2000-01-01&detail=dataonly",
    # ECB CLIFS: monthly country-level financial stress for every EU member state + UK since 1990
    "ecb_clifs": "https://data-api.ecb.europa.eu/service/data/CLIFS/M.._Z.4F.EC.CLIFS_CI.IDX?format=csvdata&startPeriod=1990-01-01&detail=dataonly",
}
SYSTEMIC_STRESS_KEY = "data/systemic-stress.json"  # sibling engine, used only as a same-day cross-check
# IMF WEO fundamentals (DataMapper, keyless JSON): one request per indicator, all countries
IMF_SOURCES = {ind: "https://www.imf.org/external/datamapper/api/v1/%s" % ind for ind in cds_long_context.IMF_INDICATORS}
# v1.6 risk layers: IMF reserve adequacy + private debt (DataMapper), ECB SUP bank→sovereign exposures, OFR Form PF
# (warm store of the OFR engine), and the sibling engines' hot packets (ESMA, EBA, NY Fed CMDI, FDIC)
IMF_LAYER_SOURCES = {ind: "https://www.imf.org/external/datamapper/api/v1/%s" % ind
                     for ind in list(cds_risk_layers.IMF_ARA) + list(cds_risk_layers.IMF_GDD)}
ECB_SUP_SOURCES = {"ecb_sup_s13": cds_risk_layers.ECB_SUP_URL}
ECB_SUP_WARM = "data/warm/ecb-sup/"
OFR_HFM_SERIES = "data/warm/ofr-hfm/series/%s.json.gz"
SIBLING_PACKETS = {"esma": "data/esma-ratings.json", "eba": "data/eba-risk-dashboard.json",
                   "cmdi": "data/nyfed-cmdi.json", "fdic": "data/fdic-bankfind.json"}
PAR_KEY = os.environ.get("PAR_KEY", "data/warm/treasury-par/curve.json.gz")
DTCC_BASE = "https://kgc0418-tdw-data-0.s3.amazonaws.com"
FIRST_PUBLIC_DAY = "2024-09-03"
FALLBACK_5Y_PAR = 4.25
MAX_WALK_BACK_DAYS = 5
TIME_BUDGET_S = 540

s3 = boto3.client("s3")


# ----------------------------------------------------------------------------- S3 helpers
def _get_json(key, default=None):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return default
    except Exception as exc:  # pragma: no cover - network
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code in ("NoSuchKey", "404"):
            return default
        raise
    if key.endswith(".gz"):
        body = gzip.GzipFile(fileobj=io.BytesIO(body)).read()
    return json.loads(body)


def _put_object(**kw):
    """Every write of the engine goes through here, so a dry run writes nothing."""
    if DRY:
        return None
    return s3.put_object(**kw)


def _put_json(key, obj, gz=False, cache="no-cache"):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    kw = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json", "CacheControl": cache}
    if gz:
        body = gzip.compress(body)
        kw["ContentEncoding"] = "gzip"
    _put_object(Body=body, **kw)
    return len(body)


# ----------------------------------------------------------------------------- DTCC download
def file_url(source, day_iso):
    tag = day_iso.replace("-", "_")
    if source == "sec":
        return "%s/sec/eod/SEC_CUMULATIVE_CREDITS_%s.zip" % (DTCC_BASE, tag)
    return "%s/cftc/eod/CFTC_CUMULATIVE_CREDITS_%s.zip" % (DTCC_BASE, tag)


def fetch_zip(source, day_iso, retries=2):
    """Bytes of the daily zip, or None when DTCC has not posted it (403/404)."""
    url = file_url(source, day_iso)
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl-cds-desk/1.0 (+https://justhodl.ai)"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code in (403, 404):
                return None
            if attempt == retries:
                raise
        except (urllib.error.URLError, TimeoutError):
            if attempt == retries:
                raise
        time.sleep(1.5 * (attempt + 1))
    return None


def load_day_trades(day_iso):
    """Normalised trades from both repositories for one file date; None when neither file exists.
    Every new trade of the file is also kept as a census record (DAY_RECORDS[day_iso])."""
    found = False
    trades, records = [], []
    for source in ("sec", "cftc"):
        raw = fetch_zip(source, day_iso)
        if raw is None:
            continue
        found = True
        for row in cds_desk.read_dissemination_zip(raw):
            t = cds_desk.normalize_trade(row, source)
            if t:
                trades.append(t)
            rec = cds_desk.print_record(row)
            if rec:
                records.append(rec)
    if found:
        DAY_RECORDS[day_iso] = records
    return trades if found else None


def prints_doc(file_day, trades, records):
    """The day's file as the desk read it, columnar: enough to measure the day again from the warehouse alone."""
    return {"file_day": file_day, "version": cds_desk.VERSION, "trade_cols": list(TRADE_COLS), "trades": [[t.get(c) for c in TRADE_COLS] for t in trades],
            "record_cols": list(RECORD_COLS), "records": [[r.get(c) for c in RECORD_COLS] for r in records]}


def trades_of(doc):
    cols = doc.get("trade_cols") or []
    return [dict(zip(cols, row)) for row in doc.get("trades") or []]


def keep_prints(file_day, trades):
    if trades is None:
        return 0
    return _put_json(PRINTS_PREFIX + file_day + ".json.gz", prints_doc(file_day, trades, DAY_RECORDS.get(file_day) or []), gz=True, cache="max-age=31536000")


# ----------------------------------------------------------------------------- rates
def par_curve():
    doc = _get_json(PAR_KEY, {}) or {}
    rows = doc.get("rows") if isinstance(doc, dict) else None
    return rows if isinstance(rows, dict) else {}


def rate_for(day_iso, par_rows):
    """Flat discount rate used by the pricer: 5Y Treasury par on/before the day minus the swap-proxy offset."""
    best = None
    for d in sorted(par_rows):
        if d <= day_iso:
            v = par_rows[d].get("5Y") if isinstance(par_rows[d], dict) else None
            if isinstance(v, (int, float)):
                best = float(v)
        else:
            break
    par = best if best is not None else FALLBACK_5Y_PAR
    return par - cds_desk.SWAP_PROXY_OFFSET_PCT


# ----------------------------------------------------------------------------- core
def process_file_date(bank, file_day, cur_trades, prev_day, prev_trades, par_rows, external=None):
    """Finalise prev_day (prev + late prints in cur) and write file_day preliminarily.
    external: {series key: (bp, "bonds" | "reference")} - sign evidence from outside the tape (see external_anchors)."""
    emap = cds_desk.entity_map(cur_trades, bank.get("entity_map") or {})
    done = []
    if prev_day and prev_trades is not None:
        ts = [t for t in prev_trades + cur_trades if t["date"] == prev_day]
        if ts:
            agg = cds_desk.aggregate_day(ts, emap, rate_for(prev_day, par_rows), cds_desk.anchors_from_bank(bank, prev_day, external=external))
            cds_desk.update_bank(bank, prev_day, agg, emap)
            done.append((prev_day, len(ts), "final"))
    ts = [t for t in cur_trades if t["date"] == file_day]
    if ts:
        agg = cds_desk.aggregate_day(ts, emap, rate_for(file_day, par_rows), cds_desk.anchors_from_bank(bank, file_day, external=external))
        cds_desk.update_bank(bank, file_day, agg, emap)
        done.append((file_day, len(ts), "preliminary"))
    bank["entity_map"] = emap
    # the census: every new trade by product, per execution day (the previous day is final once today's file is in)
    rec_prev, rec_cur = DAY_RECORDS.get(prev_day) or [], DAY_RECORDS.get(file_day) or []
    for day, recs in ((prev_day, rec_prev + rec_cur), (file_day, rec_cur)):
        mine = [r for r in recs if r["date"] == day] if day else []
        if mine:
            bank.setdefault("census", {})[day] = cds_desk.census_day(mine)
    for old in [d for d in DAY_RECORDS if d not in (file_day, prev_day)]:
        del DAY_RECORDS[old]
    return done


def run_range(bank, start_iso, end_iso, par_rows, started, external=None):
    """Walk file dates start..end sequentially; returns (log, last_day, exhausted_budget).
    external: a dict, or a function of the file day returning one (sign evidence from outside the tape)."""
    log = []
    prev_day, prev_trades = None, None
    d = date.fromisoformat(start_iso)
    end = date.fromisoformat(end_iso)
    # prime the previous file so the first day in range can be finalised
    probe = d - timedelta(days=1)
    for _ in range(MAX_WALK_BACK_DAYS):
        tr = load_day_trades(probe.isoformat())
        if tr is not None:
            prev_day, prev_trades = probe.isoformat(), tr
            break
        probe -= timedelta(days=1)
    last = None
    while d <= end:
        if time.time() - started > TIME_BUDGET_S:
            return log, last, True
        day_iso = d.isoformat()
        cur = load_day_trades(day_iso)
        if cur is None:
            d += timedelta(days=1)
            continue
        done = process_file_date(bank, day_iso, cur, prev_day, prev_trades, par_rows, external=external(day_iso) if callable(external) else external)
        keep_prints(day_iso, cur)
        log.append({"file": day_iso, "trades": len(cur), "written": done})
        prev_day, prev_trades = day_iso, cur
        last = day_iso
        d += timedelta(days=1)
    return log, last, False


def latest_available_file_day(today):
    d = today
    for _ in range(MAX_WALK_BACK_DAYS + 1):
        if fetch_zip("sec", d.isoformat()) is not None:
            return d.isoformat()
        d -= timedelta(days=1)
    return None


def _fetch_text(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-cds-desk/%s (+https://justhodl.ai)" % cds_desk.VERSION})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def load_long_sources(min_rows=100, sources=None, json_mode=False):
    """Fetch each long-history CSV (or IMF JSON); on any failure fall back to the last good copy kept in S3 (cache-first
    on failure, never on success, so the record stays current). Returns (texts, status)."""
    texts, status = {}, {}
    for name, url in (sources or LONG_SOURCES).items():
        key = LONG_SRC_PREFIX + name + (".json.gz" if json_mode else ".csv.gz")
        try:
            txt = _fetch_text(url, timeout=90 if name.startswith("ecb_") else 40)
            if json_mode:
                if not txt.lstrip().startswith("{") or '"values"' not in txt:
                    raise ValueError("non-JSON response (%d bytes)" % len(txt))
            elif txt.count("\n") < min_rows or txt.lstrip().startswith("<"):
                raise ValueError("short or non-CSV response (%d bytes)" % len(txt))
            _put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(txt.encode()), ContentType="application/json" if json_mode else "text/csv", ContentEncoding="gzip")
            texts[name], status[name] = txt, "live"
        except Exception as exc:  # pragma: no cover - network
            try:
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                texts[name] = gzip.decompress(body).decode("utf-8", "replace")
                status[name] = "cached (%s)" % type(exc).__name__
            except Exception:
                status[name] = "unavailable (%s)" % type(exc).__name__
    return texts, status


def build_long_block(bank, as_of):
    texts, status = load_long_sources()
    fred = {k: cds_long_context.parse_fred_csv(texts[k]) for k in ("BAA10Y", "AAA10Y", "BAMLH0A0HYM2", "BAMLC0A0CM") if k in texts}
    ofr = cds_long_context.parse_ofr_csv(texts["ofr_fsi"]) if "ofr_fsi" in texts else {}
    ebp = cds_long_context.parse_ebp_csv(texts["ebp"]) if "ebp" in texts else {}
    ecb = {}
    for k in ("ecb_sovciss", "ecb_sovciss_ea"):
        if k in texts:
            ecb.update(cds_long_context.parse_ecb_csv(texts[k]))
    ciss = cds_long_context.parse_ecb_csv(texts["ecb_ciss"]) if "ecb_ciss" in texts else {}
    clifs = cds_long_context.parse_ecb_csv(texts["ecb_clifs"]) if "ecb_clifs" in texts else {}
    block = cds_long_context.build_long_context(fred, ofr, ebp, bank, as_of, ecb=ecb, ciss=ciss, clifs=clifs)
    block["sources"] = {k: {"url": LONG_SOURCES[k], "status": status.get(k, "unavailable")} for k in LONG_SOURCES}
    return block


def load_fundamentals():
    """IMF WEO indicators -> ({indicator: {iso3: {year: value}}}, status)."""
    texts, status = load_long_sources(sources=IMF_SOURCES, json_mode=True)
    imf = {ind: cds_long_context.parse_imf_json(txt, ind) for ind, txt in texts.items()}
    return imf, status


def _read_ofr_series(mnemonic):
    try:
        return s3.get_object(Bucket=BUCKET, Key=OFR_HFM_SERIES % mnemonic)["Body"].read()
    except Exception:  # pragma: no cover - missing series
        return None


def attach_layers(packet, as_of, now):
    """v1.6 layers; each one is independent and reports its own status in packet['layers']['status']."""
    status = {}
    # IMF ARA + GDD
    try:
        texts, st = load_long_sources(sources=IMF_LAYER_SOURCES, json_mode=True)
        imf = {ind: cds_risk_layers.parse_imf_json(txt, ind) for ind, txt in texts.items()}
        n_ara, n_debt = cds_risk_layers.attach_imf_layers(packet, imf, as_of)
        packet["layers"]["imf_ara_gdd"]["sources"] = st
        status["imf_ara_gdd"] = "ok: %d reserve-adequacy, %d private-debt" % (n_ara, n_debt)
    except Exception as exc:
        status["imf_ara_gdd"] = "error %s: %s" % (type(exc).__name__, exc)
    # ECB SUP bank -> sovereign exposures (also mirrored as its own warm store for the data engine)
    try:
        texts, st = load_long_sources(min_rows=50, sources=ECB_SUP_SOURCES)
        sup = cds_risk_layers.parse_ecb_sup(texts["ecb_sup_s13"]) if "ecb_sup_s13" in texts else {}
        n = cds_risk_layers.attach_ecb_sup(packet, sup)
        if sup:
            bs = packet["bank_sovereign"]
            bs["source_status"] = st.get("ecb_sup_s13")
            _put_object(Bucket=BUCKET, Key=ECB_SUP_WARM + "src/sup_s13_e0010.csv", Body=texts["ecb_sup_s13"].encode(), ContentType="text/csv", CacheControl="max-age=300")
            catalog = []
            for iso3, rec in bs["by_iso3"].items():
                sid = "EA_BANKS_TO_%s" % iso3
                body = json.dumps({"id": sid, "label": "Euro-area banks' exposure to %s general government" % iso3, "unit": "EUR bn", "freq": "half-yearly",
                                   "slug": "ecb-sup", "generated_at": now, "points": rec["history"]}, separators=(",", ":")).encode()
                _put_object(Bucket=BUCKET, Key=ECB_SUP_WARM + "series/%s.json.gz" % sid, Body=gzip.compress(body), ContentType="application/json", ContentEncoding="gzip", CacheControl="max-age=300")
                catalog.append(sid)
            state = {"slug": "ecb-sup", "as_of": bs["period_end"], "generated_at": now, "catalog": catalog, "n_series": len(catalog),
                     "status": "COMPLETE-maintaining", "version": cds_risk_layers.VERSION, "engine": "justhodl-cds-desk",
                     "hot": PACKET_KEY, "hot_block": "bank_sovereign", "page": "/cds.html#bank-sovereign",
                     "source_files": [{"name": "sup_s13_e0010.csv", "url": cds_risk_layers.ECB_SUP_URL, "bytes": len(texts["ecb_sup_s13"]), "status": st.get("ecb_sup_s13")}]}
            _put_object(Bucket=BUCKET, Key=ECB_SUP_WARM + "state.json", Body=json.dumps(state).encode(), ContentType="application/json", CacheControl="no-cache")
            _put_object(Bucket=BUCKET, Key=ECB_SUP_WARM + "_last-check.json", Body=json.dumps({"checked_at": now}).encode(), ContentType="application/json", CacheControl="no-cache")
            for rec in bs["by_iso3"].values():
                rec["history"] = rec["history"][-6:]
        status["ecb_sup"] = "ok: %d sovereigns, %s" % (n, (packet.get("bank_sovereign") or {}).get("period"))
    except Exception as exc:
        status["ecb_sup"] = "error %s: %s" % (type(exc).__name__, exc)
    # sibling engines' hot packets
    sib = {k: _get_json(v, None) for k, v in SIBLING_PACKETS.items()}
    try:
        status["esma"] = "ok: %d sovereigns" % cds_risk_layers.attach_esma(packet, sib["esma"]) if sib["esma"] else "data/esma-ratings.json not published yet"
    except Exception as exc:
        status["esma"] = "error %s: %s" % (type(exc).__name__, exc)
    try:
        status["eba"] = "ok: %d sovereigns" % cds_risk_layers.attach_eba(packet, sib["eba"]) if sib["eba"] else "data/eba-risk-dashboard.json not published yet"
    except Exception as exc:
        status["eba"] = "error %s: %s" % (type(exc).__name__, exc)
    try:
        status["ofr_form_pf"] = "ok: %d series" % cds_risk_layers.attach_ofr_form_pf(packet, _read_ofr_series)
    except Exception as exc:
        status["ofr_form_pf"] = "error %s: %s" % (type(exc).__name__, exc)
    try:
        n = cds_risk_layers.attach_us_credit(packet, sib["cmdi"], sib["fdic"])
        status["us_credit"] = "ok: %d blocks (cmdi %s, fdic %s)" % (n, "yes" if sib["cmdi"] else "no", "yes" if sib["fdic"] else "no")
    except Exception as exc:
        status["us_credit"] = "error %s: %s" % (type(exc).__name__, exc)
    packet.setdefault("layers", {})["status"] = status
    packet["layers"]["version"] = cds_risk_layers.VERSION
    return status


# ----------------------------------------------------------------------------- what stands beside a name (2.0)
def _series_names(bank):
    """{series key: display name} for every single name the bank holds."""
    out = {}
    for norm, info in (bank.get("meta") or {}).items():
        if (info or {}).get("kind") == "index":
            continue
        base = norm[len(cds_desk.SUB):] if norm.startswith(cds_desk.SUB) else norm
        out[norm] = cds_desk.display_name(base, (info or {}).get("raw"))
    return out


def load_ties(bank, board=None):
    """{series key: {ticker, company, bond_bp, bond_book, bond_as_of, stock_1m_pct}} from the credit board's document: the
    listed company it tied the CDS name to, and that company's own bond spread over governments."""
    board = board if board is not None else _get_json(CBE_KEY, None)
    if not isinstance(board, dict) or not isinstance(board.get("issuers"), dict):
        return {}, "the credit board's document is not there"
    cols = board["issuers"].get("cols") or []
    ci = {k: i for i, k in enumerate(cols)}
    if not {"id", "ticker", "name", "spread_bp", "book"} <= set(ci):
        return {}, "the credit board's document has no issuer table this engine can read"
    rows = board["issuers"].get("rows") or []
    by_ticker = {r[ci["ticker"]]: r for r in rows if r[ci["ticker"]]}
    by_id = {r[ci["id"]]: r for r in rows}
    named = {str(c.get("name")).upper(): c for c in ((board.get("cds") or {}).get("priced") or []) + ((board.get("cds") or {}).get("no_level") or []) if c.get("name")}
    out = {}
    for key, name in _series_names(bank).items():
        base = key[len(cds_desk.SUB):] if key.startswith(cds_desk.SUB) else key
        b = named.get(str(name).upper()) or {}
        ticker = b.get("ticker")
        row = by_ticker.get(ticker) if ticker else by_id.get("I:" + base)
        if not ticker and row is None:
            continue
        t = {"board_id": row[ci["id"]]} if row is not None else {}
        if ticker:
            t.update(ticker=ticker, company=b.get("company"))
        if row is not None and isinstance(row[ci["spread_bp"]], (int, float)):
            t.update(bond_bp=row[ci["spread_bp"]], bond_book=row[ci["book"]], bond_as_of=board.get("as_of"), company=t.get("company") or row[ci["name"]])
            if "px_m1" in ci and isinstance(row[ci["px_m1"]], (int, float)):
                t["stock_1m_pct"] = row[ci["px_m1"]]
        out[key] = t
    return out, "ok: %d names tied, %d with a bond spread (credit board of %s)" % (len(out), sum(1 for t in out.values() if t.get("bond_bp") is not None), board.get("as_of"))


_REF_NAMES = {"south korea": "korea", "czech republic": "czechia", "united states": "united states", "hong kong": "hong kong"}


def load_reference(doc=None):
    """{iso3: {cds_bp, date, source}} - the Global Sovereign desk's 5-year CDS quote per country.  Not a print: a quote the
    sibling engine reads from worldgovernmentbonds.com, shown beside a sovereign and never in place of a traded level."""
    doc = doc if doc is not None else _get_json(SOV_REF_KEY, None)
    if not isinstance(doc, dict):
        return {}, "the Global Sovereign desk's document is not there"
    by_name = {}
    for norm, (disp, region, iso3, tier) in cds_desk.SOVEREIGNS.items():
        if iso3:
            by_name.setdefault(disp.lower(), iso3)
    out = {}
    for c in doc.get("countries") or []:
        if not isinstance(c, dict) or not isinstance(c.get("cds_bp"), (int, float)):
            continue
        name = str(c.get("country") or "").lower()
        iso3 = by_name.get(name) or by_name.get(_REF_NAMES.get(name, ""))
        if iso3:
            out[iso3] = {"cds_bp": float(c["cds_bp"]), "date": c.get("as_of"), "source": "Global Sovereign desk (worldgovernmentbonds.com quote)", "rating": c.get("rating")}
    return out, "ok: %d sovereigns with a reference quote (document of %s)" % (len(out), str(doc.get("generated_at"))[:10])


def external_anchors(bank, ties, reference):
    """{series key: (bp, source)} - sign evidence from outside the tape: a company's own bonds, a sovereign's reference quote.
    A subordinated name takes none: its senior bonds say nothing about which side of the coupon a subordinated swap trades."""
    out = {}
    for key, t in ties.items():
        if not key.startswith(cds_desk.SUB) and isinstance(t.get("bond_bp"), (int, float)) and t["bond_bp"] > 0:
            out[key] = (t["bond_bp"], "bonds")
    canon = {}
    for norm, (disp, region, iso3, tier) in cds_desk.SOVEREIGNS.items():
        if iso3:
            canon.setdefault(iso3, cds_desk.SOV_CANON.get(norm, norm))
    for iso3, ref in reference.items():
        if iso3 in canon:
            out[canon[iso3]] = (ref["cds_bp"], "reference")
    return out


def load_listings(bank, as_of, force=False, log=None):
    """{series key: {symbol, exchange, country, currency, name, how}} - the home listing of a corporate name, matched by
    name against the FMP bank's company profiles (every exchange).  Kept in the bank and matched again once a week."""
    kept = bank.get("listed") or {}
    try:
        age = (date.fromisoformat(as_of) - date.fromisoformat(str(kept.get("as_of")))).days
    except ValueError:
        age = None
    if not force and age is not None and 0 <= age < LISTED_MAX_AGE_DAYS and kept.get("names") is not None:
        return kept["names"], "kept: matched %s (%d names)" % (kept.get("as_of"), len(kept["names"]))
    want = {}
    for key, info in (bank.get("meta") or {}).items():
        if (info or {}).get("kind") not in ("corp", "sub"):
            continue
        base = key[len(cds_desk.SUB):] if key.startswith(cds_desk.SUB) else key
        if base in cds_desk.SOVEREIGNS:
            continue
        for cand in {cds_desk.listing_key(base), cds_desk.listing_key((info or {}).get("raw") or "")}:
            if len(cand) >= 3:
                want.setdefault(cand, []).append(key)
    files = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PROFILE_PREFIX):
        for o in page.get("Contents") or []:
            day = o["Key"].rsplit("/", 1)[-1].split(".")[0]
            if len(day) == 10:
                files.setdefault(day, []).append(o["Key"])
    if not files:
        return kept.get("names") or {}, "no company profiles in the FMP bank"
    n_parts = max(len(v) for v in files.values())
    day = max(d for d, v in files.items() if len(v) == n_parts)      # the newest day with every part
    best, n_rows = {}, 0
    import csv
    for key in sorted(files[day]):
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
        with gzip.GzipFile(fileobj=body) as gzf:
            for x in csv.DictReader(io.TextIOWrapper(gzf, encoding="utf-8", errors="replace", newline="")):
                n_rows += 1
                if x.get("isEtf") == "true" or x.get("isFund") == "true" or x.get("isActivelyTrading") == "false":
                    continue
                k = cds_desk.listing_key(x.get("companyName") or "")
                if k not in want:
                    continue
                try:
                    vol = float(x.get("averageVolume") or 0) * float(x.get("price") or 0)
                except ValueError:
                    vol = 0.0
                row = {"symbol": x.get("symbol"), "exchange": x.get("exchange"), "country": x.get("country"), "currency": x.get("currency"),
                       "name": x.get("companyName"), "adr": x.get("isAdr") == "true", "volume": vol, "sector": x.get("sector") or None}
                if k not in best or cds_desk.listing_rank(row) > cds_desk.listing_rank(best[k]):
                    best[k] = row
    names = {}
    for k, row in best.items():
        for key in want[k]:
            names[key] = {"symbol": row["symbol"], "exchange": row["exchange"], "country": row["country"], "currency": row["currency"], "name": row["name"],
                          "sector": row["sector"], "how": "same name as the listing"}
    bank["listed"] = {"as_of": as_of, "profiles_of": day, "rows_read": n_rows, "names": names}
    return names, "matched: %d of %d corporate names have a listing (FMP bank profiles of %s, %d rows)" % (len(names), len({k for v in want.values() for k in v}), day, n_rows)


def build_tape(bank, as_of, par_rows, today_docs=None):
    """The prints behind the single-name levels of the last TAPE_DAYS days, by series key - read from the day files the
    engine keeps in the warehouse (today_docs: files of this run that may not be written yet)."""
    start = (date.fromisoformat(as_of) - timedelta(days=TAPE_DAYS)).isoformat()
    docs = dict(today_docs or {})
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PRINTS_PREFIX):
            for o in page.get("Contents") or []:
                day = o["Key"].rsplit("/", 1)[-1].split(".")[0]
                if day >= start and day not in docs:
                    docs[day] = _get_json(o["Key"], None)
    except Exception:  # pragma: no cover - the tape is an enrichment
        pass
    emap = bank.get("entity_map") or {}
    cols = ["date", "tenor", "expiry", "ccy", "notional_mm", "capped", "coupon_bp", "coupon_how", "upfront_pct", "quoted_bp", "above_bp", "below_bp", "cleared", "platform", "package", "source"]
    names, seen = {}, set()
    for day in sorted(docs):
        for t in trades_of(docs[day] or {}):
            if t.get("kind") not in ("sov", "corp", "sub") or t.get("date", "") <= start or t.get("id") in seen:
                continue
            seen.add(t.get("id"))
            m = emap.get(t.get("upi"))
            norm = m["norm"] if m else (cds_desk.canonical_norm(cds_desk.normalize_name(t.get("name_raw") or "")) if cds_desk.entityish(t.get("name_raw") or "") else None)
            if not norm:
                continue
            key = (cds_desk.SUB + norm) if t["kind"] == "sub" else norm
            cands = dict((b, round(v, 1)) for v, b in cds_desk.candidate_spreads(dict(t), rate_for(t["date"], par_rows)))
            up = round(t["upfront"] / t["notional"] * 100.0, 3) if t.get("upfront") is not None and t.get("notional") else None
            names.setdefault(key, []).append([t["date"], t.get("tenor"), t.get("expiry"), t.get("ccy"), round((t.get("notional") or 0) / 1e6, 2), 1 if t.get("capped") else 0,
                                              t.get("coupon_bp"), t.get("coupon_how"), up, cands.get("quoted", t.get("quoted_bp")), cands.get("above_coupon"), cands.get("below_coupon"),
                                              1 if t.get("cleared") else 0, t.get("platform"), 1 if t.get("package") else 0, t.get("source")])
    for key in names:
        names[key] = sorted(names[key], key=lambda r: (r[0], r[1] or 0))[-TAPE_MAX_PER_NAME:]
    return {"engine": "justhodl-cds-desk", "version": cds_desk.VERSION, "as_of": as_of, "first": start, "cols": cols, "n_names": len(names),
            "n_prints": sum(len(v) for v in names.values()), "names": names,
            "note": "every single-name print of the last %d days as disseminated, with the level it gives if the protection was paid for above the coupon and below it; the reference obligation's identifier is not kept" % TAPE_DAYS}


def publish(bank, as_of, run_meta, par_rows=None, today_docs=None):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cds_desk.prune_bank(bank, as_of)
    beside = {}
    try:
        ties, beside["ties"] = load_ties(bank)
    except Exception as exc:  # what stands beside a name is an enrichment; the desk publishes without it
        ties, beside["ties"] = {}, "error %s: %s" % (type(exc).__name__, exc)
    try:
        reference, beside["reference"] = load_reference()
    except Exception as exc:
        reference, beside["reference"] = {}, "error %s: %s" % (type(exc).__name__, exc)
    try:
        listings, beside["listings"] = load_listings(bank, as_of, force=bool((run_meta or {}).get("refresh_listings")))
    except Exception as exc:
        listings, beside["listings"] = (bank.get("listed") or {}).get("names") or {}, "error %s: %s" % (type(exc).__name__, exc)
    for key, row in listings.items():
        ties.setdefault(key, {})["listing"] = row
    packet = cds_desk.build_packet(bank, as_of, now, run_meta, extras={"ties": ties, "reference": reference})
    packet["beside"] = {"status": beside, "ties_key": CBE_KEY, "reference_key": SOV_REF_KEY, "listings_prefix": PROFILE_PREFIX,
                        "note": "a listed company and its bonds come from the credit board, a home listing from the FMP bank's company profiles, a sovereign reference quote from the Global Sovereign desk: all read from this warehouse"}
    try:
        imf, imf_status = load_fundamentals()
        n_fund = cds_long_context.attach_fundamentals(packet, imf, as_of)
        packet["fundamentals"]["sources"] = imf_status
    except Exception as exc:  # fundamentals are an enrichment; the desk publishes without them
        n_fund = 0
        packet["fundamentals"] = {"error": "%s: %s" % (type(exc).__name__, exc)}
    try:
        layer_status = attach_layers(packet, as_of, now)
    except Exception as exc:  # layers are enrichments; the desk publishes without them
        layer_status = {"error": "%s: %s" % (type(exc).__name__, exc)}
        packet["layers"] = {"status": layer_status}
    history_bytes, long_status, n_stress = None, {}, 0
    try:
        long_block = build_long_block(bank, as_of)
        long_status = {k: v["status"] for k, v in long_block["sources"].items()}
        n_stress = cds_long_context.attach_ecb_stress(packet, long_block["series"])
        try:
            cds_long_context.reconcile_with_systemic_stress(packet, _get_json(SYSTEMIC_STRESS_KEY, None))
        except Exception as exc:  # cross-check only
            packet.setdefault("cross_reference", {})["systemic_stress"] = {"error": "%s: %s" % (type(exc).__name__, exc)}
        keys = [r["key"] for g in packet["groups"].values() for r in g["rows"] + g["unpriced"] + g["dormant"]] + ["IDX:" + i["key"] for i in packet["indices"]]
        branch_keys = [r["key"] for g in packet["groups"].values() for r in g["unpriced"]]
        history = cds_long_context.build_history(bank, as_of, long_block, keys, branch_keys)
        history["generated_at"] = now
        history_bytes = _put_json(HISTORY_KEY, history, cache="public, max-age=900")
        packet["history"] = {"key": HISTORY_KEY, "names": len(history["names"]), "long_series": sorted(long_block["series"].keys()),
                             "basis": {k: v["last"] for k, v in long_block["basis"].items()},
                             "cdx_ig_vs_2006": (long_block["series"].get("baa10y") or {}).get("cdx_ig_map"),
                             "long_last": {k: {"value": v["last"]["value"], "date": v["last"]["date"], "pct_rank_since_2006": v["pct_rank_since_2006"], "unit": v["unit"], "name": v["name"]}
                                           for k, v in long_block["series"].items()},
                             "sources": long_status}
    except Exception as exc:  # the desk must still publish if the long-context sources misbehave
        packet["history"] = {"key": HISTORY_KEY, "error": "%s: %s" % (type(exc).__name__, exc), "sources": long_status}
    tape, tape_bytes = None, None
    try:
        tape = build_tape(bank, as_of, par_rows if par_rows is not None else par_curve(), today_docs)
        tape["generated_at"] = now
        tape_bytes = _put_json(TAPE_KEY, tape, cache="public, max-age=900")
        packet["tape"] = {"key": TAPE_KEY, "days": TAPE_DAYS, "names": tape["n_names"], "prints": tape["n_prints"], "cols": tape["cols"]}
    except Exception as exc:  # the tape is an enrichment
        packet["tape"] = {"key": TAPE_KEY, "error": "%s: %s" % (type(exc).__name__, exc)}
    bank_bytes = _put_json(BANK_KEY, bank, gz=True)
    packet_bytes = _put_json(PACKET_KEY, packet, cache="public, max-age=900")
    LAST.clear()
    LAST.update(packet=packet, tape=tape, history=locals().get("history"))
    return {"packet_bytes": packet_bytes, "bank_bytes_gz": bank_bytes, "history_bytes": history_bytes, "tape_bytes": tape_bytes, "as_of": as_of, "beside": beside, "dry_run": DRY,
            "n_priced": {g: v["n_priced"] for g, v in packet["groups"].items()}, "n_unpriced": {g: v["n_unpriced"] for g, v in packet["groups"].items()},
            "products": {c["cls"]: c["n"] for c in (((packet.get("products") or {}).get("windows") or {}).get("5d") or {}).get("classes") or []},
            "n_liquid": {g: v["n_liquid"] for g, v in packet["groups"].items()}, "n_tracked": {g: v["n_tracked"] for g, v in packet["groups"].items()},
            "sovereign_coverage": packet["groups"]["sovereign"].get("coverage"), "n_indices": len(packet["indices"]), "n_fundamentals": n_fund, "n_ecb_stress": n_stress, "layers": layer_status,
            "cross_reference": {k: v for k, v in ((packet.get("cross_reference") or {}).get("systemic_stress") or {}).items() if k != "checks"},
            "breadth": packet["breadth"], "history": {k: v for k, v in packet["history"].items() if k in ("names", "long_series", "sources", "error", "cdx_ig_vs_2006")}}


def _external_now(bank):
    """Sign evidence from outside the tape as it stands today; an error leaves the desk with the tape's own evidence."""
    try:
        ties, _ = load_ties(bank)
        reference, _ = load_reference()
        return external_anchors(bank, ties, reference)
    except Exception:  # pragma: no cover - enrichment
        return {}


def lambda_handler(event, context=None):
    global DRY
    started = time.time()
    event = event or {}
    action = str(event.get("action") or "daily")
    DRY = bool(event.get("dry_run"))
    today = datetime.now(timezone.utc).date()
    par_rows = par_curve()
    if action == "rebuild":
        bank = {"version": cds_desk.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    else:
        bank = _get_json(BANK_KEY, None) or {"version": cds_desk.VERSION, "series": {}, "meta": {}, "entity_map": {}, "days": []}
    merged = cds_desk.merge_aliases(bank)   # one series per legal entity (reporter short codes, split spellings, sovereign aliases)

    if action in ("backfill", "rebuild"):
        start_iso = str(event.get("start") or FIRST_PUBLIC_DAY)
        end_iso = str(event.get("end") or today.isoformat())
        log, last, exhausted = run_range(bank, start_iso, end_iso, par_rows, started)   # history is measured on the tape's own evidence
        as_of = (bank.get("days") or [None])[-1]
        if as_of is None:
            return {"ok": False, "action": action, "error": "no files found in range", "start": start_iso, "end": end_iso}
        out = publish(bank, as_of, {"action": action, "start": start_iso, "end": end_iso, "files": len(log), "last_file": last,
                                    "budget_exhausted": exhausted, "aliases_merged": len(merged), "elapsed_s": round(time.time() - started, 1)}, par_rows=par_rows)
        out.update(ok=True, action=action, files=len(log), last_file=last, budget_exhausted=exhausted,
                   next_start=((date.fromisoformat(last) + timedelta(days=1)).isoformat() if (exhausted and last) else None))
        return out

    # daily: latest posted file (today's file appears ~00:15 UTC for the previous calendar day of trading)
    file_day = latest_available_file_day(today)
    if file_day is None:
        return {"ok": False, "action": "daily", "error": "no DTCC file posted within %d days" % MAX_WALK_BACK_DAYS}
    cur = load_day_trades(file_day)
    prev_day, prev_trades = None, None
    probe = date.fromisoformat(file_day) - timedelta(days=1)
    for _ in range(MAX_WALK_BACK_DAYS):
        tr = load_day_trades(probe.isoformat())
        if tr is not None:
            prev_day, prev_trades = probe.isoformat(), tr
            break
        probe -= timedelta(days=1)
    today_docs = {file_day: prints_doc(file_day, cur, DAY_RECORDS.get(file_day) or [])}
    if prev_day and prev_trades is not None:
        today_docs[prev_day] = prints_doc(prev_day, prev_trades, DAY_RECORDS.get(prev_day) or [])
    done = process_file_date(bank, file_day, cur, prev_day, prev_trades, par_rows, external=_external_now(bank))
    keep_prints(file_day, cur)
    if prev_day and prev_trades is not None:
        keep_prints(prev_day, prev_trades)
    as_of = (bank.get("days") or [file_day])[-1]
    out = publish(bank, as_of, {"action": "daily", "file": file_day, "prev_file": prev_day, "written": done, "aliases_merged": len(merged),
                                "refresh_listings": bool(event.get("refresh_listings")), "elapsed_s": round(time.time() - started, 1)}, par_rows=par_rows, today_docs=today_docs)
    out.update(ok=True, action="daily", file=file_day, written=done)
    return out
