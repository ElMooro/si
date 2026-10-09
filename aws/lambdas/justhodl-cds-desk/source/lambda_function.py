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

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
PACKET_KEY = os.environ.get("PACKET_KEY", "data/cds-desk.json")
BANK_KEY = os.environ.get("BANK_KEY", "data/warm/cds/bank.json.gz")
HISTORY_KEY = os.environ.get("HISTORY_KEY", "data/cds-desk-history.json")
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
}
# IMF WEO fundamentals (DataMapper, keyless JSON): one request per indicator, all countries
IMF_SOURCES = {ind: "https://www.imf.org/external/datamapper/api/v1/%s" % ind for ind in cds_long_context.IMF_INDICATORS}
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


def _put_json(key, obj, gz=False, cache="no-cache"):
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    kw = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json", "CacheControl": cache}
    if gz:
        body = gzip.compress(body)
        kw["ContentEncoding"] = "gzip"
    s3.put_object(Body=body, **kw)
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
    """Normalised trades from both repositories for one file date; None when neither file exists."""
    found = False
    trades = []
    for source in ("sec", "cftc"):
        raw = fetch_zip(source, day_iso)
        if raw is None:
            continue
        found = True
        for row in cds_desk.read_dissemination_zip(raw):
            t = cds_desk.normalize_trade(row, source)
            if t:
                trades.append(t)
    return trades if found else None


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
def process_file_date(bank, file_day, cur_trades, prev_day, prev_trades, par_rows):
    """Finalise prev_day (prev + late prints in cur) and write file_day preliminarily."""
    emap = cds_desk.entity_map(cur_trades, bank.get("entity_map") or {})
    done = []
    if prev_day and prev_trades is not None:
        ts = [t for t in prev_trades + cur_trades if t["date"] == prev_day]
        if ts:
            agg = cds_desk.aggregate_day(ts, emap, rate_for(prev_day, par_rows), cds_desk.anchors_from_bank(bank, prev_day))
            cds_desk.update_bank(bank, prev_day, agg, emap)
            done.append((prev_day, len(ts), "final"))
    ts = [t for t in cur_trades if t["date"] == file_day]
    if ts:
        agg = cds_desk.aggregate_day(ts, emap, rate_for(file_day, par_rows), cds_desk.anchors_from_bank(bank, file_day))
        cds_desk.update_bank(bank, file_day, agg, emap)
        done.append((file_day, len(ts), "preliminary"))
    bank["entity_map"] = emap
    return done


def run_range(bank, start_iso, end_iso, par_rows, started):
    """Walk file dates start..end sequentially; returns (log, last_day, exhausted_budget)."""
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
        done = process_file_date(bank, day_iso, cur, prev_day, prev_trades, par_rows)
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
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-cds-desk/1.4 (+https://justhodl.ai)"})
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
            s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(txt.encode()), ContentType="application/json" if json_mode else "text/csv", ContentEncoding="gzip")
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
    block = cds_long_context.build_long_context(fred, ofr, ebp, bank, as_of, ecb=ecb)
    block["sources"] = {k: {"url": LONG_SOURCES[k], "status": status.get(k, "unavailable")} for k in LONG_SOURCES}
    return block


def load_fundamentals():
    """IMF WEO indicators -> ({indicator: {iso3: {year: value}}}, status)."""
    texts, status = load_long_sources(sources=IMF_SOURCES, json_mode=True)
    imf = {ind: cds_long_context.parse_imf_json(txt, ind) for ind, txt in texts.items()}
    return imf, status


def publish(bank, as_of, run_meta):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cds_desk.prune_bank(bank, as_of)
    packet = cds_desk.build_packet(bank, as_of, now, run_meta)
    try:
        imf, imf_status = load_fundamentals()
        n_fund = cds_long_context.attach_fundamentals(packet, imf, as_of)
        packet["fundamentals"]["sources"] = imf_status
    except Exception as exc:  # fundamentals are an enrichment; the desk publishes without them
        n_fund = 0
        packet["fundamentals"] = {"error": "%s: %s" % (type(exc).__name__, exc)}
    history_bytes, long_status = None, {}
    try:
        long_block = build_long_block(bank, as_of)
        long_status = {k: v["status"] for k, v in long_block["sources"].items()}
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
    bank_bytes = _put_json(BANK_KEY, bank, gz=True)
    packet_bytes = _put_json(PACKET_KEY, packet, cache="public, max-age=900")
    return {"packet_bytes": packet_bytes, "bank_bytes_gz": bank_bytes, "history_bytes": history_bytes, "as_of": as_of,
            "n_liquid": {g: v["n_liquid"] for g, v in packet["groups"].items()}, "n_tracked": {g: v["n_tracked"] for g, v in packet["groups"].items()},
            "sovereign_coverage": packet["groups"]["sovereign"].get("coverage"), "n_indices": len(packet["indices"]), "n_fundamentals": n_fund,
            "breadth": packet["breadth"], "history": {k: v for k, v in packet["history"].items() if k in ("names", "long_series", "sources", "error", "cdx_ig_vs_2006")}}


def lambda_handler(event, context=None):
    started = time.time()
    event = event or {}
    action = str(event.get("action") or "daily")
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
        log, last, exhausted = run_range(bank, start_iso, end_iso, par_rows, started)
        as_of = (bank.get("days") or [None])[-1]
        if as_of is None:
            return {"ok": False, "action": action, "error": "no files found in range", "start": start_iso, "end": end_iso}
        out = publish(bank, as_of, {"action": action, "start": start_iso, "end": end_iso, "files": len(log), "last_file": last,
                                    "budget_exhausted": exhausted, "aliases_merged": len(merged), "elapsed_s": round(time.time() - started, 1)})
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
    done = process_file_date(bank, file_day, cur, prev_day, prev_trades, par_rows)
    as_of = (bank.get("days") or [file_day])[-1]
    out = publish(bank, as_of, {"action": "daily", "file": file_day, "prev_file": prev_day, "written": done, "aliases_merged": len(merged),
                                "elapsed_s": round(time.time() - started, 1)})
    out.update(ok=True, action="daily", file=file_day, written=done)
    return out
