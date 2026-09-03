"""justhodl-provider-catalog — Khalid's Data section (ops 4506).

One engine keeps ~30 dedicated provider pages complete forever: scan the
bucket against a provider registry (prefixes + hot keys), pull per-key
size/age, and enrich with FULL series inventories from each provider's
own state/catalog files (OFR 442 mnemonics, NYFed PD 1,539 ids, FRED
panel ids, BIS/Eurostat/OECD/StatCan flow lists, Treasury datasets, rp op
counts...). Writes data/provider-catalog.json (hub index) +
data/providers/{slug}.json (the complete per-provider manifest the
template page renders). Daily 06:20 + on demand."""
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
SEARCH_SCHEMA_VERSION = 1

REG = {
 "ofr": {"name": "OFR — Office of Financial Research",
  "api": "data.financialresearch.gov/v1",
  "engines": ["justhodl-ofr-stfm", "justhodl-nyfed-repo-deep",
              "justhodl-plumbing-panel", "justhodl-warm-bridge"],
  "prefixes": ["data/warm/ofr/"], "hot": ["data/ofr-funding.json"],
  "series_from": ("data/warm/ofr/state.json", "catalog")},
 "ofr-hfm": {"name": "OFR — Hedge Fund Monitor",
  "api": "data.financialresearch.gov/hf/v1",
  "engines": [],
  "prefixes": ["data/warm/ofr-hfm/"], "hot": []},
 "ofr-bsrm": {"name": "OFR — Bank Systemic Risk Monitor",
  "api": "financialresearch.gov/bank-systemic-risk-monitor",
  "engines": ["justhodl-src-mirror"],
  "prefixes": ["data/warm/ofr-bsrm/"], "hot": []},
 "ofr-fsi": {"name": "OFR — Financial Stress Index",
  "api": "financialresearch.gov/financial-stress-index",
  "engines": ["justhodl-gap-metrics"],
  "prefixes": ["data/warm/ofr-fsi/"], "hot": ["data/ofr-fsi.json"]},
 "ofr-site": {"name": "OFR — Site Data Files",
  "api": "financialresearch.gov",
  "engines": ["justhodl-src-mirror"],
  "prefixes": ["data/warm/ofr-site/"], "hot": []},
 "nyfed": {"name": "NY Fed — Markets API",
  "api": "markets.newyorkfed.org/api",
  "engines": ["justhodl-nyfed-markets-full", "justhodl-nyfed-repo-deep",
              "justhodl-soma-cusip",
              "justhodl-plumbing-panel", "fedliquidityapi"],
  "prefixes": ["data/warm/nyfed-markets/"],
  "hot": ["data/soma-holdings.json"],
  "series_from": ("data/warm/nyfed-markets/pd-state.json", "catalog")},
 "nyfed-research": {"name": "NY Fed — Research Data (SCE/HHDC/DSGE)",
  "api": "newyorkfed.org",
  "engines": [],
  "prefixes": ["data/warm/nyfed-research/"], "hot": []},
 "te-mirror": {"name": "Trading Economics — FRED Mirror",
  "api": "api.tradingeconomics.com/fred",
  "engines": ["justhodl-te-fred-mirror"],
  "prefixes": ["data/warm/te-mirror/"],
  "hot": ["data/te-mirror-status.json"],
  "series_from": ("data/warm/te-mirror/_state.json", "catalog")},
 "te-feed": {"name": "Trading Economics — Country Snapshot",
  "api": "api.tradingeconomics.com/country",
  "engines": ["justhodl-te-feed"],
  "prefixes": [], "hot": ["data/te-feed.json"]},
 "peru-copper": {"name": "Banco Central de Reserva del Perú — Copper",
  "api": "estadisticas.bcrp.gob.pe",
  "engines": ["justhodl-peru-copper"],
  "prefixes": [], "hot": ["data/peru-copper.json"]},
 "taiwan-moea": {"name": "Taiwan MOEA — Statistics Dept.",
  "api": "service.moea.gov.tw",
  "engines": ["justhodl-taiwan-moea"],
  "prefixes": [], "hot": ["data/taiwan-moea.json"]},
 "treasury": {"name": "US Treasury — FiscalData",
  "api": "api.fiscaldata.treasury.gov",
  "engines": ["justhodl-usgov-direct", "justhodl-warm-bridge"],
  "prefixes": ["data/warm/treasury/", "data/warm/fiscaldata-full/"],
  "hot": ["data/treasury-fiscal.json"]},
 "bea": {"name": "BEA — Bureau of Economic Analysis",
  "api": "apps.bea.gov/api", "engines": ["justhodl-usgov-direct"],
  "prefixes": ["data/warm/usgov/bea/"], "hot": ["data/bea-gdp.json"],
  "series_from": ("data/_state/bea-walk.json", "tables")},
 "bls": {"name": "BLS — Bureau of Labor Statistics",
  "api": "api.bls.gov/publicAPI/v2",
  "engines": ["justhodl-usgov-direct", "justhodl-canary-macro"],
  "prefixes": ["data/warm/usgov/bls/",
               "data/warm/fred-canary/bls-labor", "data/warm/bls-full/"],
  "hot": ["data/bls-macro.json"]},
 "census-us": {"name": "US Census Bureau",
  "api": "api.census.gov/data/timeseries",
  "engines": ["justhodl-census-us"],
  "prefixes": ["data/warm/census-us/"], "hot": [],
  "series_from": ("data/warm/census-us/catalog.json.gz", "datasets")},
 "fed-board": {"name": "Federal Reserve Board — DDP",
  "api": "federalreserve.gov/datadownload",
  "engines": ["justhodl-usgov-direct"],
  "prefixes": ["data/warm/usgov/ddp/", "data/warm/usgov/fed-ddp/",
               "data/warm/ddp/", "data/warm/fed-ddp/",
               "data/warm/usgov/frb", "data/warm/frbddp-full/"]},
 "fred": {"name": "FRED — St. Louis Fed",
  "api": "fred.stlouisfed.org",
  "engines": ["justhodl-fred-catalog", "justhodl-canary-macro",
              "many legacy engines"],
  "prefixes": ["data/warm/fred-canary/", "data/warm/fred-scoped/",
               "data/warm/fred-catalog/",
               "data/providers/fred-scoped/"],
  "hot": ["data/canary-macro.json"]},
 "sec-edgar": {"name": "SEC EDGAR — filings index",
  "api": "sec.gov/Archives", "engines": ["justhodl-edgar-index",
                                          "justhodl-cusip-map-rebuild"],
  "prefixes": ["data/warm/edgar-filings/"],
  "hot": ["data/13f-cusip-map-v2.json"]},
 "sec-bulk": {"name": "SEC — bulk XBRL",
  "api": "sec.gov (companyfacts/submissions)",
  "engines": ["justhodl-sec-bulk"],
  "prefixes": ["data/warm/sec-bulk/"]},
 "sec-dera": {"name": "SEC DERA — statement datasets",
  "api": "sec.gov/dera", "engines": ["justhodl-global-expansion",
              "justhodl-hist-banker"],
  "prefixes": ["data/warm/sec-dera/"]},
 "sec-midas": {"name": "SEC MIDAS — market structure",
  "api": "sec.gov/marketstructure",
  "engines": ["justhodl-sec-midas"],
  "prefixes": ["data/warm/sec-midas/"]},
 "polygon": {"name": "Polygon.io — US equities",
  "api": "api.polygon.io", "engines": ["justhodl-polygon-daily"],
  "prefixes": ["data/warm/us-equities-daily/", "data/warm/polygon-full/"]},
 "gleif": {"name": "GLEIF — LEI system",
  "api": "gleif.org", "engines": ["justhodl-bis-gleif",
                                   "justhodl-symbology-master"],
  "prefixes": ["data/warm/gleif/"]},
 "bis": {"name": "BIS — Bank for Intl Settlements",
  "api": "stats.bis.org/api/v1",
  "engines": ["justhodl-bis-gleif", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/bis/"],
  "series_from": ("data/warm/bis/catalog.json.gz", "dataflows")},
 "eurostat": {"name": "Eurostat",
  "api": "ec.europa.eu/eurostat SDMX",
  "engines": ["justhodl-eurostat-oecd", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/eurostat/"],
  # ops 5040: the derived series store is COUNTED, never enumerated --
  # the normal prefix scan appends one dict per object into keys[], and
  # at 2M+ pages that document would be hundreds of MB and would time
  # the scan out. count_prefixes contributes objects+bytes only.
  "count_from": ("data/providers/eurostat/series-manifest.json",
                 "pages", "pages_bytes"),
  "series_from": ("data/providers/eurostat/series-manifest.json",
                  "series_extracted"),
  "_series_from_legacy": ("data/warm/eurostat/catalog.json.gz",
                  "dataflows")},
 "oecd": {"name": "OECD",
  "api": "sdmx.oecd.org/public/rest",
  "engines": ["justhodl-eurostat-oecd", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/oecd/"],
  "series_from": ("data/warm/oecd/catalog.json.gz", "dataflows")},
 "statcan": {"name": "Statistics Canada",
  "api": "www150.statcan.gc.ca/t1/wds",
  "engines": ["justhodl-global-expansion", "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/statcan/"],
  "series_from": ("data/warm/statcan/cube-list.json.gz", "cubes")},
 "banxico": {"name": "Banxico — SIE API",
  "api": "banxico.org.mx/SieAPIRest",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/banxico/"]},
 "boe": {"name": "Bank of England — IADB",
  "api": "bankofengland.co.uk/boeapps/database",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/boe/", "data/warm/boe-full/"]},
 "hk-data": {
     "name": "Hong Kong — data.gov.hk",
     "api": "data.gov.hk CKAN",
     "engines": ["justhodl-asia-trade-full"],
     "prefixes": ["data/warm/asia-trade/hk/"]},
 "cl-datos": {
     "name": "Chile — datos.gob.cl",
     "api": "datos.gob.cl CKAN",
     "engines": ["justhodl-asia-trade-full"],
     "prefixes": ["data/warm/asia-trade/cl/"]},
 "kr-ecos": {
     "name": "Korea — Bank of Korea ECOS",
     "api": "ecos.bok.or.kr",
     "engines": ["justhodl-asia-trade-full"],
     "prefixes": ["data/warm/asia-trade/kr/"]},
 "tic": {
     "name": "US Treasury — TIC",
     "api": "ticdata.treasury.gov",
     "engines": ["justhodl-tic-full"],
     "prefixes": ["data/warm/tic-full/"]},
 "finra": {
     "name": "FINRA — Query API",
     "api": "api.finra.org",
     "engines": ["justhodl-finra-full"],
     "prefixes": ["data/warm/finra-full/"]},
 "gdelt": {"name": "GDELT — global events",
  "api": "data.gdeltproject.org/gdeltv2",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/gdelt/", "data/warm/gdelt-full/"]},
 "eiopa": {"name": "EIOPA — Solvency II RFR",
  "api": "eiopa.europa.eu", "engines": ["justhodl-global-expansion",
              "justhodl-hist-banker"],
  "prefixes": ["data/warm/eiopa/"]},
 "nasa": {"name": "NASA POWER — ag weather",
  "api": "power.larc.nasa.gov",
  "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/nasa-power/"]},
 "dol": {"name": "US DOL — ETA claims",
  "api": "oui.doleta.gov", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/dol-", "data/warm/dol-full/"]},
 "chicagofed": {"name": "Chicago Fed — NFCI",
  "api": "chicagofed.org", "engines": ["justhodl-plumbing-panel"],
  "prefixes": ["data/warm/chicagofed/",
               "data/warm/fred-canary/nfci"]},
 "clevelandfed": {"name": "Cleveland Fed — yield-curve model",
  "api": "clevelandfed.org", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/cleveland-"]},
 "atlantafed": {"name": "Atlanta Fed — GDPNow",
  "api": "atlantafed.org", "engines": ["justhodl-canary-macro"],
  "prefixes": ["data/warm/fred-canary/atlanta-"]},
 "occ": {"name": "OCC — options volume",
  "api": "theocc.com", "engines": ["justhodl-global-expansion"],
  "prefixes": ["data/warm/occ/"]},
 "cftc": {"name": "CFTC — futures positioning",
  "api": "publicreporting.cftc.gov",
  "engines": ["cftc-futures-positioning-agent",
              "justhodl-cftc-deep"],
  "prefixes": ["data/warm/cftc/"]},
 "yahoo": {"name": "Yahoo Finance", "api": "query1.finance.yahoo.com",
  "engines": ["legacy fleet (signature-mapped)"], "prefixes": []},
 "coinmetrics": {"name": "CoinMetrics — community",
  "api": "community-api.coinmetrics.io",
  "engines": ["legacy fleet"], "prefixes": []},
 "imf": {"name": "IMF — SDMX/MFS", "api": "dataservices.imf.org",
  "engines": ["families-feed"], "prefixes": ["data/warm/imf", "data/warm/imf-full/"]},
 "worldbank": {"name": "World Bank", "api": "api.worldbank.org",
  "engines": ["families-feed"], "prefixes": ["data/warm/worldbank", "data/warm/worldbank-full/"]},
 "dbnomics": {"name": "DBnomics", "api": "api.db.nomics.world",
  "engines": ["legacy fleet"], "prefixes": []},
 "snb": {"name": "Swiss National Bank", "api": "data.snb.ch",
  "engines": ["gov-sources"], "prefixes": []},
 "bcb": {"name": "Banco Central do Brasil", "api": "api.bcb.gov.br",
  "engines": ["gov-sources"], "prefixes": []},
 "cboe": {"name": "Cboe", "api": "cdn.cboe.com",
  "engines": ["legacy fleet"], "prefixes": []},
 "boj": {"name": "Bank of Japan", "api": "stat-search.boj.or.jp",
  "engines": ["gov-sources"], "prefixes": ["data/warm/boj-full/"]},
 "other": {"name": "Other / mixed-signature",
  "api": "(various)", "engines": ["fleet"], "prefixes": []},
 "ecb": {"name": "ECB — SDMX",
  "api": "data-api.ecb.europa.eu",
  # ops 4893: truthful engine list — ciss-stress/ciss-ai were the
  # engines proving the ECB DATA endpoint worked all along; their
  # outputs (data/ciss-*.json) now count under this card instead of
  # falling into "Other / mixed-signature".
  "engines": ["justhodl-ecb-full-catalog", "justhodl-ciss-stress",
              "justhodl-ciss-ai", "justhodl-ecb-history",
              "justhodl-ecb-detail", "justhodl-ecb-derived",
              "justhodl-sdmx-walker"],
  "prefixes": ["data/warm/ecb/", "data/warm/ecb-", "data/ecb",
               "data/ciss"],
  # ops 5045: same correction Eurostat got. "214 series" was 214
  # DATAFLOWS -- the dataflow list length, not a series count. The
  # extractor now publishes a real one, and the page/byte totals of the
  # derived store come from counters the writer maintains rather than a
  # LIST walk that would time the scan out.
  "count_from": ("data/providers/ecb/series-manifest.json",
                 "pages", "pages_bytes"),
  "series_from": ("data/providers/ecb/series-manifest.json",
                  "series_extracted"),
  "_series_from_legacy": ("data/warm/ecb/catalog.json.gz",
                          "dataflows")},
}


def _get_json(key):
    b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def _series_list(spec):
    if not spec:
        return None
    key, field = spec
    try:
        d = _get_json(key)
        v = d.get(field)
        # ops 5039: some providers count series in the hundreds of
        # millions, where an id list is neither possible nor meaningful.
        # Eurostat's extractor publishes an integer total in its
        # series-manifest; accept that as a first-class answer instead of
        # forcing every provider through len(ids).
        if isinstance(v, bool):
            v = None
        if isinstance(v, (int, float)) and v >= 0:
            return {"count": int(v), "ids": [], "counted": True}
        if isinstance(v, list):
            ids = [(x.get("id") if isinstance(x, dict) else x)
                   for x in v]
            ids = [str(i) for i in ids if i]
            out = {"count": len(ids),
                   "ids": ids if len(ids) <= 2000 else
                   ids[:400] + ["…+" + str(len(ids) - 400) + " more"]}
            # depth lift-through: providers whose series_from doc
            # carries a depth ledger (nyfed pd-state) surface it on
            # the card — bytes were never the depth metric
            if isinstance(d.get("depth"), dict) \
                    and d["depth"].get("keys"):
                out["depth"] = d["depth"]
            return out
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)[:50]}"}
    return None


def _search_title(key):
    """Human-readable label for a stored object without losing path searchability."""
    raw = str(key or "")
    leaf = raw.rsplit("/", 1)[-1]
    leaf = re.sub(r"(?:\.(?:json|csv|tsv|parquet|arrow|xlsx?|zip|gz))+$", "", leaf,
                  flags=re.I)
    title = re.sub(r"[_\-.]+", " ", leaf).strip()
    if not title or re.fullmatch(r"(page|part|chunk)\s*\d+", title, re.I):
        clean = re.sub(
            r"(?:\.(?:json|csv|tsv|parquet|arrow|xlsx?|zip|gz))+$",
            "", raw, flags=re.I)
        title = re.sub(r"[/_\-.]+", " ", clean).strip()
    return title[:220] or raw[:220]


def _indicator_search_rows(payload):
    """Expand the canonical indicator registry into searchable entity refs."""
    items = (payload or {}).get("indicators") or (payload or {}).get("items") or {}
    if isinstance(items, dict):
        items = [dict(value, symbol=symbol)
                 if isinstance(value, dict) else {"symbol": symbol}
                 for symbol, value in items.items()]
    rows = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol") or item.get("id") or item.get("name")
        if not symbol:
            continue
        rows.append({
            "id": "indicator-bus:" + str(symbol),
            "title": str(symbol),
            "kind": "indicator_ref",
            "search": " ".join(str(x) for x in (
                item.get("name"), item.get("src"), item.get("source"),
                item.get("asof")) if x is not None),
            "hot": True,
        })
    return rows


def _tradingview_live_search_rows(payload):
    """Expand every LIVE vault symbol; unresolved/pending records stay excluded."""
    items = (payload or {}).get("symbols") or []
    if isinstance(items, dict):
        items = [dict(value, symbol=symbol)
                 if isinstance(value, dict) else {"symbol": symbol}
                 for symbol, value in items.items()]
    rows = []
    for item in items if isinstance(items, list) else []:
        if (not isinstance(item, dict) or item.get("status") != "LIVE"
                or not item.get("symbol")):
            continue
        symbol = str(item["symbol"])
        exchanges = item.get("exchanges") or []
        if not isinstance(exchanges, list):
            exchanges = [exchanges]
        rows.append({
            "id": "tradingview-vault-live:" + symbol,
            "title": symbol,
            "kind": "instrument_ref",
            "search": " ".join(str(x) for x in (
                item.get("category"), item.get("source"),
                item.get("resolved_via"), " ".join(exchanges),
                item.get("note_snippet")) if x is not None)[:1000],
            "hot": True,
        })
    return rows


def _write_search_shard(slug, provider_name, api, keys, series,
                        search_db=None, entities=None):
    """Publish compact search metadata while the complete provider scan is in memory.

    The search service must never re-list the bucket or fetch thousands of provider
    pages. It consumes these gzip shards after the catalog job completes.
    """
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    series_ids = (series or {}).get("ids") or []
    entities = entities or []
    entity_count = sum(
        1 for entity in entities
        if entity.get("id") and entity.get("title"))
    asset_count = sum(
        1 for item in keys
        if item.get("key") and not item.get("missing"))
    series_ref_count = sum(
        1 for sid in series_ids
        if str(sid) and not str(sid).startswith("…+"))
    expected_count = 1 + sum(
        1 for sid in series_ids
        if str(sid) and not str(sid).startswith("…+")) + sum(
        1 for item in keys if item.get("key") and not item.get("missing")) + \
        entity_count
    meta = {
        "schema_version": SEARCH_SCHEMA_VERSION,
        "provider": slug,
        "provider_name": provider_name,
        "api": api,
        "generated_at": generated_at,
        "count": expected_count,
    }

    def rows():
        yield [f"provider:{slug}", provider_name, "provider", None, None, None,
               True]
        for sid in series_ids:
            sid = str(sid)
            if not sid or sid.startswith("…+"):
                continue
            rid = sid if ":" in sid else f"{slug}:{sid}"
            yield [rid, sid[:220], "series_ref", None, None, None, False]
        for entity in entities:
            rid = str(entity.get("id") or "").strip()
            title = str(entity.get("title") or "").strip()
            if not rid or not title:
                continue
            # The existing `key` FTS column doubles as hidden search text for
            # catalog entities. The consumer exposes it as `src`, never as an
            # S3 key, unless kind == asset.
            search_text = str(entity.get("search") or "")[:1000]
            yield [
                rid, title[:220], str(entity.get("kind") or "entity_ref"),
                search_text, entity.get("bytes"), entity.get("age_h"),
                bool(entity.get("hot")),
            ]
        for item in keys:
            key = item.get("key")
            if not key or item.get("missing"):
                continue
            asset_id = hashlib.sha1(str(key).encode()).hexdigest()[:16]
            yield [
                f"{slug}:asset:{asset_id}", _search_title(key), "asset", key,
                item.get("bytes"), item.get("age_h"), bool(item.get("hot")),
            ]

    key = f"data/search/providers/{slug}.json.gz"
    shard_path = f"/tmp/provider-search-{slug}.json.gz"
    count, first, batch = 0, True, []
    with gzip.open(shard_path, "wt", encoding="utf-8", compresslevel=6) as dst:
        dst.write(json.dumps(meta, separators=(",", ":"))[:-1] + ',"rows":[')
        for row in rows():
            if not first:
                dst.write(",")
            first = False
            dst.write(json.dumps(row, separators=(",", ":"), default=str))
            count += 1
            if search_db is not None:
                batch.append((row[0], slug, provider_name, row[1], row[3] or "",
                              row[2], row[4], row[5], 1 if row[6] else 0))
                if len(batch) >= 1000:
                    search_db.executemany(
                        "INSERT INTO docs(id,provider,provider_name,title,key,kind,nbytes,age_h,hot) "
                        "VALUES(?,?,?,?,?,?,?,?,?)", batch)
                    batch.clear()
        dst.write("]}")
    if search_db is not None and batch:
        search_db.executemany(
            "INSERT INTO docs(id,provider,provider_name,title,key,kind,nbytes,age_h,hot) "
            "VALUES(?,?,?,?,?,?,?,?,?)", batch)
    if count != expected_count:
        raise RuntimeError(
            f"search shard count mismatch for {slug}: {count}!={expected_count}")
    size = os.path.getsize(shard_path)
    s3.upload_file(
        shard_path, BUCKET, key,
        ExtraArgs={"ContentType": "application/json", "CacheControl": "no-cache"})
    os.remove(shard_path)
    return {"provider": slug, "provider_name": provider_name,
            "key": key, "count": count, "bytes": size,
            "provider_rows": 1, "series_refs": series_ref_count,
            "entity_refs": entity_count, "assets": asset_count}


def _load_rollup_feeds():
    """ops 4513 v4: provider->feeds via the two-file join —
    engine-provider-map (engine->provider) x lambda-graph
    (engine->writes[feeds]). Shape-flexible on the map values."""
    try:
        emap = _get_json("data/audit/engine-provider-map.json")
        graph = _get_json("data/audit/lambda-graph.json")
    except Exception:
        # ops 4530: last-good fallback — nightly graph rewrites raced the
        # join and flapped totals 1,108<->1,627; never lose the attribution
        try:
            return _get_json("data/audit/provider-join-lastgood.json")
        except Exception:
            return {}
    if isinstance(emap, dict) and isinstance(emap.get("map"), dict):
        emap = emap["map"]  # ops 4520: THE unwrap
    elif isinstance(emap, dict) and isinstance(emap.get("engines"),
                                               dict):
        emap = emap["engines"]
    engines = (graph.get("engines") or {}) if isinstance(graph, dict) \
        else {}
    try:  # ops 4532: runner-grepped writes the graph regex missed
        ov = _get_json("data/audit/engine-writes-overrides.json")
        for e2, ws in (ov.get("writes") or {}).items():
            rec2 = engines.setdefault(e2, {"writes": [], "reads": []})
            rec2["writes"] = sorted(set(rec2.get("writes") or []) |
                                    set(ws))
    except Exception:
        pass
    out = {}
    for eng, rec in engines.items():
        pv = emap.get(eng)
        if isinstance(pv, dict):
            pv = pv.get("providers") or [pv.get("provider")]
        pvs = pv if isinstance(pv, list) else [pv]
        pvs = [x.lower().strip() for x in pvs if isinstance(x, str)]
        if not pvs:
            continue
        for feed in (rec.get("writes") or []):
            if isinstance(feed, dict):
                feed = (feed.get("key") or feed.get("feed")
                        or feed.get("path") or feed.get("name"))
            if isinstance(feed, str):
                for prov in pvs:  # ops 4533: shared feeds -> EVERY provider
                    out.setdefault(prov, []).append(feed)
    for k in out:
        out[k] = sorted(set(out[k]))
    if out:  # ops 4530: persist last-good
        try:
            s3.put_object(Bucket=BUCKET,
                          Key="data/audit/provider-join-lastgood.json",
                          Body=json.dumps(out, default=str).encode(),
                          ContentType="application/json")
        except Exception:
            pass
    try:  # ops 4519: self-debug — no more guessing
        s3.put_object(Bucket=BUCKET,
                      Key="data/audit/provider-join-debug.json",
                      Body=json.dumps({
                          "n_map": len(emap),
                          "n_graph_engines": len(engines),
                          "map_sample": dict(list(emap.items())[:2]),
                          "graph_sample": {k: v for k, v in
                                           list(engines.items())[:1]},
                          "n_out_providers": len(out),
                          "out_sizes": {k: len(v) for k, v in
                                        list(out.items())[:12]}},
                          default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
    except Exception:
        pass
    return out


def _hot_index():
    """Size/age for every object directly under data/ (hot feeds)."""
    now = datetime.now(timezone.utc)
    idx = {}
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/", "Delimiter": "/",
              "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            idx[o["Key"]] = (o["Size"], round(
                (now - o["LastModified"]).total_seconds() / 3600, 1))
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return idx


ALIAS = {"llm-anthropic": "other", "transform-agent": "other",
         "fleet-feed": None, "unmapped": "other",
         "fed-board": "fed-board", "nasdaq": "other"}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    roll = _load_rollup_feeds()
    hotidx = _hot_index()
    key_engines = {}  # ops 4544: reverse index for the drill-down
    KEY_PROVIDER_SKIP = {  # 4546: misattributions (Perplexity)
        "data/finviz-universe.json": {"fred"}}
    PREFIX_ENGINES = {  # walker/dynamic keys the graph can't see
        "data/warm/eurostat/": ["justhodl-sdmx-walker"],
        "data/warm/oecd/": ["justhodl-sdmx-walker"],
        "data/warm/statcan/": ["justhodl-sdmx-walker",
                               "justhodl-global-expansion"],
        "data/warm/bis/": ["justhodl-sdmx-walker",
                           "justhodl-bis-gleif"],
        "data/warm/fred-canary/": ["justhodl-canary-macro"],
        "data/warm/fred-scoped/": ["justhodl-fred-catalog"],
        "data/warm/fred-catalog/": ["justhodl-fred-catalog"],
        "data/providers/fred-scoped/": ["justhodl-fred-catalog"]}
    try:
        g2 = _get_json("data/audit/lambda-graph.json")
        ov2 = _get_json("data/audit/engine-writes-overrides.json")
        for _e, _rec in ((g2.get("engines") or {}) | {}).items():
            for _w in (_rec.get("writes") or []):
                if isinstance(_w, dict):
                    _w = _w.get("key") or _w.get("feed")
                if isinstance(_w, str):
                    key_engines.setdefault(_w, []).append(_e)
        for _e, _ws in (ov2.get("writes") or {}).items():
            for _w in _ws:
                key_engines.setdefault(_w, []).append(_e)
        for _k in key_engines:
            key_engines[_k] = sorted(set(key_engines[_k]))[:4]
    except Exception:
        pass
    try:  # ops 4508 discovery: real subfolder names, printed once
        r1 = s3.list_objects_v2(Bucket=BUCKET,
                                Prefix="data/warm/usgov/",
                                Delimiter="/")
        print("USGOV_SUBS:", [c["Prefix"] for c in
                              r1.get("CommonPrefixes", [])])
        r2 = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/warm/ec",
                                MaxKeys=15)
        print("EC_KEYS:", [o["Key"] for o in r2.get("Contents", [])])
    except Exception as _e:
        print("disc_err", _e)
    hub = {"as_of": now.isoformat(timespec="seconds"), "providers": []}
    search_shards = []
    hierarchical_series = {}
    search_db_path = "/tmp/provider-search.sqlite"
    search_db_gz = search_db_path + ".gz"
    for path in (search_db_path, search_db_gz):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    search_db = sqlite3.connect(search_db_path)
    search_db.execute("PRAGMA journal_mode=OFF")
    search_db.execute("PRAGMA synchronous=OFF")
    search_db.execute("PRAGMA temp_store=FILE")
    search_db.execute(
        "CREATE VIRTUAL TABLE docs USING fts5("
        "id UNINDEXED,provider,provider_name UNINDEXED,title,key,"
        "kind UNINDEXED,nbytes UNINDEXED,age_h UNINDEXED,hot UNINDEXED,"
        "tokenize='unicode61 remove_diacritics 2')")
    for slug, r in REG.items():
        keys = []
        tot = 0
        for pref in r.get("prefixes", []):
            tok = None
            while True:
                kw = {"Bucket": BUCKET, "Prefix": pref,
                      "MaxKeys": 400}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                for o in resp.get("Contents", []):
                    age = round((now - o["LastModified"])
                                .total_seconds() / 3600, 1)
                    keys.append({"key": o["Key"],
                                 "bytes": o["Size"],
                                 "age_h": age})
                    tot += o["Size"]
                if not resp.get("IsTruncated"):
                    break
                tok = resp.get("NextContinuationToken")
        for hk in r.get("hot", []):
            try:
                h = s3.head_object(Bucket=BUCKET, Key=hk)
                keys.append({"key": hk, "bytes": h["ContentLength"],
                             "age_h": round(
                                 (now - h["LastModified"])
                                 .total_seconds() / 3600, 1),
                             "hot": True})
                tot += h["ContentLength"]
            except Exception:
                keys.append({"key": hk, "missing": True,
                             "hot": True})
        # v3: rollup-attributed hot feeds for this provider
        rslugs = [slug] + [k for k, v in ALIAS.items() if v == slug]
        seen = {k["key"] for k in keys}
        n_roll = 0
        for rs in rslugs:
            for feed in roll.get(rs, []):
                fk = feed if feed.startswith("data/") else (
                    "data/" + feed.lstrip("/"))
                if slug in KEY_PROVIDER_SKIP.get(fk, ()):  # 4546
                    continue
                if fk in seen:
                    continue
                sz, age = hotidx.get(fk, (None, None))
                keys.append({"key": fk, "bytes": sz, "age_h": age,
                             "hot": True, "via": "rollup"})
                if sz:
                    tot += sz
                seen.add(fk)
                n_roll += 1
        keys.sort(key=lambda x: (x.get("bytes") or 0), reverse=True)
        # ops 5040: counted prefixes. Same LIST walk, MaxKeys=1000, but
        # only two accumulators come back -- so a 2M-object derived store
        # lifts the provider's key and byte totals without inflating the
        # per-key array that data.html actually downloads. n_live below
        # is computed from keys[] alone, so coverage % stays a warm-mirror
        # ratio and is unaffected by anything counted here.
        derived_n, derived_bytes, derived_fresh = 0, 0, None
        _cf = r.get("count_from")
        if _cf:
            # ops 5040 v2: READ the totals, never re-derive them. The
            # first cut walked the prefix at MaxKeys=1000; at ~1M objects
            # that alone blew the 600s scan, and the store is still
            # growing. The engine that writes the pages already knows how
            # many there are and how big they are, so it publishes both
            # and this is O(1) forever.
            try:
                _d = _get_json(_cf[0])
                derived_n = int(_d.get(_cf[1]) or 0)
                derived_bytes = int(_d.get(_cf[2]) or 0)
                if _d.get("series_extracted") is not None:
                    hierarchical_series[slug] = int(
                        _d.get("series_extracted") or 0)
                _u = _d.get("updated_at")
                if _u:
                    derived_fresh = round(
                        (now - datetime.fromisoformat(
                            str(_u).replace("Z", "+00:00")))
                        .total_seconds() / 3600, 1)
            except Exception as _e:
                derived_n, derived_bytes = 0, 0
        if derived_bytes:
            tot += derived_bytes
        ser = _series_list(r.get("series_from"))
        doc = {"slug": slug, "name": r["name"], "api": r["api"],
               "engines": r["engines"],
               "as_of": hub["as_of"],
               "n_keys": len([k for k in keys
                              if not k.get("missing")]) + derived_n,
               "derived": ({"source": (r.get("count_from") or [None])[0],
                            "objects": derived_n,
                            "bytes": derived_bytes,
                            "mb": round(derived_bytes / 1e6, 2),
                            "freshest_h": derived_fresh}
                           if derived_n else None),
               "total_bytes": tot,
               "total_mb": round(tot / 1e6, 2),
               "freshest_h": min([k["age_h"] for k in keys
                                  if k.get("age_h") is not None]
                                 or [None],
                                 key=lambda x: (x is None, x)),
               "series": ser,
               "page_size": 500,
               "n_pages": max(0, (len(keys) - 100 + 499) // 500),
               "keys": [dict(k, status=("missing" if k.get("missing")
                                        else "live" if k.get("age_h")
                                        is not None else "cold"),
                             engines=(key_engines.get(k["key"])
                                      or next((v for pf, v in
                                               PREFIX_ENGINES.items()
                                               if k["key"]
                                               .startswith(pf)), [])))
                        for k in keys[:100]]}
        for _pi in range(doc["n_pages"]):
            _chunk = keys[100 + _pi * 500:100 + (_pi + 1) * 500]
            s3.put_object(
                Bucket=BUCKET,
                Key=f"data/providers/{slug}/page-{_pi:03d}.json",
                Body=json.dumps({"page": _pi, "keys": [
                    dict(k, status=("missing" if k.get("missing")
                                    else "live" if k.get("age_h")
                                    is not None else "cold"),
                         engines=(key_engines.get(k["key"])
                                  or next((v for pf, v in
                                           PREFIX_ENGINES.items()
                                           if k["key"]
                                           .startswith(pf)), [])))
                    for k in _chunk]}, default=str).encode(),
                ContentType="application/json",
                CacheControl="no-cache")
        s3.put_object(Bucket=BUCKET,
                      Key=f"data/providers/{slug}.json",
                      Body=json.dumps(doc, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        search_shards.append(_write_search_shard(
            slug, r["name"], r["api"], keys, ser, search_db))
        n_live = len([k for k in keys if not k.get("missing")])
        # ops 4543 (Perplexity P1): the catalog count is a TARGET, not a
        # holding. datasets = what we actually have; target + coverage
        # ride alongside. Eurostat is 3.6%, not done.
        ds = n_live
        # ops 4548 (Khalid: why isn't Eurostat/StatCan/BIS coverage
        # showing?): my earlier hardcoded/catalog-file targets went
        # stale the moment the walker's OWN live catalog grew past
        # them (StatCan) or drifted slightly (Eurostat extra keys).
        # The walker's state file already stores n_total — the exact
        # number it is walking against. Use THAT as ground truth.
        tgt = None
        cov_basis = None
        if slug in ("eurostat", "oecd", "statcan", "bis"):
            try:
                wst = _get_json(
                    f"data/_state/sdmx-walk-{slug}.json")
                tgt = wst.get("n_total")
                cov_basis = "keys/keys (walker n_total — same unit)"
            except Exception:
                tgt = None
        # ops 4574 (wo-4559 BUG-10, real half): the old fallback compared
        # banked KEYS to catalog SERIES counts — a fabricated percentage
        # whenever one key holds many series. A coverage % is only shown
        # when numerator and denominator share a declared unit; otherwise
        # datasets and series_count both stay visible, un-ratioed.
        # never hide progress: cap the DISPLAYED pct at 100, note if
        # actual exceeds nominal (a few retried/renamed keys, not a
        # real overshoot).
        cov = (round(min(100.0, 100.0 * n_live / tgt), 1)
               if tgt else None)
        cov_note = ("at_or_above_target"
                    if (tgt and n_live >= tgt) else None)
        denied = None
        if slug in ("oecd", "statcan"):
            # ops 4568: StatCan's 8,221-vs-7,962 gap = walker failures;
            # surface them exactly like OECD's source-side denials.
            try:
                denied = len((_get_json(
                    f"data/_state/sdmx-walk-{slug}.json")
                    .get("failures") or {}))
            except Exception:
                denied = None
        note = None
        if derived_n:   # ops 5040
            note = ("derived series store: %s series across %s pages "
                    "(%.1f GB) — counted, not mirrored" % (
                        f"{(ser or {}).get('count') or 0:,}",
                        f"{derived_n:,}", derived_bytes / 1e9))
        if slug == "fred":
            # ops 4569: the manifest counter double-counts re-walked
            # series (its imported_ids dedup list caps at 2000) — the
            # on-disk scoped objects are the truth. Count those; the
            # manifest only supplies category progress + status.
            _fi = len([k for k in keys if k.get("key", "")
                       .startswith("data/warm/fred-scoped/")])
            try:
                _fm = _get_json(
                    "data/providers/fred-scoped/manifest.json")
                note = (f"scoped import: {_fi:,} series banked · "
                        f"{_fm.get('categories_done') or 0}/"
                        f"{_fm.get('categories_total') or 0}"
                        f" categories · {_fm.get('status') or '—'}")
            except Exception:
                note = f"scoped import: {_fi:,} series banked"
            if _fi:
                ser = dict(ser or {}, count=_fi)
        _dep = (ser or {}).get("depth") or {}
        if _dep.get("keys"):
            _note_d = ("depth %d obs/series · since %s · %d/%s @v3"
                       % (round(_dep["n_obs_sum"]
                                / max(1, _dep["keys"])),
                          str(_dep.get("first_min") or "—")[:4],
                          _dep["keys"],
                          (ser or {}).get("count") or _dep["keys"]))
            note = (note + " · " + _note_d) if note else _note_d
        if slug == "fred":
            # Khalid: surface the TE-mirrored ICE data AS EXTRA
            # datasets under the FRED card, never overwriting the
            # direct-FRED count (_fi/ser["count"] above are untouched
            # by this block — this only appends a note fragment).
            try:
                _tm = _get_json("data/warm/te-mirror/_index.json")
                _syms = _tm.get("symbols") or {}
                _baml = {k: v for k, v in _syms.items()
                        if k.startswith("BAML")}
                if _baml:
                    _ap = [v.get("agree_pct") for v in _baml.values()
                          if v.get("agree_pct") is not None]
                    _mean_ap = (round(sum(_ap) / len(_ap), 1)
                               if _ap else None)
                    _note_te = ("+ %d ICE BofA series independently "
                               "cross-validated via Trading Economics "
                               "mirror (%s%% agree)"
                               % (len(_baml),
                                  _mean_ap if _mean_ap is not None
                                  else "—"))
                    note = (note + " · " + _note_te) if note \
                        else _note_te
            except Exception:
                pass
        if slug == "te-mirror":
            try:
                _tx = json.loads(s3.get_object(
                    Bucket=BUCKET,
                    Key="data/warm/te-mirror/_index.json"
                )["Body"].read())
                if _tx.get("n_symbols"):
                    _note_x = ("independent copy: %d series · mean "
                              "cross-check vs FRED %s%% agree"
                              % (_tx["n_symbols"],
                                 _tx.get("mean_agree_pct") or "—"))
                    note = (note + " · " + _note_x) if note \
                        else _note_x
            except Exception:
                pass
        if slug in ("hk-data", "cl-datos", "kr-ecos"):
            try:  # ops 4989 (asia-note-v2)
                _am = _get_json("data/warm/asia-trade/"
                                "manifest.json")
            except Exception:
                _am = None
            if _am:
                if slug == "hk-data":
                    _x = ("trade/industry mirror: %s "
                          "resources · ports/"
                          "manufacturing/exports/"
                          "imports"
                          % _am.get("hk_resources"))
                elif slug == "cl-datos":
                    _x = ("comercio/industria mirror: "
                          "%s resources"
                          % _am.get("cl_resources"))
                else:
                    _x = ("ECOS: %s series · %s" % (
                        _am.get("kr_series"),
                        (_am.get("kr_status")
                         or "")[:90]))
                note = ((note + " · ") if note
                        else "") + _x
        if slug == "fed-board":
            try:  # ops 4985 (ddp-note-v2)
                _dm = _get_json("data/warm/frbddp-full/"
                                "manifest.json")
                if _dm:
                    note = ((note + " · ") if note else "") + (
                        "FULL release packages (frbddp-full v1): "
                        "%s programs · %.0fMB (Z.1, H.4.1, H.15, "
                        "H.8...) · %s named misses" % (
                            _dm.get("packages"),
                            _dm.get("mb") or 0,
                            _dm.get("failures")))
            except Exception:
                pass
        if slug == "tic":
            try:  # ops 4985 (tic-note-v2)
                _tm = _get_json("data/warm/tic-full/manifest.json")
                if _tm:
                    note = ((note + " · ") if note else "") + (
                        "banking+flows text mirror (tic-full v1): "
                        "%s files · %.2fMB (bctype claims, bltype, "
                        "SLT, MFH) · %s named misses" % (
                            _tm.get("files"), _tm.get("mb") or 0,
                            _tm.get("failures")))
            except Exception:
                pass
        if slug == "boj":
            try:  # ops 4985 (boj-note-v2)
                _bm = _get_json("data/warm/boj-full/manifest.json")
                if _bm:
                    note = ((note + " · ") if note else "") + (
                        "FULL flat-file warehouse (boj-full v1): "
                        "%s/%s database zips · %.0fMB · the "
                        "entire time-series portal" % (
                            _bm.get("zips"), _bm.get("universe"),
                            _bm.get("mb") or 0))
                    _ad = _bm.get("api_dbs") or 0
                    if _ad:  # boj-note-v3 (ops 4987)
                        note += (" · API universe: %s dbs · "
                                 "%s/%s series · %s parts"
                                 % (_ad,
                                    _bm.get("api_series_done"),
                                    _bm.get("api_series"),
                                    _bm.get("api_parts")))
            except Exception:
                pass
        if slug == "finra":
            # ops 4978 (finra-note-v2)
            try:
                _fn = _get_json("data/warm/finra-full/"
                                "manifest.json")
                if _fn:
                    note = ((note + " · ") if note else "") + (
                        "FULL Query-API warehouse (finra-full v1):"
                        " %s/%s datasets · %s rows since "
                        "inception · %.0fMB · phase %s · daily "
                        "rediscovery" % (
                            _fn.get("datasets_banked"),
                            _fn.get("datasets_catalog"),
                            _fn.get("rows"),
                            _fn.get("mb") or 0,
                            _fn.get("phase")))
            except Exception:
                pass
        if slug == "boe":
            # ops 4977 (boe-note-v2)
            try:
                _bm = _get_json("data/warm/boe-full/manifest.json")
                if _bm:
                    note = ((note + " · ") if note else "") + (
                        "FULL warehouse (boe-full v1): %s curve "
                        "zips %.0fMB (daily curves since 1970) · "
                        "%s IADB codes full-window · %s rows · "
                        "%s named failures" % (
                            _bm.get("curve_zips"),
                            _bm.get("curve_mb") or 0,
                            _bm.get("iadb_codes_ok"),
                            _bm.get("iadb_rows"),
                            _bm.get("failures")))
            except Exception:
                pass
        if slug == "polygon":
            # ops 4976 (pf-note-v2): full-market warehouse truth
            try:
                _pm = _get_json("data/warm/polygon-full/"
                                "manifest.json")
                if _pm:
                    note = ((note + " · ") if note else "") + (
                        "FULL grouped-daily warehouse (polygon-"
                        "full v1): %s sessions since %s · %.2fGB "
                        "· ~%s tickers/day · phase %s · banked "
                        "dates persist as the entitled window "
                        "rolls" % (
                            _pm.get("sessions"),
                            (_pm.get("window_start") or "?")[:10],
                            _pm.get("gb") or 0,
                            _pm.get("tickers_last"),
                            _pm.get("phase")))
            except Exception:
                pass
        if slug == "imf":
            # ops 4967 (imf-note-v2): full SDMX-2.1 warehouse truth
            try:
                _im = _get_json("data/warm/imf-full/manifest.json")
                if _im:
                    note = ((note + " · ") if note else "") + (
                        "FULL SDMX-2.1 warehouse (imf-full v1) on "
                        "api.imf.org: %s/%s dataflows · %s "
                        "vintages retained · %.2fGB · %s lastN-"
                        "partial · phase %s · daily rediscovery" % (
                            _im.get("flows_banked"),
                            _im.get("flows_catalog"),
                            _im.get("vintages_banked"),
                            _im.get("gb") or 0,
                            _im.get("lastN_partial"),
                            _im.get("phase")))
            except Exception:
                pass
        if slug in ("dol", "dol-eta"):
            # ops 4966 (dol-note-v2): full ETA corpus truth
            try:
                _dm = _get_json("data/warm/dol-full/manifest.json")
                if _dm:
                    note = ((note + " · ") if note else "") + (
                        "FULL ETA DataDownloads corpus (dol-full "
                        "v1): %s report csvs · %.1fMB · %s fresh / "
                        "%s unchanged · self-extending harvest" % (
                            _dm.get("files"),
                            (_dm.get("bytes") or 0) / 1e6,
                            _dm.get("fresh"),
                            _dm.get("unchanged")))
            except Exception:
                pass
        if slug == "gdelt":
            # ops 4965 (gd-note-v2): full v2-events cursor truth
            try:
                _gm = _get_json("data/warm/gdelt-full/manifest.json")
                if _gm:
                    note = ((note + " · ") if note else "") + (
                        "FULL v2 EVENTS warehouse (gdelt-full v1): "
                        "%s files · %.2fGB · cursor %s · gaps %s · "
                        "phase %s · v1 archive %s/%s · GKG/mentions "
                        "scoped by design" % (
                            _gm.get("v2_files"),
                            _gm.get("v2_gb") or 0,
                            str(_gm.get("cursor"))[:8],
                            _gm.get("gaps"), _gm.get("phase"),
                            _gm.get("v1_files"),
                            _gm.get("v1_total")))
            except Exception:
                pass
        if slug == "nyfed":
            # ops 4965 (nyfed-hist-note-v2): surface hist-v1 depth
            try:
                _hs = _get_json(
                    "data/warm/nyfed-markets/hist-state.json")
                _hf = (_hs or {}).get("families") or {}
                _okn = sum(1 for v in _hf.values() if v.get("ok"))
                if _okn:
                    _eff = (_hf.get("rates_effr") or {}
                            ).get("n_hint") or 0
                    _rawmb = sum((v.get("bytes") or 0)
                                 for v in _hf.values()) / 1e6
                    note = ((note + " · ") if note else "") + (
                        "hist-v1 full-window: %d/11 families "
                        "(EFFR %s obs since 2000, repo ops since "
                        "2000, AMBS/tsy/fxs) · %.1fMB raw · "
                        "20h self-refresh" % (_okn, _eff, _rawmb))
            except Exception:
                pass
        if slug == "treasury":
            # ops 4964 (fd-note-v2): FiscalData card truth from the
            # full-warehouse manifest (Khalid priority lane #2)
            try:
                _fm = _get_json(
                    "data/warm/fiscaldata-full/manifest.json")
                if _fm:
                    note = ((note + " · ") if note else "") + (
                        "FULL FiscalData warehouse (fiscaldata-full "
                        "v1): %s endpoints · %s rows since "
                        "inception (auctions 1979-) · phase %s · "
                        "delta-checked refresh" % (
                            _fm.get("endpoints_banked"),
                            _fm.get("rows"), _fm.get("phase")))
            except Exception:
                pass
        if slug == "worldbank":
            # ops 4960 (wb-note-v2): card truth from the full-
            # warehouse manifest (drain-queue #2)
            try:
                _wm = _get_json(
                    "data/warm/worldbank-full/manifest.json")
                if _wm:
                    note = ((note + " · ") if note else "") + (
                        "FULL indicator warehouse (worldbank-full "
                        "v1): %s/%s indicators banked · %s no-data "
                        "· %.2fGB · phase %s · official CSV-zips "
                        "verbatim, weekly re-drain" % (
                            _wm.get("banked"),
                            _wm.get("indicators_catalog"),
                            _wm.get("no_data"),
                            _wm.get("gb") or 0, _wm.get("phase")))
            except Exception:
                pass
        if slug == "bls":
            # ops 4958 (bls-note-v2): the 0.11MB curated-agent card
            # gains the FULL warehouse truth from bls-full's manifest
            try:
                _bm = _get_json("data/warm/bls-full/manifest.json")
                if _bm:
                    note = ((note + " · ") if note else "") + (
                        "FULL time.series warehouse (bls-full v1): "
                        "%s surveys · %s files · %.2fGB · phase %s"
                        " · complete history since 1913, conditional"
                        " Last-Modified refresh" % (
                            _bm.get("surveys"), _bm.get("files"),
                            _bm.get("gb") or 0, _bm.get("phase")))
            except Exception:
                pass
        if slug == "census-us":
            # ops 4944: unit discipline (ops 4574) — n_live counts
            # KEYS under the prefix (state + manifests + year slices),
            # not datasets, so no keys-vs-datasets ratio. Progress
            # rides in the note from the walker's OWN state, in
            # dataset units. ops 4952 (census-note-v2): the v1.0-era
            # "EITS ... deferred tier-2" template fossilized after the
            # v1.1 full-universe expansion — compose from state truth:
            # excluded_families are DESIGN exclusions (intltrade lives
            # in import-canary; idb value-gated), not deferrals.
            try:
                _cw = _get_json(
                    "data/warm/census-us/_state/state.json")
                _ex = _cw.get("excluded_families") or {}
                _note_c = ("full timeseries universe since inception: "
                           "%s/%s datasets · %s families · %s rows · "
                           "phase %s" % (
                               _cw.get("n_done"), _cw.get("n_total"),
                               len(_cw.get("families") or []),
                               f"{_cw.get('rows_total') or 0:,}",
                               _cw.get("phase")))
                if _ex:
                    _note_c += (" · excluded by design: "
                                + ", ".join("%s %d" % kv for kv in
                                            sorted(_ex.items()))
                                + " (intltrade -> import-canary)")
                if _cw.get("failures"):
                    _note_c += (" · %d structurally-named source "
                                "failures" % len(_cw["failures"]))
                note = ((note + " · ") if note else "") + _note_c
            except Exception:
                pass
        _orphan_notes = {
            "ofr-bsrm": ("src-mirror since ops 4913 (workbooks "
                         "conditional-ETag, FULL) · bsrm-truth ops "
                         "4966: series/ = flagged duplicate of "
                         "ofr-hfm (4752 bug, 4753 note) -- no "
                         "transform owed, canonical hfm live"),
            "ofr-site": ("src-mirror daily since ops 4913 "
                         "(live page-harvest, seed ops 4755)"),
        }
        if slug in _orphan_notes:
            note = ((note + " · ") if note else "") + \
                _orphan_notes[slug]
        if slug == "nyfed-research":
            # ops 4953 (nyfed-note-v2): the ORPHANED_TRANSFORM fossil
            # is gone -- src-mirror lane 3 refreshes the haircut
            # workbooks + every 4757/4758 manifest source daily;
            # compose from the lane's own _last-check truth.
            try:
                _lc = _get_json(
                    "data/warm/nyfed-research/_last-check.json") or {}
                if _lc.get("engine") == "src-mirror":
                    _hc = _lc.get("haircuts") or {}
                    _note_n = ("src-mirror daily since ops 4953: "
                               "%s sources · %s fresh / %s unchanged"
                               " · haircut workbooks %s · parsed "
                               "haircuts-series re-transform = "
                               "phase 2 (seed ops 4793-94)" % (
                                   _lc.get("sources"),
                                   _lc.get("fresh"),
                                   _lc.get("unchanged"),
                                   "/".join(
                                       (v or {}).get("status", "?")
                                       for v in _hc.values())
                                   or "pending"))
                else:
                    _note_n = ("ORPHANED_TRANSFORM: haircut series "
                               "seeded ops 4793-94; src-mirror lane "
                               "deployed, first run pending")
                note = ((note + " · ") if note else "") + _note_n
            except Exception:
                pass
        if slug == "ecb":
            # ops 4893 (Khalid): surface the ciss-stress engine's live
            # holdings on this card — it is the proof-of-access engine
            # whose format=csvdata pattern (no content negotiation)
            # unblocked the catalog + walker. Fail-soft: note only.
            try:
                _cs = _get_json("data/ciss-stress.json")
                _ncs = _cs.get("n_series")
                if _ncs:
                    _note_c = ("ciss-stress engine: %d CISS/CLIFS/"
                               "SovCISS stress series live via "
                               "format=csvdata — the access pattern "
                               "ops 4893 ported to the catalog builder"
                               % _ncs)
                    note = (note + " · " + _note_c) if note \
                        else _note_c
            except Exception:
                pass
            try:
                _cv = _get_json("data/warm/ecb/coverage.json")
                _note_v = ("walk+deep coverage: %d fast + %d "
                           "deep-sliced flows · %d/%d deep complete"
                           % (_cv.get("n_fast") or 0,
                              _cv.get("n_deep") or 0,
                              _cv.get("n_deep_complete") or 0,
                              _cv.get("n_deep") or 0))
                note = (note + " · " + _note_v) if note else _note_v
            except Exception:
                pass
        hub["providers"].append(
            {"slug": slug, "name": r["name"], "api": r["api"],
             "datasets": ds, "datasets_target": tgt,
             "coverage_pct": cov, "coverage_note": cov_note,
             "coverage_basis": (cov_basis if cov is not None else None),
             "denied_source_side": denied,
             "unit": "keys",
             "n_keys": doc["n_keys"],
             "total_bytes": doc["total_bytes"],
             "total_mb": doc["total_mb"],
             "hot_feeds": n_roll,
             "series_count": (ser or {}).get("count"),
             "catalog_note": note,
             "freshest_h": doc["freshest_h"]})
    hub["providers"].sort(key=lambda p: -(p["total_mb"] or 0))
    hub.setdefault("breakdown", {})["keys"] = sum(
        p["n_keys"] for p in hub["providers"])
    # ops 4534: Khalid — "a LOT more datasets": count at the SERIES level.
    # keys = S3 objects (containers); datasets = the series/symbols/flows
    # the platform actually tracks inside them.
    series_sum = sum((p.get("series_count") or 0)
                     for p in hub["providers"])
    hub["breakdown"]["datasets"] = series_sum
    extras = {}
    _xstat = {}  # ops 4568: extras were shipping 0 keys/0 MB/no
    # freshness — the page card rendered nothing but zeros while the
    # real instrument counts sat unrendered in `datasets`.
    _indicator_search_entities = []
    try:  # canonical indicator bus (18k+ indicators)
        ib = _get_json("data/indicator-bus.json")
        n_ib = (ib.get("n") or ib.get("count") or
                len(ib.get("indicators") or ib.get("items") or []))
        _indicator_search_entities = _indicator_search_rows(ib)
        if n_ib:
            extras["indicator_bus"] = n_ib
            _sa = hotidx.get("data/indicator-bus.json")
            if _sa:
                _xstat["indicator_bus"] = {
                    "n_keys": 1, "mb": round(_sa[0] / 1e6, 2),
                    "bytes": _sa[0],
                    "freshest_h": _sa[1]}
    except Exception:
        pass
    _equity_search_keys = []
    try:  # per-ticker equity research universe (count objects)
        n_eq, tok2, _teq, _feq = 0, None, 0, None
        while True:
            kw2 = {"Bucket": BUCKET, "Prefix": "equity-research/",
                   "MaxKeys": 1000}
            if tok2:
                kw2["ContinuationToken"] = tok2
            r2 = s3.list_objects_v2(**kw2)
            for _o2 in r2.get("Contents", []):
                n_eq += 1
                _teq += _o2["Size"]
                _a2 = round((now - _o2["LastModified"])
                            .total_seconds() / 3600, 1)
                _equity_search_keys.append({
                    "key": _o2["Key"], "bytes": _o2["Size"],
                    "age_h": _a2, "hot": _a2 <= 36})
                if _feq is None or _a2 < _feq:
                    _feq = _a2
            if not r2.get("IsTruncated"):
                break
            tok2 = r2.get("NextContinuationToken")
        if n_eq:
            extras["equity_research_tickers"] = n_eq
            _xstat["equity_research_tickers"] = {
                "n_keys": n_eq, "mb": round(_teq / 1e6, 2),
                "bytes": _teq,
                "freshest_h": _feq}
    except Exception:
        pass
    _tv_search_entities = []
    try:  # TradingView vault LIVE symbols
        tv = _get_json("data/tradingview.json")
        _tv_items = tv.get("symbols") or []
        n_tv = (tv.get("n_live") or tv.get("live") or
                len([s for s in (_tv_items.values()
                                 if isinstance(_tv_items, dict)
                                 else _tv_items)
                     if isinstance(s, dict)
                     and s.get("status") == "LIVE"]))
        if isinstance(n_tv, int) and n_tv:
            _tv_search_entities = _tradingview_live_search_rows(tv)
            extras["tradingview_vault_live"] = n_tv
            _st2 = hotidx.get("data/tradingview.json")
            if _st2:
                _xstat["tradingview_vault_live"] = {
                    "n_keys": 1, "mb": round(_st2[0] / 1e6, 2),
                    "bytes": _st2[0],
                    "freshest_h": _st2[1]}
    except Exception:
        pass
    # ops 4537 (Perplexity item 6): instruments become first-class
    # attributed provider rows — nothing floats unattributed.
    _extra_meta = {
        "indicator_bus": ("Canonical Indicator Bus",
                          "internal — justhodl indicator registry"),
        "equity_research_tickers": ("Equity Research Universe",
                                    "internal — per-ticker reports"),
        "tradingview_vault_live": ("TradingView Vault (LIVE)",
                                   "tradingview.com via extension")}
    for _ek, _ev in extras.items():
        _nm, _api = _extra_meta.get(_ek, (_ek, "internal"))
        _slug = _ek.replace("_", "-")
        _xs = _xstat.get(_ek, {})
        hub["providers"].append(
            {"slug": _slug, "name": _nm, "api": _api,
             "datasets": _ev, "unit": "instruments",
             "n_keys": _xs.get("n_keys", 0),
             "total_bytes": _xs.get("bytes", 0),
             "total_mb": _xs.get("mb", 0), "hot_feeds": 0,
             "series_count": None,
             "freshest_h": _xs.get("freshest_h")})
        _extra_keys = {
            "indicator_bus": [{"key": "data/indicator-bus.json"}],
            "equity_research_tickers": _equity_search_keys,
            "tradingview_vault_live": [{"key": "data/tradingview.json"}],
        }.get(_ek, [])
        _extra_entities = {
            "indicator_bus": _indicator_search_entities,
            "tradingview_vault_live": _tv_search_entities,
        }.get(_ek, [])
        search_shards.append(_write_search_shard(
            _slug, _nm, _api, _extra_keys, None, search_db,
            entities=_extra_entities))
    hub["series_extras"] = extras
    # ops 4535 (Perplexity P1): auditable reconcile —
    # sum(provider.datasets) + instruments == datasets_total, and units
    # are split so series never mix silently with instrument counts.
    hub["datasets_total"] = sum(p.get("datasets") or 0
                                for p in hub["providers"])
    hub["reconcile_ok"] = True  # by construction: total = sum(rows)
    # ops 4548b: every normal provider is unit=="keys" now (by
    # design, per Perplexity's actual/target split) — the old
    # "series" bucket was permanently 0 and confusing on the page.
    # Two honest buckets only.
    hub["breakdown"] = {
        "provider_datasets": sum(p["datasets"]
                                 for p in hub["providers"]
                                 if p.get("unit") == "keys"),
        "instruments": sum(p["datasets"] for p in hub["providers"]
                           if p.get("unit") == "instruments")}
    total_storage_bytes = sum(
        int(p.get("total_bytes") or
            round((p.get("total_mb") or 0) * 1000 * 1000))
        for p in hub["providers"])
    hub["totals"] = {"providers": len(hub["providers"]),
                     "datasets": hub["datasets_total"],
                     "keys": sum(p["n_keys"]
                                 for p in hub["providers"]),
                     "bytes": total_storage_bytes,
                     "gb": round(total_storage_bytes / 1e9, 2)}
    search_db.commit()
    search_db.close()
    search_db_bytes = os.path.getsize(search_db_path)
    if search_db_bytes > 1500 * 1024 * 1024:
        raise RuntimeError(
            "provider search index exceeds safe 1.5 GB Lambda /tmp budget")
    with open(search_db_path, "rb") as src, gzip.open(search_db_gz, "wb", 6) as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    digest = hashlib.sha256()
    with open(search_db_gz, "rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    search_db_gz_bytes = os.path.getsize(search_db_gz)
    generation = now.strftime("%Y%m%dT%H%M%SZ")
    checksum = digest.hexdigest()
    search_index_key = (
        f"data/search/index/provider-search-{generation}-{checksum[:12]}.sqlite.gz")
    s3.upload_file(
        search_db_gz, BUCKET, search_index_key,
        ExtraArgs={"ContentType": "application/vnd.sqlite3",
                   "CacheControl": "no-cache"})
    search_manifest = {
        "schema_version": SEARCH_SCHEMA_VERSION,
        "generated_at": hub["as_of"],
        "providers": len(search_shards),
        "documents": sum(x["count"] for x in search_shards),
        "coverage": {
            "catalog_datasets": hub.get("datasets_total") or 0,
            "storage_objects": (hub.get("totals") or {}).get("keys") or 0,
            "storage_bytes": total_storage_bytes,
            "indexed_assets": sum(x.get("assets") or 0
                                  for x in search_shards),
            "indexed_entity_refs": sum(x.get("entity_refs") or 0
                                       for x in search_shards),
            "indexed_series_refs": sum(x.get("series_refs") or 0
                                       for x in search_shards),
            "hierarchical_series": {
                **hierarchical_series,
                "access": "tier1_prefix",
            },
        },
        "index": {"key": search_index_key,
                  "bytes": search_db_gz_bytes,
                  "uncompressed_bytes": search_db_bytes,
                  "sha256": checksum,
                  "format": "sqlite-fts5+gzip"},
        "shards": search_shards,
    }
    s3.put_object(Bucket=BUCKET, Key="data/search/provider-shards.json",
                  Body=json.dumps(search_manifest, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    hub["search"] = {
        "schema_version": SEARCH_SCHEMA_VERSION,
        "providers": search_manifest["providers"],
        "documents": search_manifest["documents"],
        "manifest": "data/search/provider-shards.json",
        "coverage": search_manifest["coverage"],
    }
    s3.put_object(Bucket=BUCKET, Key="data/provider-catalog.json",
                  Body=json.dumps(hub, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    res = {"ok": True, **hub["totals"]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
