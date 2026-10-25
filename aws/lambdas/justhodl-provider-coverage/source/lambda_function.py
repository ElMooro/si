"""
justhodl-provider-coverage v1.0.0 -- the provider-coverage scoreboard: data/provider-coverage.json (+ dated copies).

Why (Khalid, 2026-10-25: "I want 100% of the data pulled and used by my system"): "100%" has to be a measured number.
Every bank engine (justhodl-fmp-bank, justhodl-fmp-walker, justhodl-polygon-bank, justhodl-provider-bank) publishes a
manifest with the endpoints its key SERVED, the ones the plan REFUSED (by http/api code), the ones deliberately SKIPPED
(derived or duplicated data, with the reason) and what is banked.  This engine folds those manifests, the data.html
provider catalog (objects/bytes/freshness per provider card) and the engine manifest (who reads the bank prefixes) into
one document per day, so the question "are we pulling everything we pay for?" has an answer with a timestamp.

Definitions (printed in the document so nobody has to guess)
  served    = endpoint/dataset answered 200 with rows at least once
  refused   = last verdict is a plan/key refusal (401/402/403/426, provider error envelope) - a FACT about the plan
  untested  = declared in a spec, never attempted yet (backfill order) or no key present
  skipped   = deliberately not pulled, with the reason (derived from banked data, duplicate of another lane, search-only)
  coverage  = served / (served + refused + untested)  -- refused counts against coverage on purpose: it is data the
              vendor sells that this key does not reach; the fix is a plan decision, not code
  used_by   = engines whose source reads a bank prefix (engine-manifest readers) + the shared clients (fmp_bank/fmp_book)

Schedule: EventBridge Scheduler justhodl-provider-coverage-daily cron(47 6 * * ? *) UTC (after provider-catalog 06:20).
Reads only S3 (no provider calls, no key).  ~5 s x 512 MB.
"""
from __future__ import annotations

import gzip
import json
import os
import time
from datetime import datetime, timezone

import boto3

ENGINE = "justhodl-provider-coverage"
VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT = "data/provider-coverage.json"
HISTORY = "data/history/provider-coverage/"
BANKS = {
    "fmp-bank": {"provider": "fmp", "manifest": "data/warm/fmp-bank/manifest.json", "prefix": "data/warm/fmp-bank/", "engine": "justhodl-fmp-bank", "scope": "bulk + EOD (whole market)"},
    "fmp-walker": {"provider": "fmp", "manifest": "data/warm/fmp/manifest.json", "prefix": "data/warm/fmp/", "engine": "justhodl-fmp-walker", "scope": "non-bulk families (per symbol / 13F / insiders / Senate-House / transcripts / estimates / ETF / economics / calendars / news / 1-min bars)"},
    "polygon-bank": {"provider": "polygon", "manifest": "data/warm/polygon-bank/manifest.json", "prefix": "data/warm/polygon-bank/", "engine": "justhodl-polygon-bank", "scope": "REST families beyond the tape"},
    "provider-bank": {"provider": None, "manifest": "data/warm/provider-bank/manifest.json", "prefix": "data/warm/provider-bank/", "engine": "justhodl-provider-bank", "scope": "Trading Economics / CoinMarketCap / NewsAPI / Nasdaq Data Link / CryptoQuant splits"},
    "eia-bank": {"provider": "eia", "manifest": "data/warm/eia-bank/manifest.json", "prefix": "data/warm/eia-bank/", "engine": "justhodl-eia-bank", "scope": "EIA v2 full catalog (every leaf route, full history + daily newest rows)"},
}
# the lanes that existed before the banks (so the card can say the whole picture, not only the new engines)
LEGACY = {
    "fmp": ["justhodl-fmp-ratios (700-name TTM harvest -> data/fmp-ratios.json)", "233 engines calling /stable live (audit 2026-10-23)"],
    "polygon": ["justhodl-polygon-full (grouped daily, whole US market since the plan's floor)", "justhodl-tv-bars polygon-append (chart bar bank)", "options desks (SPY/QQQ/IWM/HYG chains + scanner universe)", "justhodl-polygon-futures-curves", "ETF Global desks (116 + 300 funds)", "justhodl-polygon-fx-regime"],
    "te": ["justhodl-te-feed (latest values, 209 countries)", "justhodl-te-fred-mirror (225 series)"],
    "cmc": ["justhodl-crypto-opportunities, crypto-narratives, altseason, crypto-etf-arb, crypto-intel (snapshots)"],
    "newsapi": ["justhodl-news-wire, buzz-velocity, forward-orders, asia-leads, financial-secretary"],
    "ndl": ["nasdaq-datalink-agent (Function URL, 24 datasets)"],
    "cq": ["justhodl-cryptoquant (410 metrics daily), justhodl-cq-feed (189 paths latest)"],
    "eia": ["justhodl-real-economy-collector, eia-energy-agent, justhodl-freight-pulse (17 series), justhodl-grid-queue (triggers disabled 2026-10-20)"],
}
PLAN_HINTS = {
    "fmp": "FMP Ultimate (paid): 3,000 calls/min, bulk + batch, 1-minute intraday, transcripts, 13F, full history",
    "polygon": "Massive/Polygon: Stocks (15-min delayed, 5-year history), Options, Futures Basic, ETF Global; trades/quotes and Benzinga NOT entitled (ops 6641 probe)",
    "te": "Trading Economics: plan not in the repo - every dataset is a recorded probe",
    "cmc": "CoinMarketCap: plan not in the repo - credits capped at 110/day by the bank",
    "newsapi": "NewsAPI: plan not in the repo - capped at 60 requests/day by the bank",
    "ndl": "Nasdaq Data Link: entitled databases are read from /api/v3/databases daily",
    "cq": "CryptoQuant Professional: 1-year window per series; per-exchange / per-miner / hourly are recorded probes",
    "eia": "EIA v2 (free key, 5,000 requests/hour): every leaf route is banked; refusals are recorded per leaf",
}

s3 = boto3.client("s3", region_name="us-east-1")


def _j(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return default


def _age_h(iso):
    try:
        t = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return round((datetime.now(timezone.utc) - t).total_seconds() / 3600, 1)
    except Exception:  # noqa: BLE001
        return None


def _readers(engine_manifest, prefix):
    """Engines whose declared reads touch a bank prefix (engine-manifest.json v3 shape, tolerant to variants)."""
    out = set()
    engines = engine_manifest.get("engines") if isinstance(engine_manifest, dict) else None
    items = engines.items() if isinstance(engines, dict) else [(e.get("name") or e.get("engine"), e) for e in (engines or []) if isinstance(e, dict)]
    for name, e in items:
        reads = []
        for k in ("reads", "inputs", "read_keys", "reads_prefixes"):
            v = e.get(k) if isinstance(e, dict) else None
            if isinstance(v, list):
                reads.extend(str(x if isinstance(x, str) else (x.get("key") or x.get("prefix") or "")) for x in v)
        if any(r.startswith(prefix) or r.startswith(prefix.rstrip("/")) for r in reads):
            out.add(name)
    return sorted(out)


def fold_bank(bank_id, cfg, man):
    if not man:
        return {"bank": bank_id, "engine": cfg["engine"], "scope": cfg["scope"], "present": False}
    per = man.get("per_dataset") or {}
    served = man.get("endpoints_served") or []
    refused = man.get("endpoints_refused") or man.get("endpoints_not_served") or []
    declared = man.get("datasets_declared") or len(per) or (len(served) + len(refused))
    untested = max(0, int(declared) - len(served) - len(refused)) if declared else 0
    denom = len(served) + len(refused) + untested
    rotation = {k: v.get("rotation_days") for k, v in per.items() if isinstance(v, dict) and v.get("rotation_days")}
    return {
        "bank": bank_id, "engine": cfg["engine"], "scope": cfg["scope"], "present": True,
        "version": man.get("version"), "phase": man.get("phase"), "key_status": man.get("key_status"),
        "updated_at": man.get("updated_at"), "age_h": _age_h(man.get("updated_at")),
        "objects": man.get("objects"), "bytes": man.get("bytes"), "gb": round((man.get("bytes") or 0) / 1e9, 3),
        "datasets_banked": man.get("datasets"), "datasets_declared": declared,
        "served": served, "refused": refused, "untested": untested,
        "coverage": round(len(served) / denom, 3) if denom else None,
        "skipped": man.get("skipped") or [], "universe_counts": man.get("universe_counts"),
        "rotation_days": rotation, "by_provider": man.get("by_provider"),
        "last_run": {k: (man.get("run") or {}).get(k) for k in ("calls", "retries", "puts", "unchanged", "seconds", "http", "providers") if (man.get("run") or {}).get(k) is not None},
    }


def lambda_handler(event, context=None):
    t0 = time.time()
    catalog = _j("data/provider-catalog.json") or {}
    engine_manifest = _j("engine-manifest.json") or _j("data/engine-manifest.json") or {}
    cards = {}
    provs = catalog.get("providers") if isinstance(catalog, dict) else catalog
    for p in (provs.values() if isinstance(provs, dict) else (provs or [])):
        if isinstance(p, dict) and (p.get("slug") or p.get("id")):
            cards[p.get("slug") or p.get("id")] = {k: p.get(k) for k in ("name", "datasets", "keys", "bytes", "mb", "freshest_h", "hot", "status", "engines") if k in p}
    banks = {}
    for bid, cfg in BANKS.items():
        man = _j(cfg["manifest"])
        b = fold_bank(bid, cfg, man)
        b["used_by"] = _readers(engine_manifest, cfg["prefix"]) + (["aws/shared/fmp_bank.py -> fmp_book.try_harvest (fmp_stable / equity_enrich clients)"] if cfg["provider"] == "fmp" else [])
        banks[bid] = b
    providers = {}
    pb = banks.get("provider-bank") or {}
    for prov in ("fmp", "polygon", "te", "cmc", "newsapi", "ndl", "cq", "eia"):
        if prov in ("fmp", "polygon", "eia"):
            mine = [b for b in banks.values() if BANKS[b["bank"]]["provider"] == prov]
            served = sorted({e if isinstance(e, str) else e[0] for b in mine for e in (b.get("served") or [])})
            refused = [list(e) if isinstance(e, (list, tuple)) else [e, None] for b in mine for e in (b.get("refused") or [])]
            untested = sum(int(b.get("untested") or 0) for b in mine)
            skipped = [x for b in mine for x in (b.get("skipped") or [])]
            objects = sum(int(b.get("objects") or 0) for b in mine)
            nbytes = sum(int(b.get("bytes") or 0) for b in mine)
            phase = "STEADY" if mine and all(b.get("phase") == "STEADY" for b in mine) else ("BACKFILL" if mine else None)
        else:
            bp = ((pb.get("by_provider") or {}).get(prov) or {}) if pb.get("present") else {}
            served = bp.get("served") or []
            refused = [list(e) for e in (bp.get("refused") or [])]
            untested = len(bp.get("untested") or [])
            skipped = [x for x in (pb.get("skipped") or []) if x.get("provider") == prov]
            objects, nbytes = bp.get("objects") or 0, bp.get("bytes") or 0
            phase = pb.get("phase")
        denom = len(served) + len(refused) + untested
        providers[prov] = {
            "plan": PLAN_HINTS.get(prov), "banks": [b["bank"] for b in banks.values() if BANKS[b["bank"]]["provider"] == prov] or (["provider-bank"] if prov in ("te", "cmc", "newsapi", "ndl", "cq") else []),
            "legacy_lanes": LEGACY.get(prov, []), "served": served, "served_n": len(served), "refused": refused, "untested": untested,
            "coverage": round(len(served) / denom, 3) if denom else None, "phase": phase,
            "objects_banked": objects, "gb_banked": round(nbytes / 1e9, 3), "skipped": skipped,
            "catalog_cards": {k: v for k, v in cards.items() if k.startswith(prov) or (prov == "cq" and k.startswith("cryptoquant")) or (prov == "polygon" and k.startswith("polygon")) or (prov == "te" and k.startswith("te-")) or (prov == "ndl" and k.startswith("nasdaq")) or (prov == "newsapi" and k == "newsapi") or (prov == "eia" and k.startswith("eia"))},
        }
    doc = {
        "engine": ENGINE, "version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(), "schema_version": "provider-coverage.v1",
        "definitions": {"served": "answered 200 with rows at least once", "refused": "last verdict is a plan/key refusal (401/402/403/426 or provider error envelope) - a fact about the plan",
                        "untested": "declared, not yet attempted (backfill order) or no key", "skipped": "deliberately not pulled, reason given (derived, duplicate, search-only)",
                        "coverage": "served / (served + refused + untested); refused counts against coverage: data the vendor sells that this key does not reach"},
        "providers": providers, "banks": banks,
        "totals": {"objects_banked": sum(int(b.get("objects") or 0) for b in banks.values()), "gb_banked": round(sum(int(b.get("bytes") or 0) for b in banks.values()) / 1e9, 3),
                   "served": sum(p["served_n"] for p in providers.values()), "refused": sum(len(p["refused"]) for p in providers.values()), "untested": sum(p["untested"] for p in providers.values())},
        "open_decisions": [
            "Polygon trades/quotes (last/trade, v3/quotes) and Benzinga need higher Massive tiers / the add-on - not entitled on this key (ops 6641)",
            "Quiver: no key in SSM (/justhodl/quiver/api-key absent) - lobbying-intel serves a stale cache daily",
            "Trading Economics / CoinMarketCap / NewsAPI / Nasdaq Data Link plans: read the refused lists above; each refusal is a plan line, not a bug",
        ],
        "seconds": round(time.time() - t0, 1),
    }
    body = json.dumps(doc, separators=(",", ":"), default=str).encode()
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=body, ContentType="application/json", CacheControl="public, max-age=300")
    s3.put_object(Bucket=BUCKET, Key=HISTORY + datetime.now(timezone.utc).strftime("%Y-%m-%d") + ".json", Body=body, ContentType="application/json", CacheControl="public, max-age=86400")
    return {"engine": ENGINE, "version": VERSION, "providers": {k: {"coverage": v["coverage"], "served": v["served_n"], "refused": len(v["refused"]), "untested": v["untested"], "gb": v["gb_banked"]} for k, v in providers.items()},
            "totals": doc["totals"], "seconds": doc["seconds"]}
