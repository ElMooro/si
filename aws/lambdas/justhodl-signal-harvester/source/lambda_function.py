"""
justhodl-signal-harvester — UNIVERSAL TRUTH-LAYER COVERAGE
==========================================================
The accountability stack (outcome-checker -> signal-scorecard -> confluence-meta
fade-index -> conviction-engine) is sound, but only ~47 of 577 engines hand-log
into justhodl-signals. The other ~530 publish ranked picks that NEVER enter the
ledger, so every edge/fade/confluence number is computed on ~8% of the fleet.

This engine closes that gap WITHOUT touching 530 codebases: it reads what every
engine already publishes (data/*.json), extracts each engine's top ranked picks,
and writes POINT-IN-TIME records into the SAME justhodl-signals table the existing
pipeline grades. Result: outcome-checker scores them, signal-scorecard computes
per-engine hit-rate / Wilson-LB / regime edge across the WHOLE fleet, and
confluence/fade/conviction finally see all 577 engines.

Discipline baked in (so it tells the truth, not flatters):
  • POINT-IN-TIME: baseline_price snapshotted at harvest; outcome-checker grades
    forward return from there. Price unavailable -> SKIP (never persist None).
  • DE-DUPLICATION: a given (engine, symbol) is logged at most once per 6 days,
    so the same standing pick doesn't autocorrelate into fake statistical power.
  • signal_type = "eng:<engine>" so the scorecard buckets edge PER ENGINE.
  • Broad harvest, let the truth layer sort it out: engines whose picks don't beat
    coinflip will surface as low hit-rate and get faded — which is the whole point.

OUTPUT  writes to DDB justhodl-signals (+ data/_harvest/last-run.json summary)
SCHEDULE daily 23:15 UTC (after the daily engines have refreshed). Real data only.
"""
import json
import re
import time
import uuid
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
SIGNALS_TABLE = "justhodl-signals"
SEEN_KEY = "data/_harvest/seen.json"
SUMMARY_KEY = "data/_harvest/last-run.json"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
POLYGON = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

TOP_PER_ENGINE = 8
DEDUP_DAYS = 6
WINDOWS = [7, 14, 30]
MAX_SIGNALS = 900

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")

TICKER_RE = re.compile(r"^[A-Z][A-Z.\-]{0,6}$")

# ranked-list keys to look for, best-first (summary.* then top-level)
LIST_KEYS = [
    "ai_megadeals", "contagion_candidates", "top_picks", "top_setups", "top_small_cap_picks",
    "top_smallcap_picks", "top_smallcap_deals", "green_highlights", "deepest_discounts",
    "rising_and_cheap", "squeeze_candidates", "top_candidates", "all_qualifying", "candidates",
    "top_setups", "setups", "picks", "top_25_by_score", "top_consensus_25", "ai_deals",
    "top_smart_money_only", "best_setups", "top_ideas", "qualifying", "names", "deals",
]
SYM_KEYS = ("symbol", "ticker", "t", "sym")
SCORE_KEYS = ("score", "composite", "squeeze_score", "rotation_score", "underlooked",
              "momentum_score", "conviction", "rank_score", "attractiveness", "unpriced_z")
# infra / non-opportunity outputs to skip
SKIP_SUBSTR = ("universe", "manifest", "schedule", "history", "snapshot", "state", "ledger",
               "calendar", "heatmap", "directory", "engine", "skill", "scorecard", "config",
               "regime", "macro", "plumbing", "eurodollar", "settlement", "stress", "hkma",
               "brain", "digest", "uptime", "health", "last-run", "seen")


def f2d(o):
    if isinstance(o, float):
        return Decimal(str(round(o, 6)))
    if isinstance(o, dict):
        return {k: f2d(v) for k, v in o.items()}
    if isinstance(o, list):
        return [f2d(v) for v in o]
    return o


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def list_outputs():
    keys = []
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="data/"):
        for o in pg.get("Contents", []):
            k = o["Key"]
            if not k.endswith(".json") or "/" in k[len("data/"):]:
                continue
            low = k.lower()
            if any(sub in low for sub in SKIP_SUBSTR):
                continue
            keys.append(k)
    return keys


def extract_picks(doc):
    """Find the engine's primary ranked pick list -> [(symbol, score_or_None), ...]."""
    pools = []
    if isinstance(doc, dict):
        summ = doc.get("summary") if isinstance(doc.get("summary"), dict) else {}
        for src in (summ, doc):
            for lk in LIST_KEYS:
                v = src.get(lk)
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    pools.append(v)
            # also catch any list-of-dicts-with-ticker not in LIST_KEYS (longest wins, fallback)
        if not pools:
            cand = []
            for src in (summ, doc):
                for v in src.values():
                    if isinstance(v, list) and v and isinstance(v[0], dict) \
                            and any(sk in v[0] for sk in SYM_KEYS):
                        cand.append(v)
            if cand:
                pools.append(max(cand, key=len))
    if not pools:
        return []
    picks, seen = [], set()
    for pool in pools:
        for it in pool:
            if not isinstance(it, dict):
                continue
            sym = None
            for sk in SYM_KEYS:
                if it.get(sk):
                    sym = str(it[sk]).strip().upper()
                    break
            if not sym or not TICKER_RE.match(sym) or sym in seen:
                continue
            sc = None
            for ck in SCORE_KEYS:
                if isinstance(it.get(ck), (int, float)):
                    sc = float(it[ck])
                    break
            seen.add(sym)
            picks.append((sym, sc))
            if len(picks) >= TOP_PER_ENGINE:
                return picks
    return picks


def get_price(sym):
    try:
        u = f"https://financialmodelingprep.com/stable/quote?symbol={urllib.parse.quote(sym)}&apikey={FMP}"
        d = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "jh-harv"}), timeout=10).read())
        if isinstance(d, list) and d and d[0].get("price"):
            return float(d[0]["price"])
    except Exception:
        pass
    try:
        u = f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(sym)}/prev?adjusted=true&apiKey={POLYGON}"
        d = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "jh-harv"}), timeout=10).read())
        r = (d or {}).get("results") or []
        if r and r[0].get("c"):
            return float(r[0]["c"])
    except Exception:
        pass
    return None


def current_regime():
    for key in ("data/khalid-index.json", "data/regime-read.json", "data/macro-nowcast.json"):
        d = _read(key)
        if isinstance(d, dict):
            r = d.get("regime") or (d.get("khalid_index", {}) or {}).get("regime") \
                or d.get("regime_label") or (d.get("macro_context", {}) or {}).get("regime_label")
            if r:
                return str(r)
    return None


def conf_from_score(sc):
    if sc is None:
        return 0.6
    # map a 0..100-ish score into 0.5..0.92 confidence band
    return round(max(0.5, min(0.92, 0.5 + abs(sc) / 230.0)), 3)


def lambda_handler(event, context):
    t0 = time.time()
    keys = list_outputs()
    regime = current_regime()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    # de-dup store
    seen = _read(SEEN_KEY) or {}
    cutoff = (now - timedelta(days=DEDUP_DAYS)).strftime("%Y-%m-%d")
    prune_cut = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    seen = {k: v for k, v in seen.items() if v >= prune_cut}  # prune old

    # harvest picks per engine
    harvested = []   # (engine, sym, score)
    engines_hit = 0
    for k in keys:
        doc = _read(k)
        if doc is None:
            continue
        picks = extract_picks(doc)
        if not picks:
            continue
        engine = k[len("data/"):-len(".json")]
        got = 0
        for sym, sc in picks:
            dk = f"eng:{engine}|{sym}"
            if seen.get(dk, "0000") >= cutoff:   # logged within DEDUP_DAYS
                continue
            harvested.append((engine, sym, sc))
            got += 1
        if got:
            engines_hit += 1
        if len(harvested) >= MAX_SIGNALS:
            break

    # price snapshot for unique symbols (point-in-time entry)
    usyms = sorted({h[1] for h in harvested})
    prices = {}
    with ThreadPoolExecutor(max_workers=24) as ex:
        fut = {ex.submit(get_price, s): s for s in usyms}
        for f in as_completed(fut):
            p = f.result()
            if p:
                prices[fut[f]] = p

    table = ddb.Table(SIGNALS_TABLE)
    written = 0
    ts = {f"day_{d}": (now + timedelta(days=d)).isoformat() for d in WINDOWS}
    with table.batch_writer() as bw:
        for engine, sym, sc in harvested:
            price = prices.get(sym)
            if not price:
                continue  # skip rather than poison the ledger with baseline_price=None
            sid = str(uuid.uuid4())
            item = {
                "signal_id": sid, "signal_type": f"eng:{engine}", "signal_value": str(round(sc, 3)) if sc is not None else "PICK",
                "predicted_direction": "UP", "confidence": f2d(conf_from_score(sc)),
                "measure_against": sym, "baseline_price": f2d(price),
                "baseline_benchmark_price": None, "benchmark": None,
                "check_windows": [str(d) for d in WINDOWS], "check_timestamps": ts,
                "outcomes": {}, "accuracy_scores": {}, "logged_at": now.isoformat(),
                "logged_epoch": int(now.timestamp()), "status": "pending",
                "metadata": f2d({"engine": engine, "raw_score": sc, "harvested": True}),
                "ttl": int((now + timedelta(days=365)).timestamp()), "schema_version": "2",
                "predicted_magnitude_pct": None, "predicted_target_price": None,
                "horizon_days_primary": max(WINDOWS), "regime_at_log": regime,
                "khalid_score_at_log": None,
                "rationale": f"harvested top pick from {engine}", "supporting_signals": None,
            }
            bw.put_item(Item=item)
            seen[f"eng:{engine}|{sym}"] = today
            written += 1

    s3.put_object(Bucket=S3_BUCKET, Key=SEEN_KEY, Body=json.dumps(seen).encode(),
                  ContentType="application/json")
    summary = {
        "engine": "signal-harvester", "version": VERSION, "generated_at": now.isoformat(),
        "regime_at_log": regime, "n_engine_outputs_scanned": len(keys),
        "n_engines_with_picks": engines_hit, "n_harvested": len(harvested),
        "n_written": written, "n_skipped_no_price": len(harvested) - written,
        "dedup_days": DEDUP_DAYS, "top_per_engine": TOP_PER_ENGINE,
        "note": "Point-in-time picks written to justhodl-signals; outcome-checker grades them, "
                "signal-scorecard computes per-engine edge across the full fleet.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=SUMMARY_KEY, Body=json.dumps(summary).encode(),
                  ContentType="application/json")
    print(f"[harvester] scanned={len(keys)} engines_with_picks={engines_hit} "
          f"harvested={len(harvested)} written={written} regime={regime} {summary['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "engines": engines_hit,
            "written": written, "scanned": len(keys)})}
