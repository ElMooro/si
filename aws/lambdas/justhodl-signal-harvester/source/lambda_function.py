"""Harvest explicit forecasts and ranked observations with separate instrument identities. A ranking is not a directional call. Current quote context is not a verified execution mark; the outcome lineage and prospective evaluation pipeline determine scoring eligibility."""
import json
import re
import time
import uuid
import math
import hashlib
from pathlib import Path
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

import sys
import os
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
from instrument_identity import resolve_instrument
from private_artifact import public_source_allowed
from prospective_journal import projection, ensure_protocol, register, persist_once, digest, PREFIX as JOURNAL_PREFIX
from calls_research_replay import publish_current
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from engine_trust import trust as _trust   # auto-demotion gate (consumer side)
except Exception:
    def _trust(_st, default=1.0):
        return default

VERSION = "1.3.0"
S3_BUCKET = "justhodl-dashboard-live"
SIGNALS_TABLE = "justhodl-signals"
SEEN_KEY = "data/_harvest/seen.json"
SUMMARY_KEY = "data/_harvest/last-run.json"
FMP = managed_secret(('FMP', 'FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
POLYGON = managed_secret(('POLYGON', 'POLYGON_API_KEY', 'POLYGON_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",))

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
    "top_pick_cascade",
]
SYM_KEYS = ("symbol", "ticker", "t", "sym")
SCORE_KEYS = ("score", "composite", "squeeze_score", "rotation_score", "pump_probability", "underlooked",
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
    # ops 5111: Delimiter="/" -- the undelimited walk over data/ (9.7M warehouse objects) timed out every run
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=S3_BUCKET, Prefix="data/", Delimiter="/"):
        for o in pg.get("Contents", []):
            k = o["Key"]
            if not k.endswith(".json") or "/" in k[len("data/"):] or not public_source_allowed(k):
                continue
            low = k.lower()
            if any(sub in low for sub in SKIP_SUBSTR):
                continue
            keys.append(k)
    return keys


def read_research_source(key):
    if not public_source_allowed(key): raise ValueError('private research source rejected')
    raw = s3.get_object(Bucket=S3_BUCKET, Key=key)['Body'].read(8_000_001)
    if len(raw)>8_000_000: raise ValueError('SOURCE_EXCEEDS_CAPTURE_BOUND')
    received = datetime.now(timezone.utc).isoformat()
    doc = json.loads(raw)
    if not isinstance(doc, dict): raise ValueError('UNSUPPORTED_SOURCE_SHAPE')
    return doc, hashlib.sha256(raw).hexdigest(), received


def publish_journal(projections, refs, protocol_ref, errors, scanned, total, started):
    generated = datetime.now(timezone.utc).isoformat()
    manifest = {'contract':'prospective-research-capture.v1','generated_at':generated,
                'started_at':started.isoformat(),'protocol_ref':protocol_ref,'sources':projections,'records':refs,
                'coverage':{'candidate_sources':total,'sources_scanned':scanned,'source_read_failures':errors,
                            'candidate_scan_complete':scanned==total and not errors},
                'sizing_eligible':False,'promotion_eligible':False}
    key=JOURNAL_PREFIX+'captures/'+digest(manifest)+'.json'
    capture_ref=persist_once(s3,S3_BUCKET,key,manifest)
    summary={'schema_version':'prospective-research-summary.v1','generated_at':generated,
             'status':'COLLECTING','capture':capture_ref,'protocol':protocol_ref,'coverage':manifest['coverage'],
             'records_in_capture':len(refs),'new_records':sum(r['created'] for r in refs),
             'rank_observations':sum(r['origin']=='rank_observation' for p in projections for r in p['observations']),
             'ineligible_sources':sum(bool(p['eligibility_reasons']) for p in projections),
             'unsupported_identity_count':sum(p['unsupported_identity_count'] for p in projections),
             'record_previews':refs[:50],'outcomes_in_this_capture':0,'sizing_eligible':False,'promotion_eligible':False,
             'meaning':'Captures explicit directions before future standardized measurement windows. No outcomes, independent-model validation, executable fills or net returns are established by registration.'}
    publish_current(s3,S3_BUCKET,'data/prospective-research.json',summary)
    return summary


def extract_picks(doc):
    """Keep explicit direction/identity; ranked membership alone is no UP call."""
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
                if isinstance(it.get(ck), (int, float)) and not isinstance(it.get(ck), bool) and math.isfinite(it[ck]):
                    sc = float(it[ck])
                    break
            seen.add(sym)
            identity = resolve_instrument(sym, it.get('asset_class') or it.get('asset_type'))
            direction = next((str(it[k]).upper() for k in ('predicted_direction', 'direction', 'side', 'call')
                              if str(it.get(k) or '').upper() in ('UP', 'DOWN', 'LONG', 'SHORT', 'BULLISH', 'BEARISH')), None)
            picks.append({'symbol': sym, 'score': sc, 'identity': identity,
                          'direction': 'UP' if direction in ('UP', 'LONG', 'BULLISH') else 'DOWN' if direction else 'NEUTRAL',
                          'prediction_origin': 'explicit_direction' if direction else 'rank_observation'})
            if len(picks) >= TOP_PER_ENGINE:
                return picks
    return picks


def get_price(sym, identity=None):
    identity = identity or resolve_instrument(sym)
    if not identity:
        return None  # BTC/ETH can be ETF tickers or tokens; never guess.
    fmp_symbol = identity['provider_symbols']['fmp']
    polygon_symbol = identity['provider_symbols']['polygon']
    try:
        u = f"https://financialmodelingprep.com/stable/quote?symbol={urllib.parse.quote(fmp_symbol)}&apikey={FMP}"
        d = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "jh-harv"}), timeout=10).read())
        if isinstance(d, list) and d and d[0].get('symbol') == fmp_symbol and d[0].get("price"):
            price = float(d[0]['price'])
            if math.isfinite(price) and price > 0:
                return price
    except Exception:
        pass
    try:
        u = f"https://api.polygon.io/v2/aggs/ticker/{urllib.parse.quote(polygon_symbol)}/prev?adjusted=true&apiKey={POLYGON}"
        d = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers={"User-Agent": "jh-harv"}), timeout=10).read())
        r = (d or {}).get("results") or []
        if r and (d or {}).get('ticker') == polygon_symbol and r[0].get("c"):
            price = float(r[0]['c'])
            if math.isfinite(price) and price > 0:
                return price
    except Exception:
        pass
    return None


def current_regime():
    for key in ("data/khalid-index.json", "data/regime-read.json", "data/macro-nowcast.json"):
        d = __import__('nowcast_research').guard(key,_read(key))
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
    if (event or {}).get('validation_only'):
        token_identity = resolve_instrument('BTC', 'crypto')
        fund_identity = resolve_instrument('BTC', 'etf')
        quotes = {name: get_price(identity['symbol'], identity) for name, identity in
                  (('bitcoin_spot', token_identity), ('btc_fund', fund_identity))} if event.get('probe_prices') else {}
        ok = resolve_instrument('BTC') is None and token_identity['instrument_id'] != fund_identity['instrument_id']
        if event.get('probe_prices'):
            ok = ok and all(v is not None and v > 0 for v in quotes.values())
        return {'ok': ok, 'validation_only': True, 'version': VERSION,
                'token_identity': token_identity, 'fund_identity': fund_identity,
                'quote_context': quotes, 'ledger_writes': 0, 'sizing_eligible': False}
    t0 = time.time()
    keys = list_outputs()
    regime = current_regime()
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    protocol_ref = ensure_protocol(s3, S3_BUCKET)
    collector_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    projections, research_refs, source_errors = [], [], []
    scanned = 0

    # de-dup store
    seen = _read(SEEN_KEY) or {}
    cutoff = (now - timedelta(days=DEDUP_DAYS)).strftime("%Y-%m-%d")
    prune_cut = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    seen = {k: v for k, v in seen.items() if v >= prune_cut}  # prune old

    # harvest picks per engine
    harvested = []   # (engine, identified pick record)
    ambiguous = 0
    engines_hit = 0
    for k in keys:
        scanned += 1
        try:
            doc, source_sha, received_at = read_research_source(k)
        except Exception as exc:
            reason=str(exc) if str(exc) in ('SOURCE_EXCEEDS_CAPTURE_BOUND','UNSUPPORTED_SOURCE_SHAPE') else 'SOURCE_READ_UNAVAILABLE'
            source_errors.append({'source_key':k,'reason':reason})
            continue
        picks = extract_picks(doc)
        if not picks:
            continue
        try:
            selected = projection(k, doc, picks, source_sha, received_at)
        except ValueError:
            source_errors.append({'source_key':k,'reason':'UNSUPPORTED_SOURCE_PROJECTION'})
            continue
        projections.append(selected)
        research_refs.extend(register(s3, S3_BUCKET, selected, protocol_ref, collector_sha))
        engine = k[len("data/"):-len(".json")]
        got = 0
        for pick in picks:
            sym = pick['symbol']
            if not pick['identity']:
                ambiguous += 1
                continue
            dk = f"eng:{engine}|{pick['identity']['instrument_id']}"
            if seen.get(dk, "0000") >= cutoff:   # logged within DEDUP_DAYS
                continue
            harvested.append((engine, pick))
            got += 1
        if got:
            engines_hit += 1
        if len(harvested) >= MAX_SIGNALS and (event or {}).get('capture_only') is not True:
            break

    journal = publish_journal(projections, research_refs, protocol_ref, source_errors, scanned, len(keys), now)
    if (event or {}).get('capture_only') is True:
        return {'statusCode':200,'capture_only':True,'legacy_ledger_writes':0,
                'records_in_capture':journal['records_in_capture'],'new_records':journal['new_records'],
                'capture':journal['capture'],'sizing_eligible':False}

    # Legacy quote context only; prospective research uses the immutable journal.
    identities = {pick['identity']['instrument_id']: pick['identity'] for _, pick in harvested}
    prices = {}
    with ThreadPoolExecutor(max_workers=24) as ex:
        fut = {ex.submit(get_price, identity['symbol'], identity): key for key, identity in identities.items()}
        for f in as_completed(fut):
            p = f.result()
            if p:
                prices[fut[f]] = p

    table = ddb.Table(SIGNALS_TABLE)
    written = 0
    ts = {f"day_{d}": (now + timedelta(days=d)).isoformat() for d in WINDOWS}
    with table.batch_writer() as bw:
        for engine, pick in harvested:
            sym, sc, identity = pick['symbol'], pick['score'], pick['identity']
            price = prices.get(identity['instrument_id'])
            if not price:
                continue  # skip rather than poison the ledger with baseline_price=None
            tw = _trust(f"eng:{engine}")           # regime-conditioned trust gate
            conf = max(0.05, min(0.95, conf_from_score(sc) * tw))
            sid = str(uuid.uuid4())
            item = {
                "signal_id": sid, "signal_type": f"eng:{engine}", "signal_value": str(round(sc, 3)) if sc is not None else "PICK",
                "predicted_direction": pick['direction'], "confidence": f2d(conf),
                "confidence_basis": "ranking_heuristic_not_calibrated_probability",
                "prediction_origin": pick['prediction_origin'], "instrument": identity,
                "sizing_eligible": False, "baseline_status": "QUOTE_CONTEXT_ONLY_UNVERIFIED_EXECUTION",
                "measure_against": identity['symbol'], "baseline_price": f2d(price),
                "baseline_benchmark_price": None, "benchmark": None,
                "check_windows": [str(d) for d in WINDOWS], "check_timestamps": ts,
                "outcomes": {}, "accuracy_scores": {}, "logged_at": now.isoformat(),
                "logged_epoch": int(now.timestamp()), "status": "pending",
                "metadata": f2d({"engine": engine, "raw_score": sc, "harvested": True, "trust_weight": tw}),
                "ttl": int((now + timedelta(days=365)).timestamp()), "schema_version": "2",
                "predicted_magnitude_pct": None, "predicted_target_price": None,
                "horizon_days_primary": max(WINDOWS), "regime_at_log": regime,
                "khalid_score_at_log": None,
                "rationale": f"harvested top pick from {engine}", "supporting_signals": None,
            }
            bw.put_item(Item=item)
            seen[f"eng:{engine}|{identity['instrument_id']}"] = today
            written += 1

    s3.put_object(Bucket=S3_BUCKET, Key=SEEN_KEY, Body=json.dumps(seen).encode(),
                  ContentType="application/json")
    summary = {
        "engine": "signal-harvester", "version": VERSION, "generated_at": now.isoformat(),
        "regime_at_log": regime, "n_engine_outputs_scanned": len(keys),
        "n_engines_with_picks": engines_hit, "n_harvested": len(harvested),
        "n_written": written, "n_skipped_no_price": len(harvested) - written,
        "n_skipped_ambiguous_identity": ambiguous,
        "sizing_eligible": False,
        "prospective_research": {k:journal[k] for k in ('capture','records_in_capture','new_records','coverage')},
        "dedup_days": DEDUP_DAYS, "top_per_engine": TOP_PER_ENGINE,
        "note": "Legacy picks remain quote context. The immutable prospective journal separately registers explicit directions; neither ranking heuristics nor legacy outcomes establish forecast authority.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=SUMMARY_KEY, Body=json.dumps(summary).encode(),
                  ContentType="application/json")
    print(f"[harvester] scanned={len(keys)} engines_with_picks={engines_hit} "
          f"harvested={len(harvested)} written={written} regime={regime} {summary['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "engines": engines_hit,
            "written": written, "scanned": len(keys)})}
