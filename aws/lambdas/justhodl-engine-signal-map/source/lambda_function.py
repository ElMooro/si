"""justhodl-engine-signal-map — derives engine family → signal_types map.

THE BRIDGE PROBLEM
──────────────────
conviction-engine emits contributing_engines as {engine, family, signal, ...}
where engine is a display name ("Crisis Composite") and family is an
abstraction layer ("crisis-monitor"). NEITHER matches the signal_types
that magnitude-distributions tracks (correlation_break, etf_rotation,
crypto_risk_score, etc).

This Lambda produces a stable mapping so alpha-compass can join the two:
    contributing_engine.family → [underlying signal_types]
    underlying signal_type     → owning engine + family

METHOD
──────
1. Start with a manually-curated KNOWN_ENGINE_SIGNALS table — the 15
   well-known engines that conviction-engine reads from. Each maps to:
       family (matches conviction-engine FEEDS table)
       signal_types[] (what the engine writes to DDB)
2. Scan DDB last 30d for all signal_types observed.
3. For any signal_type NOT in the known table, attempt to attribute by
   signal_id prefix (e.g. 'deepvalue_*' → deepvalue engine → equity-value).
4. Emit a single S3 doc that alpha-compass consumes.

OUTPUT data/engine-signal-map.json:
    {
        engines: {
            "deepvalue":      {"family": "equity-value", "signal_types": ["deepvalue", "epsvelocity"]},
            "crisis-monitor": {"family": "crisis-monitor", "signal_types": [...]},
            ...
        },
        by_family: {
            "equity-value":   ["deepvalue", "epsvelocity", "screener_top_pick", ...],
            "crisis-monitor": ["crisis_index_nfci", "nfci_subindex", ...],
            ...
        },
        by_signal_type: {
            "crisis_index_nfci": "crisis-monitor",
            ...
        },
        unknown_signal_types: [...],   # things to add to the curated table
    }

SCHEDULE
────────
cron(0 9 * * ? *) — daily 09:00 UTC, before alpha-compass at :50.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
SIGNALS_TABLE = "justhodl-signals"
OUTPUT_KEY = "data/engine-signal-map.json"
LOOKBACK_DAYS = 30

s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


# ─── Manually-curated table ──────────────────────────────────────────────
# Mirrors the FEEDS table in conviction-engine. Each engine listed has a
# family matching one of conviction's family labels, plus the signal_types
# it writes to DDB. UPDATE THIS as new engines come online.
#
# Format: engine_name → { family, signal_types[], aliases[] }
#   aliases — alternate names this engine might appear under in
#             conviction's contributing_engines.engine field
KNOWN_ENGINE_SIGNALS = {
    # ─── desk-posture family
    "pm-decision": {
        "family": "desk-posture",
        "signal_types": ["pm_book_posture", "pm_book_net_signal"],
        "aliases": ["PM Decision"],
    },

    # ─── crisis-monitor family
    "crisis-composite": {
        "family": "crisis-monitor",
        "signal_types": [
            # Core composites
            "crisis_index_nfci", "nfci_subindex", "fcip_composite",
            "carry_risk", "plumbing_stress",
            # Cross-asset crisis indicators (vs SPY, vs other risk benchmarks)
            "crisis_dfii10_vs_gld", "crisis_broad_dollar_vs_eem",
            "crisis_dfii10_vs_spy", "crisis_broad_dollar_vs_spy",
            "crisis_hy_oas_vs_spy", "crisis_hy_oas_vs_hyg",
            # Macro stress indicators
            "crisis_sloos_tighten", "crisis_ig_bbb_oas",
            "crisis_index_stlfsi4", "crisis_index_anfci", "crisis_index_kcfsi",
            "crisis_t10yie_extreme",
            # Rate-diff extremes
            "crisis_rate_diff_eur_3m", "crisis_rate_diff_jpy_3m",
            "crisis_sofr_iorb", "crisis_obfr_iorb",
        ],
        "aliases": ["Crisis Composite"],
    },
    "canary-grid": {
        "family": "crisis-monitor",
        "signal_types": ["canary_breadth", "canary_internal"],
        "aliases": ["Canary Grid"],
    },
    "eurodollar-stress": {
        "family": "crisis-monitor",
        "signal_types": ["eurodollar_stress"],
        "aliases": ["Eurodollar Stress"],
    },

    # ─── market-regime family
    "leading-markets": {
        "family": "market-regime",
        "signal_types": ["leading_markets_signal", "leading_markets_z"],
        "aliases": ["Leading Markets"],
    },
    "macro-composite": {
        "family": "market-regime",
        "signal_types": [
            "macro_composite_z", "macro_composite_index",
            "market_phase", "edge_regime",
            "analog_signal", "sector_breadth",
            "khalid_index",   # top-level composite — also a market-regime read
        ],
        "aliases": ["Macro Composite"],
    },

    # ─── macro-fundamental family
    "housing-cycle": {
        "family": "macro-fundamental",
        "signal_types": ["housing_cycle", "housing_regime"],
        "aliases": ["Housing Cycle"],
    },

    # ─── relative-value family
    "cross-asset-rv": {
        "family": "relative-value",
        "signal_types": [
            "etf_rotation", "correlation_break", "corr_break_comp",
            "divergence_extreme", "yc_regime",
            # Correlation-break sub-composites (vs major risk benchmarks)
            "corr_break_composite_vs_vxx", "corr_break_top_pair",
            "corr_break_composite_vs_spy",
        ],
        "aliases": ["Cross-Asset RV"],
    },

    # ─── equity-value family
    "fundamentals-xray": {
        "family": "equity-value",
        "signal_types": [
            "dcf_gap", "altman_z", "piotroski_score", "valuation_composite",
        ],
        "aliases": ["Fundamentals X-Ray"],
    },
    "mean-reversion": {
        "family": "equity-value",
        "signal_types": ["mean_reversion_z", "mean_reversion"],
        "aliases": ["Mean Reversion"],
    },
    "opportunity-engine": {
        "family": "equity-value",
        "signal_types": [
            "opportunity_score", "screener_top_pick", "momentum_top_pick",
        ],
        "aliases": ["Opportunity Engine"],
    },
    "deepvalue": {
        "family": "equity-value",
        "signal_types": ["deepvalue"],
        "aliases": ["DeepValue"],
    },
    "epsvelocity": {
        "family": "equity-value",
        "signal_types": ["epsvelocity"],
        "aliases": ["EPS Velocity"],
    },
    # nobrainer-etfs: thematic/sector ETF picks (SOXX, SLX, SMH, OIH, AIQ).
    # Each ETF is logged as its own signal_type (nobrainer_<symbol>).
    "nobrainer-etfs": {
        "family": "equity-value",
        "signal_types": [
            "nobrainer_SOXX", "nobrainer_SLX", "nobrainer_SMH",
            "nobrainer_OIH",  "nobrainer_AIQ",
        ],
        "aliases": ["NoBrainer ETFs", "Sector ETFs"],
    },

    # ─── positioning family
    "short-pressure": {
        "family": "positioning",
        "signal_types": [
            "short_pressure", "short_interest_extreme",
            "squeeze_risk",   # the high-volume signal observed in DDB
        ],
        "aliases": ["Short Pressure"],
    },
    "cot-positioning": {
        "family": "positioning",
        "signal_types": ["cot_extreme"],
        "aliases": ["CFTC Positioning", "COT Tracker"],
    },
    "earnings-pead": {
        "family": "positioning",
        "signal_types": ["earnings_pead", "post_earnings_drift"],
        "aliases": ["Earnings PEAD"],
    },

    # ─── crypto family
    "crypto-narratives": {
        "family": "crypto",
        "signal_types": ["crypto_fear_greed", "crypto_risk_score", "crypto_narrative"],
        "aliases": ["Crypto Narratives"],
    },

    # ─── momentum family (often standalone)
    "momentum": {
        "family": "momentum",
        "signal_types": [
            "momentum_spy", "momentum_gld", "momentum_qqq",
            "momentum_uso",   # USO oil ETF momentum
            "ml_risk",
        ],
        "aliases": ["Momentum"],
    },

    # ─── edge family
    "edge-composite": {
        "family": "edge-composite",
        "signal_types": ["edge_composite", "edge_signal"],
        "aliases": ["Edge Composite"],
    },
}


def _to_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _extract_id_prefix(signal_id: str) -> str:
    """For ids like 'deepvalue_ELV_*', return 'deepvalue'. Else ''."""
    if not signal_id:
        return ""
    s = str(signal_id)
    # UUID-style ids contain hyphens early — skip those
    if len(s) > 8 and s[8] == "-" and len(s.split("_")) == 1:
        return ""
    parts = s.split("_")
    if len(parts) >= 2 and parts[0].isalpha():
        return parts[0].lower()
    return ""


def discover_signal_types_from_ddb(table, lookback_days: int) -> dict:
    """Scan recent signals; collect signal_types and id-prefixes."""
    since_epoch = int((datetime.now(timezone.utc) -
                        timedelta(days=lookback_days)).timestamp())
    observed = defaultdict(int)
    id_prefixes = defaultdict(set)
    last_key = None
    n_scanned = 0
    while True:
        kw = {
            "FilterExpression": Attr("logged_epoch").gte(since_epoch),
            "ProjectionExpression": "signal_id, signal_type",
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = table.scan(**kw)
        for it in resp.get("Items", []):
            n_scanned += 1
            stype = (it.get("signal_type") or "").strip()
            if stype:
                observed[stype] += 1
            prefix = _extract_id_prefix(it.get("signal_id"))
            if prefix:
                id_prefixes[prefix].add(stype if stype else f"<{prefix}>")
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        if n_scanned > 80_000:
            print(f"[engine-map] scan cap hit at {n_scanned}")
            break
    print(f"[engine-map] scanned {n_scanned} recent signals, "
          f"{len(observed)} unique signal_types, {len(id_prefixes)} id-prefixes")
    return {
        "signal_type_counts": dict(observed),
        "id_prefixes": {k: list(v) for k, v in id_prefixes.items()},
    }


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    table = ddb.Table(SIGNALS_TABLE)

    # 1. Build curated reverse maps
    by_family = defaultdict(set)
    by_signal_type = {}
    engines_out = {}

    for engine, spec in KNOWN_ENGINE_SIGNALS.items():
        family = spec["family"]
        sig_types = spec.get("signal_types", []) or []
        engines_out[engine] = {
            "family": family,
            "signal_types": sorted(sig_types),
            "aliases": spec.get("aliases", []),
        }
        for st in sig_types:
            by_family[family].add(st)
            by_signal_type.setdefault(st, engine)

    # 2. Discover unknowns from DDB
    discovered = discover_signal_types_from_ddb(table, LOOKBACK_DAYS)
    observed = discovered["signal_type_counts"]
    id_prefixes = discovered["id_prefixes"]

    unknown_signal_types = []
    for stype, count in observed.items():
        if stype in by_signal_type:
            continue
        # Try to attribute to an engine via the id-prefix scan
        attributed = None
        for prefix, _ in id_prefixes.items():
            if prefix in engines_out and stype in (id_prefixes.get(prefix) or []):
                attributed = prefix
                break
        if attributed:
            engines_out[attributed]["signal_types"].append(stype)
            engines_out[attributed]["signal_types"] = sorted(set(engines_out[attributed]["signal_types"]))
            fam = engines_out[attributed]["family"]
            by_family[fam].add(stype)
            by_signal_type[stype] = attributed
        else:
            unknown_signal_types.append({"signal_type": stype, "count": count})

    # 3. For id-prefixes that aren't already known engines, surface them
    new_prefixes = []
    for prefix, sigs in id_prefixes.items():
        if prefix not in engines_out and not any(
                prefix in spec.get("aliases", []) for spec in KNOWN_ENGINE_SIGNALS.values()):
            new_prefixes.append({"prefix": prefix, "signal_types": [s for s in sigs if s and not s.startswith("<")]})

    output = {
        "schema_version": "1.0",
        "method": "curated_table_plus_ddb_observation",
        "generated_at": started.isoformat(),
        "lookback_days": LOOKBACK_DAYS,
        "engines": engines_out,
        "by_family": {f: sorted(list(sts)) for f, sts in by_family.items()},
        "by_signal_type": by_signal_type,
        "unknown_signal_types": sorted(unknown_signal_types,
                                        key=lambda r: -r["count"])[:60],
        "new_id_prefixes": new_prefixes,
        "totals": {
            "curated_engines":  len(KNOWN_ENGINE_SIGNALS),
            "curated_signals":  len(by_signal_type),
            "families":         len(by_family),
            "ddb_observed_types": len(observed),
            "unknown_after_attribution": len(unknown_signal_types),
        },
    }

    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, separators=(",", ":"), default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    print(f"[engine-map] curated {len(KNOWN_ENGINE_SIGNALS)} engines, "
          f"{len(by_family)} families, {len(unknown_signal_types)} unknowns")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "engines": len(KNOWN_ENGINE_SIGNALS),
            "families": len(by_family),
            "unknown_signal_types": len(unknown_signal_types),
        }),
    }


lambda_handler = handler
