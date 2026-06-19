"""
justhodl-engine-trust — THE AUTO-DEMOTION GATE (regime-conditioned)
===================================================================
signal-scorecard already grades each engine (PROMOTED/ACTIVE/INSUFFICIENT/DEPRECATED
+ a performance_multiplier) on Wilson-lower-bound of scored outcomes. What it does NOT
do is (a) condition that trust on the CURRENT regime, or (b) expose one clean number
consumers can multiply into a signal's weight. This engine does both, producing the
registry data/engine-trust.json that aws/shared/engine_trust.py serves fleet-wide.

effective_trust = base (scorecard multiplier)  ×  regime factor
  • base: the scorecard's own performance_multiplier (PROMOTED>1, DEPRECATED<1, ...)
  • regime factor: if the engine's edge in the CURRENT regime (by_regime Wilson-LB, n>=5)
    is materially worse than its overall edge, damp it; if better, modestly lift it.
    Edges are not stationary — an engine that prints in expansion can be a trap in a
    slowdown, and the gate should reflect the regime we are actually in right now.

HONEST BY DESIGN: engines with too few scored outcomes are WARMING -> effective_trust
1.0 (neutral). The gate is a no-op until the harvested ledger matures (~7-30d), then
auto-demotes the engines that prove they cannot beat a coinflip. Consumers (harvester
confidence, conviction-engine, boards) read engine-trust.json and weight accordingly.

OUTPUT data/engine-trust.json     SCHEDULE daily 12:30 UTC (after scorecard). Real data.
"""
import json
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
SCORECARD_KEY = "data/signal-scorecard.json"
OUT_KEY = "data/engine-trust.json"
MIN_REGIME_N = 5      # need this many scored outcomes in-regime to condition on it
WARMING_N = 12        # below this many scored outcomes -> neutral, do not trust/distrust
s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
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


def lambda_handler(event, context):
    t0 = time.time()
    sc = _read(SCORECARD_KEY) or {}
    rows = sc.get("scorecard", []) or []
    regime = current_regime()

    engines = []
    counts = {"PROMOTED": 0, "ACTIVE": 0, "INSUFFICIENT": 0, "DEPRECATED": 0, "WARMING": 0}
    for r in rows:
        st = r.get("signal_type")
        if not st:
            continue
        n = r.get("n_scored") or 0
        base_status = r.get("status") or "INSUFFICIENT"
        base_mult = r.get("performance_multiplier")
        base_mult = float(base_mult) if base_mult is not None else 1.0
        overall_lb = r.get("wilson_lb")

        # regime conditioning
        regime_lb = None
        regime_n = 0
        rg = (r.get("by_regime") or {}).get(regime) if regime else None
        if isinstance(rg, dict):
            regime_n = rg.get("n") or 0
            if regime_n >= MIN_REGIME_N:
                regime_lb = rg.get("wilson_lb")

        regime_factor = 1.0
        if regime_lb is not None and overall_lb is not None:
            delta = regime_lb - overall_lb
            if delta <= -0.10:
                regime_factor = 0.55          # much worse in this regime -> damp hard
            elif delta <= -0.04:
                regime_factor = 0.80
            elif delta >= 0.10:
                regime_factor = 1.20          # notably better in this regime -> lift
            elif delta >= 0.04:
                regime_factor = 1.08

        # WARMING gate: not enough scored outcomes -> neutral, never demote on noise
        if n < WARMING_N:
            disp_status = "WARMING"
            effective = 1.0
        else:
            disp_status = base_status
            effective = round(base_mult * regime_factor, 3)
        counts[disp_status] = counts.get(disp_status, 0) + 1

        engines.append({
            "signal_type": st,
            "harvested": st.startswith("eng:"),
            "status": disp_status,
            "n_scored": n,
            "hit_rate": r.get("hit_rate"),
            "wilson_lb": overall_lb,
            "edge_vs_coinflip_pct": r.get("edge_vs_coinflip_pct"),
            "base_multiplier": base_mult,
            "regime_wilson_lb": regime_lb,
            "regime_n": regime_n,
            "regime_factor": regime_factor,
            "effective_trust": effective,
        })

    # rank: most-trusted first, warming/insufficient sink, demoted last
    rank = {"PROMOTED": 0, "ACTIVE": 1, "WARMING": 2, "INSUFFICIENT": 3, "DEPRECATED": 4}
    engines.sort(key=lambda e: (rank.get(e["status"], 3), -(e["effective_trust"] or 0)))

    trusted = [e for e in engines if e["status"] in ("PROMOTED", "ACTIVE") and (e["wilson_lb"] or 0) >= 0.55][:30]
    demoted = [e for e in engines if e["status"] == "DEPRECATED"]
    out = {
        "engine": "engine-trust", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "current_regime": regime,
        "n_engines": len(engines),
        "n_harvested_engines": sum(1 for e in engines if e["harvested"]),
        "counts": counts,
        "thesis": "Regime-conditioned auto-demotion gate. effective_trust multiplies a signal's "
                  "weight; <1 = down-weight, >1 = lift, WARMING=1.0 until the ledger matures.",
        "trusted": trusted,
        "demoted": demoted,
        "engines": engines,
        "consumption": "import aws/shared/engine_trust -> trust(signal_type) -> multiplier. "
                       "Applied to harvested-signal confidence; consumable by conviction-engine and boards.",
        "caveats": "Trust is only as good as the ledger underneath it; most engines read WARMING until "
                   "~7-30d of harvested outcomes mature. Regime buckets need n>=%d to condition. "
                   "Down-weighting affects signal WEIGHT, never the hit-rate measurement itself." % MIN_REGIME_N,
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[engine-trust] engines={len(engines)} regime={regime} counts={counts} "
          f"trusted={len(trusted)} demoted={len(demoted)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_engines": len(engines),
            "counts": counts, "regime": regime})}
