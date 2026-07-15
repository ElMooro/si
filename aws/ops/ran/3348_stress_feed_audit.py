"""ops 3348 — full stress-feed audit for the unified JustHodl Stress Index. For every
candidate stress/risk feed: pull its live JSON, find the headline numeric score, its
plausible range, freshness, and whether it exposes any history array. This is the
ground-truth inventory that decides which signals enter the widened calibrator.
Read-only.
"""
import json
import boto3
from datetime import datetime, timezone
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# candidate feed -> list of dotted paths to try for the headline numeric score
FEEDS = {
    "data/global-stress.json":      ["gsi", "global_stress_index", "score", "composite.score"],
    "data/ciss-stress.json":        ["ciss", "score", "composite_score", "value", "level"],
    "data/ciss-ai.json":            ["score", "ciss", "value"],
    "data/systemic-stress.json":    ["composite.value", "composite.score", "value", "composite"],
    "data/tail-risk.json":          ["system_tail_gauge", "score", "tail_score"],
    "data/vvix-vov-regime.json":    ["score", "vov", "vvix", "regime_score"],
    "data/bank-stress.json":        ["score", "composite", "value"],
    "data/sovereign-stress.json":   ["score", "composite", "value", "level"],
    "data/sovereign-fiscal.json":   ["score", "composite", "value"],
    "data/credit-stress.json":      ["score", "composite", "value"],
    "data/risk-regime.json":        ["risk_regime_score", "score"],
    "data/polygon-fx-regime.json":  ["fx_roro.fx_roro_score", "score"],
    "data/eurodollar-stress.json":  ["composite_score", "score"],
    "data/eurodollar-plumbing.json":["stress_score", "composite_score", "score"],
    "data/crisis-canaries.json":    ["score", "n_firing", "level"],
    "data/crisis-composite.json":   ["score", "composite", "value"],
    "data/liquidity-inflection.json":["score", "composite", "value"],
    "data/factor-risk.json":        ["score", "composite", "value"],
    "data/tail-hedge.json":         ["score", "value"],
    "data/vol-radar.json":          ["score", "value"],
    "data/bond-vol-history.json":   ["score", "value", "latest"],
    "data/firm-risk-board.json":    ["firm_severity", "score"],
    "data/risk-radar.json":         ["score", "composite"],
    "data/risk-ratios.json":        ["score", "composite"],
}


def dig(obj, path):
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def find_history(obj):
    """Look for any array of dated/numeric points that could be a history series."""
    for k in ("history", "series", "snapshots", "timeline", "daily", "points"):
        v = obj.get(k) if isinstance(obj, dict) else None
        if isinstance(v, list) and len(v) > 5:
            return k, len(v)
    return None, 0


with report("3348_stress_feed_audit") as r:
    r.section("Stress-feed inventory for unified index")
    usable = []
    for key, paths in FEEDS.items():
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
            obj = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
        except Exception as e:
            r.log(f"  ✗ {key} — {type(e).__name__}")
            continue
        # find first path that yields a number
        score, field = None, None
        for p in paths:
            v = dig(obj, p)
            if isinstance(v, (int, float)):
                score, field = v, p
                break
        hist_key, hist_n = find_history(obj)
        gen = obj.get("generated_at") or obj.get("as_of") or obj.get("asOf") or "?"
        if score is not None:
            usable.append(key)
            r.log(f"  ✓ {key}: {field}={round(score,3)} | hist={hist_key or '—'}({hist_n}) | {round(age_h,1)}h | gen={str(gen)[:19]}")
        else:
            top = list(obj.keys())[:8] if isinstance(obj, dict) else str(type(obj))
            r.log(f"  ? {key}: no numeric score found | top keys: {top}")
    r.ok(f"{len(usable)}/{len(FEEDS)} feeds have an extractable headline score")
    r.log(f"USABLE: {usable}")
