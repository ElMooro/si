"""
justhodl-ai-brief — Daily executive brief that synthesizes the entire JustHodl stack.

Reads 14 data sources (every major system's S3 output):
  1. intelligence-report.json     (Khalid Index, regime, headline, scores)
  2. data/calibration-snapshot.json   (signal accuracy weights)
  3. data/sector-rotation.json    (11 sector RS + breadth)
  4. data/momentum-scanner.json   (top 50 universe momentum)
  5. data/allocator.json          (cross-asset allocation)
  6. data/asymmetric-scorer.json  (QARP setups)
  7. data/risk-sizer.json         (Kelly + circuit breakers)
  8. data/auction-crisis.json     (Treasury auction stress)
  9. data/eurodollar-stress.json  (8-signal dollar stress)
 10. data/macro-surprise.json     (composite z + regime)
 11. data/insider-trades.json     (cluster buys)
 12. data/earnings-tracker.json   (PEAD signals)
 13. data/correlation-surface.json (regime breaks)
 14. data/alert-history.json      (recent alerts)

Sends compressed prompt to Claude (claude-haiku-4-5-20251001) for synthesis.
Writes:
  - data/ai-brief.json     (structured: regime, top_3_signals, top_3_risks, ranked_actions[5])
  - data/ai-brief.md       (human-readable markdown for direct display)

Schedule: every 4 hours (via EventBridge cron(0 0,4,8,12,16,20 * * ? *))

The brief is decisive and follows Khalid's preferred structure:
  (1) Data tape, (2) Regime, (3) Best assets, (4) Worst assets,
  (5) Transitions, (6) Watch triggers, (7) DECISIVE CALL
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
SSM = boto3.client("ssm", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"


def get_anthropic_key():
    if ANTHROPIC_KEY:
        return ANTHROPIC_KEY
    try:
        return SSM.get_parameter(Name="/justhodl/anthropic/api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        print(f"[ssm-anthropic] {e}")
        return None


def load_json(key, default=None):
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {e}")
        return default if default is not None else {}


def compress_intel(intel):
    """Pull just the decisive bits from intelligence-report.json."""
    if not intel:
        return None
    return {
        "headline": intel.get("headline"),
        "headline_detail": intel.get("headline_detail"),
        "phase": intel.get("phase"),
        "action_required": intel.get("action_required"),
        "forecast": intel.get("forecast"),
        "scores": intel.get("scores", {}),
        "regime": intel.get("regime", {}),
        "risks_top_2": (intel.get("risks") or [])[:2],
    }


def compress_calibration(cal):
    if not cal:
        return None
    s = cal.get("summary", {})
    top = (cal.get("top_accuracy") or [])[:3]
    worst = (cal.get("worst_accuracy") or [])[:3]
    return {
        "weighted_accuracy_60d": s.get("weighted_avg_accuracy_60d"),
        "n_signal_types": s.get("n_signal_types_tracked"),
        "n_outcomes_60d": s.get("n_outcomes_60d"),
        "best_3": [{"sig": x.get("signal_type"), "acc": x.get("rolling", {}).get("accuracy_60d"), "weight": x.get("weight")} for x in top],
        "worst_3": [{"sig": x.get("signal_type"), "acc": x.get("rolling", {}).get("accuracy_60d"), "weight": x.get("weight")} for x in worst],
    }


def compress_sectors(s):
    if not s:
        return None
    return {
        "breadth": s.get("market_breadth"),
        "breadth_desc": s.get("market_breadth_description"),
        "spy_252d_return": (s.get("spy_returns") or {}).get("252") or (s.get("spy_returns") or {}).get(252),
        "leaders": [{"t": x["ticker"], "rs_63d": (x.get("rs_vs_spy") or {}).get("63") or (x.get("rs_vs_spy") or {}).get(63)} for x in s.get("leaders", [])[:3]],
        "laggards": [{"t": x["ticker"], "rs_63d": (x.get("rs_vs_spy") or {}).get("63") or (x.get("rs_vs_spy") or {}).get(63)} for x in s.get("laggards", [])[:3]],
        "fatiguing": [{"t": x["ticker"]} for x in s.get("fatiguing", [])[:3]],
    }


def compress_momentum(m):
    if not m:
        return None
    s = m.get("summary", {})
    rk = m.get("rankings", {})
    return {
        "top_composite": s.get("top_composite"),
        "top_score": s.get("top_composite_score"),
        "best_sector": s.get("best_sector"),
        "worst_sector": s.get("worst_sector"),
        "n_universe": s.get("n_universe"),
        "top_5_composite": [{"t": x["ticker"], "score": x.get("composite_score"), "ret_3m": x.get("ret_3m"), "sector": x.get("sector")} for x in rk.get("composite_top_50", [])[:5]],
        "top_3_accelerating": [{"t": x["ticker"], "accel": x.get("acceleration")} for x in rk.get("accelerating_top_20", [])[:3]],
    }


def compress_allocator(a):
    if not a:
        return None
    return {
        "regime": a.get("regime_headline"),
        "n_rules": f"{a.get('n_rules_applied')}/{a.get('n_rules_total')}",
        "cash_buffer_pct": a.get("cash_buffer_pct"),
        "overweights": (a.get("overweights") or [])[:5],
        "underweights": (a.get("underweights") or [])[:5],
        "weights": a.get("recommended_weights_pct"),
    }


def compress_asymmetric(a):
    """Compress asymmetric-scorer (QARP screener) output for Claude.

    Source: opportunities/asymmetric-equity.json
    Schema: summary{}, cutoffs{}, sector_breakdown{}, top_setups[], value_traps[], cross_pollination{}.
    """
    if not a:
        return None
    summary = a.get("summary") or {}
    cutoffs = a.get("cutoffs") or {}
    sector_breakdown = a.get("sector_breakdown") or {}
    top_setups = a.get("top_setups") or []
    value_traps = a.get("value_traps") or []
    cross = a.get("cross_pollination") or {}

    # Distill top 5 setups with the dimension scores Claude can reason about
    top_5 = []
    for s in top_setups[:5]:
        top_5.append({
            "symbol": s.get("symbol"),
            "sector": s.get("sector"),
            "composite_score": s.get("composite_score"),
            "dims_passed": s.get("dims_passed"),
            "quality": s.get("quality_score"),
            "safety": s.get("safety_score"),
            "value": s.get("value_score"),
            "momentum": s.get("momentum_score"),
        })

    # Top sectors by setup count
    top_sectors = sorted(sector_breakdown.items(), key=lambda x: -x[1])[:3]

    return {
        "n_setups": summary.get("n_setups"),
        "n_value_traps": summary.get("n_value_traps"),
        "n_quality_passed": summary.get("n_quality_passed"),
        "n_screened": summary.get("n_screener_total"),
        "new_this_week": summary.get("new_this_week"),
        "dropped_this_week": summary.get("dropped_this_week"),
        "n_with_stacking_signals": summary.get("n_with_stacking_signals"),
        "cutoffs_used": cutoffs,
        "top_5_setups": top_5,
        "top_3_sectors": [{"sector": k, "n_setups": v} for k, v in top_sectors],
        "first_3_value_traps": [{"symbol": t.get("symbol"), "sector": t.get("sector"), "reason": t.get("trap_reason")} for t in value_traps[:3]],
        "aaii_signal": cross.get("aaii_signal"),
        "btc_mvrv": cross.get("btc_mvrv"),
    }


def compress_risk_sizer(r):
    """Compress risk-sizer (Kelly + circuit breakers + clusters) output for Claude.

    Source: risk/recommendations.json
    Schema: regime, regime_strength, max_gross_exposure_pct, drawdown_status{},
            summary{}, constraints_applied{}, clusters[], sized_recommendations[], warnings[].
    """
    if not r:
        return None
    dd = r.get("drawdown_status") or {}
    summary = r.get("summary") or {}
    constraints = r.get("constraints_applied") or {}
    clusters = r.get("clusters") or []
    sized = r.get("sized_recommendations") or []
    warnings = r.get("warnings") or []

    # Top 5 sized positions w/ context
    top_5 = []
    for s in sized[:5]:
        top_5.append({
            "symbol": s.get("symbol"),
            "sector": s.get("sector"),
            "size_pct": s.get("recommended_size_pct"),
            "kelly_raw": s.get("kelly_raw"),
            "conviction": s.get("phase2b_composite") or s.get("raw_conviction"),
            "cluster": s.get("cluster"),
        })

    # Cluster sizes
    cluster_summary = sorted(
        [{"id": c.get("id"), "method": c.get("method"), "size": c.get("size"), "sector": c.get("sector")} for c in clusters],
        key=lambda x: -(x.get("size") or 0),
    )[:5]

    return {
        "regime": r.get("regime"),
        "regime_strength": r.get("regime_strength"),
        "max_gross_exposure_pct": r.get("max_gross_exposure_pct"),
        "current_dd_pct": dd.get("current_dd_pct"),
        "dd_size_multiplier": dd.get("size_multiplier"),
        "dd_active_trigger": dd.get("active_trigger"),
        "n_candidate_ideas": summary.get("n_candidate_ideas"),
        "n_clusters": summary.get("n_clusters"),
        "total_recommended_size_pct": summary.get("total_recommended_size_pct"),
        "kelly_fraction": constraints.get("kelly_fraction"),
        "max_single_pct": constraints.get("max_single_position_pct"),
        "max_cluster_pct": constraints.get("max_cluster_pct"),
        "top_5_sized": top_5,
        "top_5_clusters": cluster_summary,
        "warnings": [{"level": w.get("level"), "message": w.get("message")} for w in warnings[:3]],
    }


def compress_auction(a):
    if not a:
        return None
    return {
        "score": a.get("composite_score") or a.get("crisis_score"),
        "regime": a.get("regime"),
        "regime_desc": a.get("regime_description"),
    }


def compress_ed_stress(e):
    if not e:
        return None
    return {
        "score": e.get("composite_stress_score") or e.get("composite_score"),
        "severity": e.get("severity"),
        "regime": e.get("regime"),
        "regime_desc": e.get("regime_description"),
        "n_signals_used": e.get("n_signals_used"),
        "hot_signals": [
            {"id": s.get("id"), "label": s.get("label"), "score": s.get("score")}
            for s in (e.get("hot_signals") or [])
        ],
        "cold_signals": [
            {"id": s.get("id"), "label": s.get("label"), "score": s.get("score")}
            for s in (e.get("cold_signals") or [])
        ],
        "top_2_signals": (e.get("signals") or e.get("active_signals") or [])[:2],
    }


def compress_macro(m):
    if not m:
        return None
    return {
        "composite_z": m.get("composite") or m.get("composite_z"),
        "regime": m.get("regime"),
        "regime_desc": m.get("regime_description"),
    }


def compress_insiders(i):
    if not i:
        return None
    big_buys = i.get("big_buys", [])
    clusters = i.get("clusters", [])
    return {
        "n_big_buys": len(big_buys),
        "n_clusters": len(clusters),
        "top_3_clusters": [{"t": c.get("ticker"), "value": c.get("total_value"), "n_insiders": c.get("insider_count")} for c in clusters[:3]],
        "top_3_buys": [{"t": b.get("ticker"), "insider": b.get("insider"), "value": b.get("value")} for b in big_buys[:3]],
    }


def compress_earnings(e):
    if not e:
        return None
    pead = e.get("pead_signals", [])
    return {
        "n_pead_signals": len(pead),
        "top_5_pead": [{"t": s.get("ticker"), "label": s.get("signal"), "eps_surp": s.get("eps_surprise_pct"), "ret_1d": s.get("price_return_1d_pct")} for s in pead[:5]],
    }


def compress_alerts(a):
    if not a:
        return None
    alerts = a.get("alerts", [])
    by_severity = {}
    for x in alerts[-20:]:
        sev = x.get("severity", "?")
        by_severity[sev] = by_severity.get(sev, 0) + 1
    return {
        "last_run": a.get("last_run"),
        "last_run_summary": a.get("last_run_summary"),
        "by_severity_last_20": by_severity,
        "recent_high_5": [{"title": x.get("title"), "category": x.get("category")} for x in alerts[-20:] if x.get("severity") == "HIGH"][:5],
    }


def call_anthropic(prompt, key, model=ANTHROPIC_MODEL, max_tokens=2000):
    """Call Anthropic Messages API."""
    url = "https://api.anthropic.com/v1/messages"
    body = json.dumps({
        "model": model,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def lambda_handler(event=None, context=None):
    started = time.time()

    # 1. Pull all 14 sources
    print("[ai-brief] loading sources")
    intel       = load_json("intelligence-report.json")
    cal         = load_json("data/calibration-snapshot.json")
    sectors     = load_json("data/sector-rotation.json")
    momentum    = load_json("data/momentum-scanner.json")
    allocator   = load_json("data/allocator.json")
    asymmetric  = load_json("opportunities/asymmetric-equity.json")
    risk_sizer  = load_json("risk/recommendations.json")
    auction     = load_json("data/auction-crisis.json")
    ed_stress   = load_json("data/eurodollar-stress.json")
    macro       = load_json("data/macro-surprise.json")
    insiders    = load_json("data/insider-trades.json")
    earnings    = load_json("data/earnings-tracker.json")
    correlation = load_json("data/correlation-surface.json")
    alerts      = load_json("data/alert-history.json")

    # 2. Compress
    snapshot = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "intelligence":       compress_intel(intel),
        "calibration":        compress_calibration(cal),
        "sectors":            compress_sectors(sectors),
        "momentum":           compress_momentum(momentum),
        "allocator":          compress_allocator(allocator),
        "asymmetric_setups":  compress_asymmetric(asymmetric),
        "risk_sizer":         compress_risk_sizer(risk_sizer),
        "auction_stress":     compress_auction(auction),
        "eurodollar_stress":  compress_ed_stress(ed_stress),
        "macro_surprise":     compress_macro(macro),
        "insider_buys":       compress_insiders(insiders),
        "earnings_pead":      compress_earnings(earnings),
        "correlation_breaks": {"top_5": [{"a": b.get("ticker_a"), "b": b.get("ticker_b"), "delta": b.get("delta_30d_vs_90d")} for b in (correlation.get("regime_breaks") or [])[:5]]} if correlation else None,
        "alerts":             compress_alerts(alerts),
    }

    snapshot_str = json.dumps(snapshot, indent=2, default=str)
    print(f"[ai-brief] snapshot size: {len(snapshot_str):,} chars")

    # 3. Build prompt
    prompt = f"""You are the Chief Investment Strategist for JustHodl.AI — a Bloomberg-terminal-grade financial intelligence platform owned by Khalid. Below is a JSON snapshot of every major signal in the system as of right now.

Synthesize this into a DECISIVE executive brief in Khalid's preferred 7-section format:

(1) **DATA TAPE** — markdown table of the 8-10 most important readings (with values and z-scores/percentiles where present)
(2) **REGIME** — one-line classification + signature (e.g., "LATE_CYCLE_NARROW_LEADERSHIP — tech-only rally with credit stress building")
(3) **BEST ASSETS** — top 3-5 names/sectors with median 3m return %, sourced from momentum/allocator/asymmetric data
(4) **WORST ASSETS** — bottom 3-5 with median % decline, mean-reversion candidates
(5) **TRANSITION PROBABILITIES & TIMELINE** — what shifts are likely in next 1-4 weeks, with probability estimates
(6) **WATCH TRIGGERS** — 4-6 specific data thresholds that would flip the regime (e.g., "VIX > 25, Khalid Index > 70, RRP > $50B")
(7) **DECISIVE CALL** — one of: LONG / TRIM / EXIT / LEVER / HEDGE — with concrete % allocations and explicit thresholds for changing the call. Be willing to issue "EXIT ALL RISK" if data warrants. Khalid prefers DECISIVE over hedged.

Rules:
- Use ONLY data from the snapshot. Never make up numbers.
- Cite specific signal names when you reference them (e.g., "calibration says edge_regime hits 92% but market_phase only 38%")
- If two systems disagree, name both and resolve to the higher-weight calibrated signal
- Be concise. Khalid reads this 4x/day so density matters.
- Output GitHub-flavored Markdown only. No preamble, no greeting.

```json
{snapshot_str}
```
"""

    # 4. Call Claude
    api_key = get_anthropic_key()
    if not api_key:
        print("[ai-brief] no Anthropic key — saving snapshot only, no AI brief")
        out_md = "# AI Brief Unavailable\n\nMissing Anthropic API key. Snapshot saved at data/ai-brief.json.\n"
        out = {
            "version": "1.0",
            "generated_at": snapshot["as_of"],
            "duration_s": round(time.time() - started, 2),
            "snapshot": snapshot,
            "brief_md": out_md,
            "error": "missing_anthropic_key",
        }
    else:
        print(f"[ai-brief] calling Claude {ANTHROPIC_MODEL}")
        try:
            resp = call_anthropic(prompt, api_key, max_tokens=2500)
            content = resp.get("content", [])
            text_blocks = [b.get("text", "") for b in content if b.get("type") == "text"]
            brief_md = "\n".join(text_blocks).strip()
            usage = resp.get("usage", {})
            print(f"[ai-brief] got {len(brief_md)} chars  in_tok={usage.get('input_tokens')}  out_tok={usage.get('output_tokens')}")
            out = {
                "version": "1.0",
                "generated_at": snapshot["as_of"],
                "duration_s": round(time.time() - started, 2),
                "model": ANTHROPIC_MODEL,
                "snapshot": snapshot,
                "brief_md": brief_md,
                "usage": usage,
            }
        except Exception as e:
            print(f"[ai-brief] Claude call failed: {e}")
            out = {
                "version": "1.0",
                "generated_at": snapshot["as_of"],
                "duration_s": round(time.time() - started, 2),
                "snapshot": snapshot,
                "brief_md": f"# Brief generation failed\n\n{e}",
                "error": str(e),
            }

    # 5. Write outputs
    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key="data/ai-brief.json", Body=body, ContentType="application/json", CacheControl="public, max-age=600")
    md_body = (out.get("brief_md") or "").encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key="data/ai-brief.md", Body=md_body, ContentType="text/markdown", CacheControl="public, max-age=600")
    print(f"[ai-brief] wrote ai-brief.json ({len(body):,}b) and ai-brief.md ({len(md_body):,}b) in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "duration_s": out["duration_s"],
            "brief_chars": len(out.get("brief_md") or ""),
            "snapshot_keys": list(snapshot.keys()),
            "error": out.get("error"),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
