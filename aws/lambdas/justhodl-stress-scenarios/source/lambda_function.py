"""
justhodl-stress-scenarios — Macro stress scenario engine.

Defines 5 named historical-archetype scenarios, computes:
  1. Each scenario's current probability based on live market data
  2. Portfolio impact per scenario (winners + losers from a curated universe)
  3. Probability-weighted expected return for diversified holdings

Reads live state from:
  data/report.json              — Khalid Index, regime, key levels
  data/correlation-breaks.json  — current correlation breaks
  data/credit-stress.json       — HY/IG/CCC OAS
  data/divergence.json          — cross-asset stress signals
  data/regime-composite.json    — meta-regime score
  data/eurodollar-stress.json   — funding/dollar stress

Writes:
  data/stress-scenarios.json    — full scenario probabilities + portfolio impact

Telegram alert when:
  - Highest-prob scenario flips (e.g., from 'Goldilocks' to 'Credit Event')
  - Any scenario crosses 35% probability (was below in prior run)

Schedule: cron(25 * ? * * *) — hourly at :25
"""
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
import urllib.request

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY_OUT = "data/stress-scenarios.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


# ─── Scenario definitions ──────────────────────────────────────────────
# Each scenario specifies:
#   - probes: list of (sidecar_key, json_path, comparator, threshold, weight)
#     that contribute to scenario probability
#   - impact_map: {ticker/asset: expected_pct_move_in_scenario}
#   - winners: top assets in this scenario
#   - losers: assets that get hit
#   - narrative: 1-line description

SCENARIOS = [
    {
        "key": "GOLDILOCKS",
        "name": "Goldilocks Continuation",
        "narrative": "Disinflation + soft landing. Rates ease, credit stays tight, equities grind higher. Best for growth + duration; worst for cash + commodity bears.",
        "probes": [
            # Higher Khalid Index = more risk-on = goldilocks-like
            ("khalid_index", "score", "gte", 60, 0.30),
            # Tight credit (low HY OAS) = no stress brewing
            ("credit_stress", "summary.hy_oas_bps", "lte", 350, 0.25),
            # No regime change = consensus holds
            ("regime_composite", "composite_score", "gte", 30, 0.20),
            # Low VIX = complacency = late-stage goldilocks
            ("report", "vix.value", "lte", 22, 0.15),
            # Eurodollar stress low
            ("eurodollar_stress", "composite_score", "lte", 30, 0.10),
        ],
        "winners": [
            {"ticker": "QQQ", "expected_pct": +6.5, "rationale": "growth duration + multiple expansion"},
            {"ticker": "TLT", "expected_pct": +4.5, "rationale": "rates ease toward neutral"},
            {"ticker": "IWM", "expected_pct": +8.0, "rationale": "small caps rerate on credit access"},
            {"ticker": "XBI", "expected_pct": +9.0, "rationale": "biotech duration + capital availability"},
            {"ticker": "GLD", "expected_pct": +3.0, "rationale": "real yields fall mildly"},
        ],
        "losers": [
            {"ticker": "UUP", "expected_pct": -3.5, "rationale": "DXY weakens on Fed cuts"},
            {"ticker": "UVXY", "expected_pct": -15.0, "rationale": "vol compression"},
            {"ticker": "DBA", "expected_pct": -2.5, "rationale": "ag commodities soft"},
        ],
    },
    {
        "key": "FED_PIVOT",
        "name": "Fed Pivot / QE Restart",
        "narrative": "Aggressive rate cuts (50bp+) and/or QE balance-sheet expansion. Steepener trade, risk-on broadly, dollar weakens, gold rallies. Defensive duration suffers (paradoxically) as long-end sells on growth re-pricing.",
        "probes": [
            ("credit_stress", "summary.hy_oas_bps", "gte", 500, 0.25),
            ("eurodollar_stress", "composite_score", "gte", 60, 0.20),
            ("regime_composite", "composite_score", "lte", -10, 0.20),
            ("khalid_index", "score", "lte", 30, 0.20),
            ("divergence", "composite_score", "gte", 60, 0.15),
        ],
        "winners": [
            {"ticker": "IWM", "expected_pct": +12.0, "rationale": "small caps rerate on credit access"},
            {"ticker": "KRE", "expected_pct": +14.0, "rationale": "regional banks led by NIM relief"},
            {"ticker": "GDX", "expected_pct": +18.0, "rationale": "gold miners on real-yield collapse"},
            {"ticker": "GLD", "expected_pct": +10.0, "rationale": "store-of-value bid"},
            {"ticker": "BTC", "expected_pct": +25.0, "rationale": "liquidity-sensitive risk asset"},
        ],
        "losers": [
            {"ticker": "UUP", "expected_pct": -7.0, "rationale": "DXY collapse on cuts"},
            {"ticker": "TLT", "expected_pct": -4.0, "rationale": "long-end sells on growth re-pricing"},
            {"ticker": "XLP", "expected_pct": -3.0, "rationale": "defensives lag the bull"},
        ],
    },
    {
        "key": "CREDIT_EVENT",
        "name": "Credit Event / HY Spike",
        "narrative": "HY +200bp, IG +50bp. Liquidation cascade, correlations to 1, vol explodes. Treasuries rally on flight-to-quality. Everything risk gets hit including gold for first 5 trading days, then gold separates.",
        "probes": [
            ("credit_stress", "summary.hy_oas_bps", "gte", 600, 0.30),
            ("credit_stress", "summary.ccc_oas_bps", "gte", 1100, 0.20),
            ("divergence", "composite_score", "gte", 70, 0.15),
            ("regime_composite", "composite_score", "lte", -30, 0.15),
            ("eurodollar_stress", "composite_score", "gte", 65, 0.20),
        ],
        "winners": [
            {"ticker": "TLT", "expected_pct": +12.0, "rationale": "flight to quality"},
            {"ticker": "UUP", "expected_pct": +6.0, "rationale": "DXY safe-haven bid"},
            {"ticker": "VIXY", "expected_pct": +60.0, "rationale": "vol explosion"},
            {"ticker": "GOVT", "expected_pct": +5.5, "rationale": "duration rally on rate cut expectation"},
        ],
        "losers": [
            {"ticker": "HYG", "expected_pct": -12.0, "rationale": "high-yield mark-down"},
            {"ticker": "JNK", "expected_pct": -13.0, "rationale": "junk credit cascading"},
            {"ticker": "SPY", "expected_pct": -15.0, "rationale": "general equity drawdown"},
            {"ticker": "IWM", "expected_pct": -22.0, "rationale": "small caps hit hardest on credit"},
            {"ticker": "KRE", "expected_pct": -25.0, "rationale": "regional bank credit exposure"},
            {"ticker": "BTC", "expected_pct": -30.0, "rationale": "liquidity withdrawal in initial cascade"},
        ],
    },
    {
        "key": "DOLLAR_CRISIS",
        "name": "Dollar Crisis / Funding Stress",
        "narrative": "DXY -10%, cross-currency basis -50bp, EM relief rally. Inflation fears reignite, commodities spike, US Treasuries reprice higher (yields up). Foreign demand for UST weakens.",
        "probes": [
            ("eurodollar_stress", "composite_score", "gte", 70, 0.35),
            ("report", "dxy.value", "lte", 95, 0.20),
            ("correlation_breaks", "n_breaks", "gte", 5, 0.20),
            ("divergence", "composite_score", "gte", 50, 0.15),
            ("credit_stress", "summary.hy_oas_bps", "gte", 450, 0.10),
        ],
        "winners": [
            {"ticker": "GLD", "expected_pct": +15.0, "rationale": "currency debasement hedge"},
            {"ticker": "GDX", "expected_pct": +22.0, "rationale": "miners lever gold move"},
            {"ticker": "DBC", "expected_pct": +8.0, "rationale": "broad commodities reprice"},
            {"ticker": "EEM", "expected_pct": +12.0, "rationale": "EM equities benefit from weaker USD"},
            {"ticker": "TIP", "expected_pct": +6.0, "rationale": "real yields stay low while breakevens rise"},
            {"ticker": "BTC", "expected_pct": +20.0, "rationale": "non-sovereign store of value"},
        ],
        "losers": [
            {"ticker": "UUP", "expected_pct": -10.0, "rationale": "DXY collapse"},
            {"ticker": "TLT", "expected_pct": -8.0, "rationale": "long-end yields spike on foreign selling"},
            {"ticker": "XLP", "expected_pct": -4.0, "rationale": "consumer staples margin pressure"},
            {"ticker": "QQQ", "expected_pct": -6.0, "rationale": "growth multiple compression"},
        ],
    },
    {
        "key": "CHINA_SLOWDOWN",
        "name": "China Slowdown / Global Disinflation",
        "narrative": "CNY -15%, commodities -20%, global supply chain stress, deflationary impulse. Winners: USD assets, Treasuries, defensives. Losers: materials, EM, German exporters.",
        "probes": [
            ("report", "dxy.value", "gte", 110, 0.25),
            ("regime_composite", "composite_score", "gte", 30, 0.15),  # if US regime stays strong while China weakens
            ("divergence", "composite_score", "gte", 40, 0.20),
            ("credit_stress", "summary.hy_oas_bps", "gte", 400, 0.15),
            ("correlation_breaks", "n_breaks", "gte", 3, 0.25),
        ],
        "winners": [
            {"ticker": "TLT", "expected_pct": +10.0, "rationale": "deflationary impulse rallies duration"},
            {"ticker": "UUP", "expected_pct": +6.5, "rationale": "DXY safe-haven"},
            {"ticker": "XLP", "expected_pct": +4.0, "rationale": "defensive sector outperforms"},
            {"ticker": "XLU", "expected_pct": +5.0, "rationale": "utilities benefit from lower rates"},
        ],
        "losers": [
            {"ticker": "XME", "expected_pct": -18.0, "rationale": "materials sector hit"},
            {"ticker": "FXI", "expected_pct": -15.0, "rationale": "China equity directly"},
            {"ticker": "EEM", "expected_pct": -12.0, "rationale": "EM equities tied to China"},
            {"ticker": "DBC", "expected_pct": -15.0, "rationale": "commodities crash"},
            {"ticker": "EWG", "expected_pct": -10.0, "rationale": "German exporter exposure"},
            {"ticker": "GLD", "expected_pct": -5.0, "rationale": "USD strength offsets store-of-value bid"},
        ],
    },
]


def get_path(obj, path):
    """Navigate nested dict via dot-separated path. Returns None if missing."""
    if obj is None: return None
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[s3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def evaluate_probe(state, sidecar_key, json_path, comparator, threshold):
    """Returns (matched: bool, actual_value: any)."""
    sidecar = state.get(sidecar_key)
    if sidecar is None: return False, None
    val = get_path(sidecar, json_path)
    if val is None: return False, None
    try:
        val_f = float(val)
    except (TypeError, ValueError):
        return False, val

    if comparator == "gte": matched = val_f >= threshold
    elif comparator == "lte": matched = val_f <= threshold
    elif comparator == "eq": matched = abs(val_f - threshold) < 0.001
    elif comparator == "gt": matched = val_f > threshold
    elif comparator == "lt": matched = val_f < threshold
    else: matched = False
    return matched, val_f


def compute_scenario_probability(scenario, state):
    """Returns dict with probability + per-probe detail."""
    matched_weight = 0.0
    total_weight = 0.0
    probe_results = []
    for probe in scenario["probes"]:
        sidecar_key, json_path, comp, threshold, weight = probe
        matched, actual = evaluate_probe(state, sidecar_key, json_path, comp, threshold)
        total_weight += weight
        if matched:
            matched_weight += weight
        probe_results.append({
            "sidecar": sidecar_key, "path": json_path,
            "test": f"{comp} {threshold}", "actual": actual,
            "matched": matched, "weight": weight,
        })

    if total_weight == 0:
        prob = 0
    else:
        prob = matched_weight / total_weight

    return {
        "key": scenario["key"],
        "name": scenario["name"],
        "narrative": scenario["narrative"],
        "probability": round(prob, 3),
        "probability_pct": round(prob * 100, 1),
        "n_probes_matched": sum(1 for p in probe_results if p["matched"]),
        "n_probes_total": len(probe_results),
        "probes": probe_results,
        "winners": scenario["winners"],
        "losers": scenario["losers"],
    }


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        print(f"[tg] sent: {msg[:80]}")
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print("[stress-scenarios] starting")

    # Load all input sidecars
    state = {
        "report": get_s3_json("data/report.json", {}),
        "credit_stress": get_s3_json("data/credit-stress.json", {}),
        "regime_composite": get_s3_json("data/regime-composite.json", {}),
        "eurodollar_stress": get_s3_json("data/eurodollar-stress.json", {}),
        "divergence": get_s3_json("data/divergence.json", {}),
        "correlation_breaks": get_s3_json("data/correlation-breaks.json", {}),
        "khalid_index": get_s3_json("data/report.json", {}).get("khalid_index", {})
                          if isinstance(get_s3_json("data/report.json", {}).get("khalid_index"), dict)
                          else {},
    }

    # Refresh khalid_index reference (we just read report twice, dedupe)
    rpt = state["report"]
    state["khalid_index"] = rpt.get("khalid_index", {}) if isinstance(rpt.get("khalid_index"), dict) else {}

    prior_run = get_s3_json(S3_KEY_OUT, {}) or {}

    # Evaluate every scenario
    scenarios_out = [compute_scenario_probability(s, state) for s in SCENARIOS]

    # Sort by probability descending
    scenarios_out.sort(key=lambda x: -x["probability"])

    # Top scenario summary
    top = scenarios_out[0] if scenarios_out else None

    # Probability-weighted expected return for each universe asset
    asset_impact = {}  # ticker -> {weighted_pct, scenarios_count, by_scenario}
    for s in scenarios_out:
        p = s["probability"]
        for w in s["winners"]:
            t = w["ticker"]
            if t not in asset_impact:
                asset_impact[t] = {"weighted_pct": 0, "scenarios_count": 0, "by_scenario": []}
            asset_impact[t]["weighted_pct"] += p * w["expected_pct"]
            asset_impact[t]["scenarios_count"] += 1
            asset_impact[t]["by_scenario"].append({
                "scenario": s["key"], "prob": p, "expected_pct": w["expected_pct"],
            })
        for l in s["losers"]:
            t = l["ticker"]
            if t not in asset_impact:
                asset_impact[t] = {"weighted_pct": 0, "scenarios_count": 0, "by_scenario": []}
            asset_impact[t]["weighted_pct"] += p * l["expected_pct"]
            asset_impact[t]["scenarios_count"] += 1
            asset_impact[t]["by_scenario"].append({
                "scenario": s["key"], "prob": p, "expected_pct": l["expected_pct"],
            })

    # Round + sort
    asset_impact_list = [
        {"ticker": t, "expected_return_pct": round(d["weighted_pct"], 2),
          "n_scenarios": d["scenarios_count"], "by_scenario": d["by_scenario"]}
        for t, d in asset_impact.items()
    ]
    asset_impact_list.sort(key=lambda x: -x["expected_return_pct"])

    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "method": "stress_scenarios_v1",
        "n_scenarios": len(scenarios_out),
        "top_scenario": {
            "key": top["key"] if top else None,
            "name": top["name"] if top else None,
            "probability_pct": top["probability_pct"] if top else None,
        },
        "scenarios": scenarios_out,
        "asset_impact": {
            "top_5_winners": asset_impact_list[:5],
            "top_5_losers": asset_impact_list[-5:][::-1],
            "all": asset_impact_list,
        },
        "input_sidecar_freshness": {
            k: (state[k].get("generated_at") if isinstance(state[k], dict) else None)
            for k in state.keys()
        },
        "duration_s": round(time.time() - t0, 2),
    }

    put_s3_json(S3_KEY_OUT, output)
    print(f"[stress-scenarios] top: {top['key'] if top else '?'} @ "
          f"{top['probability_pct'] if top else 0:.1f}%")
    for s in scenarios_out:
        print(f"[stress-scenarios]   {s['key']:<16} {s['probability_pct']:>5.1f}%  "
              f"{s['n_probes_matched']}/{s['n_probes_total']} probes")

    # ─── ALERTS ────────────────────────────────────────────────────────
    try:
        prior_top_key = (prior_run.get("top_scenario") or {}).get("key")
        new_top_key = top["key"] if top else None

        # Top scenario flip
        if prior_top_key and new_top_key and prior_top_key != new_top_key:
            top_winners = ", ".join(w["ticker"] for w in (top.get("winners") or [])[:3])
            top_losers = ", ".join(l["ticker"] for l in (top.get("losers") or [])[:3])
            maybe_telegram(
                f"⚠️ <b>STRESS SCENARIO FLIP</b>\n"
                f"<b>{prior_top_key} → {new_top_key}</b> ({top['probability_pct']:.0f}%)\n"
                f"<i>{top['narrative'][:200]}</i>\n\n"
                f"<b>Winners:</b> {top_winners}\n"
                f"<b>Losers:</b> {top_losers}"
            )

        # Any scenario newly crossing 35%
        prior_probs = {s.get("key"): (s.get("probability_pct") or 0)
                        for s in (prior_run.get("scenarios") or [])}
        for s in scenarios_out:
            cur = s["probability_pct"]
            prior_p = prior_probs.get(s["key"], 0)
            if cur >= 35 and prior_p < 35:
                top_winners = ", ".join(w["ticker"] for w in s["winners"][:3])
                top_losers = ", ".join(l["ticker"] for l in s["losers"][:3])
                maybe_telegram(
                    f"📈 <b>SCENARIO ELEVATED: {s['key']} crossed 35%</b>\n"
                    f"Current: {cur:.0f}% (was {prior_p:.0f}%)\n"
                    f"<i>{s['narrative'][:200]}</i>\n\n"
                    f"<b>Best positioning:</b> {top_winners}\n"
                    f"<b>Avoid:</b> {top_losers}"
                )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "top_scenario": output["top_scenario"],
            "scenarios_count": len(scenarios_out),
            "top_winners": [w["ticker"] for w in (top.get("winners") or [])[:3]] if top else [],
        }),
    }
