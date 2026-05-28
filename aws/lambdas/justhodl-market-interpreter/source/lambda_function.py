"""
justhodl-market-interpreter — AI Market Interpretation Engine
==============================================================
Generates Claude-grounded "what this means for markets & risk assets"
interpretations for the configured macro contexts. Refreshes hourly.

For each context: pulls current value + percentile + nearest-episode analog
from episode-reference, fetches the page's regime, looks up relevant crisis-KB
frameworks, asks Claude for a structured JSON interpretation, writes result to
data/interpretations/<context>.json.

USES THE jhcore LAYER (arch #2):
  - jhcore.s3io  for read/write
  - jhcore.claude for Anthropic API
  - jhcore.kb    for crisis-KB framework lookup

OUTPUT SCHEMA (per context):
  {
    "context": "yield-curve",
    "generated_at": "...",
    "bottom_line": "punchy 2-3 sentence synthesis",
    "asset_reads": [
      {"asset": "US Equities (SPX/NDX)", "direction": "MIXED",
       "why": "rationale citing current data + framework"},
      ...6 rows
    ],
    "key_risks": ["...", "..."],
    "what_to_watch": ["...", "..."],
    "model": "claude-haiku-4-5-20251001"
  }
"""
import json
import os
import time
from datetime import datetime, timezone

from jhcore import s3io, claude, kb

# Context configurations — each maps a page/topic to its data sources + KB hints
CONTEXTS = [
    {
        "id": "yield-curve",
        "label": "US Treasury Yield Curve",
        "primary_indicator": "T10Y2Y",
        "cross_indicators": ["DGS10", "DFII10", "T10YIE", "DGS2"],
        "regime_source": {"key": "data/yield-curve.json", "field": "regime"},
        "kb_keywords": ["yield curve", "inversion", "recession", "Hugh Hendry", "term premium"],
    },
    {
        "id": "vix-curve",
        "label": "VIX & Equity Volatility",
        "primary_indicator": "VIXCLS",
        "cross_indicators": ["BAMLH0A0HYM2"],
        "regime_source": {"key": "data/vix-curve.json", "field": "composite_regime"},
        "kb_keywords": ["VIX", "volatility", "contango", "backwardation", "fear", "vol-of-vol"],
    },
    {
        "id": "credit-spreads",
        "label": "Credit Spreads (HY/IG)",
        "primary_indicator": "BAMLH0A0HYM2",
        "cross_indicators": ["BAMLC0A0CM", "DFII10"],
        "regime_source": {"key": "data/credit-spreads.json", "field": "regime"},
        "kb_keywords": ["credit", "high yield", "spread", "default", "refinancing", "cycle"],
    },
    {
        "id": "dollar",
        "label": "US Dollar (DXY)",
        "primary_indicator": "DTWEXBGS",
        "cross_indicators": ["DFII10", "DEXUSEU"],
        "regime_source": {"key": "data/dollar-radar.json", "field": "regime"},
        "kb_keywords": ["dollar", "DXY", "milkshake", "EM", "carry"],
    },
    {
        "id": "eurodollar",
        "label": "Eurodollar / Dollar Funding Stress",
        "primary_indicator": "STLFSI4",
        "cross_indicators": ["NFCI", "VIXCLS", "DTWEXBGS"],
        "regime_source": {"key": "data/eurodollar-stress.json", "field": "severity"},
        "kb_keywords": ["eurodollar", "dollar funding", "squeeze", "offshore", "SOFR", "1997 Asia"],
    },
    {
        "id": "systemic-stress",
        "label": "Systemic & Sovereign Stress",
        "primary_indicator": "STLFSI4",
        "cross_indicators": ["NFCI", "BAMLH0A0HYM2", "VIXCLS"],
        "regime_source": {"key": "data/systemic-stress.json", "field": "regime"},
        "kb_keywords": ["systemic", "sovereign", "financial conditions", "stress", "crisis"],
    },
    {
        "id": "real-rates",
        "label": "Real Yields & Inflation Expectations",
        "primary_indicator": "DFII10",
        "cross_indicators": ["T10YIE", "DGS10"],
        "regime_source": {},
        "kb_keywords": ["real yield", "TIPS", "breakeven", "inflation expectations"],
    },
]

ASSETS = ["US Equities (SPX/NDX)", "Credit (HY / IG)", "Crypto (BTC/ETH)",
          "Gold", "US Dollar (DXY)", "Duration / Bonds"]
ASSET_DIRS = ["BULLISH", "MIXED", "NEUTRAL", "CAUTION", "BEARISH"]

SYSTEM_PROMPT = (
    "You are an institutional macro analyst writing for a sophisticated "
    "individual investor. You think in regimes, percentiles, and historical "
    "analogs. You cite specific data points. You flag when a 'friendly' headline "
    "is contradicted by another data point. You are never hand-wavy; every "
    "asset call has a concrete reason."
)


def build_prompt(ctx, ref, regime, primary, kb_chunks):
    cross_details = []
    for sid in ctx.get("cross_indicators", []):
        ind = ref.get("indicators", {}).get(sid) or {}
        if ind:
            ne = ind.get("nearest_episode") or {}
            cross_details.append(
                f"- {ind.get('label', sid)}: current {ind.get('current')}{ind.get('unit','')} "
                f"({ind.get('percentile')}th pctile), nearest analog {ne.get('name','?')} ({ne.get('type','?')})"
            )

    pri_ne = primary.get("nearest_episode") or {}
    primary_summary = (
        f"PRIMARY INDICATOR — {primary.get('label')}: current {primary.get('current')}"
        f"{primary.get('unit','')} ({primary.get('percentile')}th percentile of 1990-today), "
        f"closest historical analog = {pri_ne.get('name','?')} ({pri_ne.get('type','?')})"
    )

    kb_text = ""
    if kb_chunks:
        kb_text = "\n\nRELEVANT FRAMEWORKS:\n" + "\n\n".join(
            f"### {c['framework']}\n{c['excerpt'][:1200]}" for c in kb_chunks
        )

    asset_list = ", ".join(ASSETS)
    dirs = "/".join(ASSET_DIRS)

    return (
        f"CONTEXT: {ctx['label']}\n"
        f"CURRENT REGIME: {regime or 'unknown'}\n\n"
        f"{primary_summary}\n\n"
        "CROSS-INDICATORS:\n" + "\n".join(cross_details) +
        kb_text +
        "\n\nTASK: Produce a structured JSON interpretation for this context. "
        f"For each of these 6 assets — {asset_list} — give a directional read "
        f"({dirs}) with a 1-2 sentence rationale that cites specific data points "
        "from above. Catch divergences (e.g. if the regime headline says one thing "
        "but a cross-indicator at extreme percentile contradicts it, flag and adjust).\n\n"
        "Output schema (JSON):\n"
        "{\n"
        '  "bottom_line": "2-3 sentence synthesis with specific numbers + nearest analog. Punchy.",\n'
        '  "asset_reads": [\n'
        '    {"asset": "<one of the 6>", "direction": "<one of ' + dirs + '>", "why": "<rationale citing data>"}\n'
        "  ],\n"
        '  "key_risks": ["<1-line risk>", "<1-line risk>"],\n'
        '  "what_to_watch": ["<watch item>", "<watch item>"]\n'
        "}\n\n"
        "Constraints: bottom_line ≤ 60 words. Each why ≤ 35 words. Reference actual numbers (current values, percentiles, analog names). Don't invent data."
    )


def interpret_one(ctx, ref):
    """Run the full interpret pipeline for one context. Returns the result dict."""
    primary = ref.get("indicators", {}).get(ctx["primary_indicator"]) or {}
    if not primary:
        return {"context": ctx["id"], "err": f"primary indicator {ctx['primary_indicator']} not in episode-reference"}

    # Regime (best-effort)
    regime = ""
    rsrc = ctx.get("regime_source") or {}
    if rsrc.get("key"):
        d = s3io.get_json(rsrc["key"], default={})
        if d and rsrc.get("field"):
            v = d.get(rsrc["field"])
            if v is not None:
                regime = str(v).upper()

    # KB framework lookup
    kb_chunks = kb.lookup(ctx.get("kb_keywords", []), max_chunks=2)

    prompt = build_prompt(ctx, ref, regime, primary, kb_chunks)
    result = claude.complete_json(prompt, system=SYSTEM_PROMPT, max_tokens=1500, temperature=0.25)
    if not result:
        return {"context": ctx["id"], "err": "claude returned no parseable JSON"}

    # Normalize + tag
    result["context"] = ctx["id"]
    result["label"] = ctx["label"]
    result["regime"] = regime
    result["primary_indicator"] = ctx["primary_indicator"]
    result["primary_current"] = primary.get("current")
    result["primary_percentile"] = primary.get("percentile")
    result["primary_nearest"] = primary.get("nearest_episode")
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    result["model"] = "claude-haiku-4-5-20251001"
    return result


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[interpreter] start contexts={len(CONTEXTS)}")

    ref = s3io.get_json("data/episode-reference.json", default=None)
    if not ref:
        return {"statusCode": 500, "body": json.dumps({"err": "episode-reference unavailable"})}

    summary = {"generated_at": datetime.now(timezone.utc).isoformat(), "contexts": [], "n_ok": 0, "n_err": 0}

    for ctx in CONTEXTS:
        try:
            r = interpret_one(ctx, ref)
            ok = bool(r.get("bottom_line") and r.get("asset_reads"))
            if ok:
                s3io.put_json(f"data/interpretations/{ctx['id']}.json", r,
                              cache_control="public, max-age=1800")
                summary["n_ok"] += 1
                summary["contexts"].append({"id": ctx["id"], "status": "ok"})
                print(f"[interpreter] {ctx['id']}: OK ({len(r.get('asset_reads', []))} assets)")
            else:
                summary["n_err"] += 1
                summary["contexts"].append({"id": ctx["id"], "status": "err", "err": r.get("err", "no content")})
                print(f"[interpreter] {ctx['id']}: ERR {r.get('err')}")
        except Exception as e:
            summary["n_err"] += 1
            summary["contexts"].append({"id": ctx["id"], "status": "exc", "err": str(e)[:200]})
            print(f"[interpreter] {ctx['id']}: EXC {e}")

    summary["duration_s"] = round(time.time() - started, 2)
    s3io.put_json("data/interpretations/_summary.json", summary, cache_control="public, max-age=900")

    return {"statusCode": 200, "body": json.dumps({"n_ok": summary["n_ok"], "n_err": summary["n_err"], "duration_s": summary["duration_s"]})}
