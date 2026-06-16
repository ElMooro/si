"""
justhodl-ciss-ai — institutional AI interpretation layer over the CISS stress data.

Reads data/ciss-stress.json (58 ECB CISS+CLIFS series with levels, percentiles,
z-scores, 1y change) and produces a strategist-grade read: current systemic-stress
regime, which market is the stress source, sovereign + cross-country picture, and —
the part that matters for positioning — implications for GLOBAL RISK ASSETS and the
GLOBAL LIQUIDITY CYCLE. CISS is a financial-conditions barometer: rising = tightening
conditions / risk-off, falling/low = easing / risk-on.

Output: data/ciss-ai.json   ·  Schedule: daily 07:40 UTC (after ciss-stress).
"""
import json
import re
from datetime import datetime, timezone

import boto3
from llm_router import complete

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
SRC_KEY = "data/ciss-stress.json"
OUT_KEY = "data/ciss-ai.json"

SYSTEM = ("You are an institutional systemic-risk strategist. You read the ECB CISS "
          "(Composite Indicator of Systemic Stress, 0-1) and CLIFS indices. CISS measures "
          "real-time financial-system stress: <0.04 calm, ~0.1 elevated, ~0.2 stressed, "
          ">=0.45 crisis (GFC/COVID/EU-debt peaks ran 0.5-0.8). Higher CISS = tighter "
          "financial conditions = headwind for risk assets and a draining liquidity cycle; "
          "low/falling CISS = easy conditions = risk-on tailwind. Be precise, quantitative, "
          "and honest. Never invent numbers. Output STRICT JSON only, no preamble or fences.")


def _summary(d):
    ser = d.get("series", [])
    by = {}
    for s in ser:
        by.setdefault(s["category"], []).append(s)
    lines = []
    head = next((s for s in by.get("ea_headline", [])), None)
    if head:
        lines.append("EA COMPOSITE CISS = %.4f (%s) | pctile %.0f%% of 1980-now | z=%.2f | 1y chg %+.4f | %.0f%% of all-time peak"
                     % (head["latest"], d.get("ea_regime"), head["pctile"], head["zscore"], head.get("chg_1y", 0), head.get("pct_of_peak") or 0))
    if by.get("ea_subindex"):
        lines.append("EA SUB-INDICES (level | pctile):")
        for s in sorted(by["ea_subindex"], key=lambda x: -x["pctile"]):
            lines.append("  - %s: %.4f | %.0f%%" % (s["indicator"], s["latest"], s["pctile"]))
    if by.get("sovereign_ea"):
        for s in by["sovereign_ea"]:
            lines.append("EA %s: %.4f | pctile %.0f%%" % (s["indicator"], s["latest"], s["pctile"]))
    # most-stressed countries by CISS percentile
    cc = sorted(by.get("country_ciss", []), key=lambda x: -x["pctile"])[:5]
    if cc:
        lines.append("HIGHEST country CISS (pctile): " + ", ".join("%s %.0f%% (%.3f)" % (s["country"], s["pctile"], s["latest"]) for s in cc))
    sov = sorted(by.get("sovereign_country", []), key=lambda x: -x["pctile"])[:5]
    if sov:
        lines.append("HIGHEST sovereign stress (pctile): " + ", ".join("%s %.0f%%" % (s["country"], s["pctile"]) for s in sov))
    cl = sorted(by.get("clifs", []), key=lambda x: -x["pctile"])[:6]
    if cl:
        lines.append("HIGHEST CLIFS (pctile): " + ", ".join("%s %.0f%%" % (s["country"], s["pctile"]) for s in cl))
    return "\n".join(lines)


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat()
    d = json.loads(S3.get_object(Bucket=BUCKET, Key=SRC_KEY)["Body"].read())
    summary = _summary(d)
    prompt = (
        "Current ECB systemic-stress snapshot (as of %s):\n\n%s\n\n"
        "Interpret for an institutional macro desk. Return STRICT JSON with keys:\n"
        '{"headline": "one sharp sentence on the current systemic-stress read",\n'
        ' "regime": "CALM|ELEVATED|STRESS|CRISIS",\n'
        ' "regime_read": "2-3 sentences: where overall systemic stress sits vs history and the trend",\n'
        ' "stress_source": "which market/sub-index is carrying the most stress right now, and which are calm",\n'
        ' "sovereign": "1-2 sentences on euro-area + country sovereign stress and any dispersion/fragmentation risk",\n'
        ' "cross_country": "1-2 sentences on cross-country dispersion (which countries elevated vs calm)",\n'
        ' "risk_assets": "2-3 sentences: concrete implications for GLOBAL RISK ASSETS (equities, credit, EM) given these stress levels",\n'
        ' "liquidity": "2-3 sentences: implications for the GLOBAL LIQUIDITY CYCLE / financial conditions",\n'
        ' "watch": ["3-4 specific, falsifiable things to watch that would flip the read"]}'
        % (d.get("ea_composite_date"), summary)
    )
    try:
        raw = complete(prompt, tier="reason", max_tokens=1300, system=SYSTEM)
    except Exception as e:
        raw = ""
        err = repr(e)
    txt = (raw or "").strip()
    txt = re.sub(r"^```(?:json)?|```$", "", txt, flags=re.MULTILINE).strip()
    m = re.search(r"\{.*\}", txt, re.DOTALL)
    parsed = None
    if m:
        try:
            parsed = json.loads(m.group(0))
        except Exception:
            parsed = None

    out = {
        "engine": "ciss-ai", "version": "1.0.0", "generated_at": now,
        "model": "glm-5.1 (Claude Sonnet fallback)",
        "based_on": d.get("ea_composite_date"),
        "ea_composite": d.get("ea_composite"), "ea_regime": d.get("ea_regime"),
        "interpretation": parsed,
        "ok": bool(parsed),
    }
    if not parsed:
        out["raw"] = txt[:1200]
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({"ok": bool(parsed), "regime": out["ea_regime"]})}
