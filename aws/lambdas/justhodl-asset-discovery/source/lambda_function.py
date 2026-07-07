"""
justhodl-asset-discovery (v1.0)
═══════════════════════════════
Monthly emerging-asset discovery agent. Once a month it assembles a
compact, fully real context from the fleet's own engines — the current
asset-compass universe + macro forward, the strongest/weakest Finviz
industries by 3m/6m performance, and the crypto cycle phase — and makes
ONE governed reason-tier LLM call proposing up to 6 investable candidates
that are NOT already covered by the platform.

Honesty contract:
  - The model is explicitly permitted (and instructed) to return an
    empty list when nothing genuinely new emerged this month.
  - Every numeric in the prompt comes from our own engines (real data);
    the LLM contributes knowledge synthesis, never numbers we publish.
  - Output status is PROVISIONAL per the Edge-Accuracy standard: these
    are research candidates, not signals; no edge is claimed.
  - If the router gates the call (budget cap / mode=off / providers
    down), the document is still written with candidates=[] and
    llm_status=GATED_OR_DOWN — the engine never fabricates.

Cost governance: routed through aws/shared/llm_router.complete()
(tier="reason" → GLM-5.1 when enabled, Sonnet/Haiku fallback per policy;
economy mode auto-downgrades; content cache; DDB cost ledger; daily
budget cap; per-engine caps). One real call per month by design.

OUTPUT
  data/asset-discovery.json            — latest document
  discovery/history/YYYY-MM.json       — immutable monthly record
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import boto3

from llm_router import complete

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/asset-discovery.json"
S3 = boto3.client("s3", region_name="us-east-1")

MAX_CANDIDATES = 6

SYSTEM = (
    "You are the monthly asset-discovery analyst for an institutional "
    "multi-asset research platform. You respond with STRICT JSON only — "
    "no markdown fences, no prose before or after. Honesty over volume: "
    "if nothing genuinely new emerged, return an empty candidates list. "
    "Never propose anything already in the covered universe. Only "
    "propose assets investable through liquid US-listed instruments "
    "(ETF/ETP/common stock) and give the exact ticker."
)

PROMPT_TEMPLATE = """Month under review: {month}

COVERED UNIVERSE (do NOT propose these): {universe}

MACRO FORWARD (market-implied, from our curve/Cleveland-Fed engine):
{macro}

STRONGEST INDUSTRIES (Finviz official aggregates, 3m/6m perf %):
{strong}

WEAKEST INDUSTRIES (3m/6m perf %):
{weak}

CRYPTO CYCLE: {crypto}

TASK: Propose up to {maxn} newly emerging or structurally under-covered
investable assets / asset classes for this platform to begin monitoring.
Prioritize: (a) new structural drivers (policy, technology, supply
shocks, flows) that emerged or accelerated in the last ~2 quarters;
(b) assets with asymmetric setups (deep drawdown + intact structural
demand, or early-stage secular growth not yet crowded); (c) genuine
diversification vs the covered universe.

Respond with STRICT JSON:
{{"candidates": [{{
  "ticker": "US-listed ticker (required, uppercase)",
  "name": "instrument name",
  "asset_class": "equity|commodity|crypto|fixed_income|reit|thematic_etf|other",
  "thesis": "<= 40 words: why now",
  "structural_driver": "<= 20 words: the underlying force",
  "asymmetry_note": "<= 25 words: upside vs downside shape",
  "risk": "<= 25 words: what kills it",
  "confirming_data": "<= 20 words: what data would confirm or refute"
}}],
 "month_read": "<= 50 words: the single most important emerging-asset
                development this month, or 'nothing material'"}}
Return {{"candidates": [], "month_read": "..."}} if nothing qualifies."""


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def _fmt_inds(rows, n=8, reverse=True):
    rows = [r for r in rows if r.get("perf_q") is not None]
    rows = sorted(rows, key=lambda r: r.get("perf_q") or 0, reverse=reverse)[:n]
    return "\n".join("  - %s: 3m %+.1f%% / 6m %s%%"
                     % (r.get("name"), r.get("perf_q"),
                        ("%+.1f" % r["perf_h"]) if r.get("perf_h")
                        is not None else "n/a") for r in rows) or "  (none)"


def _extract_json(text):
    """Strict-ish JSON extraction: strip fences, find outermost object."""
    if not text:
        return None
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(),
               flags=re.MULTILINE).strip()
    try:
        return json.loads(t)
    except Exception:
        pass
    m = re.search(r"\{.*\}", t, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None


def _valid_candidates(obj, universe):
    out, dropped = [], []
    for c in (obj or {}).get("candidates", [])[:MAX_CANDIDATES * 2]:
        if not isinstance(c, dict):
            dropped.append("non-dict"); continue
        tk = str(c.get("ticker") or "").strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", tk):
            dropped.append("bad ticker %r" % c.get("ticker")); continue
        if tk in universe:
            dropped.append("%s already covered" % tk); continue
        if not (c.get("thesis") or "").strip():
            dropped.append("%s no thesis" % tk); continue
        out.append({"ticker": tk,
                    "name": (c.get("name") or "").strip()[:80],
                    "asset_class": (c.get("asset_class") or "other")[:20],
                    "thesis": c.get("thesis", "").strip()[:320],
                    "structural_driver":
                        c.get("structural_driver", "").strip()[:160],
                    "asymmetry_note":
                        c.get("asymmetry_note", "").strip()[:200],
                    "risk": c.get("risk", "").strip()[:200],
                    "confirming_data":
                        c.get("confirming_data", "").strip()[:160]})
        if len(out) >= MAX_CANDIDATES:
            break
    return out, dropped


def lambda_handler(event=None, context=None):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    month = now.strftime("%Y-%m")

    # ── real context from our own engines ──
    ac = s3_json("data/asset-compass.json")
    universe = sorted({a.get("ticker") for a in (ac.get("assets") or [])
                       if a.get("ticker")})
    mf = ac.get("macro_forward") or {}
    macro = ("  rf now {rf}% -> 1y fwd {rff}% ({dir}); 1y inflation "
             "{infl}%; real growth proxy {g}%").format(
        rf=mf.get("rf_now_pct"), rff=mf.get("rf_1y_forward_pct"),
        dir=mf.get("rf_direction_next_year"),
        infl=mf.get("infl_1y_expected_pct"),
        g=(mf.get("growth") or {}).get("real_growth_proxy_pct"))

    fg = s3_json("data/finviz-groups.json")
    inds = fg.get("industries") or []
    strong = _fmt_inds(inds, 8, True)
    weak = _fmt_inds(inds, 8, False)

    cyc = s3_json("data/crypto-cycle-risk.json")
    crypto = ("risk %s, phase %s" % (cyc.get("composite_score")
                                     or cyc.get("score"),
                                     cyc.get("phase")
                                     or cyc.get("cycle_phase"))
              if cyc else "unavailable")

    prompt = PROMPT_TEMPLATE.format(month=month, universe=", ".join(universe)
                                    or "(compass unavailable)", macro=macro,
                                    strong=strong, weak=weak, crypto=crypto,
                                    maxn=MAX_CANDIDATES)

    # ── the one governed call ──
    txt = complete(prompt, tier="reason", max_tokens=3000, system=SYSTEM,
                   cache_ttl=20 * 86400)   # month-scoped: cache absorbs retries
    llm_status, model_note = "OK", "router-selected (reason tier)"
    obj = _extract_json(txt)
    if not txt or not txt.strip():
        llm_status, obj = "GATED_OR_DOWN", {}
        model_note = "router returned empty (budget cap / mode / providers)"
    elif obj is None:
        llm_status = "PARSE_FAIL"
        model_note = "model returned non-JSON; raw head kept for audit"

    cands, dropped = _valid_candidates(obj, set(universe))

    out = {
        "schema_version": "1.0",
        "generated_at": now.isoformat(),
        "engine": "justhodl-asset-discovery",
        "status": "PROVISIONAL",
        "month": month,
        "llm_status": llm_status,
        "llm_note": model_note,
        "candidates": cands,
        "month_read": ((obj or {}).get("month_read") or "").strip()[:400],
        "dropped_by_validator": dropped,
        "context_snapshot": {
            "universe_n": len(universe),
            "macro_forward": {k: mf.get(k) for k in
                              ("rf_now_pct", "rf_1y_forward_pct",
                               "rf_direction_next_year",
                               "infl_1y_expected_pct")},
            "industries_seen": len(inds),
            "crypto_cycle": crypto,
        },
        "methodology": (
            "One reason-tier LLM call per month over a context built "
            "entirely from the platform's own engines. The validator "
            "enforces ticker shape, universe exclusion and thesis "
            "presence; the model may honestly return zero candidates. "
            "PROVISIONAL: research candidates only — no edge claimed "
            "until proposals are scored against forward returns."),
        "elapsed_s": round(time.time() - t0, 1),
    }
    if llm_status == "PARSE_FAIL":
        out["raw_head"] = (txt or "")[:400]

    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    S3.put_object(Bucket=BUCKET, Key="discovery/history/%s.json" % month,
                  Body=body, ContentType="application/json")
    print("asset-discovery %s: llm=%s candidates=%d dropped=%d"
          % (month, llm_status, len(cands), len(dropped)))
    return {"ok": True, "month": month, "llm_status": llm_status,
            "candidates": len(cands)}
