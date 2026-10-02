"""market_read -- the AI desk's read of the market, and the ledger that grades it.

Khalid (2026-09-09): "i want to read its interpretation for the market and for macro and for best
opportunities ... stocks, bonds, metals, crypto ... live vibrant instead of passive silent where i cant test
its performance."

Doctrine (fusion spec, unchanged): quant computes, the LLM only explains and challenges. So:

  1. BOARD  -- the fleet's own fresh artifacts, read from S3 with freshness stamped per source (STALE / MISSING
              is shown, never filled): fusion regime + per-entity fusion, risk-gate posture, Khalid risk
              authority, Katlin war room + picks, Bottom actionable sequences, Fortress, bond war room, crisis
              composite, global business cycle, regime composite, metals, crypto, BTC cycle, the Brain's own
              regime read, and the signal scorecard.
  2. PLAYBOOK -- for each asset class the current setup is written as a sentence from the board's numbers,
              embedded through the live RoBERTa-SEC endpoint and matched to the operator's nearest Brain notes
              ("what my own notes say about this setup"). Skipped honestly when no embedding endpoint is live.
  3. READ  -- one LLM call (Sonnet, proprietary tier) over the board + playbook, strict JSON: overall, macro,
              stocks, bonds, metals, crypto, best opportunities, what would change its mind, data gaps, and up
              to six dated CALLS restricted to tickers the fleet actually surfaced.
  4. LEDGER -- every call is logged to DynamoDB justhodl-signals as signal_type `ai_market_read` through the
              fleet contract (signals_emit.log_signal), so outcome-checker prices it forward at 5/21/63 days
              and signal-scorecard grades the AI next to every other engine. The page shows every call with
              its graded return: performance you can test, not prose.

Private artifact: ai/market-read/latest.json (+history) -- it quotes Brain notes, so only the owner route
serves it. The public read model carries stances, counts and the graded hit rates only.
"""
from __future__ import annotations

import json
import copy
import hashlib
import math
import re
import time
from datetime import datetime, timezone
from typing import Tuple, Any, Dict, List, Optional

SIGNAL_TYPE = "ai_market_read"
WINDOWS = [5, 21, 63]
MAX_CALLS = 6
MIN_READ_GAP_S = 20 * 60
MAX_FUTURE_SKEW_H = 5.0 / 60.0
STANCE_ENUMS = {
    "stocks": {"RISK_ON", "SELECTIVE", "DEFENSIVE", "AVOID"},
    "bonds": {"LONG_DURATION", "NEUTRAL", "SHORT_DURATION", "AVOID"},
    "metals": {"ACCUMULATE", "HOLD", "TRIM", "AVOID"},
    "crypto": {"ACCUMULATE", "HOLD", "REDUCE", "AVOID"},
}

SOURCES = {
    # key: (s3 key, freshness SLA hours, private?)
    "fusion": ("data/jh-fusion.json", 30, False),
    "risk_gate": ("data/risk-gate.json", 36, False),
    "khalid_risk": ("data/khalid-risk.json", 30, False),
    "katlin": ("data/katlin.json", 30, False),
    "bottom": ("data/bottom.json", 30, False),
    "fortress": ("data/fortress.json", 30, False),
    "bonds": ("data/bond-warroom.json", 30, False),
    "crisis": ("data/crisis-composite.json", 36, False),
    "gbc": ("data/global-business-cycle.json", 200, False),
    "regime_composite": ("data/regime-composite.json", 36, False),
    "metals": ("screener/metals-miners.json", 60, False),
    "crypto": ("crypto-intel.json", 30, False),
    "btc_cycle": ("data/crypto-cycle-risk.json", 60, False),
    "scorecard": ("data/signal-scorecard.json", 60, False),
    "brain": ("data/brain.json", 400, True),
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:
        return None


def _stamp(doc) -> Optional[str]:
    if not isinstance(doc, dict):
        return None
    for k in ("generated_at", "as_of", "updated_at", "generated", "timestamp", "run_at"):
        v = doc.get(k)
        if isinstance(v, str) and len(v) >= 10:
            return v
    return None


def _age_h(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    try:
        s = str(iso).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s[:26] + s[26:] if "T" in s else s + "T00:00:00+00:00")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return None


def pick(doc, *paths, default=None):
    """First present value along dotted paths ('a.b.0.c')."""
    for p in paths:
        cur = doc
        ok = True
        for part in p.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
                cur = cur[int(part)]
            else:
                ok = False
                break
        if ok:
            return cur
    return default


def _num(v):
    """Reported finite JSON number; no coercion, alias substitution or rounding."""
    if type(v) is int:
        return v
    if type(v) is float and math.isfinite(v):
        return v
    return None


def _rows(v, n):
    return [r for r in (v or []) if isinstance(r, dict)][:n] if isinstance(v, list) else []


def _tk(r):
    for k in ("ticker", "symbol", "sym", "entity_id", "id"):
        v = r.get(k)
        if isinstance(v, str) and v:
            return v.split(":")[-1].upper().replace("X:", "").replace("BTCUSD", "BTC-USD").replace("ETHUSD", "ETH-USD")
    return None


# ────────────────────────────────────────────────────────────────── board
def build_board(s3, public_bucket: str, private_bucket: Optional[str] = None) -> Dict[str, Any]:
    docs, sources = {}, {}
    for name, (key, sla_h, private) in SOURCES.items():
        source_bucket = (private_bucket or public_bucket) if private else public_bucket
        d = _get(s3, source_bucket, key)
        if d is not None: d = __import__("crisis_authority").guard(key, d)
        st = _stamp(d)
        age = _age_h(st)
        status = "MISSING" if d is None else ("FUTURE" if age is not None and age < -MAX_FUTURE_SKEW_H else ("STALE" if (age is None or age > sla_h) else "FRESH"))
        sources[name] = {"key": key, "generated_at": st, "age_h": age, "sla_h": sla_h, "status": status, "private": private}
        docs[name] = d if d is not None else {}
    F, RG, KR, KT, BT, FT, BD, CR, GB, RC, MT, CY, BC, SC, BR = [docs[k] for k in ("fusion", "risk_gate", "khalid_risk", "katlin", "bottom", "fortress", "bonds", "crisis", "gbc", "regime_composite", "metals", "crypto", "btc_cycle", "scorecard", "brain")]

    regime = {
        "fusion_regime": pick(F, "regime.label"), "fusion_regime_score": _num(pick(F, "regime.score")),
        "fusion_regime_legs": [{"engine": l.get("engine") or l.get("engine_id") or l.get("id"), "score": _num(l.get("score")), "label": l.get("label") or l.get("state")} for l in _rows(pick(F, "regime.legs"), 8)],
        "risk_gate_posture": pick(RG, "posture", "state"), "risk_gate_composite": _num(pick(RG, "composite.score", "composite_score", "composite")),
        "risk_gate_sizing": _num(pick(RG, "sizing_multiplier", "sizing.multiplier")),
        "risk_gate_drivers": [str(x)[:80] for x in (pick(RG, "drivers", "composite.drivers") or [])[:6]] if isinstance(pick(RG, "drivers", "composite.drivers"), list) else [],
        "authority_mode": pick(KR, "policy.mode", "mode"), "authority_allows_new_entries": pick(KR, "policy.allows_new_entries", "allows_new_entries"),
        "authority_cap_pct": _num(pick(KR, "policy.cap_pct", "policy.max_gross_pct", "cap_pct")),
        "katlin_posture": pick(KT, "war_room.posture", "posture", "war_room.decision"), "katlin_thermometer": _num(pick(KT, "war_room.thermometer", "war_room.score")),
        "katlin_hard_vetoes": pick(KT, "war_room.hard_vetoes", "war_room.vetoes"),
        "crisis_composite": _num(pick(CR, "composite", "score", "composite_score", "crisis_composite")), "crisis_state": pick(CR, "state", "label", "regime", "defcon"),
        "gbc_phase": pick(GB, "phase", "global.phase", "headline.phase", "cycle_phase"), "gbc_downturn_prob_6m": _num(pick(GB, "downturn_probability_6m.probability_now", "downturn_probability_6m", "headline.downturn_probability_6m")),
        "regime_composite": pick(RC, "regime", "label", "state"), "regime_composite_score": _num(pick(RC, "score", "composite")),
        "brain_regime_read": {k: pick(BR, "regime_read." + k) for k in ("regime", "risk_asset_view", "favor_now", "avoid", "summary", "as_of")} if isinstance(BR.get("regime_read"), dict) else None,
    }
    breadth = pick(BT, "market.breadth") or {}
    stocks = {
        "katlin_prime": [{"ticker": _tk(r), "tier": r.get("tier"), "score": _num(pick(r, "score", "katlin_score")), "desk": r.get("desk") or r.get("asset_class")} for r in _rows(KT.get("picks"), 340) if r.get("tier") == "KATLIN_PRIME"][:10],
        "katlin_ready": [{"ticker": _tk(r), "tier": r.get("tier"), "score": _num(pick(r, "score", "katlin_score")), "desk": r.get("desk") or r.get("asset_class")} for r in _rows(KT.get("picks"), 340) if r.get("tier") == "READY"][:10],
        "bottom_actionable": [{"ticker": _tk(r), "state": r.get("state"), "score": _num(r.get("score")), "desk": r.get("desk"), "grade": r.get("grade")} for r in _rows(BT.get("top_picks"), 12)],
        "bottom_breadth": {k: _num(v) if isinstance(v, (int, float)) else v for k, v in breadth.items()} if isinstance(breadth, dict) else breadth,
        "bottom_read": pick(BT, "market.read"),
        "fortress_top": [{"ticker": _tk(r), "score": _num(pick(r, "score", "fortress_score")), "state": r.get("state") or r.get("verdict")} for r in _rows(pick(FT, "top", "board", "top_picks", "rows"), 8)],
        "fusion_top": [],
        "fusion_bottom": [],
        "market_benchmarks": pick(BT, "market.benchmarks"),
    }
    ents = F.get("entities") if isinstance(F.get("entities"), dict) else {}
    frows = []
    for eid, e in ents.items():
        if not isinstance(e, dict):
            continue
        best = e
        hz = e.get("horizons")
        if isinstance(hz, dict) and hz:
            bh = e.get("best_horizon")
            best = hz.get(bh) if bh in hz else next(iter(hz.values()))
        fs = _num(best.get("fusion_score") if isinstance(best, dict) else None)
        if fs is None:
            continue
        frows.append({"ticker": eid.split(":")[-1].replace("BTCUSD", "BTC-USD").replace("ETHUSD", "ETH-USD"), "fusion_score": fs, "direction": best.get("direction"), "confidence": _num(best.get("confidence")),
                      "contradiction": best.get("contradiction_score"), "horizon": best.get("horizon"), "capital": best.get("capital_decision")})
    frows.sort(key=lambda r: -(abs(r["fusion_score"]) * (r["confidence"] if r["confidence"] is not None else 0.5)))
    stocks["fusion_top"] = [r for r in frows if r["fusion_score"] > 0][:8]
    stocks["fusion_bottom"] = [r for r in frows if r["fusion_score"] < 0][:6]
    bonds = {
        "headline": pick(BD, "headline", "read", "war_room.headline", "summary"),
        "us10y": _num(pick(BD, "us.10y", "sovereign.US.10y", "yields.US10Y", "us10y")), "us2y": _num(pick(BD, "us.2y", "sovereign.US.2y", "yields.US2Y", "us2y")),
        "curve_2s10s": _num(pick(BD, "us.2s10s", "curve.2s10s", "spreads.2s10s")), "move": _num(pick(BD, "move", "vol.move", "MOVE")),
        "hy_oas": _num(pick(BD, "credit.hy_oas", "spreads.hy_oas", "hy_oas")), "ig_oas": _num(pick(BD, "credit.ig_oas", "spreads.ig_oas")),
        "jgb10y": _num(pick(BD, "japan.10y", "sovereign.JP.10y", "jgb10y")), "bund10y": _num(pick(BD, "germany.10y", "sovereign.DE.10y", "bund10y")),
        "periphery": pick(BD, "periphery", "spreads.periphery"), "flags": pick(BD, "flags", "war_room.flags", "alerts"),
        "bottom_bonds": [r for r in stocks["bottom_actionable"] if (r.get("desk") or "") == "bonds"],
    }
    metals = {
        "headline": pick(MT, "headline", "read", "summary"), "gold": _num(pick(MT, "gold.price", "spot.gold", "gold")), "silver": _num(pick(MT, "silver.price", "spot.silver", "silver")),
        "gold_silver_ratio": _num(pick(MT, "gold_silver_ratio", "ratios.gold_silver")), "miners_vs_gold": pick(MT, "miners_vs_gold", "relative.miners_vs_gold"),
        "top": [{"ticker": _tk(r), "score": _num(r.get("score")), "note": (r.get("note") or r.get("read") or "")[:80]} for r in _rows(pick(MT, "top", "leaders", "rows", "picks"), 6)],
        "bottom_metals": [r for r in stocks["bottom_actionable"] if (r.get("desk") or "") in ("gold_metals", "commodities")],
    }
    crypto = {
        "headline": pick(CY, "headline", "read", "summary"), "btc": _num(pick(CY, "btc.price", "prices.BTC", "btc_price")), "eth": _num(pick(CY, "eth.price", "prices.ETH")),
        "btc_dominance": _num(pick(CY, "btc_dominance", "dominance.btc")), "stablecoin_flow": pick(CY, "stablecoin_flow", "flows.stablecoins"), "liquidity": pick(CY, "liquidity", "liquidity_read"),
        "btc_cycle_phase": pick(BC, "phase", "cycle.phase", "headline.phase"), "btc_cycle_score": _num(pick(BC, "score", "cycle.score")),
        "katlin_crypto": [r for r in stocks["katlin_prime"] + stocks["katlin_ready"] if (r.get("desk") or "").lower().startswith("crypto")][:6],
        "bottom_crypto": [r for r in stocks["bottom_actionable"] if (r.get("desk") or "") == "crypto"],
        "fusion_crypto": [r for r in frows if r["ticker"] in ("BTC-USD", "ETH-USD", "BTC", "ETH")][:2],
    }
    perf = {}
    for r in _rows(pick(SC, "scorecard", "rows"), 400):
        if r.get("signal_type") in ("jh_fusion", "katlin", "bottom", SIGNAL_TYPE, "fortress"):
            perf[r["signal_type"]] = {k: r.get(k) for k in ("n_scored", "hit_rate", "alpha_status", "multiplier") if k in r}
    # candidate tickers the LLM may call on: anything the fleet surfaced (never a name it invents)
    cands = {}
    for r in stocks["katlin_prime"] + stocks["katlin_ready"] + stocks["bottom_actionable"] + stocks["fortress_top"] + stocks["fusion_top"] + stocks["fusion_bottom"] + metals["top"]:
        t = r.get("ticker")
        if t and re.fullmatch(r"[A-Z0-9.\-]{1,10}", t):
            cands[t] = cands.get(t, 0) + 1
    for t in ("SPY", "QQQ", "IWM", "TLT", "GLD", "SLV", "BTC-USD", "ETH-USD", "HYG", "UUP"):
        cands.setdefault(t, 0)
    return {"as_of": now_iso(), "sources": sources, "regime": regime, "stocks": stocks, "bonds": bonds, "metals": metals, "crypto": crypto,
            "fleet_scorecard": perf, "candidates": sorted(cands, key=lambda t: -cands[t])[:60]}


# ────────────────────────────────────────────────────────────── playbook
def setup_sentences(board: Dict[str, Any]) -> Dict[str, str]:
    r, s, b, m, c = board["regime"], board["stocks"], board["bonds"], board["metals"], board["crypto"]
    br = (s.get("bottom_breadth") or {})
    return {
        "market": "Market regime %s (%s). Risk gate %s composite %s sizing %s. Capital authority %s, new entries %s. Katlin war room %s. Crisis composite %s (%s). Global business cycle %s, downturn probability %s." % (
            r.get("fusion_regime"), r.get("fusion_regime_score"), r.get("risk_gate_posture"), r.get("risk_gate_composite"), r.get("risk_gate_sizing"), r.get("authority_mode"), r.get("authority_allows_new_entries"),
            r.get("katlin_posture"), r.get("crisis_composite"), r.get("crisis_state"), r.get("gbc_phase"), r.get("gbc_downturn_prob_6m")),
        "stocks": "Stocks: %d Katlin PRIME and %d READY picks, %d Wyckoff bottoms actionable, breadth %s, fusion leaders %s, fusion laggards %s." % (
            len(s.get("katlin_prime") or []), len(s.get("katlin_ready") or []), len(s.get("bottom_actionable") or []), json.dumps(br)[:160],
            ", ".join(x["ticker"] for x in (s.get("fusion_top") or [])[:5]), ", ".join(x["ticker"] for x in (s.get("fusion_bottom") or [])[:4])),
        "bonds": "Bonds: US 10y %s, 2y %s, 2s10s %s, MOVE %s, HY OAS %s, IG OAS %s, JGB 10y %s, Bund %s. %s" % (b.get("us10y"), b.get("us2y"), b.get("curve_2s10s"), b.get("move"), b.get("hy_oas"), b.get("ig_oas"), b.get("jgb10y"), b.get("bund10y"), str(b.get("headline") or "")[:200]),
        "metals": "Metals: gold %s, silver %s, gold/silver %s, miners vs gold %s. %s" % (m.get("gold"), m.get("silver"), m.get("gold_silver_ratio"), m.get("miners_vs_gold"), str(m.get("headline") or "")[:200]),
        "crypto": "Crypto: BTC %s, ETH %s, dominance %s, cycle phase %s (%s), stablecoin flow %s, liquidity %s. %s" % (c.get("btc"), c.get("eth"), c.get("btc_dominance"), c.get("btc_cycle_phase"), c.get("btc_cycle_score"), c.get("stablecoin_flow"), c.get("liquidity"), str(c.get("headline") or "")[:200]),
    }


def playbook(s3, rt, private_bucket: str, ds_id: Optional[str], endpoint: Optional[str], sentences: Dict[str, str], embed_fn, nearest_fn, k: int = 4) -> Dict[str, Any]:
    if not (ds_id and endpoint):
        return {"available": False, "reason": "no live embedding endpoint / dataset -- deploy RoBERTa-SEC and embed the Brain to get the playbook lens", "notes": {}}
    out = {"available": True, "endpoint": endpoint, "dataset_id": ds_id, "notes": {}}
    try:
        vecs = embed_fn(rt, endpoint, list(sentences.values()))
    except Exception as e:
        return {"available": False, "reason": "embedding failed: %s" % str(e)[:120], "notes": {}}
    for (name, _), vec in zip(sentences.items(), vecs):
        if not vec:
            continue
        try:
            out["notes"][name] = [{"similarity": n["similarity"], "label": n["label"], "pinned": n["pinned"],
                                   "note_id": n.get("note_id") or n.get("id"), "text": n.get("text", ""),
                                   "text_private": True}
                                  for n in nearest_fn(s3, private_bucket, ds_id, endpoint, vec, k=k)]
        except Exception as e:
            out["notes"][name] = [{"error": str(e)[:100]}]
    return out


# ─────────────────────────────────────────────────────────────────── LLM
SYSTEM = (
    "You are the AI desk of JustHodl.AI, an institutional quantitative platform. You read a BOARD of numbers the fleet's engines "
    "computed and a PLAYBOOK of the operator's own notes retrieved for this setup. Rules: use ONLY facts in the board and playbook; "
    "never invent a number, a level or a ticker; where a source is STALE or MISSING say so instead of guessing; be direct and specific; "
    "explain the mechanism, not slogans; disagree with an engine when the board contradicts it and say why. Output STRICT JSON only, no prose "
    "outside the JSON, with exactly these keys: overall (string, 4-7 sentences: the situation as a whole), macro (string), stocks (object: stance "
    "one of RISK_ON/SELECTIVE/DEFENSIVE/AVOID, read string), bonds (object: stance one of LONG_DURATION/NEUTRAL/SHORT_DURATION/AVOID, read string), "
    "metals (object: stance one of ACCUMULATE/HOLD/TRIM/AVOID, read string), crypto (object: stance one of ACCUMULATE/HOLD/REDUCE/AVOID, read string), "
    "best_opportunities (array of up to 8 objects: ticker, side LONG/SHORT, why, horizon_days, from_engines array), what_would_change_my_mind (array of strings), "
    "data_gaps (array of strings), calls (array of up to %d objects: ticker, direction UP or DOWN, horizon_days 21 or 63, confidence 0.5-0.85, thesis). "
    "Every ticker in best_opportunities and calls MUST come from the CANDIDATES list. Calls are dated predictions that will be graded against real prices; "
    "make only the ones you would stake a reputation on." % MAX_CALLS
)


LESSONS_KEY = "ai/market-read/lessons.json"
LESSON_SYSTEM = ("You are the AI desk's own post-mortem. You get the desk's past dated calls with their graded real-price outcomes. Write the lessons the "
                 "desk should carry into its next read: what it got right, what it got wrong, and the mechanism behind each miss. Output STRICT JSON: "
                 "{\"lessons\": [{\"lesson\": string, \"evidence\": string, \"weight\": 1-5}], \"summary\": string}. Max 6 lessons. Use only the graded rows given.")


def write_lessons(graded_rows: List[dict], prior: Dict[str, Any], complete_fn, fallback_fn=None) -> Dict[str, Any]:
    """Learn from mistakes: turn graded calls into carried lessons (only when new graded rows exist)."""
    rows = [r for r in graded_rows if r.get("windows")]
    if not rows:
        return prior or {"lessons": [], "summary": "", "graded_rows_seen": 0, "updated_at": None}
    key = sorted(r["ticker"] + ":" + str(r.get("logged_at")) for r in rows)
    fingerprint = "|".join(key)[:2000]
    if prior and prior.get("fingerprint") == fingerprint:
        return prior
    prompt = "PRIOR LESSONS:\n%s\n\nGRADED CALLS (direction, horizon, thesis, returns %% since call at 5/21/63 trading days, correct?):\n%s\n\nProduce the JSON." % (
        json.dumps((prior or {}).get("lessons") or [], default=str)[:3000], json.dumps(rows[-40:], default=str)[:12000])
    raw = complete_fn(prompt, tier="critical", max_tokens=1200, contains_proprietary=True, system=LESSON_SYSTEM, on_demand=True, no_cache=True)
    txt = str(raw or "").strip()
    if not txt and fallback_fn is not None:
        txt = str(fallback_fn(prompt, max_tokens=1200, system=LESSON_SYSTEM) or "").strip()
    mm = re.search(r"\{.*\}", txt, re.S)
    try:
        j = json.loads(mm.group(0) if mm else txt)
        lessons = [l for l in (j.get("lessons") or []) if isinstance(l, dict) and l.get("lesson")][:6]
    except Exception:
        return {**(prior or {}), "error": "lessons did not parse", "raw": txt[:300], "updated_at": now_iso()}
    return {"lessons": lessons, "summary": str(j.get("summary") or "")[:600], "graded_rows_seen": len(rows), "fingerprint": fingerprint, "updated_at": now_iso()}



from deterministic_desk import desk_read

def deterministic_read(board):
    """LLM-silent path: table-driven desk. Missing gate = NO_READ."""
    return desk_read(board)
    rg = (board.get("regime") or {})
    posture = str(rg.get("risk_gate_posture") or "").upper()
    sizing = rg.get("risk_gate_sizing")
    fusion = rg.get("fusion_regime") or "unknown"
    if any(x in posture for x in ("SEVERE", "OFF", "DEFENS")):
        stocks, bonds, metals, crypto = "DEFENSIVE", "LONG_DURATION", "HOLD", "REDUCE"
    elif any(x in posture for x in ("ON", "RISK_ON")):
        stocks, bonds, metals, crypto = "SELECTIVE", "NEUTRAL", "HOLD", "HOLD"
    else:
        stocks, bonds, metals, crypto = "SELECTIVE", "NEUTRAL", "HOLD", "HOLD"
    overall = (
        "Deterministic fleet read (LLM voice offline). Risk-gate posture %s, sizing %s, fusion %s. "
        "Stances map the gate onto stocks/bonds/metals/crypto until the critical voice returns."
        % (posture or "n/a", sizing if sizing is not None else "n/a", fusion)
    )
    macro = "Risk-gate and fusion are the governors. Other engines stay on the board as evidence, not as a second vote."
    def arm(st, txt):
        return {"stance": st, "read": txt}
    return {
        "overall": overall,
        "macro": macro,
        "stocks": arm(stocks, "Mapped from risk-gate %s." % (posture or "n/a")),
        "bonds": arm(bonds, "Duration stance follows the same gate, not a separate bond vote."),
        "metals": arm(metals, "Default hold unless the gate is severe."),
        "crypto": arm(crypto, "Crypto sized last; gate-off cuts risk."),
        "what_would_change_my_mind": ["Critical voice returns a valid parse", "Risk-gate posture flip"],
        "data_gaps": ["LLM market-read empty"],
        "best_opportunities": [],
        "calls": [],
        "fallback": True,
    }

DEFAULT_BUDGET = (24000, 24000, 9000)       # board, fleet digest, playbook -- chars sent to the voice
OWNED_BUDGET = (16000, 10000, 7000)         # ops 5822 (2026-09-18): the endpoint serves 16,384 tokens; ~33k chars + system + lessons ~ 11.5k prompt tokens + 1,400 answer tokens


def build_prompt(board: Dict[str, Any], play: Dict[str, Any], lessons: Optional[Dict[str, Any]] = None, playbook_text: bool = True,
                 budget: tuple = DEFAULT_BUDGET, schema_hint: bool = False) -> str:
    """The read prompt, identical for every voice (Anthropic, GLM, the owned model) apart from the size budget and,
    for a smaller voice, an exact JSON skeleton to copy (schema_hint)."""
    text = _build_prompt(board, play, lessons, playbook_text, budget)
    if schema_hint:
        text = text[:-len("Produce the JSON.")] if text.endswith("Produce the JSON.") else text + "\n"
        text += ("Produce the JSON. Copy exactly this shape (same keys, stances only from the lists, ticker only from CANDIDATES, no other keys, no prose before or after). "
                 "calls: an empty calls array is valid abstention. Candidate availability never requires a prediction. Use only typed numeric confidence fractions 0.5-0.85 and integer calendar-day horizons 21 or 63:\n") + SCHEMA_SKELETON
    return text


def repair_prompt(raw: str, error: str) -> str:
    """Second chance for a near-miss answer: same content, corrected to the contract."""
    return ("Your previous answer was rejected by the validator: %s\n\nReturn the SAME analysis as one JSON object that matches exactly this shape "
            "(same keys, stances only from the lists, every ticker from the original CANDIDATES, every asset object with a non-empty \"read\"), "
            "and nothing else:\n%s\n\nYour previous answer:\n%s" % (str(error)[:300], SCHEMA_SKELETON, str(raw)[:5000]))


def _build_prompt(board: Dict[str, Any], play: Dict[str, Any], lessons: Optional[Dict[str, Any]] = None, playbook_text: bool = True,
                  budget: tuple = DEFAULT_BUDGET) -> str:
    b_board, b_digest, b_play = budget
    slim = json.loads(json.dumps(board, default=str))
    slim.pop("candidates", None)
    fleet_digest = slim.pop("fleet_digest", [])
    # The operator's notes go to the proprietary tier only (same boundary brain-sync already uses); text is bounded.
    play_refs = {name: [{k: note.get(k) for k in ("similarity", "label", "pinned", "note_id")} | ({"text": str(note.get("text") or "")[:280]} if playbook_text else {"text_private": True})
                        for note in notes] for name, notes in (play.get("notes") or {}).items()}
    lesson_block = json.dumps((lessons or {}).get("lessons") or [], default=str)[:3000]
    return "LESSONS FROM YOUR OWN GRADED CALLS (carry them; do not repeat a graded mistake):\n%s\n\n" % lesson_block + "BOARD (governed core artifacts, with freshness):\n%s\n\nFLEET DIGEST (every fresh, non-private registered feed; stale/missing coverage is in BOARD.fleet_coverage):\n%s\n\nPLAYBOOK (private note references and labels only; no note prose leaves the private boundary):\n%s\n\nCANDIDATES (only tickers allowed in opportunities/calls):\n%s\n\nProduce the JSON." % (
        json.dumps(slim, default=str)[:b_board], json.dumps(fleet_digest, default=str)[:b_digest],
        json.dumps(play_refs or {"unavailable": play.get("reason")}, default=str)[:b_play],
        ", ".join(board.get("candidates") or []))


SCHEMA_SKELETON = (
    '{"overall": "<4-7 sentences>", "macro": "<2-4 sentences: growth, inflation, policy, the dollar -- from the board>", '
    '"stocks": {"stance": "RISK_ON|SELECTIVE|DEFENSIVE|AVOID", "read": "<why, from the board>"}, '
    '"bonds": {"stance": "LONG_DURATION|NEUTRAL|SHORT_DURATION|AVOID", "read": "<why>"}, '
    '"metals": {"stance": "ACCUMULATE|HOLD|TRIM|AVOID", "read": "<why>"}, '
    '"crypto": {"stance": "ACCUMULATE|HOLD|REDUCE|AVOID", "read": "<why>"}, '
    '"best_opportunities": [{"ticker": "<from CANDIDATES>", "side": "LONG|SHORT", "why": "<string>", "horizon_days": 21, "from_engines": ["<engine>"]}], '
    '"what_would_change_my_mind": ["<string>"], "data_gaps": ["<string>"], '
    '"calls": [{"ticker": "<from CANDIDATES>", "direction": "UP|DOWN", "horizon_days": 21, "confidence": 0.6, "thesis": "<string>"}]}'
)

# What a smaller voice tends to write instead of the contract -- coerced conservatively, every coercion recorded.
_READ_ALIASES = ("read", "reading", "view", "rationale", "comment", "commentary", "summary", "analysis", "note", "text", "why")
_STANCE_ALIASES = ("stance", "posture", "position", "view", "bias", "call", "rating")
_STANCE_SYNONYMS = {
    "stocks": {"RISK_OFF": "DEFENSIVE", "RISKOFF": "DEFENSIVE", "BEARISH": "DEFENSIVE", "CAUTIOUS": "SELECTIVE", "NEUTRAL": "SELECTIVE",
               "BULLISH": "RISK_ON", "RISKON": "RISK_ON", "OVERWEIGHT": "RISK_ON", "UNDERWEIGHT": "DEFENSIVE"},
    "bonds": {"LONG": "LONG_DURATION", "SHORT": "SHORT_DURATION", "DURATION_LONG": "LONG_DURATION", "DURATION_SHORT": "SHORT_DURATION",
              "OVERWEIGHT": "LONG_DURATION", "UNDERWEIGHT": "SHORT_DURATION", "BULLISH": "LONG_DURATION", "BEARISH": "SHORT_DURATION"},
    "metals": {"BUY": "ACCUMULATE", "ADD": "ACCUMULATE", "OVERWEIGHT": "ACCUMULATE", "NEUTRAL": "HOLD", "SELL": "TRIM", "REDUCE": "TRIM", "UNDERWEIGHT": "TRIM"},
    "crypto": {"BUY": "ACCUMULATE", "ADD": "ACCUMULATE", "OVERWEIGHT": "ACCUMULATE", "NEUTRAL": "HOLD", "SELL": "REDUCE", "TRIM": "REDUCE", "UNDERWEIGHT": "REDUCE"},
}


def _canon(value) -> str:
    # Complete enum tokens only: punctuation/digits/negated prose cannot become an action.
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z]+(?:[ _-][A-Za-z]+)*", value.strip()):
        return ""
    return value.strip().upper().replace("-", "_").replace(" ", "_")


def normalize_read_doc(doc: Any) -> Tuple[Any, List[str]]:
    """Accept explicit aliases only, retaining every row; conflicting aliases reject the answer."""
    if not isinstance(doc, dict): return doc, []
    out = copy.deepcopy(doc)
    fixes: List[str] = []
    def alias(row, name, alts, label, transform=None):
        present = [(k, row[k]) for k in (name, *alts) if k in row]
        if not present: return
        values = [(k, transform(v) if transform else v) for k, v in present]
        if any(value != values[0][1] for _, value in values[1:]):
            raise ValueError(label + " has conflicting aliases")
        key, value = values[0]
        if key != name: fixes.append(label + " <- " + key)
        if value != present[0][1]: fixes.append(label + " canonicalised")
        row[name] = value
    alias(out, "overall", ("summary", "situation", "overview"), "overall")
    alias(out, "macro", ("macro_view", "macro_read", "economy"), "macro")
    for asset in STANCE_ENUMS:
        row = out.get(asset)
        if not isinstance(row, dict): continue
        alias(row, "read", _READ_ALIASES[1:], asset + ".read")
        synonyms = _STANCE_SYNONYMS[asset]
        alias(row, "stance", _STANCE_ALIASES[1:], asset + ".stance", lambda v: synonyms.get(_canon(v), _canon(v)))
    for key, name, alt, mapping in (
        ("best_opportunities", "side", "direction", {"BUY": "LONG", "UP": "LONG", "SELL": "SHORT", "DOWN": "SHORT"}),
        ("calls", "direction", "side", {"LONG": "UP", "BUY": "UP", "BULLISH": "UP", "SHORT": "DOWN", "SELL": "DOWN", "BEARISH": "DOWN"}),
    ):
        rows = out.get(key)
        if not isinstance(rows, list): continue
        for index, row in enumerate(rows):
            if not isinstance(row, dict): continue
            label = key + "[" + str(index) + "]"
            alias(row, name, (alt,), label + "." + name, lambda v: mapping.get(_canon(v), _canon(v)))
            alias(row, "horizon_days", ("horizon",), label + ".horizon_days")
            if key == "calls": alias(row, "thesis", ("why",), label + ".thesis")
            else: alias(row, "why", ("reason",), label + ".why")
    return out, fixes


def parse_read_text(txt: str, candidates: set) -> Dict[str, Any]:
    """Parse one bounded JSON object. Retain source text privately, never infer an action."""
    contract = {"contract": "market-read-inputs.v1", "confidence_unit": "fraction",
                "window_unit": "calendar_day", "source_qualified": False,
                "forecast_qualified": False, "sizing_eligible": False}
    if not isinstance(txt, str):
        return {"parse_error": True, "validation_error": "answer must be text", "input_validation": contract}
    raw = txt.encode("utf-8", errors="surrogatepass")
    contract.update(raw_sha256=hashlib.sha256(raw).hexdigest(), raw_bytes=len(raw))
    if len(raw) > 131072:
        return {"parse_error": True, "validation_error": "answer exceeds 128 KiB", "input_validation": contract}
    contract["received_text"] = txt
    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("duplicate JSON object key")
            out[key] = value
        return out
    def finite(value):
        raise ValueError("non-finite JSON number")
    body = txt.strip()
    fence = re.fullmatch(r"```(?:json)?\s*([\s\S]*?)\s*```", body)
    if fence:
        body = fence.group(1)
        contract["wrapper"] = "whole_json_fence"
    try:
        received = json.loads(body, object_pairs_hook=unique, parse_constant=finite)
        # Exponent overflow is not passed to parse_constant by the JSON decoder.
        json.dumps(received, allow_nan=False)
        normalized, fixes = normalize_read_doc(received)
        out = validate_read(normalized, set(candidates or []))
    except (ValueError, TypeError, OverflowError, RecursionError) as exc:
        return {"parse_error": True, "validation_error": str(exc)[:300], "raw": txt[:2000], "input_validation": contract}
    contract["status"] = "typed_input_only"
    out["input_validation"] = contract
    out["received_input"] = received
    if fixes:
        out["coercions"] = fixes
    return out


def compose_read(board: Dict[str, Any], play: Dict[str, Any], complete_fn, lessons: Optional[Dict[str, Any]] = None, playbook_text: bool = True,
                 budget: tuple = DEFAULT_BUDGET) -> Dict[str, Any]:
    prompt = build_prompt(board, play, lessons, playbook_text, budget)
    raw = complete_fn(prompt, tier="critical", max_tokens=2400, contains_proprietary=True, system=SYSTEM, on_demand=True, no_cache=True)
    txt = str(raw or "").strip()
    if not txt:
        fb = deterministic_read(board)
        fb["parse_error"] = False
        fb["empty"] = True
        fb["fallback"] = True
        return fb
    return parse_read_text(txt, set(board.get("candidates") or []))


def _text(value: Any, name: str, minimum: int = 1, maximum: int = 4000) -> str:
    if not isinstance(value, str) or not minimum <= len(value.strip()) <= maximum:
        raise ValueError("%s must be a bounded non-empty string" % name)
    return value.strip()


def _input_integer(value, name, allowed=None, maximum=365):
    if type(value) is not int or value < 1 or value > maximum or allowed is not None and value not in allowed:
        raise ValueError(name + " must be an explicit allowed integer calendar-day horizon")
    return value


def _input_confidence(value):
    if type(value) not in (int, float) or not 0.5 <= value <= 0.85 or not math.isfinite(value):
        raise ValueError("confidence must be a finite numeric fraction in [0.5, 0.85]")
    return value


def _input_units(row):
    for key, expected in (("confidence_unit", "fraction"), ("horizon_unit", "calendar_day"), ("window_unit", "calendar_day")):
        if key in row and row[key] != expected:
            raise ValueError(key + " conflicts with the declared input contract")


def _input_call(row, candidates=None):
    if not isinstance(row, dict):
        raise ValueError("call must be an object")
    _input_units(row)
    ticker = row.get("ticker")
    if not isinstance(ticker, str) or not re.fullmatch(r"[A-Z0-9.\-]{1,10}", ticker) or candidates is not None and ticker not in candidates:
        raise ValueError("ticker is not an allowed candidate")
    if row.get("direction") not in ("UP", "DOWN"):
        raise ValueError("direction must be UP or DOWN")
    return {"ticker": ticker, "direction": row["direction"],
            "horizon_days": _input_integer(row.get("horizon_days"), "call.horizon_days", (21, 63)),
            "confidence": _input_confidence(row.get("confidence")),
            "thesis": _text(row.get("thesis"), "call.thesis", 4, 800)}


def validate_read(doc: Any, candidates: set) -> Dict[str, Any]:
    if not isinstance(doc, dict):
        raise ValueError("LLM output must be an object")
    out = {"overall": _text(doc.get("overall"), "overall", 20, 4000),
           "macro": _text(doc.get("macro"), "macro", 10, 3000), "withheld_inputs": []}
    for asset, allowed in STANCE_ENUMS.items():
        row = doc.get(asset)
        if not isinstance(row, dict) or not isinstance(row.get("stance"), str) or row["stance"] not in allowed:
            raise ValueError("%s.stance is invalid" % asset)
        out[asset] = {"stance": row["stance"], "read": _text(row.get("read"), "%s.read" % asset, 5, 2400)}
    def collection(key):
        value = doc.get(key, [])
        if not isinstance(value, list):
            raise ValueError(key + " must be an array")
        return value
    def withheld(key, index, row, reason):
        out["withheld_inputs"].append({"collection": key, "source_index": index,
                                       "received": copy.deepcopy(row), "reason": reason})
    for key, limit in (("what_would_change_my_mind", 12), ("data_gaps", 20)):
        out[key] = []
        for index, row in enumerate(collection(key)):
            try:
                value = _text(row, key, 2, 400)
                if len(out[key]) >= limit: raise ValueError("collection capacity exceeded")
                out[key].append(value)
            except ValueError as exc: withheld(key, index, row, str(exc))
    out["best_opportunities"] = []
    for index, row in enumerate(collection("best_opportunities")):
        try:
            if not isinstance(row, dict) or not isinstance(row.get("ticker"), str) or row["ticker"] not in candidates or row.get("side") not in ("LONG", "SHORT"):
                raise ValueError("opportunity requires an allowed ticker and explicit side")
            _input_units(row)
            horizon = _input_integer(row.get("horizon_days"), "opportunity.horizon_days")
            engines = row.get("from_engines", [])
            if not isinstance(engines, list) or len(engines) > 12 or not all(isinstance(v, str) and v.strip() and len(v) <= 80 for v in engines):
                raise ValueError("from_engines must be a bounded array of engine identifiers")
            value = {"ticker": row["ticker"], "side": row["side"], "why": _text(row.get("why"), "opportunity.why", 4, 800),
                     "horizon_days": horizon, "from_engines": list(engines), "source_index": index}
            if len(out["best_opportunities"]) >= 8: raise ValueError("collection capacity exceeded")
            out["best_opportunities"].append(value)
        except ValueError as exc: withheld("best_opportunities", index, row, str(exc))
    out["calls"] = []
    for index, row in enumerate(collection("calls")):
        try:
            value = _input_call(row, candidates)
            if len(out["calls"]) >= MAX_CALLS: raise ValueError("collection capacity exceeded")
            value["source_index"] = index
            out["calls"].append(value)
        except ValueError as exc: withheld("calls", index, row, str(exc))
    return out


def _reported_number(value):
    # DynamoDB decimals are numeric; text and booleans are not measurements.
    from decimal import Decimal
    if type(value) not in (int, float, Decimal):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, OverflowError):
        return None


def reported_window(outcome):
    """Project explicit legacy reported values, never manufacture a grade.

    This checks types, not the price source, entry/exit timing, corporate actions,
    costs, statistical independence or out-of-sample methodology.
    """
    if not isinstance(outcome, dict):
        return None
    ret = _reported_number(outcome.get("return_pct"))
    reasons = []
    if ret is None:
        reasons.append("finite numeric reported return unavailable")
    correct = outcome.get("correct")
    if type(correct) is not bool:
        reasons.append("explicit boolean reported grade unavailable")
    if outcome.get("status") not in (None, "GRADED", "SCORED", "COMPLETED"):
        reasons.append("outcome status is not a completed report")
    valid = not reasons
    return {"return_pct": ret, "correct": correct if valid else None,
            "excess_return": _reported_number(outcome.get("excess_return")),
            "reported_grade_valid": valid, "withheld_reasons": reasons,
            "forecast_qualified": False, "sizing_eligible": False}


def performance_qualification():
    return {"contract": "market-read-reported-outcomes.v1", "window_unit": "calendar_day",
            "status": "legacy_reported_diagnostics", "forecast_qualified": False,
            "out_of_sample_verified": False, "cost_adjusted": False, "sizing_eligible": False,
            "reason": "Typed reported outcomes are descriptive. Source marks, point-in-time decisions, costs and out-of-sample performance remain unqualified."}


# ──────────────────────────────────────────────────────────────── ledger
def log_calls(table, read_id: str, calls: List[dict], log_signal, yprice) -> List[dict]:
    rows = []
    for c in calls:
        try:
            value = _input_call(c)
        except ValueError as exc:
            rows.append({"received_input": copy.deepcopy(c), "read_id": read_id,
                         "logged": False, "why": str(exc), "input_contract": "market-read-inputs.v1"})
            continue
        t = value["ticker"]
        try:
            px = _reported_number(yprice(t))
            if px is not None and px <= 0: px = None
        except Exception:
            px = None
        row = {**value, "read_id": read_id, "logged_at": now_iso(), "baseline_price": px,
               "input_contract": "market-read-inputs.v1", "received_input": copy.deepcopy(c),
               "source_index": c.get("source_index"),
               "signal_id": "%s#%s#%s" % (SIGNAL_TYPE, t, datetime.now(timezone.utc).date().isoformat())}
        if px is None:
            row["logged"] = False
            row["why"] = "no finite positive numeric price"
        else:
            try:
                row["logged"] = bool(log_signal(table, SIGNAL_TYPE, t, value["direction"], WINDOWS, px, confidence=value["confidence"], rationale=value["thesis"],
                                                metadata={"read_id": read_id, "horizon_days": value["horizon_days"], "engine": "justhodl-ai",
                                                          "input_contract": "market-read-inputs.v1", "source_index": c.get("source_index")}))
            except Exception as e:
                row["logged"] = False
                row["why"] = str(e)[:120]
        rows.append(row)
    return rows


def grade_calls(table, calls: List[dict]) -> Dict[str, Any]:
    """Pull each call's item back from the ledger: outcome-checker fills outcomes.day_N as windows mature."""
    graded, hits, n = [], {str(w): [0, 0] for w in WINDOWS}, 0
    unique = {}
    for c in calls:
        if c.get("signal_id") and c.get("logged") is not False:
            unique[c["signal_id"]] = c
    for c in list(unique.values())[-80:]:
        item = None
        try:
            item = table.get_item(Key={"signal_id": c["signal_id"]}).get("Item")
        except Exception:
            item = None
        oc = (item or {}).get("outcomes") or {}
        row = {k: c.get(k) for k in ("ticker", "direction", "horizon_days", "confidence", "logged_at", "read_id", "baseline_price", "thesis")}
        row["status"] = (item or {}).get("status") or ("not in ledger" if c.get("logged") is False else "pending")
        row["windows"] = {}
        row["withheld_windows"] = {}
        for w in WINDOWS:
            o = oc.get("day_%d" % w) if isinstance(oc, dict) else None
            projected = reported_window(o)
            if projected is not None:
                if projected["reported_grade_valid"]:
                    row["windows"][str(w)] = projected
                    hits[str(w)][0] += int(projected["correct"])
                    hits[str(w)][1] += 1
                else:
                    # Downstream lesson adapters consume windows. Invalid reports
                    # remain inspectable separately and cannot become lessons.
                    row["withheld_windows"][str(w)] = projected
        graded.append(row)
        n += 1
    summary = {str(w): {"hits": hits[str(w)][0], "n": hits[str(w)][1], "hit_rate": round(hits[str(w)][0] / hits[str(w)][1], 3) if hits[str(w)][1] else None} for w in WINDOWS}
    return {"n_calls": n, "by_window": summary, "rows": graded, "qualification": performance_qualification()}
