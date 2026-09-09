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
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

SIGNAL_TYPE = "ai_market_read"
WINDOWS = [5, 21, 63]
MAX_CALLS = 6
MIN_READ_GAP_S = 20 * 60

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
        return round((datetime.now(timezone.utc) - dt).total_seconds() / 3600, 1)
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
        if ok and cur not in (None, "", [], {}):
            return cur
    return default


def _num(v):
    try:
        return round(float(v), 4)
    except Exception:
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
def build_board(s3, public_bucket: str) -> Dict[str, Any]:
    docs, sources = {}, {}
    for name, (key, sla_h, private) in SOURCES.items():
        d = _get(s3, public_bucket, key)
        st = _stamp(d)
        age = _age_h(st)
        status = "MISSING" if d is None else ("STALE" if (age is None or age > sla_h) else "FRESH")
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
        "katlin_prime": [{"ticker": _tk(r), "tier": r.get("tier"), "score": _num(r.get("score") or r.get("katlin_score")), "desk": r.get("desk") or r.get("asset_class")} for r in _rows(KT.get("picks"), 340) if r.get("tier") == "KATLIN_PRIME"][:10],
        "katlin_ready": [{"ticker": _tk(r), "tier": r.get("tier"), "score": _num(r.get("score") or r.get("katlin_score")), "desk": r.get("desk") or r.get("asset_class")} for r in _rows(KT.get("picks"), 340) if r.get("tier") == "READY"][:10],
        "bottom_actionable": [{"ticker": _tk(r), "state": r.get("state"), "score": _num(r.get("score")), "desk": r.get("desk"), "grade": r.get("grade")} for r in _rows(BT.get("top_picks"), 12)],
        "bottom_breadth": {k: _num(v) if isinstance(v, (int, float)) else v for k, v in breadth.items()} if isinstance(breadth, dict) else breadth,
        "bottom_read": pick(BT, "market.read"),
        "fortress_top": [{"ticker": _tk(r), "score": _num(r.get("score") or r.get("fortress_score")), "state": r.get("state") or r.get("verdict")} for r in _rows(pick(FT, "top", "board", "top_picks", "rows"), 8)],
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
    frows.sort(key=lambda r: -(abs(r["fusion_score"]) * (r["confidence"] or 0.5)))
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
            out["notes"][name] = [{"similarity": n["similarity"], "label": n["label"], "pinned": n["pinned"], "text": n["text"][:280]} for n in nearest_fn(s3, private_bucket, ds_id, endpoint, vec, k=k)]
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


def compose_read(board: Dict[str, Any], play: Dict[str, Any], complete_fn) -> Dict[str, Any]:
    slim = json.loads(json.dumps(board, default=str))
    slim.pop("candidates", None)
    prompt = "BOARD (fleet artifacts, with freshness):\n%s\n\nPLAYBOOK (operator's nearest notes per setup):\n%s\n\nCANDIDATES (only tickers allowed in opportunities/calls):\n%s\n\nProduce the JSON." % (
        json.dumps(slim, default=str)[:24000], json.dumps(play.get("notes") or {"unavailable": play.get("reason")}, default=str)[:9000], ", ".join(board.get("candidates") or []))
    raw = complete_fn(prompt, tier="critical", max_tokens=2400, contains_proprietary=True, system=SYSTEM, on_demand=True, no_cache=True)
    txt = str(raw or "").strip()
    if not txt:
        return {"parse_error": True, "raw": "", "empty": True}
    m = re.search(r"\{.*\}", txt, re.S)
    try:
        j = json.loads(m.group(0) if m else txt)
    except Exception:
        return {"parse_error": True, "raw": txt[:2000]}
    cands = set(board.get("candidates") or [])
    j["calls"] = [c for c in (j.get("calls") or []) if isinstance(c, dict) and c.get("ticker") in cands and c.get("direction") in ("UP", "DOWN")][:MAX_CALLS]
    j["best_opportunities"] = [o for o in (j.get("best_opportunities") or []) if isinstance(o, dict) and o.get("ticker") in cands][:8]
    j["rejected_tickers"] = sorted({c.get("ticker") for c in (j.get("calls_raw") or []) if isinstance(c, dict)} - cands) if j.get("calls_raw") else []
    return j


# ──────────────────────────────────────────────────────────────── ledger
def log_calls(table, read_id: str, calls: List[dict], log_signal, yprice) -> List[dict]:
    rows = []
    for c in calls:
        t = c["ticker"]
        px = None
        try:
            px = yprice(t)
        except Exception:
            px = None
        row = {"ticker": t, "direction": c["direction"], "horizon_days": int(c.get("horizon_days") or 21), "confidence": float(c.get("confidence") or 0.6),
               "thesis": str(c.get("thesis") or "")[:240], "read_id": read_id, "logged_at": now_iso(), "baseline_price": px,
               "signal_id": "%s#%s#%s" % (SIGNAL_TYPE, t, datetime.now(timezone.utc).date().isoformat())}
        if px is None:
            row["logged"] = False
            row["why"] = "no price"
        else:
            try:
                row["logged"] = bool(log_signal(table, SIGNAL_TYPE, t, c["direction"], WINDOWS, px, confidence=row["confidence"], rationale=row["thesis"],
                                                metadata={"read_id": read_id, "horizon_days": row["horizon_days"], "engine": "justhodl-ai"}))
            except Exception as e:
                row["logged"] = False
                row["why"] = str(e)[:120]
        rows.append(row)
    return rows


def grade_calls(table, calls: List[dict]) -> Dict[str, Any]:
    """Pull each call's item back from the ledger: outcome-checker fills outcomes.day_N as windows mature."""
    graded, hits, n = [], {str(w): [0, 0] for w in WINDOWS}, 0
    for c in calls[-80:]:
        item = None
        try:
            item = table.get_item(Key={"signal_id": c["signal_id"]}).get("Item")
        except Exception:
            item = None
        oc = (item or {}).get("outcomes") or {}
        row = {k: c.get(k) for k in ("ticker", "direction", "horizon_days", "confidence", "logged_at", "read_id", "baseline_price", "thesis")}
        row["status"] = (item or {}).get("status") or ("not in ledger" if c.get("logged") is False else "pending")
        row["windows"] = {}
        for w in WINDOWS:
            o = oc.get("day_%d" % w) if isinstance(oc, dict) else None
            if isinstance(o, dict) and (o.get("return_pct") is not None or o.get("price") is not None):
                ret = _num(o.get("return_pct"))
                if ret is None and o.get("price") and c.get("baseline_price"):
                    ret = round((float(o["price"]) / float(c["baseline_price"]) - 1) * 100, 3)
                correct = o.get("correct")
                if correct is None and ret is not None:
                    correct = (ret > 0) if c["direction"] == "UP" else (ret < 0)
                row["windows"][str(w)] = {"return_pct": ret, "correct": bool(correct) if correct is not None else None, "excess_return": _num(o.get("excess_return"))}
                if correct is not None:
                    hits[str(w)][0] += int(bool(correct))
                    hits[str(w)][1] += 1
        graded.append(row)
        n += 1
    summary = {str(w): {"hits": hits[str(w)][0], "n": hits[str(w)][1], "hit_rate": round(hits[str(w)][0] / hits[str(w)][1], 3) if hits[str(w)][1] else None} for w in WINDOWS}
    return {"n_calls": n, "by_window": summary, "rows": graded}
