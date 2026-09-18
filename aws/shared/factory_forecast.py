"""One forecast contract for the owned model, shared by the market exam (scripts/factory_market_exam.py) and the
student's Monday wall post (justhodl-ai wall_post.py): the anonymized-bars prompt, the strict-JSON answer parser, and
the wall's probability-vector form. 2026-09-18."""
from __future__ import annotations

import json
import re

from factory_core import DIRECTIONS, REGIMES

SYSTEM = ("You are a market analyst sitting an exam. You see 20 daily bars of one instrument, rebased to 100 at the first open, "
          "with relative volume. You do not know the instrument or the dates. Answer STRICT JSON only: "
          '{"direction": "UP|DOWN|FLAT", "regime": "RANGE|TRANSITION|TREND", "crisis_probability": 0.0-1.0, "why": "<one or two sentences>"}. '
          "direction = where the close will be after the next 5 sessions versus the next open, beyond the flat threshold; "
          "regime = how the next 5 sessions will move (RANGE = choppy, TREND = efficient move, TRANSITION = between); "
          "crisis_probability = the chance the next 5 sessions print a close-to-close drawdown beyond the crisis threshold.")


def bars_prompt(bars: list, asset_class: str, flat_threshold, crisis_threshold, horizon: int = 5) -> str:
    rows = ["bar,open,high,low,close,vol_rel"] + ["%d,%s,%s,%s,%s,%s" % (i + 1, b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v_rel")) for i, b in enumerate(bars)]
    return ("Asset class: %s. Flat threshold: %s (fraction). Crisis drawdown threshold: %s (fraction). Horizon: next %s sessions.\n\n%s\n\nRespond with the JSON object only."
            % (asset_class, flat_threshold, crisis_threshold, horizon, "\n".join(rows)))


def rebase(rows: list) -> list:
    """Raw OHLCV rows (o/h/l/c/v) -> anonymized bars: prices rebased to 100 at the first open, volume relative to the mean."""
    if not rows:
        return []
    base = float(rows[0]["o"]) or 1.0
    vols = [float(r.get("v") or 0) for r in rows]
    mean_v = (sum(vols) / len(vols)) if any(vols) else 1.0
    out = []
    for r, v in zip(rows, vols):
        out.append({k: round(100.0 * float(r[k]) / base, 3) for k in ("o", "h", "l", "c")} | {"v_rel": round(v / mean_v, 3) if mean_v else None})
    return out


def qwen_prompt(system: str, user: str) -> str:
    return "<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n" % (system, user)


def parse_answer(text: str):
    m = re.search(r"\{.*\}", str(text or ""), re.S)
    if not m:
        return None
    try:
        j = json.loads(m.group(0))
    except Exception:
        return None
    if not isinstance(j, dict):
        return None
    direction = re.sub(r"[^A-Z]", "", str(j.get("direction") or "").upper())
    regime = re.sub(r"[^A-Z]", "", str(j.get("regime") or "").upper())
    direction = {"BULLISH": "UP", "BEARISH": "DOWN", "SIDEWAYS": "FLAT", "NEUTRAL": "FLAT"}.get(direction, direction)
    regime = {"CHOPPY": "RANGE", "TRENDING": "TREND", "RANGING": "RANGE"}.get(regime, regime)
    if direction not in DIRECTIONS or regime not in REGIMES:
        return None
    try:
        crisis = float(j.get("crisis_probability", j.get("crisis", 0.0)))
        if isinstance(j.get("crisis"), bool) and "crisis_probability" not in j:
            crisis = 0.8 if j["crisis"] else 0.1
    except Exception:
        return None
    if not (0.0 <= crisis <= 1.0):
        crisis = crisis / 100.0 if 1.0 < crisis <= 100.0 else 0.5
    return {"direction": direction, "regime": regime, "crisis_probability": round(crisis, 4), "why": str(j.get("why") or "")[:400]}


def as_prediction(answer: dict, confidence: float = 0.7) -> dict:
    """The wall's full contract: labels plus probability vectors (derived when the voice gives labels only)."""
    rest = round((1.0 - confidence) / 2.0, 6)
    return {"direction": answer["direction"], "regime": answer["regime"], "crisis_probability": answer["crisis_probability"],
            "direction_probabilities": {d: (round(confidence, 6) if d == answer["direction"] else rest) for d in DIRECTIONS},
            "regime_probabilities": {r: (round(confidence, 6) if r == answer["regime"] else rest) for r in REGIMES}}
