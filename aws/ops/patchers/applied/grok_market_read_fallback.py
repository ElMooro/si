#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-ai/source/market_read.py"
FN = '''
def deterministic_read(board):
    """When the voice is silent, still publish an engine-grounded read."""
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

'''

def main():
    t = TARGET.read_text()
    if "def deterministic_read(" in t:
        print("fallback already present")
        return 0
    t = t.replace("def compose_read(", FN + "def compose_read(", 1)
    a = '''    txt = str(raw or "").strip()
    if not txt:
        return {"parse_error": True, "raw": "", "empty": True}
'''
    b = '''    txt = str(raw or "").strip()
    if not txt:
        fb = deterministic_read(board)
        fb["parse_error"] = False
        fb["empty"] = True
        fb["fallback"] = True
        return fb
'''
    if a not in t:
        raise SystemExit("compose empty-branch miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
