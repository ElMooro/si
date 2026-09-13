#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-ai/source/market_read.py"
OLD = '''def deterministic_read(board):
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
'''
NEW = '''from deterministic_desk import desk_read

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
'''

def main():
    t = TARGET.read_text()
    if "from deterministic_desk import desk_read" in t:
        print("already clean")
        return 0
    if OLD not in t:
        raise SystemExit("deterministic_read block changed")
    t = t.replace(OLD, NEW, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
