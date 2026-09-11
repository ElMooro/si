#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-morning-intelligence/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "data/brain-constitution.json" in t:
        print("morning already on constitution")
        return 0
    a = '        "brain":"data/brain.json",'
    b = '        "brain":"data/brain-constitution.json",'
    if a not in t:
        raise SystemExit("morning brain map miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
