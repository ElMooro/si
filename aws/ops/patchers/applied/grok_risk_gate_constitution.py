#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-risk-gate/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("risk-gate already overlays constitution")
        return 0
    a = "import json\n"
    if "from consume_brain import" not in t:
        t = t.replace(a, a + "from consume_brain import load_constitution, overlay_payload\n", 1)
    a = '''        "elapsed_s": round(time.time() - t0, 1),
    }
    artifact = json.dumps(out, default=str, allow_nan=False)
'''
    b = '''        "elapsed_s": round(time.time() - t0, 1),
    }
    out = overlay_payload(out, load_constitution(s3))
    artifact = json.dumps(out, default=str, allow_nan=False)
'''
    if a not in t:
        raise SystemExit("risk-gate out block miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
