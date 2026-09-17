#!/usr/bin/env python3
"""ops 5618 — auction-desk: buybacks are TGA cash-out, not an easing call."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-auction-desk/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    if "ops 5618 buyback wording" in t:
        print("already patched")
        return
    pairs = [
        ('as a liquidity injection ("easy-policy signal"), risk-asset supportive.',
         'as a TGA cash-out (ops 5618 buyback wording), not an easing or duration-bid call.'),
        ("easy-policy signal", "tga-cash-out signal"),
        ("EASY-POLICY", "TGA-CASH-OUT"),
        ("RISK-ASSET BULLISH", "TGA CASH-OUT; NOT AN EASING CALL"),
    ]
    n = 0
    for a, b in pairs:
        if a in t:
            t = t.replace(a, b)
            n += 1
    if n == 0:
        raise SystemExit("no buyback wording strings found")
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched replacements", n, "bytes", len(t))


if __name__ == "__main__":
    main()
