#!/usr/bin/env python3
"""Add plumbing / fails / eurodollar concepts the compiler was missing."""
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-brain-compiler/source/lambda_function.py"
NEEDLE = '    "stablecoins":               (r"stablecoin|usdt|usdc", ["stablecoin", "usdt", "usdc"]),\n}'
INSERT = '''    "stablecoins":               (r"stablecoin|usdt|usdc", ["stablecoin", "usdt", "usdc"]),
    "settlement fails / FTD / FTR": (r"fail(s)? to (deliver|receive)|\\bftd\\b|\\bftr\\b|settlement fail", ["settlement-fails", "fails", "ftd", "ftr"]),
    "eurodollar shortage":       (r"eurodollar|offshore dollar|dollar shortage", ["eurodollar", "eurodollar-stress"]),
    "plumbing / OFR / NYPD":     (r"plumbing|ofr |nypd|dealer positioning", ["plumbing", "ofr", "nypd"]),
    "treasury rehypothecation":  (r"rehypothec", ["rehypo", "treasury-rehypo"]),
    "net liquidity WALCL/TGA/RRP": (r"net (system )?liquidity|walcl|wtregen|rrpontsyd", ["WALCL", "WTREGEN", "RRPONTSYD", "liquidity-pulse"]),
}
'''

def main():
    text = TARGET.read_text()
    if "settlement fails / FTD / FTR" in text:
        print("concepts already extended")
        return 0
    if NEEDLE not in text:
        raise SystemExit("compiler concept anchor miss")
    TARGET.write_text(text.replace(NEEDLE, INSERT, 1))
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
