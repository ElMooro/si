"""Ops 1118: Fix heartbeat feed paths (cboe-options, macro-regime)."""
from __future__ import annotations
import os

SRC = os.path.join("aws", "lambdas", "justhodl-feed-heartbeat", "source", "lambda_function.py")

def main():
    with open(SRC) as f:
        content = f.read()
    # Fix cboe-options path
    old1 = "('cboe-options', 'data/cboe-options.json', 60, False)"
    new1 = "('cboe-options', 'data/cboe-options-chain.json', 60, False)"
    # Fix macro-regime path (use cross-asset-regime which exists)
    old2 = "('macro-regime', 'data/macro-regime.json', 1440, False)"
    new2 = "('macro-regime', 'data/cross-asset-regime.json', 1440, False)"
    if old1 not in content:
        print("cboe pattern not found")
        return
    if old2 not in content:
        print("macro pattern not found")
        return
    content = content.replace(old1, new1).replace(old2, new2)
    with open(SRC, "w") as f:
        f.write(content)
    print("Fixed heartbeat feed paths")

if __name__ == "__main__":
    main()
