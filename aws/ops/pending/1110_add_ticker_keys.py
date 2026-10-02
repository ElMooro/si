"""Ops 1110: Add missing ticker list keys to ticker_360 hub.
Adds big_buys, clusters, upcoming_14d, and other producer-specific keys
so _extract_ticker can find ticker data in insider, earnings, etc.
"""
from __future__ import annotations
import os

SRC = os.path.join("aws", "shared", "ticker_360.py")

def main():
    with open(SRC) as f:
        content = f.read()

    old = '''_TICKER_LIST_KEYS = ("by_ticker", "tickers", "stocks", "rows", "items",
                     "board", "squeeze_candidates", "top_squeeze",
                     "top_covering", "top_distribution", "top_crowded",
                     "top_accumulation", "candidates", "setups", "names",
                     "top_picks", "data", "tickers_list")'''

    new = '''_TICKER_LIST_KEYS = ("by_ticker", "tickers", "stocks", "rows", "items",
                     "board", "squeeze_candidates", "top_squeeze",
                     "top_covering", "top_distribution", "top_crowded",
                     "top_accumulation", "candidates", "setups", "names",
                     "top_picks", "data", "tickers_list",
                     # ops 1110: producer-specific ticker lists
                     "big_buys", "big_sells", "clusters",
                     "upcoming_14d", "upcoming", "earnings",
                     "positions", "holdings")'''

    if old not in content:
        print("PATTERN NOT FOUND - manual review needed")
        return

    content = content.replace(old, new)
    with open(SRC, "w") as f:
        f.write(content)
    print("Patched _TICKER_LIST_KEYS with 8 additional keys")

if __name__ == "__main__":
    main()
