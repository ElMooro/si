#!/usr/bin/env python3
"""ops 5617 — auction-crisis: stop calling weak BTC / high AAH a failed auction."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-auction-crisis-detector/source/auction_crisis_v2.py"


def main() -> None:
    t = TARGET.read_text()
    if "ops 5617 soft-demand" in t:
        print("already patched")
        return
    old = '        "p_failed_auction_30d": {'
    new = '        # ops 5617 soft-demand: BTC<2 or AAH>95 is weak demand, not a failed auction\n        "p_soft_demand_30d": {'
    if old not in t:
        raise SystemExit("p_failed_auction_30d block not found")
    t = t.replace(old, new, 1)
    t = t.replace("P_failed_auction_30d", "P_soft_demand_30d")
    t = t.replace("p_failed_auction_30d", "p_soft_demand_30d")
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched", TARGET, len(t))


if __name__ == "__main__":
    main()
