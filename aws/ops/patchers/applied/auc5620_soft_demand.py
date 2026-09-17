#!/usr/bin/env python3
"""ops 5620 — rename p_failed_auction_30d even if indent differs."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-auction-crisis-detector/source/auction_crisis_v2.py"


def main() -> None:
    t = TARGET.read_text()
    if "p_soft_demand_30d" in t and "p_failed_auction_30d" not in t:
        print("already renamed")
        return
    if "p_failed_auction_30d" not in t:
        raise SystemExit("p_failed_auction_30d not in file")
    t2 = t.replace("P_failed_auction_30d", "P_soft_demand_30d")
    t2 = t2.replace("p_failed_auction_30d", "p_soft_demand_30d")
    if "ops 5620 soft-demand" not in t2:
        t2 = t2.replace(
            '"p_soft_demand_30d": {',
            '# ops 5620 soft-demand: BTC<2 or AAH>95 is weak demand, not a failed auction\n        "p_soft_demand_30d": {',
            1,
        )
    compile(t2, str(TARGET), "exec")
    TARGET.write_text(t2)
    print("renamed p_failed_auction_30d -> p_soft_demand_30d", len(t2))


if __name__ == "__main__":
    main()
