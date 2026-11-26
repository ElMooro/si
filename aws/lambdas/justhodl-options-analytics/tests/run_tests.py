"""Dependency-free checks for the options engine's list of names (read from the source, nothing is imported or fetched)."""
import ast
import re
from pathlib import Path

SRC = (Path(__file__).resolve().parents[1] / "source" / "lambda_function.py").read_text(encoding="utf-8")
TREE = ast.parse(SRC)
UNIVERSE = next(ast.literal_eval(n.value) for n in TREE.body if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "UNIVERSE")
FIRST = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD", "AVGO", "NFLX", "MU", "INTC", "SMCI", "ARM", "PLTR", "CRM", "ORCL", "ADBE", "QCOM", "MRVL",
         "SPY", "QQQ", "IWM", "GME", "AMC", "COIN", "MARA", "RIOT", "MSTR", "HOOD", "SOFI", "RIVN", "LCID", "CVNA", "UPST", "AFRM", "RBLX", "SNAP", "DKNG", "CHWY",
         "ASTS", "RGTI", "IONQ", "BA", "DIS", "BAC", "JPM", "XOM", "CVX", "UNH", "LLY", "WMT", "F", "GM", "PYPL", "UBER", "NKE", "DELL"]


def check(cond, msg):
    if not cond:
        raise SystemExit("FAIL " + msg)


def main():
    check(UNIVERSE[:58] == FIRST, "the first 58 names are as they were, in their order")
    check(len(UNIVERSE) == len(set(UNIVERSE)) == 155, "155 names, none twice (%d, %d)" % (len(UNIVERSE), len(set(UNIVERSE))))
    check(all(re.fullmatch(r"[A-Z]{1,5}", t) for t in UNIVERSE), "plain US tickers only")
    for fund in ("DIA", "XLF", "XLE", "XLK", "TLT", "HYG", "LQD", "GLD", "USO", "EEM", "IBIT"):
        check(fund in UNIVERSE, fund + " is covered")
    check('"not_analyzed": sorted(set(UNIVERSE) - {r["ticker"] for r in rows})' in SRC, "a name that returned nothing is listed, not dropped in silence")
    check('"universe_size": len(UNIVERSE)' in SRC and '"version": "1.1.0"' in SRC, "the document says how many names and which version")
    print("OK options universe: %d names" % len(UNIVERSE))


if __name__ == "__main__":
    main()
