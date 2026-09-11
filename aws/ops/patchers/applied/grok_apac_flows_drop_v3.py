#!/usr/bin/env python3
"""Shell-lane only. Drops the dead FMP /api/v3 fallback in justhodl-apac-flows.
Does not belong in aws/ops/pending (would cancel the ops queue).

  python3 aws/ops/staged/grok_apac_flows_drop_v3.py
  python3 -m py_compile aws/lambdas/justhodl-apac-flows/source/lambda_function.py
  git add aws/lambdas/justhodl-apac-flows/source/lambda_function.py
  git commit -m "apac-flows: drop dead FMP /api/v3 fallback after /stable/"
  git pull --rebase origin main && git push origin main
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TARGET = ROOT / "lambdas/justhodl-apac-flows/source/lambda_function.py"
OLD = (
    '        for url in ("https://financialmodelingprep.com/stable/stock-price-change?symbol=%s&apikey=%s" % (sym, key),\n'
    '                    "https://financialmodelingprep.com/api/v3/stock-price-change/%s?apikey=%s" % (sym, key)):\n'
)
NEW = (
    '        for url in ("https://financialmodelingprep.com/stable/stock-price-change?symbol=%s&apikey=%s" % (sym, key),):\n'
)

def main():
    text = TARGET.read_text()
    if "api/v3/stock-price-change" not in text:
        print("already clean:", TARGET)
        return 0
    if OLD not in text:
        raise SystemExit("anchor miss — file drifted; edit by hand")
    TARGET.write_text(text.replace(OLD, NEW, 1))
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
