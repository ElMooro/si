"""ops 2691 — use the mariostoev/finviz package's load_filter_dict() to get the
authoritative, live filter-code list directly from FinViz, to resolve the correct
codes for Triangle patterns and Price-crossed-SMA (my guesses returned unfiltered
results, confirming wrong syntax rather than erroring)."""
import subprocess
subprocess.run(["pip", "install", "finviz", "--break-system-packages", "-q"], check=False)
from finviz.screener import Screener
filters = Screener.load_filter_dict()
print("total filter categories:", len(filters))
for k in filters:
    if "pattern" in k.lower() or "sma" in k.lower() or "triangle" in k.lower():
        print(f"\n=== {k} ===")
        v = filters[k]
        if isinstance(v, dict):
            for opt, code in v.items():
                if "triangle" in str(opt).lower() or "triangle" in str(code).lower() or \
                   "price crossed" in str(opt).lower() or "price" in str(opt).lower() and "sma" in str(opt).lower():
                    print(f"  '{opt}' -> {code}")
print("DONE 2691")
