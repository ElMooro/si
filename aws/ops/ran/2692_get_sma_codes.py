"""ops 2692 — get the exact SMA20/50/200 category codes (my first grep missed these,
FinViz's category name is 'Simple Moving Average' not literally containing 'sma')."""
import subprocess
subprocess.run(["pip", "install", "finviz", "--break-system-packages", "-q"], check=False)
from finviz.screener import Screener
filters = Screener.load_filter_dict()
for k in filters:
    if "moving average" in k.lower() or "20-day" in k.lower() or "50-day" in k.lower() or "200-day" in k.lower():
        print(f"\n=== {k} ===")
        v = filters[k]
        if isinstance(v, dict):
            for opt, code in v.items():
                print(f"  '{opt}' -> {code}")
print("DONE 2692")
