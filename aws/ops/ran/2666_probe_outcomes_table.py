"""ops 2666 — probe justhodl-outcomes DynamoDB table: exact schema, ticker field name,
scale, date range, and distinct signal_type count — the raw data for signal genealogy."""
import boto3, json
from collections import Counter
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table("justhodl-outcomes")

# small sample first
resp = table.scan(Limit=5)
items = resp.get("Items", [])
print(f"=== sample records ({len(items)}) ===")
for it in items:
    print(json.dumps(it, default=str, indent=1)[:600])
    print("---")

print("\n=== full scan for scale + schema stats (paginated) ===")
all_items, kwargs, pages = [], {}, 0
while True:
    r = table.scan(**kwargs)
    all_items.extend(r.get("Items", []))
    pages += 1
    lek = r.get("LastEvaluatedKey")
    if not lek or pages > 40:
        break
    kwargs["ExclusiveStartKey"] = lek

print(f"total items scanned: {len(all_items)} across {pages} pages")
sig_types = Counter(it.get("signal_type") for it in all_items)
print(f"\ndistinct signal_types: {len(sig_types)}")
for st, n in sig_types.most_common(30):
    print(f"  {st}: {n}")

tick_field = None
for f in ["symbol","ticker","name"]:
    if all_items and f in all_items[0]:
        tick_field = f; break
print(f"\nticker field name guess: {tick_field}")
tickers = Counter(it.get(tick_field) for it in all_items[:2000]) if tick_field else {}
print(f"distinct tickers (first 2000 sample): {len(tickers)}")

dates = sorted(it.get("logged_at","") for it in all_items if it.get("logged_at"))
print(f"\ndate range: {dates[0] if dates else '?'} to {dates[-1] if dates else '?'}")
print("DONE 2666")
