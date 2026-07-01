"""ops 2667 — does ANY signal_type carry a ticker/symbol, and what's the full distinct
signal_type list (not just top 30)? This determines whether lead-lag can be per-ticker
or must be regime/macro-timeline only."""
import boto3, json
from collections import Counter
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table("justhodl-outcomes")
all_items, kwargs, pages = [], {}, 0
while True:
    r = table.scan(**kwargs)
    all_items.extend(r.get("Items", []))
    pages += 1
    lek = r.get("LastEvaluatedKey")
    if not lek or pages > 40: break
    kwargs["ExclusiveStartKey"] = lek
print(f"total: {len(all_items)}")

# check every possible ticker-ish key across ALL records
all_keys = Counter()
for it in all_items:
    for k in it.keys(): all_keys[k] += 1
    oc = it.get("outcome") or {}
    for k in oc.keys(): all_keys["outcome."+k] += 1
print("\nall keys seen across records + counts:")
for k, n in all_keys.most_common(40): print(f"  {k}: {n}")

print("\n=== eng: prefixed signal types specifically — do THEY carry a ticker? ===")
eng_items = [it for it in all_items if str(it.get("signal_type","")).startswith("eng:")]
print(f"eng: records: {len(eng_items)}")
eng_types = Counter(it.get("signal_type") for it in eng_items)
for st, n in eng_types.most_common(20): print(f"  {st}: {n}")
if eng_items:
    print("\nsample eng: record full dump:")
    print(json.dumps(eng_items[0], default=str, indent=1))

print("\n=== full distinct signal_type list (all 271) ===")
sig_types = Counter(it.get("signal_type") for it in all_items)
print(sorted(sig_types.keys()))
print("DONE 2667")
