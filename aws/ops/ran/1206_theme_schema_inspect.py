"""1206 — Inspect actual theme-rotation.json schema to fix the cascade parser.

Last run had themes_tracked=0 because compute_theme_heat() couldn't parse
the real field names. Need to see exactly what fields exist.
"""
import json
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1206_theme_schema_inspect.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def read_safe(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:200]}


# theme-rotation.json structure
print("[1206] 1. data/theme-rotation.json — FULL STRUCTURE")
tr = read_safe("data/theme-rotation.json")
if not tr.get("_error"):
    out["theme_rotation"] = {
        "top_keys": list(tr.keys())[:30] if isinstance(tr, dict) else None,
    }
    print(f"  Top keys: {list(tr.keys())[:30]}")
    # Inspect each section
    for k in list(tr.keys())[:20]:
        v = tr[k]
        if isinstance(v, dict):
            sub_keys = list(v.keys())[:10]
            print(f"\n  {k}: dict with keys {sub_keys}")
            # If looks like theme keys, show one sample
            if sub_keys:
                first_key = sub_keys[0]
                if isinstance(v[first_key], dict):
                    print(f"    sample {first_key}: keys={list(v[first_key].keys())[:25]}")
                    print(f"      content: {json.dumps(v[first_key], default=str)[:400]}")
                else:
                    print(f"    sample {first_key}: {str(v[first_key])[:200]}")
            out.setdefault("theme_rotation_details", {})[k] = {
                "type": "dict", "keys": sub_keys,
                "sample": v[sub_keys[0]] if sub_keys else None,
            }
        elif isinstance(v, list):
            print(f"\n  {k}: list [{len(v)}]")
            if v:
                if isinstance(v[0], dict):
                    print(f"    sample[0] keys: {list(v[0].keys())[:25]}")
                    print(f"      content: {json.dumps(v[0], default=str)[:400]}")
                else:
                    print(f"    sample[0]: {str(v[0])[:200]}")
                if len(v) > 1 and isinstance(v[1], dict):
                    print(f"    sample[1]: {json.dumps(v[1], default=str)[:300]}")
            out.setdefault("theme_rotation_details", {})[k] = {
                "type": "list", "n_items": len(v),
                "sample": v[0] if v else None,
            }
        else:
            print(f"\n  {k}: {str(v)[:150]}")
            out.setdefault("theme_rotation_details", {})[k] = {
                "type": type(v).__name__, "value": str(v)[:200],
            }
else:
    print(f"  ❌ {tr['_error']}")


# themes.json structure
print(f"\n\n[1206] 2. data/themes.json — FULL STRUCTURE")
th = read_safe("data/themes.json")
if not th.get("_error"):
    out["themes_doc"] = {
        "top_keys": list(th.keys())[:30] if isinstance(th, dict) else None,
    }
    print(f"  Top keys: {list(th.keys())[:30]}")
    if isinstance(th, dict):
        themes_data = th.get("themes")
        if isinstance(themes_data, dict):
            theme_keys = list(themes_data.keys())[:15]
            print(f"\n  themes dict keys: {theme_keys}")
            if theme_keys:
                first = theme_keys[0]
                print(f"    sample '{first}': {json.dumps(themes_data[first], default=str)[:500]}")
            out["themes_sample"] = {k: themes_data[k] for k in theme_keys[:3]}
        elif isinstance(themes_data, list):
            print(f"\n  themes list [{len(themes_data)}]")
            if themes_data:
                print(f"    sample[0]: {json.dumps(themes_data[0], default=str)[:500]}")
            out["themes_sample"] = themes_data[:3]


# velocity-acceleration: how is theme info attached to tickers?
print(f"\n\n[1206] 3. velocity-acceleration.json — theme fields on tickers")
va = read_safe("data/velocity-acceleration.json")
if not va.get("_error"):
    # Find any tier with items
    for tier in ["fresh_fires", "confirmed_today", "aging", "emerging", "watch"]:
        items = va.get(tier) or []
        if items:
            sample = items[0]
            theme_fields = {k: v for k, v in sample.items()
                             if "theme" in k.lower() or "industry" in k.lower() or "sector" in k.lower()}
            print(f"  {tier}[0] theme fields: {theme_fields}")
            print(f"  {tier}[0] all keys: {list(sample.keys())[:30]}")
            out.setdefault("velocity_theme_fields", {})[tier] = {
                "theme_fields": theme_fields,
                "all_keys": list(sample.keys())[:30],
            }
            break


# stock-exposure-lookup
print(f"\n\n[1206] 4. etf-flows/stock-exposure-lookup.json — schema for a known ticker")
sel = read_safe("etf-flows/stock-exposure-lookup.json")
if not sel.get("_error"):
    if isinstance(sel, dict):
        # Try a few likely tickers
        for t in ["MRVL", "AVGO", "NVDA", "ARM", "PANW"]:
            if t in sel:
                print(f"  {t}: keys={list(sel[t].keys())[:20]}")
                print(f"    content: {json.dumps(sel[t], default=str)[:500]}")
                out.setdefault("exposure_sample", {})[t] = sel[t]
                break
        # Or if it has metadata wrapper
        if "tickers" in sel or "stocks" in sel:
            sub = sel.get("tickers") or sel.get("stocks")
            print(f"  wrapped in tickers/stocks: keys={list(sub.keys())[:10] if isinstance(sub, dict) else 'list'}")
        print(f"\n  top-level keys: {list(sel.keys())[:15]}")


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1206] DONE")
