"""Inspect universe.json schema + fix the audit reader if needed.
Also confirm the compound find of FCX (tier-3) is real — that's the big news.
"""
import json, time, boto3
S3 = boto3.client("s3", region_name="us-east-1")

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Inspect universe.json top-level schema")
    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/universe.json")
    body = obj["Body"].read()
    log(f"  size: {len(body):,}b")
    d = json.loads(body)
    log(f"  type: {type(d).__name__}")
    if isinstance(d, dict):
        log(f"  keys: {sorted(list(d.keys()))[:15]}")
        for k in sorted(d.keys()):
            v = d[k]
            if isinstance(v, list):
                log(f"    {k}: list[{len(v)}]")
                if v and isinstance(v[0], dict):
                    log(f"      first item keys: {sorted(list(v[0].keys()))[:15]}")
            elif isinstance(v, dict):
                log(f"    {k}: dict[{len(v)}]")
            else:
                log(f"    {k}: {type(v).__name__} = {str(v)[:60]}")
    elif isinstance(d, list):
        log(f"  list of {len(d)}, first item: {d[0] if d else None}")

    section("2) Pull all tickers from universe with a robust reader")
    tickers = set()
    if isinstance(d, dict):
        # Try multiple paths
        for key in ["records", "universe", "tickers", "stocks", "data", "all_qualifying"]:
            v = d.get(key)
            if isinstance(v, list):
                for r in v:
                    if isinstance(r, dict):
                        sym = r.get("symbol") or r.get("ticker")
                        if sym:
                            tickers.add(sym.upper())
                    elif isinstance(r, str):
                        tickers.add(r.upper())
                if tickers:
                    log(f"  found {len(tickers)} via key '{key}'")
                    break
        # If still empty and dict — maybe it's keyed by ticker
        if not tickers:
            for k, v in d.items():
                if isinstance(v, dict) and len(k) <= 6 and k.isupper():
                    tickers.add(k)
            if tickers:
                log(f"  found {len(tickers)} as top-level dict keys")

    section("3) Spot check key names")
    key_names = ["AAPL","MSFT","GOOGL","AMZN","NVDA","TSLA","CSGP","EPAM",
                 "FCX","OXY","CNC","HUM","MOH","LLY","AVGO","AMD","JPM"]
    present = [k for k in key_names if k in tickers]
    log(f"  present: {present}")
    log(f"  total in universe: {len(tickers)}")
    
    section("4) Confirm FCX tier-3 compound finding")
    cs = json.loads(S3.get_object(Bucket="justhodl-dashboard-live", Key="data/compound-signals.json")["Body"].read())
    for r in cs.get("compound", []):
        if r["symbol"] == "FCX":
            log(f"  ✓ FCX confirmed: n_systems={r['n_systems']}, compound={r['compound_score']}")
            log(f"    systems: {r['systems']}")
            for sys, det in (r.get("details") or {}).items():
                log(f"    {sys}: {json.dumps(det, default=str)[:200]}")
            break
    else:
        log("  ⚠ FCX not in compound output")


if __name__ == "__main__":
    main()
    import os
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "inspect_universe_schema.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
