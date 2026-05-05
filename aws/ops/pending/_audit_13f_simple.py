"""Quick audit of 13F data structure for smart-money planning."""
import json, os, time
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

try:
    section("13F data summary")
    head = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
    log(f"size: {head['ContentLength']:,}b  modified: {head['LastModified']}")

    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/13f-positions.json")
    data = json.loads(obj["Body"].read())
    log(f"top keys: {sorted(data.keys())}")
    log(f"funds_parsed: {data.get('funds_parsed')}")
    log(f"as_of_quarter: {data.get('as_of_quarter')}")

    by_fund = data.get("by_fund", {})
    log(f"funds count: {len(by_fund)}")
    for fname in list(by_fund.keys())[:20]:
        f = by_fund[fname]
        n_pos = f.get("n_positions") or len(f.get("top_positions", []))
        tv = (f.get("total_value_usd") or 0)/1e9
        log(f"  {fname:<25} {n_pos:>4} pos  ${tv:>6.1f}B AUM")

    # Try to compute per-stock buyer count from by_fund
    section("Per-stock smart-money signal (computed from by_fund)")
    from collections import defaultdict
    stock_actions = defaultdict(lambda: {"NEW": [], "ADD": [], "TRIM": [], "EXIT": [], "HOLD": []})
    for fname, f in by_fund.items():
        for pos in f.get("top_positions", []):
            tk = pos.get("ticker") or pos.get("name", "?")
            ch = pos.get("change", "HOLD")
            stock_actions[tk][ch].append((fname, pos.get("pct_of_portfolio", 0)))
    
    rank = []
    for tk, acts in stock_actions.items():
        n_new = len(acts["NEW"])
        n_add = len(acts["ADD"])
        n_trim = len(acts["TRIM"])
        n_exit = len(acts["EXIT"])
        buy_count = n_new + n_add
        sell_count = n_trim + n_exit
        if buy_count >= 2:  # at least 2 funds buying
            rank.append((tk, buy_count, sell_count, acts))
    rank.sort(key=lambda x: -x[1])
    
    log(f"  tickers with ≥2 fund buyers: {len(rank)}")
    log("")
    log(f"  ── Top 20 tickers by smart-money buyer count ──")
    log(f"    {'Ticker':<8} {'Buy':>3} {'Sell':>4}  Funds buying")
    for tk, b, s, acts in rank[:20]:
        funds_str = ", ".join(f[0] for f in (acts["NEW"] + acts["ADD"]))[:65]
        log(f"    {tk:<8} {b:>3} {s:>4}  {funds_str}")
    
    # Specific high-conviction: NEW initiations
    section("NEW initiations (a fund didn't own this last quarter, now does)")
    new_init = []
    for tk, acts in stock_actions.items():
        n_new = len(acts["NEW"])
        if n_new >= 1:
            new_init.append((tk, n_new, acts["NEW"]))
    new_init.sort(key=lambda x: -x[1])
    log(f"  tickers with ≥1 NEW initiation: {len(new_init)}")
    for tk, n, news in new_init[:15]:
        funds = ", ".join(f[0] for f in news)
        log(f"    {tk:<8} {n} initiations  by: {funds}")

except Exception as e:
    import traceback
    log(f"❌ {e}")
    log(traceback.format_exc()[:1500])

out = "aws/ops/reports/latest"
os.makedirs(out, exist_ok=True)
with open(os.path.join(out, "audit_13f_simple.md"), "w") as f:
    f.write("\n".join(REPORT))
print("[report written]")
