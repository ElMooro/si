"""
Audit existing 13F infrastructure to plan the smart-money cluster scanner:
- What's actually in data/13f-positions.json?
- Which funds are parsed?
- What's the per-stock aggregate look like (buyers count per stock)?
- How fresh is the data?
"""
import json, os, time
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) S3 13F freshness")
    head = S3.head_object(Bucket=BUCKET, Key="data/13f-positions.json")
    log(f"  size: {head['ContentLength']:,}b")
    log(f"  modified: {head['LastModified']}")

    section("2) Top-level structure")
    obj = S3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
    data = json.loads(obj["Body"].read())
    log(f"  top keys: {sorted(data.keys())}")
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  as_of_quarter: {data.get('as_of_quarter')}")
    log(f"  funds_parsed: {data.get('funds_parsed')}")
    log(f"  funds_failed: {data.get('funds_failed')}")
    by_fund = data.get("by_fund", {})
    log(f"  funds count: {len(by_fund)}")

    section("3) Funds list")
    for fname, f in by_fund.items():
        n_pos = f.get("n_positions") or len(f.get("top_positions", []))
        tv = f.get("total_value_usd") or 0
        filed = f.get("filed_at", "?")
        log(f"  {fname:<25} {n_pos:>4} pos  ${tv/1e9:>7.1f}B AUM  filed {filed}")

    section("4) Per-stock aggregate (smart-money signal source)")
    # Look for per-stock aggregate field
    by_stock = data.get("by_stock") or data.get("by_ticker") or {}
    if by_stock:
        log(f"  per-stock entries: {len(by_stock)}")
        # Show top buyers
        tickers_with_buyers = []
        for tk, info in by_stock.items():
            if isinstance(info, dict):
                buyers = info.get("n_buyers") or info.get("n_adds") or 0
                new_buyers = info.get("n_new") or 0
                exits = info.get("n_exits") or 0
                tickers_with_buyers.append((tk, buyers + new_buyers, info))
        tickers_with_buyers.sort(key=lambda x: -x[1])
        log("")
        log(f"  ── Top 15 tickers by buyer count (NEW + ADD) ──")
        for tk, n, info in tickers_with_buyers[:15]:
            log(f"    {tk:<8} {n} smart-money buyers   info: {json.dumps({k:v for k,v in info.items() if k != 'fund_actions'})[:200]}")
    else:
        log("  ⚠ no by_stock aggregate field — must compute from by_fund")
        log("")
        # Compute from by_fund
        from collections import defaultdict
        actions = defaultdict(lambda: {"NEW": [], "ADD": [], "TRIM": [], "EXIT": [], "HOLD": []})
        for fname, f in by_fund.items():
            for pos in f.get("top_positions", []):
                tk = pos.get("ticker") or pos.get("cusip", "?")
                ch = pos.get("change", "HOLD")
                actions[tk][ch].append((fname, pos))
        # Tickers with most NEW or ADD across funds
        rank = []
        for tk, acts in actions.items():
            buyers = acts["NEW"] + acts["ADD"]
            sellers = acts["TRIM"] + acts["EXIT"]
            net = len(buyers) - len(sellers)
            rank.append((tk, len(buyers), len(sellers), net, acts))
        rank.sort(key=lambda x: -x[1])
        log("")
        log(f"  ── Top 15 tickers by # funds with NEW or ADD action ──")
        log(f"    {'Ticker':<8} {'Buyers':>6} {'Sellers':>7} {'Net':>4}  Funds buying")
        for tk, b, s, net, acts in rank[:15]:
            buyers = acts["NEW"] + acts["ADD"]
            funds_str = ", ".join(f[0] for f in buyers[:5])
            log(f"    {tk:<8} {b:>6} {s:>7} {net:>+4}  {funds_str}")

    section("5) Sample fund — top 5 positions")
    sample_fund = next(iter(by_fund.keys())) if by_fund else None
    if sample_fund:
        f = by_fund[sample_fund]
        log(f"  fund: {sample_fund}  ({f.get('name','?')})")
        log(f"  top 5 positions:")
        for p in f.get("top_positions", [])[:5]:
            log(f"    {p.get('ticker'):<8} {p.get('change'):<5} ${p.get('value_usd', 0)/1e9:>5.1f}B  {p.get('pct_of_portfolio', 0):>5.2f}%  {p.get('name', '')[:40]}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "audit_13f_for_smart_money.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
