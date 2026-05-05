"""
Verify the insider-cluster-scanner is fully production-ready:
1. Lambda config (memory/timeout/env)
2. EventBridge schedule
3. S3 freshness
4. Top 25 clusters with full enrichment
5. Cross-reference with nobrainer leaderboard (compound-score candidates)
"""
import json, os, time, base64
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
EB = boto3.client("events", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Lambda configuration")
    cfg = L.get_function(FunctionName="justhodl-insider-cluster-scanner")["Configuration"]
    log(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
    log(f"  modified: {cfg['LastModified']}")
    env = cfg.get("Environment", {}).get("Variables", {})
    for k in sorted(env.keys()):
        v = env[k]
        if "KEY" in k or "TOKEN" in k:
            v = f"***{v[-6:]}" if len(v) > 6 else "***"
        log(f"    {k}={v}")

    section("2) EventBridge schedule")
    rules = EB.list_rule_names_by_target(
        TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-insider-cluster-scanner"
    )
    if rules.get("RuleNames"):
        for rn in rules["RuleNames"]:
            r = EB.describe_rule(Name=rn)
            log(f"  rule: {rn}  expr={r.get('ScheduleExpression')}  state={r.get('State')}")
    else:
        log("  ⚠ no schedule")

    section("3) S3 output freshness")
    head = S3.head_object(Bucket=BUCKET, Key="data/insider-clusters.json")
    log(f"  size: {head['ContentLength']:,}b")
    log(f"  modified: {head['LastModified']}")

    section("4) Full top-25 leaderboard")
    obj = S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")
    data = json.loads(obj["Body"].read())
    log(f"  schema: {data.get('schema_version')}")
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  stats: {json.dumps(data.get('stats', {}))}")
    clusters = data.get("clusters", [])
    log(f"  total clusters: {len(clusters)}")
    log("")
    log(f"    {'#':>2} {'Ticker':<8} {'Score':>6} {'Signal':<22} {'Ins':>3} {'TX':>3} {'$Total':>10} {'%52H':>6} {'Mcap':<10} {'Sector':<22} {'CEO':>4} {'CFO':>4}")
    for i, c in enumerate(clusters[:25], 1):
        f_ = c.get("fundamentals") or {}
        tk = c.get("ticker", "?")
        sc = c.get("score", 0) or 0
        sg = (c.get("signal_type") or "")[:22]
        ni = c.get("n_insiders", 0)
        nt = c.get("n_transactions", 0)
        v = c.get("total_value", 0) or 0
        ph = f_.get("pct_from_52w_high") or 0
        mc = f_.get("market_cap") or 0
        ms = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M" if mc else "?"
        sec = (f_.get("sector") or "")[:22]
        ceo = "✓" if c.get("has_ceo") else ""
        cfo = "✓" if c.get("has_cfo") else ""
        log(f"    {i:>2} {tk:<8} {sc:>6.1f} {sg:<22} {ni:>3} {nt:>3} ${v/1e6:>8.2f}M {ph:>+5.0f}% {ms:<10} {sec:<22} {ceo:>4} {cfo:>4}")

    section("5) Detailed view of top-3 clusters (full insider lists)")
    for c in clusters[:3]:
        log("")
        log(f"  ── #{clusters.index(c)+1} {c.get('ticker')} ({c.get('company','?')}) ──")
        log(f"    score: {c.get('score')}")
        log(f"    signal: {c.get('signal_type')}  rationale: {c.get('rationale','')}")
        log(f"    {c.get('n_insiders')} insiders, {c.get('n_transactions')} TX, ${c.get('total_value', 0):,.0f} total")
        log(f"    avg price: ${c.get('avg_price', 0):.2f}  window: {c.get('first_buy','?')} → {c.get('last_buy','?')}")
        f_ = c.get("fundamentals") or {}
        if f_:
            log(f"    fundamentals: mcap=${(f_.get('market_cap') or 0)/1e9:.2f}B  ph={f_.get('pct_from_52w_high'):+.1f}%  pe={f_.get('pe_ratio')}")
        log("    insiders:")
        for i in (c.get("insiders") or [])[:10]:
            log(f"      • {(i.get('name') or '?')[:32]:<32} {(i.get('role') or '?')[:38]:<38} ${i.get('total_value', 0):>11,.0f}")

    section("6) Cross-reference: insider-cluster ∩ nobrainer leaderboard")
    try:
        nb_obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
        nb_data = json.loads(nb_obj["Body"].read())
        nb_top = nb_data.get("summary", {}).get("top_25_overall", [])
        nb_tickers = {c.get("ticker"): c for c in nb_top if c.get("ticker")}
        log(f"  nobrainer top-25 set: {sorted(nb_tickers.keys())}")
        log("")

        cluster_tickers = {c.get("ticker"): c for c in clusters if c.get("ticker")}
        log(f"  insider-cluster set ({len(cluster_tickers)}): {sorted(cluster_tickers.keys())}")
        log("")

        intersect = sorted(set(nb_tickers.keys()) & set(cluster_tickers.keys()))
        log(f"  ── COMPOUND SIGNALS (both lists) ──")
        if intersect:
            for tk in intersect:
                nb = nb_tickers[tk]
                cl = cluster_tickers[tk]
                log(f"    {tk}: nobrainer score={nb.get('asymmetric_score')} ({nb.get('flag','')}) + insider score={cl.get('score')} ({cl.get('signal_type')})")
        else:
            log("    (none today — different universes)")
    except Exception as e:
        log(f"  ❌ {e}")

    section("7) Stats summary")
    s = data.get("stats", {})
    log(f"  filings scanned: {s.get('n_form4_filings_scanned')}")
    log(f"  filings parsed: {s.get('n_form4_parsed')}")
    log(f"  buy transactions: {s.get('n_buy_transactions')}")
    log(f"  unique tickers: {s.get('n_unique_tickers')}")
    log(f"  clusters: {s.get('n_clusters')}")
    log(f"  STRONG signals (score≥70): {s.get('n_strong_signals')}")
    log(f"  smart_money_dual: {s.get('n_smart_money_dual')}")
    log(f"  ceo_conviction: {s.get('n_ceo_conviction')}")
    log(f"  cluster_buys: {s.get('n_cluster_buys')}")
    log(f"  contrarian (down >25% from 52H): {s.get('n_contrarian_clusters')}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "audit_insider_production.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
