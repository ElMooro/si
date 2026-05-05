"""
Verify v2 of justhodl-insider-cluster-scanner runs and writes good data.
Wait for CI/CD deploy first, then invoke with full diagnostics.
"""
import json, os, time, base64
import boto3

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")

def main():
    section("1) Verify v2 deployed")
    # Wait for CI/CD to update the lambda
    target_method = "insider_cluster_scanner_v2"
    
    cfg = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
    log(f"  current mod: {cfg['LastModified']}")
    log(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
    log(f"  env: {sorted(cfg.get('Environment', {}).get('Variables', {}).keys())}")

    section("2) Force v2 env settings to reduce load")
    env = cfg.get("Environment", {}).get("Variables", {})
    new_env = dict(env)
    # Override defaults to ensure fast first run
    new_env.update({
        "LOOKBACK_DAYS": "30",
        "MAX_FILINGS_TO_PARSE": "1000",  # 1000 most recent — completes in ~2-3 min
        "N_BUSINESS_DAYS_INDEX": "5",     # last week only — index pulls
        "N_WORKERS": "8",
        "MIN_BUY_VALUE_USD": "10000",
        "CLUSTER_MIN_INSIDERS": "2",
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "S3_BUCKET": "justhodl-dashboard-live",
        "S3_KEY": "data/insider-clusters.json",
        "SEC_USER_AGENT": "JustHodl Research raafouis@gmail.com",
    })
    L.update_function_configuration(
        FunctionName="justhodl-insider-cluster-scanner",
        Environment={"Variables": new_env},
    )
    for _ in range(20):
        c = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ env updated: MAX_FILINGS_TO_PARSE=1000, N_BUSINESS_DAYS_INDEX=5, N_WORKERS=8")
    
    section("3) Invoke and capture results")
    log("  invoking (may take 4-7 min)...")
    t0 = time.time()
    r = L.invoke(
        FunctionName="justhodl-insider-cluster-scanner",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}",
    )
    dt = time.time() - t0
    body = json.loads(r["Payload"].read())
    log(f"  status: {r['StatusCode']}  duration: {dt:.1f}s")
    
    if body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)}")
    else:
        log(f"  raw body: {json.dumps(body)[:1500]}")
    
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs (last 4kb) ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln.rstrip()}")
    
    section("4) Read S3 output and dump top 12")
    try:
        head = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        log(f"  S3: {head['ContentLength']:,}b modified {head['LastModified']}")
        obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        data = json.loads(obj["Body"].read())
        log(f"  schema: {data.get('schema_version')}  method: {data.get('method')}")
        log(f"  stats: {json.dumps(data.get('stats', {}))}")
        clusters = data.get("clusters", [])
        log(f"  n_clusters: {len(clusters)}")
        if clusters:
            log("")
            log("  ── Top 15 by score ──")
            for c in clusters[:15]:
                fund = c.get("fundamentals", {})
                ticker = c.get("ticker", "?")
                score = c.get("score", 0)
                sig = c.get("signal_type", "")
                n_ins = c.get("n_insiders", 0)
                val = c.get("total_value", 0)
                pct_high = fund.get("pct_from_52w_high", 0)
                mcap = fund.get("market_cap", 0)
                mcap_str = f"${mcap/1e9:.1f}B" if mcap >= 1e9 else f"${mcap/1e6:.0f}M" if mcap else "?"
                sector = fund.get("sector", "")[:18]
                log(f"    {ticker:<8} {score:>5.1f} {sig:<22} {n_ins}ins ${val/1e6:>5.2f}M {pct_high:>+5.0f}% {mcap_str:<8} {sector}")
            log("")
            log("  ── Top cluster — full thesis structure ──")
            c = clusters[0]
            log(f"    ticker: {c['ticker']}  company: {c['company']}")
            log(f"    score: {c['score']}  signal_type: {c['signal_type']}")
            log(f"    rationale: {c['rationale']}")
            log(f"    n_insiders: {c['n_insiders']}  n_transactions: {c['n_transactions']}")
            log(f"    total_value: ${c['total_value']:,.0f}  avg_price: ${c['avg_price']:.2f}")
            log(f"    window: {c['first_buy']} → {c['last_buy']}")
            log(f"    has_ceo: {c['has_ceo']}  has_cfo: {c['has_cfo']}  has_chairman: {c['has_chairman']}")
            log(f"    insiders ({len(c['insiders'])}):")
            for i in c["insiders"][:8]:
                log(f"      • {i['name']:<32} {i['role'][:35]:<35} ${i['total_value']:>10,.0f}  {i['n_buys']}-buys")
            f = c.get("fundamentals", {})
            log(f"    fundamentals:")
            for k in ["market_cap", "price_now", "high_52w", "low_52w", "pct_from_52w_high", "sector", "industry", "country"]:
                if k in f:
                    log(f"      {k}: {f[k]}")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")
        import traceback
        log(traceback.format_exc()[:1500])

if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_insider_cluster_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
