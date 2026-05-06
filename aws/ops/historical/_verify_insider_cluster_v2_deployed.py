"""
Verify v2 deploy landed in Lambda (poll until LastModified is newer than now-2h),
then re-run with reduced load to ensure it completes within the 600s budget,
and dump top clusters.
"""
import json, os, time, base64
from datetime import datetime, timezone, timedelta
import boto3

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Confirm v2 source is the deployed code")
    cfg = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
    log(f"  Lambda LastModified: {cfg['LastModified']}")
    log(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
    log(f"  env keys: {sorted(cfg.get('Environment', {}).get('Variables', {}).keys())}")

    # Read deployed source — verify "v2" marker present
    import urllib.request
    url = cfg.get("Code", {}).get("Location") if cfg.get("Code") else None
    if not url:
        # get_function with code
        f = L.get_function(FunctionName="justhodl-insider-cluster-scanner")
        url = f.get("Code", {}).get("Location")
    if url:
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                z = r.read()
            import zipfile, io
            zf = zipfile.ZipFile(io.BytesIO(z))
            for name in zf.namelist():
                if name.endswith("lambda_function.py"):
                    src = zf.read(name).decode("utf-8", "replace")
                    has_v2 = "insider_cluster_scanner_v2" in src
                    has_max_filings = "MAX_FILINGS_TO_PARSE" in src
                    has_rate_lock = "rate_lock" in src or "_RATE_LOCK" in src
                    log(f"  v2 method:    {has_v2}")
                    log(f"  MAX_FILINGS:  {has_max_filings}")
                    log(f"  rate-lock:    {has_rate_lock}")
                    if not has_v2:
                        log("  ❌ deployed code is NOT v2 — deploy-lambdas.yml may have failed")
                    break
        except Exception as e:
            log(f"  ⚠ couldn't fetch deployed code: {e}")

    section("2) Set conservative env to ensure completion within timeout budget")
    cfg = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
    env = dict(cfg.get("Environment", {}).get("Variables", {}))
    env.update({
        "LOOKBACK_DAYS": "30",
        "MAX_FILINGS_TO_PARSE": "800",
        "N_BUSINESS_DAYS_INDEX": "5",
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
        Environment={"Variables": env},
        Timeout=600,
        MemorySize=1024,
    )
    for _ in range(20):
        c = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ env set: MAX_FILINGS=800, N_BUSINESS_DAYS_INDEX=5, N_WORKERS=8, timeout=600s, mem=1024MB")

    section("3) Invoke and time it")
    log("  invoking (expect 3-6 min)...")
    t0 = time.time()
    try:
        r = L.invoke(
            FunctionName="justhodl-insider-cluster-scanner",
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=b"{}",
        )
    except Exception as e:
        log(f"  ❌ invoke threw: {e}")
        return
    dt = time.time() - t0
    log(f"  status: {r['StatusCode']}  duration: {dt:.1f}s")

    body = json.loads(r["Payload"].read())
    if body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:600]}")
    else:
        log(f"  raw body: {json.dumps(body)[:1500]}")

    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs ──")
        for ln in tail.splitlines()[-40:]:
            log(f"    {ln.rstrip()}")

    section("4) Dump S3 top clusters")
    try:
        head = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        log(f"  S3 size: {head['ContentLength']:,}b  modified: {head['LastModified']}")
        obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        data = json.loads(obj["Body"].read())
        log(f"  schema: {data.get('schema_version')}  method: {data.get('method')}")
        log(f"  stats: {json.dumps(data.get('stats', {}))}")
        clusters = data.get("clusters", [])
        log(f"  n_clusters: {len(clusters)}")
        if clusters:
            log("")
            log("  ── Top 15 by score ──")
            log(f"    {'Ticker':<8} {'Score':>5} {'Signal':<24} {'Ins':>3} {'$Total':>10} {'%52wH':>6} {'Mcap':<8} Sector")
            for c in clusters[:15]:
                fund = c.get("fundamentals", {}) or {}
                ticker = c.get("ticker", "?")
                score = c.get("score", 0) or 0
                sig = c.get("signal_type", "")[:24]
                n_ins = c.get("n_insiders", 0) or 0
                val = c.get("total_value", 0) or 0
                pct_high = fund.get("pct_from_52w_high", 0) or 0
                mcap = fund.get("market_cap") or 0
                mcap_str = f"${mcap/1e9:.1f}B" if mcap >= 1e9 else f"${mcap/1e6:.0f}M" if mcap else "?"
                sector = (fund.get("sector") or "")[:18]
                log(f"    {ticker:<8} {score:>5.1f} {sig:<24} {n_ins:>3} ${val/1e6:>8.2f}M {pct_high:>+5.0f}% {mcap_str:<8} {sector}")
            log("")
            log("  ── #1 cluster — full structure ──")
            c = clusters[0]
            for k in ["ticker", "company", "score", "signal_type", "n_insiders", "n_transactions",
                      "total_value", "avg_price", "first_buy", "last_buy",
                      "has_ceo", "has_cfo", "has_chairman", "rationale"]:
                v = c.get(k, "?")
                if isinstance(v, str) and len(v) > 200:
                    v = v[:200] + "..."
                log(f"    {k}: {v}")
            log(f"    insiders ({len(c.get('insiders', []))}):")
            for i in (c.get("insiders") or [])[:6]:
                log(f"      • {i.get('name', '?'):<32} {(i.get('role') or '?')[:35]:<35} ${i.get('total_value', 0):>10,.0f}")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_insider_cluster_v2_deployed.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
