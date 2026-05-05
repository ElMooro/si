"""
The 14% buy-extraction rate is correct (most Form 4s aren't buys). The fix
is to scan a much bigger sample so we find more clusters.

Set production env to:
  MAX_FILINGS_TO_PARSE = 3000   (3x the prior 800)
  N_BUSINESS_DAYS_INDEX = 7     (full week)
  N_WORKERS = 12               (within SEC 10/s rate limit, with 0.12s lock = ~8 effective)
  Memory = 1536MB              (more headroom)
  Timeout = 900s               (15 min, max for Lambda)

Then async-invoke and poll S3 for the new output.
"""
import json, os, time
from datetime import datetime, timezone, timedelta
from botocore.config import Config
import boto3

LONG_CFG = Config(read_timeout=300, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name="us-east-1", config=LONG_CFG)
S3 = boto3.client("s3", region_name="us-east-1")
LOGS = boto3.client("logs", region_name="us-east-1")

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Tune Lambda config for higher volume")
    cfg = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
    env = dict(cfg.get("Environment", {}).get("Variables", {}))
    env.update({
        "MAX_FILINGS_TO_PARSE": "3000",
        "N_BUSINESS_DAYS_INDEX": "7",
        "N_WORKERS": "12",
        "MIN_BUY_VALUE_USD": "5000",
        "CLUSTER_MIN_INSIDERS": "2",
        "LOOKBACK_DAYS": "30",
    })
    L.update_function_configuration(
        FunctionName="justhodl-insider-cluster-scanner",
        Environment={"Variables": env},
        MemorySize=1536,
        Timeout=900,
    )
    for _ in range(20):
        c = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ MAX_FILINGS=3000, days=7, workers=12, mem=1536MB, timeout=900s, MIN_BUY=$5K")

    section("2) Async invoke")
    initial = None
    try:
        h = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        initial = h["LastModified"]
        log(f"  initial S3 mod: {initial}")
    except Exception:
        pass

    t0 = time.time()
    r = L.invoke(
        FunctionName="justhodl-insider-cluster-scanner",
        InvocationType="Event",
        Payload=b"{}",
    )
    log(f"  invoked async (status: {r['StatusCode']})")

    section("3) Poll S3 every 30s for fresh output (12 min budget)")
    deadline = time.time() + 720
    fresh = False
    while time.time() < deadline:
        time.sleep(30)
        try:
            h = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
            mod = h["LastModified"]
            elapsed = int(time.time() - t0)
            if initial is None or mod > initial:
                log(f"  ✓ S3 updated at {mod} (elapsed: {elapsed}s, size: {h['ContentLength']:,}b)")
                fresh = True
                break
            else:
                log(f"  [{elapsed}s] still old")
        except Exception as e:
            log(f"  [{int(time.time()-t0)}s] S3 head: {e}")

    section("4) CloudWatch tail (latest stream)")
    try:
        grp = "/aws/lambda/justhodl-insider-cluster-scanner"
        streams = LOGS.describe_log_streams(
            logGroupName=grp, orderBy="LastEventTime", descending=True, limit=1
        )
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            events = LOGS.get_log_events(logGroupName=grp, logStreamName=stream,
                                          limit=300, startFromHead=True)
            msgs = [e["message"].rstrip() for e in events.get("events", [])]
            log(f"  stream: {stream}  events: {len(msgs)}")
            for m in msgs[-25:]:
                log(f"    {m}")
    except Exception as e:
        log(f"  ❌ logs: {e}")

    section("5) Read S3 + dump top 20 clusters")
    if fresh:
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
            data = json.loads(obj["Body"].read())
            log(f"  schema: {data.get('schema_version')}")
            log(f"  generated_at: {data.get('generated_at')}")
            log(f"  stats: {json.dumps(data.get('stats', {}))}")
            clusters = data.get("clusters", [])
            log(f"  n_clusters: {len(clusters)}")
            log("")
            log(f"    {'Ticker':<8} {'Score':>5} {'Signal':<22} {'Ins':>3} {'$Total':>10} {'%52H':>5} {'Mcap':<8} Sector")
            for c in clusters[:20]:
                f = c.get("fundamentals") or {}
                tk = c.get("ticker", "?")
                sc = c.get("score", 0) or 0
                sg = (c.get("signal_type") or "")[:22]
                ni = c.get("n_insiders", 0)
                v  = c.get("total_value", 0) or 0
                ph = f.get("pct_from_52w_high") or 0
                mc = f.get("market_cap") or 0
                ms = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M" if mc else "?"
                sec = (f.get("sector") or "")[:18]
                log(f"    {tk:<8} {sc:>5.1f} {sg:<22} {ni:>3} ${v/1e6:>8.2f}M {ph:>+4.0f}% {ms:<8} {sec}")
            log("")
            log("  ── Top cluster — full structure ──")
            if clusters:
                c = clusters[0]
                for k in ["ticker", "company", "score", "signal_type", "n_insiders", "n_transactions",
                          "total_value", "avg_price", "first_buy", "last_buy",
                          "has_ceo", "has_cfo", "has_chairman", "rationale"]:
                    v = c.get(k, "?")
                    if isinstance(v, str) and len(v) > 240:
                        v = v[:240] + "..."
                    log(f"    {k}: {v}")
                log(f"    insiders ({len(c.get('insiders', []))}):")
                for i in (c.get("insiders") or [])[:8]:
                    log(f"      • {(i.get('name') or '?')[:32]:<32} {(i.get('role') or '?')[:35]:<35} ${i.get('total_value', 0):>10,.0f}")
        except Exception as e:
            log(f"  ❌ {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "tune_insider_volume.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
