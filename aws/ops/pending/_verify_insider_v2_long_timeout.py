"""
Async verify: invoke insider-cluster v2 with InvocationType=Event so we don't
block waiting. Then poll CloudWatch + S3 for completion.
"""
import json, os, time, base64
from datetime import datetime, timezone, timedelta
from botocore.config import Config
import boto3

# Use long-timeout client for sync invokes
LONG_CFG = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name="us-east-1", config=LONG_CFG)
S3 = boto3.client("s3", region_name="us-east-1")
LOGS = boto3.client("logs", region_name="us-east-1")

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Async-invoke insider-cluster-scanner")
    # Drop env to even more conservative settings
    cfg = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
    env = dict(cfg.get("Environment", {}).get("Variables", {}))
    env.update({
        "MAX_FILINGS_TO_PARSE": "500",
        "N_BUSINESS_DAYS_INDEX": "3",
        "N_WORKERS": "8",
        "MIN_BUY_VALUE_USD": "10000",
        "CLUSTER_MIN_INSIDERS": "2",
        "LOOKBACK_DAYS": "30",
    })
    L.update_function_configuration(
        FunctionName="justhodl-insider-cluster-scanner",
        Environment={"Variables": env},
    )
    for _ in range(20):
        c = L.get_function_configuration(FunctionName="justhodl-insider-cluster-scanner")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ env: MAX_FILINGS=500, N_BUSINESS_DAYS_INDEX=3, N_WORKERS=8")

    log("  invoking ASYNC (Event)…")
    t0 = time.time()
    r = L.invoke(
        FunctionName="justhodl-insider-cluster-scanner",
        InvocationType="Event",
        Payload=b"{}",
    )
    log(f"  status: {r['StatusCode']} (202 = queued)")

    section("2) Poll S3 every 30s for fresh output")
    deadline = time.time() + 480  # 8 min
    initial_mod = None
    try:
        h = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        initial_mod = h["LastModified"]
        log(f"  initial S3 mod: {initial_mod}")
    except Exception:
        log(f"  (S3 not present yet)")

    fresh = False
    while time.time() < deadline:
        time.sleep(30)
        try:
            h = S3.head_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
            mod = h["LastModified"]
            elapsed = int(time.time() - t0)
            if initial_mod is None or mod > initial_mod:
                log(f"  ✓ S3 updated at {mod} (elapsed: {elapsed}s, size: {h['ContentLength']:,}b)")
                fresh = True
                break
            else:
                log(f"  [{elapsed}s] still old: {mod}")
        except Exception as e:
            log(f"  [{int(time.time()-t0)}s] S3 head failed: {e}")

    if not fresh:
        log(f"  ⚠ S3 not updated within 8min budget. Pulling CloudWatch logs anyway…")

    section("3) Pull last CloudWatch log stream")
    grp = "/aws/lambda/justhodl-insider-cluster-scanner"
    try:
        streams = LOGS.describe_log_streams(
            logGroupName=grp, orderBy="LastEventTime", descending=True, limit=1
        )
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            log(f"  stream: {stream}")
            events = LOGS.get_log_events(logGroupName=grp, logStreamName=stream,
                                          limit=300, startFromHead=True)
            msgs = [e["message"].rstrip() for e in events.get("events", [])]
            log(f"  total events: {len(msgs)}")
            log("")
            log("  ── log content (last 60 lines) ──")
            for m in msgs[-60:]:
                log(f"    {m}")
    except Exception as e:
        log(f"  ❌ logs: {e}")

    section("4) Read S3 output")
    try:
        obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/insider-clusters.json")
        data = json.loads(obj["Body"].read())
        log(f"  schema: {data.get('schema_version')}  method: {data.get('method')}")
        log(f"  generated_at: {data.get('generated_at')}")
        log(f"  stats: {json.dumps(data.get('stats', {}))}")
        clusters = data.get("clusters", [])
        log(f"  n_clusters: {len(clusters)}")
        if clusters:
            log("")
            log("  ── Top 15 clusters ──")
            log(f"    {'Ticker':<8} {'Score':>5} {'Signal':<24} {'Ins':>3} {'$Total':>10} {'%52H':>5} {'Mcap':<8} Sector")
            for c in clusters[:15]:
                f = c.get("fundamentals", {}) or {}
                tk = c.get("ticker", "?")
                sc = c.get("score", 0) or 0
                sg = (c.get("signal_type") or "")[:24]
                ni = c.get("n_insiders", 0)
                v  = c.get("total_value", 0) or 0
                ph = f.get("pct_from_52w_high", 0) or 0
                mc = f.get("market_cap") or 0
                ms = f"${mc/1e9:.1f}B" if mc >= 1e9 else f"${mc/1e6:.0f}M" if mc else "?"
                sec = (f.get("sector") or "")[:18]
                log(f"    {tk:<8} {sc:>5.1f} {sg:<24} {ni:>3} ${v/1e6:>8.2f}M {ph:>+4.0f}% {ms:<8} {sec}")
    except Exception as e:
        log(f"  ❌ S3: {e}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_insider_v2_async.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
