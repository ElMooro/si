"""Probe L5 nobrainer-rationale state. Defensive — wrapped in try/except blocks."""
import os, json, time, base64
import boto3

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def safe_run():
    REGION = "us-east-1"
    BUCKET = "justhodl-dashboard-live"
    L = boto3.client("lambda", region_name=REGION)
    S3 = boto3.client("s3", region_name=REGION)

    section("L5 env config")
    try:
        cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        env = cfg.get("Environment", {}).get("Variables", {})
        for k, v in env.items():
            if k in ("ANTHROPIC_KEY", "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN"):
                log(f"  {k}: ***{len(v)}c")
            else:
                log(f"  {k}: {v}")
        log(f"  modified: {cfg['LastModified']}")
    except Exception as e:
        log(f"  ❌ env: {e}")

    section("S3 nobrainers-rationale.json")
    try:
        head = S3.head_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        log(f"  size: {head['ContentLength']:,}b  modified: {head['LastModified']}")
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        log(f"  generated_at: {data.get('generated_at')}")
        log(f"  summary: {json.dumps(data.get('summary', {}))[:400]}")
        theses = data.get("theses", [])
        log(f"  n_theses: {len(theses)}")
        n_dummy = sum(1 for t in theses if "[SKIP_CLAUDE" in str(t.get("rationale") or t.get("thesis") or t.get("body") or ""))
        n_real = len(theses) - n_dummy
        log(f"  ⚙ dummy [SKIP_CLAUDE]: {n_dummy} | real Claude: {n_real}")
        if theses:
            t = theses[0]
            log(f"  ── sample item keys: {list(t.keys())}")
            sym = t.get("symbol", "?")
            theme = t.get("theme_etf") or t.get("theme", "?")
            txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
            log(f"  ── {sym}/{theme} ({len(txt)} chars) ──")
            for ln in txt.splitlines()[:30]:
                log(f"    {ln}")
    except Exception as e:
        log(f"  ❌ S3: {e}")

    section("Force-invoke L5 (top_n=3)")
    try:
        r = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                     InvocationType="RequestResponse", LogType="Tail",
                     Payload=json.dumps({"top_n": 3}).encode())
        body = json.loads(r["Payload"].read().decode())
        log(f"  status: {r['StatusCode']}")
        log(f"  body: {json.dumps(body)[:1000]}")
        if "LogResult" in r:
            tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
            log("  ── log tail (last 30 lines) ──")
            for ln in tail.splitlines()[-30:]:
                log(f"    {ln}")
    except Exception as e:
        log(f"  ❌ invoke: {e}")

def main():
    try:
        safe_run()
    except Exception as e:
        log(f"  ❌ FATAL: {e}")
    out = "aws/ops/reports/latest/probe_l5_v2.md"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print(f"[written {out}]")

if __name__ == "__main__":
    main()
