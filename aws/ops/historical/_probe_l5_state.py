"""Quick probe of L5 nobrainer-rationale current state.
Read env, read S3 output, check whether theses are real or [SKIP_CLAUDE] dummies."""
import os, json, time, base64
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("L5 current env config")
    cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
    env = cfg.get("Environment", {}).get("Variables", {})
    redacted = {k: (f"***{len(v)}c" if k in {"ANTHROPIC_KEY","ANTHROPIC_API_KEY","TELEGRAM_BOT_TOKEN"} else v) for k,v in env.items()}
    log(f"  env: {redacted}")
    log(f"  modified: {cfg['LastModified']}")
    log(f"  state: {cfg['State']}")

    section("S3 nobrainers-rationale.json — current contents")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    summary = data.get("summary", {})
    log(f"  summary: {json.dumps(summary)[:400]}")
    theses = data.get("theses", [])
    log(f"  n_theses: {len(theses)}")
    n_dummy = sum(1 for t in theses if "[SKIP_CLAUDE" in (t.get("rationale") or t.get("thesis") or t.get("body") or ""))
    n_real  = len(theses) - n_dummy
    log(f"  dummy [SKIP_CLAUDE]: {n_dummy} | real Claude: {n_real}")
    if theses:
        for t in theses[:1]:
            sym = t.get("symbol", "?")
            theme = t.get("theme_etf") or t.get("theme", "?")
            txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
            log(f"  ── sample {sym}/{theme} ({len(txt)} chars) ──")
            for ln in txt.splitlines()[:25]:
                log(f"    {ln}")

    section("Force-invoke L5 with top_n=3, capture LogResult")
    r = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                 InvocationType="RequestResponse", LogType="Tail",
                 Payload=json.dumps({"top_n": 3}).encode())
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}")
    log(f"  body: {json.dumps(body)[:1500]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8","replace")
        log("  ── tail (last 30 lines) ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln}")

if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest/probe_l5_state.md"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out,"w",encoding="utf-8") as f: f.write("\n".join(REPORT))
    print(f"[written]")
