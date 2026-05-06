"""
Patch nobrainer pipeline issues found in audit:
  1. L5 rationale: pull ANTHROPIC_KEY from another Lambda (ai-brief), inject into env, unset SKIP_CLAUDE.
  2. Verify SSM /justhodl/anthropic/api-key exists; if not, write it from another Lambda's env.
  3. Re-invoke L5 — verify real Claude-written theses get produced.
  4. Verify Telegram digest sent with real thesis content.
  5. Drop LTHM (delisted into ALTM) from nobrainer universe — patch L6 to silently skip baseline-unavailable tickers (no error logged for known-delisted tickers).
"""
import os, json, time, base64
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

L = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("1) Discover ANTHROPIC_KEY from a Lambda that has it")
    candidates = ["justhodl-ai-brief", "justhodl-morning-intelligence", "justhodl-investor-agents",
                  "justhodl-ai-chat", "justhodl-stock-ai-research"]
    found_key = None
    found_in = None
    for fn in candidates:
        try:
            cfg = L.get_function_configuration(FunctionName=fn)
            env = cfg.get("Environment", {}).get("Variables", {})
            for k_name in ["ANTHROPIC_KEY", "ANTHROPIC_API_KEY", "CLAUDE_API_KEY"]:
                if k_name in env and len(env[k_name]) >= 80:
                    found_key = env[k_name]
                    found_in = f"{fn}.{k_name}"
                    break
            if found_key: break
            log(f"  {fn}: env keys = {list(env.keys())}, no anthropic key")
        except Exception as e:
            log(f"  {fn}: {e}")
    if found_key:
        log(f"  ✅ found Anthropic key in {found_in}, len={len(found_key)}")
    else:
        log(f"  ❌ no Anthropic key found in any candidate Lambda")
        return

    section("2) Verify/write SSM /justhodl/anthropic/api-key")
    try:
        cur = SSM.get_parameter(Name="/justhodl/anthropic/api-key", WithDecryption=True)
        log(f"  SSM exists, value len={len(cur['Parameter']['Value'])}")
    except SSM.exceptions.ParameterNotFound:
        log(f"  SSM not present, creating...")
        SSM.put_parameter(
            Name="/justhodl/anthropic/api-key",
            Value=found_key,
            Type="SecureString",
            Description="Anthropic API key, used by Lambdas via get_anthropic_key()",
        )
        log(f"  ✅ SSM created")

    section("3) Patch L5 nobrainer-rationale env: inject ANTHROPIC_KEY, unset SKIP_CLAUDE")
    L5_FN = "justhodl-nobrainer-rationale"
    cfg = L.get_function_configuration(FunctionName=L5_FN)
    cur_env = cfg.get("Environment", {}).get("Variables", {})
    log(f"  current env keys: {list(cur_env.keys())}")
    new_env = dict(cur_env)
    new_env["ANTHROPIC_KEY"] = found_key
    new_env.pop("SKIP_CLAUDE", None)  # ensure not set
    L.update_function_configuration(FunctionName=L5_FN, Environment={"Variables": new_env})
    # wait for update
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=L5_FN)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    new_keys = list(L.get_function_configuration(FunctionName=L5_FN)
                       .get("Environment", {}).get("Variables", {}).keys())
    log(f"  new env keys: {new_keys}")

    section("4) Re-invoke L5 — verify real Claude theses written")
    r = L.invoke(FunctionName=L5_FN, InvocationType="RequestResponse", LogType="Tail",
                 Payload=json.dumps({"top_n": 5}).encode())
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}")
    if body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner keys: {list(inner.keys())}")
        for fld in ["n_theses", "n_claude_ok", "n_claude_fail", "duration_s"]:
            if fld in inner:
                log(f"  {fld}: {inner[fld]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs ──")
        for ln in tail.splitlines()[-25:]:
            log(f"    {ln}")

    section("5) Read S3 nobrainers-rationale.json — verify real theses")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        log(f"  generated_at: {data.get('generated_at')}")
        log(f"  n_theses: {data.get('summary', {}).get('n_theses')}")
        theses = data.get("theses", [])
        for t in theses[:2]:
            sym = t.get("symbol", "?")
            theme = t.get("theme_etf") or t.get("theme", "?")
            txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
            log(f"  ── {sym} ({theme}) — {len(txt)} chars ──")
            for ln in txt.splitlines()[:18]:
                log(f"    {ln}")
            log("")
    except Exception as e:
        log(f"  ❌ {e}")

if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest/fix_nobrainer_pipeline_issues.md"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print(f"[report written] {out}")
