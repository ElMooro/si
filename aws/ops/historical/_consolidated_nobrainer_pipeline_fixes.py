"""
Consolidated nobrainer pipeline hardening:
  A) Inject ANTHROPIC_KEY into L5 from any other Lambda that has it
  B) Remove SKIP_CLAUDE if set, harden env vars
  C) Force-invoke L5 with top_n=9 to produce real Claude theses
  D) Verify S3 nobrainers-rationale.json contains real (non-dummy) theses
  E) Re-invoke L4 (asymmetric-hunter) so L5 has a fresh leaderboard, then re-invoke L5

Idempotent: safe to re-run.
"""
import os, json, time, base64
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S")
    print(f"- `{ts}`   {m}")
    REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def find_anthropic_key():
    candidates = [
        "justhodl-ai-brief",
        "justhodl-morning-intelligence",
        "justhodl-investor-agents",
        "justhodl-ai-chat",
        "justhodl-stock-ai-research",
        "justhodl-stock-screener",
    ]
    for fn in candidates:
        try:
            env = L.get_function_configuration(FunctionName=fn).get("Environment", {}).get("Variables", {})
            for kn in ["ANTHROPIC_KEY", "ANTHROPIC_API_KEY", "CLAUDE_API_KEY"]:
                v = env.get(kn, "")
                if v and len(v) >= 80:
                    log(f"  ✅ Anthropic key found in {fn}.{kn} (len={len(v)})")
                    return v
            log(f"  {fn}: env keys = {list(env.keys())}, no anthropic key")
        except Exception as e:
            log(f"  {fn}: lookup failed: {e}")
    # Try GitHub Actions secret
    secret_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY_NEW")
    if secret_key and len(secret_key) >= 80:
        log(f"  ✅ Anthropic key from Actions secret (len={len(secret_key)})")
        return secret_key
    return None

def find_telegram_token():
    try:
        v = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
        log(f"  ✅ Telegram token from SSM (len={len(v)})")
        return v
    except Exception as e:
        log(f"  SSM telegram lookup failed: {e}")
    secret = os.environ.get("TELEGRAM_BOT_TOKEN")
    if secret:
        log(f"  ✅ Telegram token from Actions secret (len={len(secret)})")
        return secret
    return None

def main():
    section("A) Discover Anthropic key + Telegram token")
    anthropic_key = find_anthropic_key()
    if not anthropic_key:
        log("  ❌ NO Anthropic key found anywhere — abort")
        return
    tg_token = find_telegram_token()

    section("B) Update L5 env vars")
    cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
    cur_env = cfg.get("Environment", {}).get("Variables", {})
    log(f"  current L5 env keys: {list(cur_env.keys())}")
    log(f"  has ANTHROPIC_KEY: {'ANTHROPIC_KEY' in cur_env}")
    log(f"  SKIP_CLAUDE value: {cur_env.get('SKIP_CLAUDE','<unset>')}")
    new_env = dict(cur_env)
    new_env["ANTHROPIC_KEY"] = anthropic_key
    if tg_token:
        new_env["TELEGRAM_BOT_TOKEN"] = tg_token
    new_env.pop("SKIP_CLAUDE", None)
    new_env.setdefault("N_THESES", "9")
    new_env.setdefault("MIN_SCORE", "78")
    new_env.setdefault("N_DIGEST", "5")
    L.update_function_configuration(
        FunctionName="justhodl-nobrainer-rationale",
        Environment={"Variables": new_env},
        Timeout=600,
        MemorySize=512,
    )
    for _ in range(20):
        cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if cfg.get("LastUpdateStatus") == "Successful":
            log(f"  ✅ config update settled at {cfg['LastModified']}")
            break
        time.sleep(1)
    final_env = cfg.get("Environment", {}).get("Variables", {})
    log(f"  final L5 env keys: {sorted(final_env.keys())}")

    section("C) Re-invoke L4 (asymmetric-hunter) for fresh leaderboard")
    t0 = time.time()
    r = L.invoke(FunctionName="justhodl-asymmetric-hunter",
                 InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read().decode())
    log(f"  L4 status: {r['StatusCode']}  duration: {round(time.time()-t0, 1)}s")
    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  L4 result: tier_a={inner.get('n_tier_a_nobrainer','?')} mu_grade={inner.get('n_mu_grade','?')}")

    section("D) Force-invoke L5 with top_n=9, send_telegram=True")
    t0 = time.time()
    r = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                 InvocationType="RequestResponse", LogType="Tail",
                 Payload=json.dumps({"top_n": 9, "send_telegram": True}).encode())
    body = json.loads(r["Payload"].read().decode())
    log(f"  L5 status: {r['StatusCode']}  duration: {round(time.time()-t0, 1)}s")
    log(f"  body keys: {list(body.keys())}")
    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:600]}")
    else:
        log(f"  raw body: {json.dumps(body)[:1500]}")

    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs (last 4kb) ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln}")

    section("E) Verify S3 nobrainers-rationale.json has real Claude content")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        gen_at = data.get("generated_at") or data.get("summary", {}).get("generated_at", "?")
        log(f"  generated_at: {gen_at}")
        log(f"  size: {len(json.dumps(data)):,} chars")
        s = data.get("summary", {})
        log(f"  summary: n_theses={s.get('n_theses')}  n_claude_ok={s.get('n_claude_ok')}  n_claude_fail={s.get('n_claude_fail')}")
        theses = data.get("theses") or data.get("rationales") or []
        log(f"  n_theses entries: {len(theses)}")
        n_dummy = 0
        n_real = 0
        for t in theses:
            txt = t.get("thesis") or t.get("rationale") or t.get("body") or ""
            if "[SKIP_CLAUDE=1]" in txt:
                n_dummy += 1
            else:
                n_real += 1
        log(f"  n_real_claude: {n_real}  n_dummy: {n_dummy}")
        log("")
        if theses and n_real > 0:
            for i, t in enumerate(theses[:2]):
                sym = t.get("ticker") or t.get("symbol") or "?"
                score = t.get("asymmetric_score") or t.get("score") or "?"
                txt = t.get("thesis") or t.get("rationale") or t.get("body") or ""
                if "[SKIP_CLAUDE=1]" in txt:
                    continue
                log(f"  ── REAL thesis {i+1}: {sym}  score={score} ──")
                log(f"  length: {len(txt)} chars")
                for ln in txt.splitlines()[:30]:
                    log(f"    {ln}")
                log("")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "consolidated_nobrainer_fixes.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
