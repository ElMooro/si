"""
Fix the L5 nobrainer-rationale Lambda so it actually writes Claude theses.

Steps:
  1. Pull the Anthropic key from justhodl-morning-intelligence env vars (known to exist there)
  2. Pull the Telegram bot token from SSM /justhodl/telegram/bot_token
  3. Update L5's env to include ANTHROPIC_KEY + TELEGRAM_BOT_TOKEN, ensure SKIP_CLAUDE not set
  4. Wait for config update to settle
  5. Force-invoke L5 with payload {"top_n": 9} — write real theses for all 9 TIER_A nobrainers
  6. Pull S3 data/nobrainers-rationale.json — verify real Claude content
  7. Confirm Telegram digest sent (look for last message in chat)
"""
import json, os, time
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

def main():
    section("1) Pull Anthropic key from justhodl-morning-intelligence")
    src = L.get_function_configuration(FunctionName="justhodl-morning-intelligence")
    src_env = src.get("Environment", {}).get("Variables", {})
    log(f"  source env keys: {list(src_env.keys())}")
    anthropic_key = src_env.get("ANTHROPIC_KEY") or src_env.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        log("  ❌ no ANTHROPIC_KEY in morning-intelligence — checking other Lambdas")
        for fn in ["justhodl-ai-brief", "justhodl-ai-chat", "justhodl-investor-agents"]:
            try:
                env = L.get_function_configuration(FunctionName=fn).get("Environment", {}).get("Variables", {})
                if env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY"):
                    anthropic_key = env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY")
                    log(f"  ✅ found in {fn}")
                    break
            except Exception as e:
                pass
    if anthropic_key:
        log(f"  ✅ key length: {len(anthropic_key)} (sk-...{anthropic_key[-6:]})")
    else:
        log("  ❌ NO Anthropic key found anywhere — abort")
        return

    section("2) Pull Telegram token from SSM")
    try:
        tg_token = SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
        log(f"  ✅ telegram token length: {len(tg_token)}")
    except Exception as e:
        tg_token = None
        log(f"  ⚠ telegram token: {e}")

    section("3) Update L5 env vars")
    cur = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
    cur_env = cur.get("Environment", {}).get("Variables", {})
    log(f"  current env keys: {list(cur_env.keys())}")

    new_env = dict(cur_env)
    new_env["ANTHROPIC_KEY"] = anthropic_key
    if tg_token:
        new_env["TELEGRAM_BOT_TOKEN"] = tg_token
    # ensure SKIP_CLAUDE removed if present
    new_env.pop("SKIP_CLAUDE", None)
    # tune for production
    new_env.setdefault("N_THESES", "9")
    new_env.setdefault("MIN_SCORE", "78")
    new_env.setdefault("N_DIGEST", "5")

    log(f"  new env keys: {list(new_env.keys())}")
    L.update_function_configuration(
        FunctionName="justhodl-nobrainer-rationale",
        Environment={"Variables": new_env},
        Timeout=600,
        MemorySize=512,
    )
    # wait for config update to settle
    for _ in range(20):
        cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if cfg.get("LastUpdateStatus") == "Successful":
            log(f"  ✅ config update settled (LastModified: {cfg['LastModified']})")
            break
        time.sleep(1)

    section("4) Force-invoke L5 with top_n=9")
    t0 = time.time()
    r = L.invoke(
        FunctionName="justhodl-nobrainer-rationale",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps({"top_n": 9, "send_telegram": True}).encode(),
    )
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}  duration: {round(time.time()-t0, 1)}s")
    log(f"  body keys: {list(body.keys())}")
    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:600]}")
    else:
        log(f"  raw: {json.dumps(body)[:1500]}")

    if "LogResult" in r:
        import base64
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail logs (last 4kb) ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln}")

    section("5) Verify L5 wrote real Claude theses to S3")
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
        data = json.loads(obj["Body"].read())
        theses = data.get("theses") or data.get("rationales") or []
        log(f"  n_theses: {len(theses)}")
        log(f"  generated_at: {data.get('generated_at', '?')}")
        log(f"  n_claude_ok: {data.get('summary', {}).get('n_claude_ok', '?')}")
        log(f"  n_claude_fail: {data.get('summary', {}).get('n_claude_fail', '?')}")
        # sample first 2 theses
        for i, t in enumerate(theses[:2]):
            sym = t.get("ticker") or t.get("symbol") or "?"
            score = t.get("asymmetric_score") or t.get("score") or "?"
            txt = t.get("thesis") or t.get("rationale") or t.get("body") or ""
            log("")
            log(f"  ── thesis {i+1}: {sym}  score={score} ──")
            log(f"  length: {len(txt)} chars")
            is_dummy = "[SKIP_CLAUDE=1]" in txt
            log(f"  is_dummy: {is_dummy}")
            for ln in txt.splitlines()[:30]:
                log(f"    {ln}")
    except Exception as e:
        log(f"  ❌ S3 read: {e}")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "fix_l5_env_and_invoke.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
