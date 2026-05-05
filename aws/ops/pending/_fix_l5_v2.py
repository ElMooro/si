"""Bulletproof L5 fix — uses GitHub Actions secrets directly. Always writes report."""
import os, json, time, base64, traceback
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = []

def log(m):
    ts = time.strftime("%H:%M:%S")
    line = f"- `{ts}`   {m}"
    print(line)
    REPORT.append(line)

def section(t):
    print(f"\n# {t}\n")
    REPORT.append(f"\n# {t}\n")

def write_report(name="fix_l5_v2"):
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"{name}.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print(f"[report written: {name}.md]")

def main():
    L = boto3.client("lambda", region_name=REGION)
    S3 = boto3.client("s3", region_name=REGION)

    section("0) Checking GitHub Actions secrets in env")
    available = {}
    for k in ["ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY_NEW", "TELEGRAM_BOT_TOKEN"]:
        v = os.environ.get(k, "")
        available[k] = f"len={len(v)}" if v else "<empty>"
    log(f"  available secrets: {available}")

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_API_KEY_NEW")
    if not anthropic_key:
        log("  ❌ no anthropic key in env — try AWS Lambda lookup")
        for fn in ["justhodl-ai-brief", "justhodl-morning-intelligence", "justhodl-ai-chat"]:
            try:
                env = L.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
                log(f"  {fn} env keys: {sorted(env.keys())}")
                for kn in ["ANTHROPIC_KEY","ANTHROPIC_API_KEY"]:
                    v = env.get(kn,"")
                    if v and len(v) >= 80:
                        anthropic_key = v
                        log(f"  ✅ FOUND in {fn}.{kn} len={len(v)}")
                        break
                if anthropic_key: break
            except Exception as e:
                log(f"  {fn}: {type(e).__name__}: {e}")

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not anthropic_key:
        log("  ❌ FATAL — no Anthropic key anywhere — abort")
        return False

    section("1) Update L5 env vars")
    cur = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
    cur_env = cur.get("Environment",{}).get("Variables",{})
    log(f"  cur L5 env keys: {sorted(cur_env.keys())}")
    log(f"  has ANTHROPIC_KEY: {'ANTHROPIC_KEY' in cur_env}")
    log(f"  SKIP_CLAUDE: {cur_env.get('SKIP_CLAUDE','<unset>')}")

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
    )
    for _ in range(20):
        cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if cfg.get("LastUpdateStatus") == "Successful":
            log(f"  ✅ config update settled at {cfg['LastModified']}")
            break
        time.sleep(1)

    section("2) Force-invoke L5 with top_n=3 (small batch)")
    t0 = time.time()
    r = L.invoke(
        FunctionName="justhodl-nobrainer-rationale",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps({"top_n": 3, "send_telegram": False}).encode(),
    )
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}  duration: {round(time.time()-t0,1)}s")
    log(f"  body keys: {list(body.keys())}")
    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:600]}")
    else:
        log(f"  raw body: {json.dumps(body)[:1500]}")

    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8","replace")
        log("  ── tail (last 30 lines) ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln}")

    section("3) Verify S3 nobrainers-rationale.json")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    data = json.loads(obj["Body"].read())
    s = data.get("summary", {})
    log(f"  generated_at: {data.get('generated_at','?')}")
    log(f"  summary: {json.dumps(s)[:400]}")
    theses = data.get("theses") or []
    log(f"  n_theses: {len(theses)}")
    n_dummy = sum(1 for t in theses if "[SKIP_CLAUDE" in (t.get("rationale") or t.get("thesis") or t.get("body") or ""))
    n_real = len(theses) - n_dummy
    log(f"  n_real: {n_real}  n_dummy: {n_dummy}")
    for i, t in enumerate(theses[:2]):
        sym = t.get("symbol", t.get("ticker", "?"))
        score = t.get("asymmetric_score", t.get("score", "?"))
        txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
        log("")
        log(f"  ── thesis {i+1}: {sym} score={score} (len={len(txt)}) ──")
        for ln in txt.splitlines()[:25]:
            log(f"    {ln}")
    return True

if __name__ == "__main__":
    try:
        ok = main()
        log("")
        log(f"=== FINAL: {'OK' if ok else 'FAILED'}")
    except Exception as e:
        log("")
        log(f"❌ EXCEPTION: {type(e).__name__}: {e}")
        log("```")
        for ln in traceback.format_exc().splitlines():
            log(ln)
        log("```")
    finally:
        write_report()
