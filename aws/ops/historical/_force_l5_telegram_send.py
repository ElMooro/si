"""
Force L5 to send fresh Telegram digest of top 5 nobrainers + verify message_id.
"""
import json, os, time
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("Force L5 invoke with FORCE_TELEGRAM=1 environment override")

    cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
    env = dict(cfg.get("Environment", {}).get("Variables", {}))

    # Force-send + skip rate-limit dedup
    env["FORCE_TELEGRAM"] = "1"
    L.update_function_configuration(
        FunctionName="justhodl-nobrainer-rationale",
        Environment={"Variables": env},
    )
    for _ in range(15):
        c = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log("  ✓ FORCE_TELEGRAM=1 set")

    log("  invoking L5 (will send Telegram digest)")
    r = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                 InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}")

    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:600]}")

    if "LogResult" in r:
        import base64
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail (last 4kb) ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln.rstrip()}")

    # Reset FORCE_TELEGRAM=0 (don't spam Telegram on every cron run)
    env["FORCE_TELEGRAM"] = "0"
    L.update_function_configuration(
        FunctionName="justhodl-nobrainer-rationale",
        Environment={"Variables": env},
    )
    log("  ✓ FORCE_TELEGRAM reset to 0")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "force_l5_telegram.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
# triggered Tue May  5 17:20:27 UTC 2026
