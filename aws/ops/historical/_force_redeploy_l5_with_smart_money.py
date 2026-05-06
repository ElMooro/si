"""
Manually force-deploy L5 because the auto-deploy workflow didn't pick up the source change.
Also force-invoke after to verify all 3 signals load.
"""
import io, json, os, time, zipfile, base64
from botocore.config import Config
import boto3

REGION = "us-east-1"
cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name=REGION, config=cfg)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Read repo source + verify smart-money patch present")
    src_path = "aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py"
    with open(src_path) as f:
        src = f.read()
    log(f"  repo source: {len(src):,} chars")
    for marker in ["smart_money_by_ticker", "_smart_money_block", "smart-money-clusters.json", "build_thesis_prompt(c, cl, sm)"]:
        ok = marker in src
        log(f"  {'✓' if ok else '❌'} {marker}")

    section("2) Build zip + force-deploy L5")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    log(f"  zip size: {len(zb):,}b")
    L.update_function_code(FunctionName="justhodl-nobrainer-rationale", ZipFile=zb)
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed mod={c['LastModified']}")

    section("3) Force-invoke + verify smart-money load + compound logic")
    t0 = time.time()
    r = L.invoke(
        FunctionName="justhodl-nobrainer-rationale",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}",
    )
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}  duration: {dur:.1f}s")
    body = json.loads(r["Payload"].read().decode())
    log(f"  body: {json.dumps(body)[:500]}")

    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        # Extract loaded lines + compound hits
        loads = []
        compounds = []
        for ln in tail.splitlines():
            ln = ln.rstrip()
            if "loaded" in ln and "clusters" in ln:
                loads.append(ln)
            elif "ALSO has" in ln:
                compounds.append(ln)
        log("")
        log("  ── load lines ──")
        for ln in loads:
            log(f"    {ln.strip()}")
        log("")
        log(f"  ── COMPOUND hits ({len(compounds)}) ──")
        for ln in compounds:
            log(f"    {ln.strip()}")
        log("")
        log("  ── full tail ──")
        for ln in tail.splitlines()[-20:]:
            log(f"    {ln.rstrip()}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "force_redeploy_l5_smart_money.md"), "w") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
