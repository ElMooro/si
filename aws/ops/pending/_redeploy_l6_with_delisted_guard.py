"""
Force redeploy L6 nobrainer-tracker with the DELISTED_TICKERS guard patch.
Verify the new code is active by inspecting the deployed source.
"""
import os, json, time, io, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
L6_FN = "justhodl-nobrainer-tracker"
L6_SRC = "aws/lambdas/justhodl-nobrainer-tracker/source/lambda_function.py"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("1) Verify local source has DELISTED_TICKERS")
    with open(L6_SRC, "r", encoding="utf-8") as f:
        src = f.read()
    has_constant = "DELISTED_TICKERS" in src
    has_guard = "if ticker in DELISTED_TICKERS" in src
    log(f"  DELISTED_TICKERS constant: {has_constant}")
    log(f"  delisted-skip guard: {has_guard}")
    if not has_constant or not has_guard:
        log(f"  ❌ patch not present, aborting")
        return

    section("2) Build zip and update Lambda code")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zip_bytes = buf.getvalue()
    log(f"  zip size: {len(zip_bytes):,}b")
    L.update_function_code(FunctionName=L6_FN, ZipFile=zip_bytes)
    for _ in range(20):
        cfg = L.get_function_configuration(FunctionName=L6_FN)
        if cfg.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✅ deployed, mod={cfg['LastModified']}")

    section("3) Verify deployed Lambda source has the patch")
    # Re-read code by getting the actual function code URL
    import urllib.request
    code_url = L.get_function(FunctionName=L6_FN)["Code"]["Location"]
    try:
        deployed_zip = urllib.request.urlopen(code_url, timeout=30).read()
        with zipfile.ZipFile(io.BytesIO(deployed_zip)) as zf:
            with zf.open("lambda_function.py") as fh:
                deployed_src = fh.read().decode()
        log(f"  deployed source: {len(deployed_src):,} chars")
        log(f"  deployed has DELISTED_TICKERS: {'DELISTED_TICKERS' in deployed_src}")
        log(f"  deployed has guard: {'if ticker in DELISTED_TICKERS' in deployed_src}")
        # Show first 5 lines containing the constant
        for ln in deployed_src.splitlines():
            if "DELISTED_TICKERS" in ln:
                log(f"    {ln}")
    except Exception as e:
        log(f"  ⚠ verify-deploy: {e}")

    section("4) Force-invoke and verify n_errors=0 (no LTHM error)")
    import base64
    r = L.invoke(FunctionName=L6_FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}")
    log(f"  body: {json.dumps(body)[:1500]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail (last 25) ──")
        for ln in tail.splitlines()[-25:]:
            log(f"    {ln}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {e}")
    out = "aws/ops/reports/latest/redeploy_l6_with_delisted_guard.md"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out,"w",encoding="utf-8") as f: f.write("\n".join(REPORT))
    print("[written]")
