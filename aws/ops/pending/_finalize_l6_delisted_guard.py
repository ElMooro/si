"""
Finalize L6 patch — add the early-skip guard that the regex missed,
then redeploy and verify.
"""
import os, json, time, io, zipfile, base64, re
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
    section("1) Patch source — add early-skip guard inside the loop")
    with open(L6_SRC, "r", encoding="utf-8") as f:
        src = f.read()

    if "DELISTED_TICKERS" not in src:
        log(f"  ❌ DELISTED_TICKERS constant missing, aborting")
        return

    if "if ticker in DELISTED_TICKERS: continue" in src:
        log(f"  guard already present")
    else:
        # Find the ticker = c["ticker"] line in the for-loop and add guard immediately after
        old = "        ticker = c[\"ticker\"]\n        theme = c[\"theme_etf\"]\n        score = c[\"asymmetric_score\"]"
        new = (
            "        ticker = c[\"ticker\"]\n"
            "        theme = c[\"theme_etf\"]\n"
            "        score = c[\"asymmetric_score\"]\n"
            "\n"
            "        # Silent skip for known-delisted tickers (e.g. LTHM merged into ALTM)\n"
            "        if ticker in DELISTED_TICKERS:\n"
            "            n_skipped += 1\n"
            "            print(f\"[track] SKIP {ticker}/{theme} — delisted/merged\")\n"
            "            log_results.append({\"ticker\": ticker, \"theme\": theme, \"skipped\": True, \"reason\": \"delisted\"})\n"
            "            continue"
        )
        if old in src:
            src = src.replace(old, new, 1)
            log(f"  ✓ added guard inside for-loop")
        else:
            log(f"  ❌ anchor not found")
            return

    with open(L6_SRC, "w", encoding="utf-8") as f:
        f.write(src)
    log(f"  source written: {len(src):,} chars")

    section("2) Deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zip_bytes = buf.getvalue()
    log(f"  zip: {len(zip_bytes):,}b")
    L.update_function_code(FunctionName=L6_FN, ZipFile=zip_bytes)
    for _ in range(20):
        cfg = L.get_function_configuration(FunctionName=L6_FN)
        if cfg.get("LastUpdateStatus") == "Successful": break
        time.sleep(1)
    log(f"  ✅ deployed mod={cfg['LastModified']}")

    section("3) Force-invoke and check no LTHM error")
    r = L.invoke(FunctionName=L6_FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    body = json.loads(r["Payload"].read().decode())
    log(f"  status: {r['StatusCode']}")
    log(f"  body: {json.dumps(body)[:500]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        log("  ── tail ──")
        for ln in tail.splitlines()[-30:]:
            log(f"    {ln}")
        if "baseline price unavailable" in tail:
            log("  ⚠ baseline-price-unavailable still in logs — guard may not have caught it")
        else:
            log("  ✅ no baseline-price-unavailable error")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {e}")
    out = "aws/ops/reports/latest/finalize_l6_delisted_guard.md"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out,"w",encoding="utf-8") as f: f.write("\n".join(REPORT))
    print("[written]")
