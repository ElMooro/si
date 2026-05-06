"""Phase L2 — force-deploy universe builder with AI_SUPPLY_CHAIN_SEED merge."""
import io, json, time, base64, zipfile, os
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
    src = open("aws/lambdas/justhodl-universe-builder/source/lambda_function.py").read()
    
    section("1) Verify source has AI_SUPPLY_CHAIN_SEED wired")
    log(f"  AI_SUPPLY_CHAIN_SEED defined: {'AI_SUPPLY_CHAIN_SEED = [' in src}")
    log(f"  Wired into gather_seeds: {'+ AI_SUPPLY_CHAIN_SEED' in src}")

    section("2) Force-deploy")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName="justhodl-universe-builder", ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-universe-builder")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed at {c['LastModified']}")

    section("3) Force-invoke")
    r = L.invoke(FunctionName="justhodl-universe-builder",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")

    section("4) Verify pump-list coverage")
    u = json.loads(S3.get_object(Bucket=BUCKET, Key="data/universe.json")["Body"].read())
    stocks = u.get("stocks", [])
    log(f"  total stocks: {len(stocks)}")

    targets = ["AXTI","LWLG","AAOI","AEHR","SNDK","ICHR","MRVL","INTC","VIAV","LITE",
               "CRDO","MU","TER","WOLF","ON","QRVO","COHR","FN"]
    sym_set = {(s.get("symbol") or "").upper(): s for s in stocks}
    log("")
    log("  ── pump-list coverage ──")
    captured = 0
    for t in targets:
        if t in sym_set:
            captured += 1
            s = sym_set[t]
            mcap = (s.get("market_cap") or 0) / 1e6
            sec = (s.get("sector") or "")[:20]
            log(f"    ✓ {t:<6} ${mcap:>6.0f}M  {sec}")
        else:
            log(f"    ❌ {t:<6}  MISSING")
    log("")
    log(f"  Coverage: {captured}/{len(targets)} = {captured*100/len(targets):.0f}%")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_l2_redeploy_universe.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
