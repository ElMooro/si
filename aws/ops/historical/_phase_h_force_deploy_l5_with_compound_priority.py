"""
PHASE H — Force-deploy patched L5 + verify FCX gets a thesis.
Then re-run the celebration with FCX thesis included.
"""
import io, json, os, time, base64, zipfile
from botocore.config import Config
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=600, connect_timeout=10))
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Force-deploy L5 with compound-priority candidate selection")
    src = open("aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py").read()
    log(f"  source: {len(src)} chars")
    markers = [
        "compound_by_ticker = {}",
        "_compound_priority",
        "tier3_names = [c for c in compound_by_ticker.values() if c.get(\"n_systems\", 0) >= 3]",
        "Force-include all tier-3 compound names",
    ]
    for m in markers:
        log(f"    {'✓' if m in src else '❌'} {m[:60]}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    L.update_function_code(FunctionName="justhodl-nobrainer-rationale", ZipFile=buf.getvalue())
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed at {c['LastModified']}")

    section("2) Force-invoke L5")
    t0 = time.time()
    r = L.invoke(FunctionName="justhodl-nobrainer-rationale",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}, dur: {dur:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")

    section("3) Inspect logs for compound priority + FCX")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        priority_lines = [ln for ln in tail.splitlines() if "_compound_priority" in ln or "loaded" in ln and "compound" in ln]
        thesis_lines = [ln for ln in tail.splitlines() if "thesis ok" in ln]
        log(f"  ── priority/load lines ({len(priority_lines)}) ──")
        for ln in priority_lines:
            log(f"    {ln.strip()}")
        log(f"  ── thesis lines ({len(thesis_lines)}) ──")
        for ln in thesis_lines:
            log(f"    {ln.strip()}")

    section("4) Check fresh L5 output for FCX thesis")
    r5 = json.loads(S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")["Body"].read())
    log(f"  generated_at: {r5.get('generated_at')}")
    fcx = next((t for t in r5.get("theses", []) if t.get("ticker") == "FCX"), None)

    log("  Tickers in L5:")
    for t in r5.get("theses", []):
        priority = t.get("candidate", {}).get("_compound_priority")
        log(f"    {t.get('ticker')}{' [' + priority + ']' if priority else ''}")

    if fcx:
        log("")
        log(f"  ✓ FCX thesis written ({len(fcx.get('thesis',''))} chars)")
        text = fcx.get("thesis", "")
        log("  ── FCX thesis (first 40 lines) ──")
        for ln in text.splitlines()[:40]:
            log(f"    {ln[:140]}")
    else:
        log("  ⚠ FCX still not in theses — verify candidate logic")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_h_l5_compound_priority.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
