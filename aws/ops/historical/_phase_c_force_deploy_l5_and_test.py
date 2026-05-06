"""
PHASE C — Force-deploy L5 with full compound integration + run it to confirm
deep-value + EPS-velocity now show up in the Claude prompts.
"""
import io, json, os, time, base64, zipfile
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
    section("1) Force-deploy L5 with all 4 signals")
    src = open("aws/lambdas/justhodl-nobrainer-rationale/source/lambda_function.py").read()
    log(f"  source: {len(src)} chars")
    markers = [
        "deep_value_by_ticker = {}",
        "eps_velocity_by_ticker = {}",
        "_deep_value_block",
        "_eps_velocity_block",
        "build_thesis_prompt(c, cl, sm, dv, ev)",
        "DEEP-VALUE SIGNAL",
        "EPS REVISION VELOCITY",
    ]
    for m in markers:
        log(f"    {'✓' if m in src else '❌'} {m[:50]}")

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

    section("2) Force-invoke L5 with full compound integration (~120-180s)")
    from botocore.config import Config
    cfg2 = Config(read_timeout=600, connect_timeout=10, retries={"max_attempts": 1})
    L2 = boto3.client("lambda", region_name=REGION, config=cfg2)
    t0 = time.time()
    r = L2.invoke(
        FunctionName="justhodl-nobrainer-rationale",
        InvocationType="RequestResponse", LogType="Tail", Payload=b"{}"
    )
    dur = time.time() - t0
    log(f"  status: {r['StatusCode']}, dur: {dur:.1f}s")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")

    section("3) CloudWatch tail — verify all 4 signals loaded + compound hits")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        loaded_lines = [ln for ln in tail.splitlines() if "loaded" in ln and ("clusters" in ln or "qualifiers" in ln or "deep-value" in ln or "EPS" in ln.lower())]
        compound_lines = [ln for ln in tail.splitlines() if "COMPOUND" in ln]
        log(f"  ── load lines ({len(loaded_lines)}) ──")
        for ln in loaded_lines:
            log(f"    {ln.strip()}")
        log(f"  ── compound hits ({len(compound_lines)}) ──")
        for ln in compound_lines:
            log(f"    {ln.strip()}")
        log(f"  ── tail (last 15 lines) ──")
        for ln in tail.splitlines()[-15:]:
            log(f"    {ln.rstrip()}")

    section("4) Read fresh thesis output, scan for compound mentions")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  n_theses: {data.get('n_theses')}  n_ok: {data.get('n_claude_ok')}  n_fail: {data.get('n_claude_fail')}")
    theses = data.get("theses", [])

    keywords = ["insider", "ceo bought", "boardroom",
                 "13f", "smart money", "burry", "klarman", "druckenmiller",
                 "net cash", "deep value", "ben graham",
                 "eps revision", "consensus", "analyst upgrade", "mu pattern", "earnings revision"]
    mention_count = {kw: 0 for kw in keywords}
    for t in theses:
        text = (t.get("thesis") or "").lower()
        for kw in keywords:
            if kw in text:
                mention_count[kw] += 1
    log(f"  ── compound-related mentions across {len(theses)} theses ──")
    for kw, count in sorted(mention_count.items(), key=lambda x: -x[1]):
        if count > 0:
            log(f"    {count} theses mention '{kw}'")

    section("5) Spot-check: print 1 thesis to confirm prompt is rich")
    if theses:
        t = theses[0]
        log(f"  ── first thesis ({t.get('ticker')}/{t.get('theme')}) ──")
        log(f"  preview (first 80 lines):")
        for ln in (t.get("thesis") or "").splitlines()[:25]:
            log(f"    {ln[:120]}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_c_l5_compound.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
