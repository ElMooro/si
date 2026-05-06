"""
Force-invoke L5 nobrainer-rationale after the insider-cluster integration.
Verify deployed code, run, and dump:
  1. Whether insider data was loaded (look for log line)
  2. How many candidates had matching insider clusters (log lines)
  3. Sample thesis showing INSIDER CLUSTER SIGNAL section in output
"""
import json, os, time, base64
from botocore.config import Config
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

# Use longer timeout for this big invoke
cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name=REGION, config=cfg)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Verify L5 deployed with insider integration")
    cfg_resp = L.get_function(FunctionName="justhodl-nobrainer-rationale")["Configuration"]
    log(f"  modified: {cfg_resp['LastModified']}")
    log(f"  state: {cfg_resp['State']}  mem={cfg_resp['MemorySize']}MB  timeout={cfg_resp['Timeout']}s")

    # Fetch deployed code and check for insider integration markers
    import urllib.request
    code_url = L.get_function(FunctionName="justhodl-nobrainer-rationale")["Code"]["Location"]
    import zipfile
    import io
    zb = urllib.request.urlopen(code_url).read()
    z = zipfile.ZipFile(io.BytesIO(zb))
    src = z.read("lambda_function.py").decode("utf-8")
    markers = [
        ("insider_by_ticker = {}", "insider data dict"),
        ("data/insider-clusters.json", "loads insider S3"),
        ("INSIDER CLUSTER SIGNAL", "prompt section"),
        ("def _insider_block", "helper function"),
        ("build_thesis_prompt(c, cl)", "passes insider to prompt"),
    ]
    for m, desc in markers:
        ok = m in src
        log(f"  {'✓' if ok else '❌'} {desc}: {m[:50]}{'...' if len(m)>50 else ''}")

    section("2) Force-invoke L5 sync (will take ~60-90s)")
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
    log(f"  body keys: {list(body.keys())}")
    if body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        log(f"  inner: {json.dumps(inner)[:500]}")

    section("3) CloudWatch tail — look for compound-signal hits")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        loaded_line = None
        compound_lines = []
        thesis_lines = []
        for ln in tail.splitlines():
            if "loaded" in ln and "insider clusters" in ln:
                loaded_line = ln
            elif "ALSO has insider cluster" in ln:
                compound_lines.append(ln)
            elif "thesis ok" in ln:
                thesis_lines.append(ln)
        if loaded_line:
            log(f"  ✓ {loaded_line.strip()}")
        else:
            log(f"  ⚠ no insider-load log line found")
        if compound_lines:
            log(f"  ✓ {len(compound_lines)} candidates have COMPOUND insider+nobrainer:")
            for cl in compound_lines:
                log(f"    {cl.strip()}")
        else:
            log(f"  no overlap between top nobrainers and insider clusters today (expected — different universes)")
        log(f"  thesis lines: {len(thesis_lines)}")
        log("")
        log("  ── full tail (last 25 lines) ──")
        for ln in tail.splitlines()[-25:]:
            log(f"    {ln.strip()}")

    section("4) Read fresh thesis output, find any compound signal in text")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  n_theses: {data.get('n_theses')}  n_ok: {data.get('n_claude_ok')}  n_fail: {data.get('n_claude_fail')}")
    theses = data.get("theses", [])
    # Find any thesis where the underlying prompt would have had insider data
    # Note: we can't see the prompt — only the output. But we can check if Claude mentioned insider buying.
    insider_mentions = []
    for t in theses:
        text = (t.get("thesis") or "").lower()
        if any(kw in text for kw in ["insider", "ceo bought", "cfo bought", "boardroom", "executive bought"]):
            insider_mentions.append((t.get("ticker"), len(t.get("thesis") or "")))
    log(f"  theses mentioning insider buying: {len(insider_mentions)}/{len(theses)}")
    for tk, sz in insider_mentions[:5]:
        log(f"    {tk}: {sz} chars, mentions insiders")

if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_l5_compound_insider.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
