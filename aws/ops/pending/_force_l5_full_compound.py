"""
Force-invoke L5 with FULL compound integration (insider + smart-money).
Verify deployed code + check for compound hits in tail logs.
"""
import io, json, os, time, base64
import zipfile
import urllib.request
from botocore.config import Config
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 1})
L = boto3.client("lambda", region_name=REGION, config=cfg)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m): 
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Verify deployed L5 has all 3 signal sources")
    cfg_resp = L.get_function(FunctionName="justhodl-nobrainer-rationale")
    log(f"  modified: {cfg_resp['Configuration']['LastModified']}")
    code_url = cfg_resp["Code"]["Location"]
    zb = urllib.request.urlopen(code_url).read()
    z = zipfile.ZipFile(io.BytesIO(zb))
    src = z.read("lambda_function.py").decode("utf-8")
    markers = [
        ("data/nobrainers.json", "L4 layer"),
        ("data/insider-clusters.json", "insider load"),
        ("data/smart-money-clusters.json", "smart-money load"),
        ("def _insider_block", "insider helper"),
        ("def _smart_money_block", "smart-money helper"),
        ("INSIDER CLUSTER SIGNAL", "insider prompt section"),
        ("13F SMART-MONEY CLUSTER SIGNAL", "smart-money prompt section"),
        ("smart_money_by_ticker = {}", "smart-money dict"),
        ("build_thesis_prompt(c, cl, sm)", "passes both clusters"),
    ]
    for m, desc in markers:
        ok = m in src
        log(f"  {'✓' if ok else '❌'} {desc}: {m[:55]}{'...' if len(m)>55 else ''}")

    section("2) Force-invoke L5 (sync, ~140s)")
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

    section("3) Tail logs — find compound signals + load lines")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")
        loads = []
        compounds = []
        for ln in tail.splitlines():
            ln = ln.rstrip()
            if "loaded" in ln and ("insider" in ln or "smart-money" in ln):
                loads.append(ln)
            elif "ALSO has" in ln:
                compounds.append(ln)
        log(f"  load lines:")
        for ln in loads:
            log(f"    {ln.strip()}")
        log("")
        log(f"  COMPOUND signal hits ({len(compounds)}):")
        for ln in compounds:
            log(f"    {ln.strip()}")
        log("")
        log(f"  ── full tail (last 25) ──")
        for ln in tail.splitlines()[-25:]:
            log(f"    {ln.rstrip()}")

    section("4) Inspect fresh thesis output for compound mentions")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    log(f"  n_theses: {data.get('n_theses')}")
    theses = data.get("theses", [])
    insider_mentions = []
    sm_mentions = []
    for t in theses:
        text = (t.get("thesis") or "").lower()
        if any(kw in text for kw in ["insider", "ceo bought", "boardroom", "executive bought"]):
            insider_mentions.append(t.get("ticker"))
        if any(kw in text for kw in ["13f", "smart money", "smart-money", "burry", "klarman", "ackman", "soros", "berkshire", "lone pine", "scion", "baupost"]):
            sm_mentions.append(t.get("ticker"))
    log(f"  theses mentioning insider buying: {len(insider_mentions)}/{len(theses)}: {insider_mentions[:8]}")
    log(f"  theses mentioning smart-money: {len(sm_mentions)}/{len(theses)}: {sm_mentions[:8]}")

    section("5) Final compound-signal summary across the 3 systems")
    # Load all 3 leaderboards
    nb_obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
    nb_data = json.loads(nb_obj["Body"].read())
    nb_top = nb_data.get("summary", {}).get("top_25_overall", [])
    nb_set = {c["ticker"] for c in nb_top if c.get("ticker")}

    ic_obj = S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")
    ic_data = json.loads(ic_obj["Body"].read())
    ic_set = {c.get("ticker") for c in ic_data.get("clusters", []) if c.get("ticker") and c.get("score", 0) >= 50}

    sm_obj = S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")
    sm_data = json.loads(sm_obj["Body"].read())
    sm_set = {c.get("ticker") for c in sm_data.get("clusters", []) if c.get("ticker") and c.get("score", 0) >= 55}

    log(f"  Nobrainers (top 25): {sorted(nb_set)}")
    log(f"  Insiders (≥50): {sorted(ic_set)}")
    log(f"  Smart Money (≥55): {sorted(sm_set)}")
    log("")
    log(f"  ── COMPOUND OVERLAPS ──")
    log(f"  Nobrainer ∩ Insider:    {sorted(nb_set & ic_set)}")
    log(f"  Nobrainer ∩ SmartMoney: {sorted(nb_set & sm_set)}")
    log(f"  Insider ∩ SmartMoney:   {sorted(ic_set & sm_set)}")
    log(f"  ALL THREE:              {sorted(nb_set & ic_set & sm_set)}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_l5_full_compound.md"), "w") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
