"""
Dump full Claude-written theses for top 5 nobrainers from S3 to verify quality.
"""
import json, os, time
import boto3

S3 = boto3.client("s3", region_name="us-east-1")

REPORT = []
def log(m):
    print(m); REPORT.append(m)

def main():
    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/nobrainers-rationale.json")
    data = json.loads(obj["Body"].read())

    log(f"# Real Claude-written theses\n")
    log(f"generated_at: {data.get('generated_at')}")
    log(f"n_theses: {data.get('n_theses')}")
    log(f"n_claude_ok: {data.get('n_claude_ok')}")
    log(f"n_claude_fail: {data.get('n_claude_fail')}")
    log("")

    theses = data.get("theses") or []
    for t in theses[:5]:
        sym = t.get("symbol") or t.get("ticker") or "?"
        theme = t.get("theme") or t.get("theme_etf") or "?"
        score = t.get("asymmetric_score") or t.get("score") or "?"
        flag = t.get("flag") or ""
        log(f"\n## {sym} ({theme}) — score {score}  {flag}\n")
        txt = t.get("rationale") or t.get("thesis") or t.get("body") or ""
        log("```")
        for ln in txt.splitlines():
            log(ln.rstrip())
        log("```\n")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "real_theses_dump.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
