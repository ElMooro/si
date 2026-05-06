"""
Final verification:
  1. Confirm L5 sent Telegram digest (look up /justhodl/telegram/last_message_id state)
  2. Verify L4 nobrainers.json schema matches what nobrainers.html expects
  3. Show full breakdown of 9 TIER_A nobrainers + 33 TIER_B + 25 MU-grade
  4. Confirm L6 logged a fresh signal post-fix (force a candidate that's not in dedup)
  5. Read live S3 URL of nobrainers.html (head_object check)
"""
import json, os, time
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")

def main():
    section("1) L4 nobrainers.json — full breakdown")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
    data = json.loads(obj["Body"].read())
    log(f"  generated_at: {data.get('generated_at')}")
    summary = data.get("summary", {})
    log(f"  TIER_A:    {summary.get('n_tier_a_nobrainer')}")
    log(f"  TIER_B:    {summary.get('n_tier_b_high_conviction')}")
    log(f"  TIER_C:    {summary.get('n_tier_c_watchlist')}")
    log(f"  MU-grade:  {summary.get('n_mu_grade')}")
    log(f"  total scored: {summary.get('n_candidates_scored')}")

    log("")
    log("  ── 9 TIER_A NOBRAINERS ──")
    top25 = summary.get("top_25_overall", [])
    tier_a = [c for c in top25 if c.get("flag") == "TIER_A_NOBRAINER"]
    for c in tier_a:
        sym = c.get("ticker", "?")
        theme = c.get("theme_etf", "?")
        score = c.get("asymmetric_score", "?")
        f = c.get("factors", {})
        log(f"    {sym:<6} {theme:<6} score={score:>5}  ta={f.get('theme_attribution'):>5}  inflate={f.get('primary_inflated'):>5}  supply={f.get('supply_inflection'):>5}  val={f.get('valuation_asym'):>5}  catalyst={f.get('catalyst_prox'):>5}")

    section("2) Top 12 MU-grade (mcap_to_rev<=3 with high score)")
    mu = []
    for c in top25:
        fund = c.get("fundamentals", {})
        mcr = fund.get("mcap_to_rev")
        if isinstance(mcr, (int, float)) and mcr <= 3:
            mu.append(c)
    mu.sort(key=lambda x: -x.get("asymmetric_score", 0))
    for c in mu[:12]:
        sym = c.get("ticker", "?")
        theme = c.get("theme_etf", "?")
        score = c.get("asymmetric_score", "?")
        fund = c.get("fundamentals", {})
        mcr = fund.get("mcap_to_rev", "?")
        ps = fund.get("p_s", "?")
        rev = fund.get("revenue_ttm")
        rev_str = f"${rev/1e9:.2f}B" if isinstance(rev, (int,float)) and rev else "?"
        log(f"    {sym:<6} {theme:<6} score={score:>5}  mcap/rev={mcr:>5}  P/S={ps:>5}  rev={rev_str}")

    section("3) Verify schema match for nobrainers.html")
    # nobrainers.html expects: ranked, summary, theses
    # Let's check what's actually in the file
    log(f"  data top-level keys: {list(data.keys())[:15]}")
    # Open html and grep for what it tries to access
    html = open("nobrainers.html", encoding="utf-8").read()
    # Find key references
    import re
    json_keys = sorted(set(re.findall(r'(?:data|d|json|nb|noBrainers)\.(\w+)', html)))
    log(f"  schema expectations in HTML: {json_keys[:20]}")

    section("4) L5 Telegram digest verification")
    # Read Lambda env to confirm Telegram set
    cfg = L.get_function_configuration(FunctionName="justhodl-nobrainer-rationale")
    env = cfg.get("Environment", {}).get("Variables", {})
    log(f"  L5 env keys: {list(env.keys())}")
    log(f"  TELEGRAM_BOT_TOKEN present: {'TELEGRAM_BOT_TOKEN' in env}")
    log(f"  ANTHROPIC_KEY present: {'ANTHROPIC_KEY' in env}")
    log(f"  N_DIGEST: {env.get('N_DIGEST')}  N_THESES: {env.get('N_THESES')}  MIN_SCORE: {env.get('MIN_SCORE')}")
    log(f"  SKIP_CLAUDE: {env.get('SKIP_CLAUDE')}")

    # Check SSM for last digest message_id (if L5 stores it)
    try:
        param = SSM.get_parameter(Name="/justhodl/telegram/last_nobrainer_digest")
        log(f"  last_nobrainer_digest: {param['Parameter']['Value'][:200]}")
    except Exception as e:
        log(f"  no SSM param /justhodl/telegram/last_nobrainer_digest")

    section("5) S3 dashboard pages — themes/nobrainers reachability")
    for key in ["themes.html", "nobrainers.html"]:
        try:
            head = S3.head_object(Bucket=BUCKET, Key=key)
            log(f"  {key}: {head['ContentLength']:,}b  modified {head['LastModified']}  ✓ reachable")
        except Exception as e:
            log(f"  ⚠ {key} not in S3 (served from GitHub Pages): {e}")

    section("6) GitHub Pages liveness check via curl from inside Action")
    import urllib.request
    for url in ["https://justhodl.ai/nobrainers.html",
                "https://justhodl.ai/themes.html",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/nobrainers.json"]:
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=10) as r:
                log(f"  {r.status}  {url}")
        except Exception as e:
            log(f"  ❌ {url}: {e}")

if __name__ == "__main__":
    main()
    out_dir = "aws/ops/reports/latest"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "verify_final_state.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
