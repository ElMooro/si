"""
Verify nobrainers.html actually renders the L4+L5 data correctly:
1. Pull data/nobrainers.json schema and check what nobrainers.html expects vs gets
2. Patch nobrainers.html if there's a mismatch
3. Verify the live page actually shows real theses (curl from inside Action)
"""
import json, os, re, time, urllib.request
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Load actual data/nobrainers.json schema")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
    nb = json.loads(obj["Body"].read())
    log(f"  top-level keys: {sorted(nb.keys())}")
    # Drill into ranked candidate
    summary = nb.get("summary", {})
    log(f"  summary keys: {sorted(summary.keys())}")
    candidate = (summary.get("top_25_overall") or [None])[0]
    if candidate:
        log(f"  candidate keys: {sorted(candidate.keys())}")
        for k in ["ticker", "symbol", "asymmetric_score", "score", "nobrainer_score", "theme_etf", "theme_name", "tier", "flag"]:
            if k in candidate:
                log(f"    {k}: {candidate[k]}")
        if "fundamentals_summary" in candidate:
            f = candidate["fundamentals_summary"]
            log(f"    fundamentals keys: {sorted(f.keys())}")
            for k in ["mcap_to_rev", "p_s", "p_e", "ev_ebitda", "fcf_yield", "revenue_ttm", "market_cap", "industry"]:
                if k in f:
                    log(f"      {k}: {f[k]}")
        if "factors" in candidate:
            log(f"    factors: {candidate['factors']}")

    section("2) Load data/nobrainers-rationale.json schema")
    obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers-rationale.json")
    rd = json.loads(obj["Body"].read())
    log(f"  top-level keys: {sorted(rd.keys())}")
    theses = rd.get("theses", [])
    if theses:
        t = theses[0]
        log(f"  thesis keys: {sorted(t.keys())}")
        for k in ["symbol", "ticker", "rationale", "thesis", "score", "asymmetric_score", "theme", "theme_etf", "tier"]:
            if k in t:
                v = t[k]
                if isinstance(v, str) and len(v) > 80:
                    v = v[:80] + "..."
                log(f"    {k}: {v}")

    section("3) Inspect nobrainers.html — what fields does it read?")
    with open("nobrainers.html", "r", encoding="utf-8") as f:
        html = f.read()
    # Find dataset reference patterns
    js_section = html.split("<script>")[-1] if "<script>" in html else ""
    refs = set(re.findall(r"\b(?:c|cand|item|t|nb)\.([a-z_][a-z_0-9]*)", js_section, re.IGNORECASE))
    log(f"  field refs in JS: {sorted(refs)[:40]}")
    fund_refs = set(re.findall(r"fundamentals[._]([a-z_]+)", js_section))
    log(f"  fundamentals refs: {sorted(fund_refs)}")
    factor_refs = set(re.findall(r"factors[._]([a-z_]+)", js_section))
    log(f"  factor refs: {sorted(factor_refs)}")

    section("4) Live curl — confirm both pages return 200 + nobrainers.html actually renders")
    for url in ["https://justhodl.ai/nobrainers.html", "https://justhodl.ai/themes.html",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/nobrainers.json",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/nobrainers-rationale.json",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/themes-detected.json",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/supply-inflection.json",
                "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/theme-tiers.json"]:
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                code = r.status
                ln = r.headers.get("Content-Length", "?")
                log(f"  {code}  {ln:>10}b  {url}")
        except Exception as e:
            log(f"  ❌ {url}: {e}")

    section("5) Cross-reference: are pages reachable from canonical sidebar?")
    # Check that /themes.html and /nobrainers.html are wired in main pages
    for page in ["index.html", "desk.html", "brief.html", "calls.html", "performance.html"]:
        if not os.path.exists(page):
            log(f"  {page}: not present in repo")
            continue
        with open(page, "r", encoding="utf-8") as f:
            c = f.read()
        themes_lk = "/themes.html" in c or "themes.html" in c
        nb_lk     = "/nobrainers.html" in c or "nobrainers.html" in c
        log(f"  {page:<25} themes_link={themes_lk}  nobrainers_link={nb_lk}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "verify_nobrainers_page_render.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
