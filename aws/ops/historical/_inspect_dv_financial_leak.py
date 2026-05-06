"""Inspect why AMP/HUM/GS are still appearing in deep-value top-25 despite financial exclusion."""
import json, time, boto3
S3 = boto3.client("s3", region_name="us-east-1")

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Inspect deep-value all_qualifying for AMP, HUM, GS, EG, CNC, AIZ")
    obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/deep-value.json")
    d = json.loads(obj["Body"].read())
    target = {"AMP", "HUM", "GS", "EG", "CNC", "AIZ", "MET", "TRV", "PFG"}
    log(f"  total all_qualifying: {len(d.get('all_qualifying', []))}")
    log(f"  top_25_overall: {len(d.get('summary', {}).get('top_25_overall', []))}")
    log(f"  top_25_excluded: {len(d.get('summary', {}).get('top_25_excluded_financials', []))}")
    log("")
    log("  ── Per-target inspection ──")
    for r in d.get("all_qualifying", []):
        if r.get("symbol") in target:
            f = r.get("fundamentals") or {}
            log(f"    {r['symbol']:<5} flag={r['flag']:<25} sector={f.get('sector','?')!r:<22} industry={f.get('industry','?')[:30]!r}")

    section("2) Check what's IN top_25_overall vs top_25_excluded")
    log("  ── top_25_overall ──")
    for c in d.get("summary", {}).get("top_25_overall", [])[:15]:
        log(f"    {c.get('symbol'):<6} {c.get('score'):>6.1f}  flag={c.get('flag','')[:24]:<24}  sector={c.get('sector','')[:25]}")
    log("")
    log("  ── top_25_excluded_financials ──")
    for c in d.get("summary", {}).get("top_25_excluded_financials", [])[:15]:
        log(f"    {c.get('symbol'):<6} {c.get('score'):>6.1f}  flag={c.get('flag','')[:24]:<24}  sector={c.get('sector','')[:25]}")

    section("3) Diagnose")
    # If the top_25_overall contains entries with TIER_A flag but the FMP /quote is returning blank sector,
    # the financial-exclusion check sees an empty sector and doesn't match
    sec_blank = sum(1 for c in d.get("summary", {}).get("top_25_overall", []) if not c.get("sector"))
    log(f"  top_25_overall entries with blank sector: {sec_blank}")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    import os
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "inspect_dv_financial_leak.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
