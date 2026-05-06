"""
PHASE J — Verify compound page renders FCX prominently.
Pull the live page + check that the JS will properly highlight FCX/tier-3.
Also verify the compound-signals.json data has the structure the page expects.
"""
import json, time, urllib.request, boto3
S3 = boto3.client("s3", region_name="us-east-1")

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Read compound-signals.json data shape")
    cs = json.loads(S3.get_object(Bucket="justhodl-dashboard-live", Key="data/compound-signals.json")["Body"].read())
    log(f"  schema_version: {cs.get('schema_version')}")
    log(f"  generated_at:   {cs.get('generated_at')}")
    log(f"  feed_stats:     {json.dumps(cs.get('feed_stats', {}))}")
    log(f"  stats:          {json.dumps(cs.get('stats', {}))}")

    fcx = next((r for r in cs.get("compound", []) if r["symbol"] == "FCX"), None)
    if not fcx:
        log("  ❌ FCX missing from compound list")
        return
    log("")
    log("  ── FCX full record ──")
    log(f"  symbol:         {fcx['symbol']}")
    log(f"  n_systems:      {fcx['n_systems']}")
    log(f"  systems:        {fcx['systems']}")
    log(f"  scores:         {fcx['scores']}")
    log(f"  compound_score: {fcx['compound_score']}")
    log(f"  details keys:   {list((fcx.get('details') or {}).keys())}")

    section("2) Read compound-signals.html — verify it'll render FCX correctly")
    try:
        with urllib.request.urlopen("https://justhodl.ai/compound-signals.html", timeout=10) as r:
            page = r.read().decode("utf-8", "replace")
            log(f"  status: {r.status}, size: {len(page):,}b")
    except Exception as e:
        log(f"  ❌ {e}")
        return

    # Look for the JS rendering logic
    expected_pieces = [
        ("data/compound-signals.json", "fetches compound data"),
        ("tier3-grid", "tier-3 section grid"),
        ("tier2-grid", "tier-2 section grid"),
        ("compound_score", "uses compound_score field"),
        ("n_systems", "uses n_systems field"),
        ("renderCard", "card render function"),
        ("renderDetail", "detail per-system renderer"),
    ]
    log("")
    log("  ── page JS expectations ──")
    for needle, desc in expected_pieces:
        ok = needle in page
        log(f"    {'✓' if ok else '❌'} {needle:<35}  {desc}")

    # Check if the page handles the case where details have actual data
    section("3) Simulate what the FCX card will display")
    log("  Based on the data structure, the FCX card will show:")
    log(f"    • Header: 'FCX' with score badge '{int(fcx['compound_score'])}'")
    log(f"    • Pills: " + " ".join("[" + s + "]" for s in fcx["systems"]))
    details = fcx.get("details") or {}
    for sys in fcx["systems"]:
        d = details.get(sys, {})
        log(f"    • {sys} block: {json.dumps(d)[:200]}")


if __name__ == "__main__":
    main()
    import os
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_j_compound_page_render.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
