"""
1. Force-redeploy deep-value Lambda from current source
2. Re-invoke deep-value to get clean (non-financial) top_25
3. Inspect smart-money cluster schema - figure out the correct field name
4. Re-run compound aggregation
"""
import io, json, os, time, base64, zipfile, urllib.request
import boto3
from collections import defaultdict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
L = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

REPORT = []
def log(m):
    ts = time.strftime("%H:%M:%S"); print(f"- `{ts}`   {m}"); REPORT.append(f"- `{ts}`   {m}")
def section(t): print(f"\n# {t}\n"); REPORT.append(f"\n# {t}\n")


def main():
    section("1) Verify deep-value deployed code matches repo")
    repo_src = open("aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py", "r").read()
    has_filter_repo = "top_25_excluded_financials" in repo_src
    log(f"  repo source has top_25_excluded_financials: {has_filter_repo}")

    code_url = L.get_function(FunctionName="justhodl-deep-value-screener")["Code"]["Location"]
    zb = urllib.request.urlopen(code_url).read()
    z = zipfile.ZipFile(io.BytesIO(zb))
    deployed_src = z.read("lambda_function.py").decode("utf-8")
    has_filter_deployed = "top_25_excluded_financials" in deployed_src
    log(f"  deployed code has top_25_excluded_financials: {has_filter_deployed}")

    if not has_filter_deployed and has_filter_repo:
        log("  ⚠ deployed code is stale — force-redeploying")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zi = zipfile.ZipInfo("lambda_function.py")
            zi.external_attr = 0o644 << 16
            zf.writestr(zi, repo_src)
        L.update_function_code(FunctionName="justhodl-deep-value-screener", ZipFile=buf.getvalue())
        for _ in range(30):
            c = L.get_function_configuration(FunctionName="justhodl-deep-value-screener")
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(1)
        log(f"  ✓ force-redeployed, mod={c['LastModified']}")
    else:
        log("  ✓ deployed code matches repo")

    section("2) Re-invoke deep-value")
    r = L.invoke(FunctionName="justhodl-deep-value-screener", InvocationType="RequestResponse",
                  LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}")
    body = json.loads(r["Payload"].read())
    log(f"  body: {body.get('body','')[:300]}")

    # Read fresh output
    obj = S3.get_object(Bucket=BUCKET, Key="data/deep-value.json")
    dv = json.loads(obj["Body"].read())
    top = dv.get("summary", {}).get("top_25_overall", [])[:8]
    log(f"  ── top 8 in deep-value top_25 ──")
    for c in top:
        sec = c.get("sector", "")[:20]
        log(f"    {c.get('symbol'):<6} {c.get('score',0):>6.1f}  {c.get('flag','')[:20]:<20}  {sec}")
    excluded = dv.get("summary", {}).get("top_25_excluded_financials", [])[:5]
    log(f"  ── top 5 excluded (financials/REITs) ──")
    for c in excluded:
        log(f"    {c.get('symbol'):<6} {c.get('score',0):>6.1f}  {c.get('flag',''):<26}  {c.get('sector','')[:20]}")

    section("3) Inspect smart-money cluster schema")
    sm = json.loads(S3.get_object(Bucket=BUCKET, Key="data/smart-money-clusters.json")["Body"].read())
    clusters = sm.get("clusters", [])
    log(f"  total clusters: {len(clusters)}")
    if clusters:
        sample = clusters[0]
        log(f"  sample fields: {list(sample.keys())}")
        # Find likely ticker/symbol field
        for k, v in sample.items():
            if isinstance(v, str) and 1 <= len(v) <= 6 and v.isupper():
                log(f"    possible ticker field: {k} = '{v}'")

    log("")
    log("  ── top 8 by score ──")
    sorted_clusters = sorted(clusters, key=lambda x: -(x.get("score") or 0))[:8]
    for c in sorted_clusters:
        log(f"    score={c.get('score',0):>6.1f}  ticker={c.get('ticker','')!r:<10}  symbol={c.get('symbol','')!r:<10}  signal={c.get('signal_type','')}")

    section("4) Re-aggregate compound signals with correct schemas")
    # Determine ticker field from inspection
    ticker_field = "ticker" if (clusters and clusters[0].get("ticker")) else "symbol"
    log(f"  using smart-money ticker field: '{ticker_field}'")

    feed_map = {
        "nobrainers":   ("data/nobrainers.json",            "summary.top_25_overall",  "ticker"),
        "insiders":     ("data/insider-clusters.json",      "clusters",                "ticker"),
        "smart_money":  ("data/smart-money-clusters.json",  "clusters",                ticker_field),
        "deep_value":   ("data/deep-value.json",            "summary.top_25_overall",  "symbol"),
        "eps_velocity": ("data/eps-revision-velocity.json", "summary.top_25_overall",  "symbol"),
    }
    presence = defaultdict(lambda: {"systems": set(), "scores": {}, "details": {}})
    feed_stats = {}
    for name, (key, path, sym_field) in feed_map.items():
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            cursor = d
            for p in path.split("."):
                cursor = cursor.get(p, [])
                if cursor is None: cursor = []
            count = len(cursor) if isinstance(cursor, list) else 0
            feed_stats[name] = count
            for c in cursor:
                if not isinstance(c, dict): continue
                sym = (c.get(sym_field) or "").upper().strip()
                if not sym: continue
                score = c.get("score") or c.get("asymmetric_score") or 0
                presence[sym]["systems"].add(name)
                presence[sym]["scores"][name] = score
                if name == "nobrainers":
                    presence[sym]["details"][name] = {"theme": c.get("theme_etf"), "tier": c.get("tier"), "flag": c.get("flag")}
                elif name == "insiders":
                    presence[sym]["details"][name] = {
                        "signal": c.get("signal_type"), "n_insiders": c.get("n_insiders"),
                        "total_value": c.get("total_value"),
                        "ceo": c.get("has_ceo"), "cfo": c.get("has_cfo"),
                        "rationale": c.get("rationale", "")[:120],
                    }
                elif name == "smart_money":
                    presence[sym]["details"][name] = {
                        "signal": c.get("signal_type"),
                        "n_buying": c.get("n_funds_adding"),
                        "legend_funds": c.get("legend_funds", []),
                    }
                elif name == "deep_value":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"),
                        "net_cash_pct": c.get("net_cash_pct"),
                        "mcap_to_rev": c.get("mcap_to_rev"),
                        "pct_from_52w_high": c.get("pct_from_52w_high"),
                    }
                elif name == "eps_velocity":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"),
                        "fy2_lift_pct": c.get("fy2_lift_pct"),
                        "fwd_rev_growth_pct": c.get("fwd_rev_growth_pct"),
                    }
        except Exception as e:
            log(f"  {name}: ERROR {e}")
            feed_stats[name] = 0

    multi = {sym: data for sym, data in presence.items() if len(data["systems"]) >= 2}
    ranked = []
    for sym, data in multi.items():
        n = len(data["systems"])
        score = sum(data["scores"].values())
        ranked.append({
            "symbol": sym, "n_systems": n,
            "systems": sorted(list(data["systems"])),
            "scores": data["scores"], "details": data["details"],
            "compound_score": round(score * (1 + 0.5 * (n - 1)), 1),
        })
    ranked.sort(key=lambda x: (-x["n_systems"], -x["compound_score"]))

    out = {
        "schema_version": 1,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "feed_stats": feed_stats,
        "stats": {
            "n_total_names": len(presence),
            "n_multi_signal": len(multi),
            "n_3_plus": sum(1 for r in ranked if r["n_systems"] >= 3),
        },
        "compound": ranked,
    }
    body = json.dumps(out, default=str).encode()
    S3.put_object(Bucket=BUCKET, Key="data/compound-signals.json", Body=body, ContentType="application/json")
    log(f"  ✓ wrote {len(body)}b")
    log(f"  feed_stats: {json.dumps(feed_stats)}")
    log(f"  total tracked: {len(presence)}, multi: {len(multi)}, 3+: {out['stats']['n_3_plus']}")
    log("")
    log("  ── full compound leaderboard ──")
    for r in ranked[:25]:
        sys_str = ",".join(r["systems"])
        log(f"  {r['symbol']:<6} #{r['n_systems']}  score={r['compound_score']:>7.1f}  ({sys_str})")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "fix_dv_and_sm_field.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
