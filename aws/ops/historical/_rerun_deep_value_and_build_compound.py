"""
1. Re-deploy + re-run deep-value with financial-exclusion fix
2. Read all 5 signal feeds and compute compound-signal table
3. Output to S3 + write report
"""
import io, json, os, time, zipfile, base64
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
    section("1) Force-redeploy deep-value Lambda from current source")
    src = open("aws/lambdas/justhodl-deep-value-screener/source/lambda_function.py", "r").read()
    log(f"  source size: {len(src)} chars")
    if "FINANCIAL_BOOK_EXCLUDED" not in src:
        log("  ❌ source missing financial exclusion patch")
        return

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, src)
    zb = buf.getvalue()
    L.update_function_code(FunctionName="justhodl-deep-value-screener", ZipFile=zb)
    for _ in range(30):
        c = L.get_function_configuration(FunctionName="justhodl-deep-value-screener")
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)
    log(f"  ✓ deployed, mod={c['LastModified']}")

    section("2) Re-invoke deep-value")
    r = L.invoke(FunctionName="justhodl-deep-value-screener",
                  InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
    log(f"  status: {r['StatusCode']}")
    if "LogResult" in r:
        tail = base64.b64decode(r["LogResult"]).decode()
        for ln in tail.splitlines()[-12:]:
            log(f"    {ln.rstrip()}")

    section("3) Load all 5 signal feeds")
    feeds = {
        "nobrainers":     ("data/nobrainers.json",          "summary.top_25_overall",   "ticker"),
        "insiders":       ("data/insider-clusters.json",    "clusters",                 "ticker"),
        "smart_money":    ("data/smart-money-clusters.json", "clusters",                "symbol"),
        "deep_value":     ("data/deep-value.json",          "summary.top_25_overall",   "symbol"),
        "eps_velocity":   ("data/eps-revision-velocity.json","summary.top_25_overall",  "symbol"),
    }
    presence = defaultdict(lambda: {"systems": set(), "scores": {}, "details": {}})
    feed_stats = {}

    for name, (key, path, sym_field) in feeds.items():
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
            # Walk path
            cursor = d
            for p in path.split("."):
                cursor = cursor.get(p, [])
                if cursor is None:
                    cursor = []
                    break
            count = len(cursor) if isinstance(cursor, list) else 0
            feed_stats[name] = count
            log(f"  {name}: {count} entries from {key}")
            # Index
            for c in cursor:
                if not isinstance(c, dict):
                    continue
                sym = (c.get(sym_field) or "").upper().strip()
                if not sym:
                    continue
                score = c.get("score") or c.get("asymmetric_score") or 0
                presence[sym]["systems"].add(name)
                presence[sym]["scores"][name] = score
                # Save useful summary detail
                if name == "nobrainers":
                    presence[sym]["details"][name] = {
                        "theme": c.get("theme_etf"),
                        "tier": c.get("tier"),
                        "flag": c.get("flag"),
                    }
                elif name == "insiders":
                    presence[sym]["details"][name] = {
                        "signal": c.get("signal_type"),
                        "n_insiders": c.get("n_insiders"),
                        "total_value": c.get("total_value"),
                        "ceo": c.get("has_ceo"),
                        "cfo": c.get("has_cfo"),
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
                    }
                elif name == "eps_velocity":
                    presence[sym]["details"][name] = {
                        "flag": c.get("flag"),
                        "fy2_lift_pct": c.get("fy2_lift_pct"),
                        "fwd_rev_growth_pct": c.get("fwd_rev_growth_pct"),
                    }
        except Exception as e:
            log(f"  {name}: ERROR {e}")

    section("4) Compound-signal table — names on 2+ lists")
    multi = {sym: data for sym, data in presence.items() if len(data["systems"]) >= 2}
    log(f"  total names tracked: {len(presence)}")
    log(f"  names on 2+ lists: {len(multi)}")
    log(f"  names on 3+ lists: {sum(1 for d in presence.values() if len(d['systems']) >= 3)}")

    # Compute compound score = sum(scores) * (1 + 0.5 * n_systems)
    ranked = []
    for sym, data in multi.items():
        n_systems = len(data["systems"])
        sum_scores = sum(data["scores"].values())
        compound_score = sum_scores * (1 + 0.5 * (n_systems - 1))
        ranked.append({
            "symbol": sym,
            "n_systems": n_systems,
            "systems": sorted(list(data["systems"])),
            "scores": data["scores"],
            "details": data["details"],
            "compound_score": round(compound_score, 1),
        })
    ranked.sort(key=lambda x: (-x["n_systems"], -x["compound_score"]))

    log("")
    log(f"  ── Compound leaderboard ──")
    log(f"  {'Sym':<6} {'#Sys':>4} Systems hit                            Compound")
    for r in ranked[:25]:
        sys_str = ", ".join(s[:5] for s in r["systems"])[:50]
        log(f"  {r['symbol']:<6} {r['n_systems']:>4}  {sys_str:<50} {r['compound_score']:>7.1f}")

    section("5) Write data/compound-signals.json")
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
    log(f"  wrote {len(body)}b to data/compound-signals.json")


if __name__ == "__main__":
    main()
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "rerun_dv_and_compound.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
