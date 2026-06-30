"""ops 2554 — exact shapes for stock-bond correlation + FX carry, to wire the flows."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
def rd(k):
    return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())

print("=== correlation-breaks (stock<->bond rotation) ===")
cb = rd("correlation-breaks")
print("top keys:", list(cb))
print("labels:", cb.get("labels"))
print("instruments:", cb.get("instruments"))
# correlation matrix?
for k in ["corr_matrix","current_corr","matrices","corr_current","correlations"]:
    if k in cb:
        print(f"{k}: {json.dumps(cb[k])[:400]}")
print("top_breaking_pairs (first 4):")
for p in (cb.get("top_breaking_pairs") or [])[:4]:
    print("  ", {kk: p.get(kk) for kk in ("labels","current_corr","baseline_corr","delta","z_score","direction")})
print("regime/headline:", cb.get("regime"), "|", cb.get("headline"), "|", cb.get("interpretation"))

print("\n=== correlation-surface (stock-bond pair) ===")
cs = rd("correlation-surface")
print("macro_regime:", cs.get("macro_regime"), "|", cs.get("macro_regime_description"))
print("avg_30d_abs_corr:", cs.get("avg_30d_abs_correlation"))
for p in (cs.get("headline_pairs") or [])[:6]:
    if isinstance(p, dict): print("  pair:", {kk: p.get(kk) for kk in list(p)[:6]})

print("\n=== carry-surface (FX flows) ===")
cy = rd("carry-surface")
print("top keys:", list(cy))
print("regime_summary.fx:", json.dumps(cy.get("regime_summary", {}).get("fx"))[:300])
mf = cy.get("massive_fx", {})
print("massive_fx keys:", list(mf) if isinstance(mf, dict) else mf)
print("massive_fx:", json.dumps(mf)[:400])
fx = [a for a in (cy.get("all_assets") or []) if a.get("asset_class") == "fx"]
print(f"fx assets: {len(fx)} · top by carry:")
for a in sorted(fx, key=lambda x: -(x.get("carry_pct") or 0))[:5]:
    print("  ", {kk: a.get(kk) for kk in ("symbol","long_currency","short_currency","carry_pct","carry_pct_z","global_rank")})
print("DONE 2554")
