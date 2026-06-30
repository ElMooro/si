"""ops 2564 — read current upside-theses, report AI coverage across top names."""
import boto3, json, datetime
s3 = boto3.client("s3", "us-east-1")
out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
ga = out.get("generated_at","")
try: age=int((datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(ga)).total_seconds())
except: age=-1
print(f"generated {ga} (age {age}s) · elapsed {out.get('elapsed_s')}s · n_ai={out.get('n_ai')} / n_cand={out.get('n_candidates')}")
top = out.get("top_ranked", [])[:14]
n_with_ai = sum(1 for t in top if out["theses"].get(t,{}).get("ai"))
print(f"AI coverage of top 14: {n_with_ai}/14")
for t in top[:4]:
    d = out["theses"][t]; ai = d.get("ai")
    print(f"\n── {t} ({d.get('name') or ''}) · CANSLIM {d['canslim']['score']} SQGLP {d['sqglp']['score']} Lynch {d['lynch']['score']} · {d['n_engines']} engines ──")
    if ai:
        print("  ▶", ai.get("headline"))
        print("  why_boom:", str(ai.get("why_boom"))[:200])
        print("  multibagger_case:", str(ai.get("multibagger_case"))[:200])
        print("  catalysts:", ai.get("catalysts"))
        print("  risks:", ai.get("risks"), "| breaks:", str(ai.get("what_breaks_it"))[:80])
        print("  best_framework:", ai.get("best_framework"), "· conviction", ai.get("conviction"))
    else:
        print("  AI: none ·", d["why"][:140])
print("\nDONE 2564")
